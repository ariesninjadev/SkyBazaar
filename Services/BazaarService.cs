using System.Threading.Tasks;
using System;
using System.Linq;
using Microsoft.Extensions.Configuration;
using dev;
using System.Collections.Generic;
using Cassandra;
using Cassandra.Data.Linq;
using Coflnet.Sky.SkyBazaar.Models;
using Cassandra.Mapping;
using Newtonsoft.Json;
using Microsoft.Extensions.Logging;
using Coflnet.Sky.Core;
using System.Linq.Expressions;
using RestSharp;
using System.Collections.Concurrent;
using System.Threading;

namespace Coflnet.Sky.SkyAuctionTracker.Services
{
    public interface ISessionContainer
    {
        /// <summary>
        /// The cassandra session
        /// </summary>
        ISession Session { get; }
    }
    public class BazaarService : ISessionContainer
    {
        private const string TABLE_NAME_DAILY = "QuickStatusDaly";
        private const string TABLE_NAME_HOURLY = "QuickStatusHourly";
        private const string TABLE_NAME_MINUTES = "QuickStatusMin";
        private const string TABLE_NAME_SECONDS = "QuickStatusSeconds";
        private const string DEFAULT_ITEM_TAG = "STOCK_OF_STONKS";
        private static bool ranCreate;
        private IConfiguration config;
        private ILogger<BazaarService> logger;
        ISession _session;
        /// <summary>
        /// <inheritdoc/>
        /// </summary>
        public ISession Session => _session;

        private static Prometheus.Counter insertCount = Prometheus.Metrics.CreateCounter("sky_bazaar_status_insert", "How many inserts were made");
        private static Prometheus.Counter insertFailed = Prometheus.Metrics.CreateCounter("sky_bazaar_status_insert_failed", "How many inserts failed");
        private static Prometheus.Counter checkSuccess = Prometheus.Metrics.CreateCounter("sky_bazaar_check_success", "How elements where found in cassandra");
        private static Prometheus.Counter checkFail = Prometheus.Metrics.CreateCounter("sky_bazaar_check_fail", "How elements where not found in cassandra");
        private static Prometheus.Counter aggregateCount = Prometheus.Metrics.CreateCounter("sky_bazaar_aggregation", "How many aggregations were started (should be one every 5 min)");

        private List<StorageQuickStatus> currentState = new List<StorageQuickStatus>();
        private SemaphoreSlim sessionOpenLock = new SemaphoreSlim(1);
        private SemaphoreSlim insertConcurrencyLock = new SemaphoreSlim(500);

        private static readonly Prometheus.Histogram checkSuccessHistogram = Prometheus.Metrics
            .CreateHistogram("sky_bazaar_check_success_histogram", "Histogram of successfuly checked elements",
                new Prometheus.HistogramConfiguration
                {
                    Buckets = Prometheus.Histogram.LinearBuckets(start: 1, width: 10_000_000, count: 40)
                }
            );

        private static readonly Prometheus.Histogram checkFailHistogram = Prometheus.Metrics
            .CreateHistogram("sky_bazaar_check_fail_histogram", "Histogram of failed checked elements",
                new Prometheus.HistogramConfiguration
                {
                    Buckets = Prometheus.Histogram.LinearBuckets(start: 1, width: 10_000_000, count: 40)
                }
            );

        public BazaarService(IConfiguration config, ILogger<BazaarService> logger, ISession session)
        {
            this.config = config;
            this.logger = logger;
            _session = session;
        }

        internal async Task NewPull(int i, BazaarPull bazaar)
        {
            await AddEntry(bazaar);
        }


        private static void RemoveRedundandInformation(int i, BazaarPull pull, List<BazaarPull> lastMinPulls)
        {
            var lastPull = lastMinPulls.First();
            var lastPullDic = lastPull
                    .Products.ToDictionary(p => p.ProductId);

            var sellChange = 0;
            var buyChange = 0;
            var productCount = pull.Products.Count;

            var toRemove = new List<ProductInfo>();

            for (int index = 0; index < productCount; index++)
            {
                var currentProduct = pull.Products[index];
                var currentStatus = currentProduct.QuickStatus;
                var lastProduct = lastMinPulls.SelectMany(p => p.Products)
                                .Where(p => p.ProductId == currentStatus.ProductId)
                                .OrderByDescending(p => p.Id)
                                .FirstOrDefault();

                var lastStatus = new QuickStatus();
                if (lastProduct != null)
                {
                    lastStatus = lastProduct.QuickStatus;
                }
                // = lastPullDic[currentStatus.ProductId].QuickStatus;

                var takeFactor = i % 60 == 0 ? 30 : 3;

                if (currentStatus.BuyOrders == lastStatus.BuyOrders)
                {
                    // nothing changed
                    currentProduct.BuySummery = null;
                    buyChange++;
                }
                else
                {
                    currentProduct.BuySummery = currentProduct.BuySummery.Take(takeFactor).ToList();
                }
                if (currentStatus.SellOrders == lastStatus.SellOrders)
                {
                    // nothing changed
                    currentProduct.SellSummary = null;
                    sellChange++;
                }
                else
                {
                    currentProduct.SellSummary = currentProduct.SellSummary.Take(takeFactor).ToList();
                }
                if (currentProduct.BuySummery == null && currentProduct.SellSummary == null)
                {
                    toRemove.Add(currentProduct);
                }
            }
            //Console.WriteLine($"Not saving {toRemove.Count}");

            foreach (var item in toRemove)
            {
                pull.Products.Remove(item);
            }

            Console.WriteLine($"  BuyChange: {productCount - buyChange}  SellChange: {productCount - sellChange}");
            //context.Update(lastPull);
        }

        internal async Task<IEnumerable<ItemPrice>> GetCurrentPrices(List<string> tags)
        {
            return currentState.Select(s => new ItemPrice
            {
                ProductId = s.ProductId,
                BuyPrice = s.BuyPrice,
                SellPrice = s.SellPrice
                GreatestBuy = s.GreatestBuy
                CheapestSell = s.CheapestSell
            });
        }


        internal async Task CheckAggregation(ISession session, IEnumerable<BazaarPull> bazaar)
        {
            var timestamp = bazaar.Last().Timestamp;
            var boundary = TimeSpan.FromMinutes(5);
            if (bazaar.All(b => IsTimestampWithinGroup(b.Timestamp, boundary)))
                return; // nothing to do
            Console.WriteLine("aggregating minutes " + timestamp);
            _ = Task.Run(async () => await RunAgreggation(session, timestamp));
        }

        private static async Task RunAgreggation(ISession session, DateTime timestamp)
        {
            aggregateCount.Inc();
            var ids = await GetAllItemIds();
            foreach (var itemId in ids)
            {
                await AggregateMinutes(session, timestamp - TimeSpan.FromMinutes(30), TimeSpan.FromMinutes(10), itemId, timestamp + TimeSpan.FromSeconds(1));
            }
            if (IsTimestampWithinGroup(timestamp, TimeSpan.FromHours(2)))
                return;
            Console.WriteLine("aggregating hours");
            foreach (var itemId in ids)
            {
                await AggregateHours(session, timestamp - TimeSpan.FromHours(24), itemId);
            }

            if (IsTimestampWithinGroup(timestamp, TimeSpan.FromDays(1)))
                return;
            foreach (var itemId in ids)
            {
                await AggregateDays(session, DateTime.UtcNow - TimeSpan.FromDays(2), itemId);
            }
        }

        private static bool IsTimestampWithinGroup(DateTime timestamp, TimeSpan boundary)
        {
            return timestamp.Subtract(TimeSpan.FromSeconds(20)).RoundDown(boundary) == timestamp.RoundDown(boundary);
        }

        public async Task Aggregate(ISession session)
        {
            var minutes = GetMinutesTable(session);

            // minute loop
            var startDate = new DateTime(2020, 3, 10);
            var length = TimeSpan.FromHours(6);
            // stonks have always been on bazaar
            string[] ids = await GetAllItemIds();
            var list = new ConcurrentQueue<string>();
            foreach (var item in ids)
            {
                var itemId = item;
                var minTime = new DateTime(2022, 3, 1);
                var maxTime = new DateTime(2022, 3, 3);
                var count = await GetDaysTable(session).Where(r => r.TimeStamp > minTime && r.TimeStamp < maxTime && r.ProductId == itemId).Count().ExecuteAsync();
                if (count != 0)
                {
                    Console.WriteLine("Item already aggregated " + itemId);
                    continue;
                }
                list.Enqueue(item);
            }
            await Task.Delay(60000);
            var workers = new List<Task>();
            for (int i = 0; i < 1; i++)
            {
                var worker = Task.Run(async () =>
                {
                    while (list.TryDequeue(out string itemId))
                    {
                        await NewMethod(session, startDate, length, itemId);
                    }
                    Console.WriteLine("Done aggregating, yay ");

                });
                workers.Add(worker);
            }

            await Task.WhenAll(workers);


        }

        private static async Task NewMethod(ISession session, DateTime startDate, TimeSpan length, string itemId)
        {
            Console.WriteLine("doing: " + itemId);
            await AggregateMinutes(session, startDate, length, itemId, DateTime.UtcNow);
            // hour loop
            await AggregateHours(session, startDate, itemId);
            // day loop
            await AggregateDays(session, startDate, itemId);

            await Task.Delay(10000);
        }

        private static async Task AggregateDays(ISession session, DateTime startDate, string itemId)
        {
            await AggregateMinutesData(session, startDate, TimeSpan.FromDays(2), itemId, GetDaysTable(session), (a, b, c, d) =>
            {
                return CreateBlockAggregated(a, b, c, d, GetHoursTable(a));
            }, TimeSpan.FromDays(1));
        }

        private static async Task AggregateHours(ISession session, DateTime startDate, string itemId)
        {
            await AggregateMinutesData(session, startDate, TimeSpan.FromDays(1), itemId, GetHoursTable(session), (a, b, c, d) =>
            {
                return CreateBlockAggregated(a, b, c, d, GetMinutesTable(a));
            }, TimeSpan.FromHours(2));
        }

        private static async Task AggregateMinutes(ISession session, DateTime startDate, TimeSpan length, string itemId, DateTime endDate)
        {
            await AggregateMinutesData(session, startDate, length, itemId, GetMinutesTable(session), CreateBlock, TimeSpan.FromMinutes(5), 29, endDate);
        }

        private static async Task<string[]> GetAllItemIds()
        {
            var client = new RestClient("https://sky.coflnet.com");
            var stringRes = await client.ExecuteAsync(new RestRequest("/api/items/bazaar/tags"));
            var ids = JsonConvert.DeserializeObject<string[]>(stringRes.Content);
            return ids;
        }

        private static async Task AggregateMinutesData(ISession session, DateTime startDate, TimeSpan length, string itemId, Table<AggregatedQuickStatus> minTable,
            Func<ISession, string, DateTime, DateTime, Task<AggregatedQuickStatus>> Aggregator, TimeSpan detailedLength, int minCount = 29, DateTime stopTime = default)
        {
            if (stopTime == default)
                stopTime = DateTime.UtcNow;
            for (var start = startDate; start + length < stopTime; start += length)
            {
                var end = start + length;
                // check the bigger table for existing records
                var existing = await minTable.Where(SelectExpression(itemId, start - detailedLength, end)).ExecuteAsync();
                var lookup = existing.GroupBy(e => e.TimeStamp.RoundDown(detailedLength)).Select(e => e.First()).ToDictionary(e => e.TimeStamp.RoundDown(detailedLength));
                var addCount = 0;
                var skipped = 0;
                var lineMinCount = start < new DateTime(2022, 1, 1) ? 1 : minCount;
                for (var detailedStart = start; detailedStart < end; detailedStart += detailedLength)
                {
                    if (lookup.TryGetValue(detailedStart.RoundDown(detailedLength), out AggregatedQuickStatus sum) && sum.Count >= lineMinCount)
                    {
                        skipped++;
                        continue;
                    }

                    var detailedEnd = detailedStart + detailedLength;
                    AggregatedQuickStatus result = await Aggregator(session, itemId, detailedStart, detailedEnd);
                    if (result == null)
                        continue;
                    await session.ExecuteAsync(minTable.Insert(result));
                    addCount += result.Count;
                }
                if (length < TimeSpan.FromMinutes(10) && Random.Shared.NextDouble() > 0.1)
                    continue;
                Console.WriteLine($"checked {start} {itemId} {addCount}\t{skipped}");
            }
        }

        private static async Task<AggregatedQuickStatus> CreateBlock(ISession session, string itemId, DateTime detailedStart, DateTime detailedEnd)
        {
            var block = (await GetSmalestTable(session).Where(a => a.ProductId == itemId && a.TimeStamp >= detailedStart && a.TimeStamp < detailedEnd).ExecuteAsync())
                        .ToList().Select(qs =>
                        {
                            qs.BuyPrice = qs.BuyOrders.FirstOrDefault()?.PricePerUnit ?? qs.BuyPrice;
                            qs.SellPrice = qs.SellOrders.FirstOrDefault()?.PricePerUnit ?? qs.SellPrice;
                            return qs;
                        });
            if (block.Count() == 0)
                return null; // no data for this 
            var result = new AggregatedQuickStatus(block.First());
            result.MaxBuy = (float)block.Max(b => b.BuyPrice);
            result.MaxSell = (float)block.Max(b => b.SellPrice);
            result.MinBuy = (float)block.Min(b => b.BuyPrice);
            result.MinSell = (float)block.Min(b => b.SellPrice);
            result.Count = (short)block.Count();
            return result;
        }
        private static async Task<AggregatedQuickStatus> CreateBlockAggregated(ISession session, string itemId, DateTime detailedStart, DateTime detailedEnd, Table<AggregatedQuickStatus> startingTable)
        {
            var block = (await startingTable.Where(a => a.ProductId == itemId && a.TimeStamp >= detailedStart && a.TimeStamp < detailedEnd).ExecuteAsync()).ToList();
            if (block.Count() == 0)
                return null; // no data for this 
            var result = new AggregatedQuickStatus(block.First());
            result.MaxBuy = (float)block.Max(b => b.MaxBuy);
            result.MaxSell = (float)block.Max(b => b.MaxSell);
            result.MinBuy = (float)block.Min(b => b.MinBuy);
            result.MinSell = (float)block.Min(b => b.MinSell);
            result.Count = (short)block.Sum(b => b.Count);
            return result;
        }

        private static Expression<Func<AggregatedQuickStatus, bool>> SelectExpression(string itemId, DateTime start, DateTime end)
        {
            return a => a.ProductId == itemId && a.TimeStamp >= start && a.TimeStamp < end;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public async Task Create()
        {
            if (ranCreate)
                return;
            ranCreate = true;

            var session = await GetSession();

            // await session.ExecuteAsync(new SimpleStatement("DROP table Flip;"));
            Table<StorageQuickStatus> tenseconds = GetSmalestTable(session);
            await tenseconds.CreateIfNotExistsAsync();

            var minutes = GetMinutesTable(session);
            await minutes.CreateIfNotExistsAsync();
            var hours = GetHoursTable(session);
            await hours.CreateIfNotExistsAsync();
            var daily = GetDaysTable(session);
            await daily.CreateIfNotExistsAsync();

            try
            {
                session.Execute($"ALTER TABLE {TABLE_NAME_SECONDS} ADD referenceid BIGINT;");
            }
            catch (Exception e)
            {
                Console.WriteLine("referenceid exists already");
            }
            //Console.WriteLine("there are this many booster cookies: " + JsonConvert.SerializeObject(session.Execute("Select count(*) from " + TABLE_NAME_SECONDS + " where ProductId = 'BOOSTER_COOKIE' and Timestamp > '2021-12-07'").FirstOrDefault()));
        }

        public static Table<AggregatedQuickStatus> GetDaysTable(ISession session)
        {
            return new Table<AggregatedQuickStatus>(session, new MappingConfiguration(), TABLE_NAME_DAILY);
        }

        public static Table<AggregatedQuickStatus> GetHoursTable(ISession session)
        {
            return new Table<AggregatedQuickStatus>(session, new MappingConfiguration(), TABLE_NAME_HOURLY);
        }

        public static Table<AggregatedQuickStatus> GetMinutesTable(ISession session)
        {
            return new Table<AggregatedQuickStatus>(session, new MappingConfiguration(), TABLE_NAME_MINUTES);
        }

        public static Table<StorageQuickStatus> GetSmalestTable(ISession session)
        {
            return new Table<StorageQuickStatus>(session, new MappingConfiguration(), TABLE_NAME_SECONDS);
        }

        public async Task AddEntry(BazaarPull pull)
        {
            var session = await GetSession();
            await AddEntry(new List<BazaarPull> { pull }, session);
        }

        public async Task AddEntry(IEnumerable<BazaarPull> pull, ISession session = null)
        {
            if (session == null)
                session = await GetSession();

            //session.CreateKeyspaceIfNotExists("bazaar_quickstatus");
            //session.ChangeKeyspace("bazaar_quickstatus");
            //var mapper = new Mapper(session);
            var table = GetSmalestTable(session);
            var inserts = pull.SelectMany(p => p.Products.Select(item =>
            {
                if (item.QuickStatus == null)
                    throw new NullReferenceException("Quickstatus can't be null " + item.ProductId);
                var flip = new StorageQuickStatus()
                {
                    TimeStamp = p.Timestamp,
                    ProductId = item.ProductId,
                    SerialisedBuyOrders = MessagePack.MessagePackSerializer.Serialize(item.BuySummery),
                    SerialisedSellOrders = MessagePack.MessagePackSerializer.Serialize(item.SellSummary),
                    BuyMovingWeek = item.QuickStatus.BuyMovingWeek,
                    BuyOrdersCount = item.QuickStatus.BuyOrders,
                    BuyPrice = item.QuickStatus.BuyPrice,
                    BuyVolume = item.QuickStatus.BuyVolume,
                    SellMovingWeek = item.QuickStatus.SellMovingWeek,
                    SellOrdersCount = item.QuickStatus.SellOrders,
                    SellPrice = item.QuickStatus.SellPrice,
                    SellVolume = item.QuickStatus.SellVolume,
                    ReferenceId = item.Id
                };
                return flip;
            })).ToList();

            currentState = inserts;

            Console.WriteLine($"inserting {string.Join(',', pull.Select(p => p.Timestamp))}   at {DateTime.UtcNow}");
            await Task.WhenAll(inserts.GroupBy(i => i.ProductId).Select(async status =>
            {
                var maxTries = 5;
                var statement = new BatchStatement();
                foreach (var item in status)
                {
                    statement.Add(table.Insert(item));
                }
                for (int i = 0; i < maxTries; i++)
                    try
                    {
                        await insertConcurrencyLock.WaitAsync();
                        statement.SetConsistencyLevel(ConsistencyLevel.Quorum);
                        await session.ExecuteAsync((IStatement)statement);
                        insertCount.Inc();
                        return;
                    }
                    catch (Exception e)
                    {
                        insertFailed.Inc();
                        logger.LogError(e, $"storing {status.Key} {status.First().TimeStamp} failed {i} times");
                        await Task.Delay(500 * (i + 1));
                        if (i >= maxTries - 1)
                            throw e;
                    }
                    finally
                    {
                        insertConcurrencyLock.Release();
                    }
            }));
            return;
        }

        public async Task<ISession> GetSession()
        {
            if (_session != null)
                return _session;
            throw new NotImplementedException("the session should be injected from DI");
        }

        public async Task<IEnumerable<AggregatedQuickStatus>> GetStatus(string productId, DateTime start, DateTime end, int count = 1)
        {
            if (end == default)
                end = DateTime.UtcNow;
            if (start == default)
                start = new DateTime(2020, 3, 10);
            var session = await GetSession();
            var mapper = new Mapper(session);
            string tableName = GetTable(start, end);
            //return await GetSmalestTable(session).Where(f => f.ProductId == productId && f.TimeStamp <= end && f.TimeStamp > start).Take(count).ExecuteAsync();
            if (tableName == TABLE_NAME_SECONDS)
            {
                return (await GetSmalestTable(session).Where(f => f.ProductId == productId && f.TimeStamp <= end && f.TimeStamp > start)
                    .OrderByDescending(d => d.TimeStamp).Take(count).ExecuteAsync().ConfigureAwait(false))
                    .ToList().Select(s => new AggregatedQuickStatus(s));
            }
            var loadedFlip = await mapper.FetchAsync<AggregatedQuickStatus>("SELECT * FROM " + tableName
                + " where ProductId = ? and TimeStamp > ? and TimeStamp <= ? Order by Timestamp DESC", productId, start, end).ConfigureAwait(false);

            return loadedFlip.ToList();
        }

        private static string GetTable(DateTime start, DateTime end)
        {
            var length = (end - start);
            if (length < TimeSpan.FromHours(1))
                return TABLE_NAME_SECONDS;  // one every 10 seconds
            if (length < TimeSpan.FromHours(24))
                return TABLE_NAME_MINUTES; // 1 per 5 min
            if (length < TimeSpan.FromDays(7.01f))
                return TABLE_NAME_HOURLY; // 1 per 2 hours
            return TABLE_NAME_DAILY; // one daily
        }
    }
}
