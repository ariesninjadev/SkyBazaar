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
using Microsoft.EntityFrameworkCore;
using System.Collections.Concurrent;
using Microsoft.Extensions.DependencyInjection;
using OpenTracing.Util;

namespace Coflnet.Sky.SkyAuctionTracker.Services
{
    public class BazaarService
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

        private static Prometheus.Counter insertCount = Prometheus.Metrics.CreateCounter("sky_bazaar_status_insert", "How many inserts were made");
        private static Prometheus.Counter insertFailed = Prometheus.Metrics.CreateCounter("sky_bazaar_status_insert_failed", "How many inserts failed");
        private static Prometheus.Counter checkSuccess = Prometheus.Metrics.CreateCounter("sky_bazaar_check_success", "How elements where found in cassandra");
        private static Prometheus.Counter checkFail = Prometheus.Metrics.CreateCounter("sky_bazaar_check_fail", "How elements where not found in cassandra");

        private List<StorageQuickStatus> currentState = new List<StorageQuickStatus>();

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

        public BazaarService(IConfiguration config, ILogger<BazaarService> logger)
        {
            this.config = config;
            this.logger = logger;
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
            });

            var prices = tags.Select(async t =>
            {
                var prices = await GetStatus(t, DateTime.UtcNow - TimeSpan.FromSeconds(25), DateTime.UtcNow, 1).ConfigureAwait(false);
                var price = prices.LastOrDefault();
                if (price == null)
                    return new ItemPrice() { ProductId = t };
                return new ItemPrice()
                {
                    ProductId = t,
                    BuyPrice = price.BuyPrice,
                    SellPrice = price.SellPrice
                };
            });
            return await Task.WhenAll(prices);
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
            return timestamp.Subtract(TimeSpan.FromSeconds(10)).RoundDown(boundary) == timestamp.RoundDown(boundary);
        }

        public async Task TestSamples(IServiceScopeFactory scopeFactory, System.Threading.CancellationToken stoppingToken)
        {
            var session = await GetSession();
            var table = GetSmalestTable(session);
            var maxDateTime = new DateTime(2022, 2, 4);
            var highestId = 0;
            for (int i = 0; i < 20_000; i++)
            {
                using var scope = scopeFactory.CreateScope();
                using var context = scope.ServiceProvider.GetRequiredService<HypixelContext>();
                if (highestId == 0)
                    highestId = context.BazaarPrices.Max(p => p.Id);
                var ids = new List<int>();
                for (int j = 0; j < 50; j++)
                {
                    ids.Add(Random.Shared.Next(300, highestId));
                }
                var refernces = await context.BazaarPrices.Include(p => p.PullInstance).Where(p => ids.Contains(p.Id) && p.PullInstance.Timestamp < maxDateTime).ToListAsync();
                if (refernces.Count < 48)
                {
                    highestId -= 10_000;
                }


                var tasks = refernces.Select(async item =>
                {
                    try
                    {
                        var minTime = item.PullInstance.Timestamp - TimeSpan.FromSeconds(0.5);
                        var maxTime = minTime + TimeSpan.FromSeconds(1);
                        var exists = (await table.Where(s => s.ProductId == item.ProductId && s.TimeStamp >= minTime && s.TimeStamp < maxTime).ExecuteAsync()).ToList();
                        if (exists.Any(s => item.Id == s.ReferenceId))
                        {
                            checkSuccess.Inc();
                            checkSuccessHistogram.Observe(item.Id);
                            return;
                        }
                        checkFail.Inc();
                        checkFailHistogram.Observe(item.Id);
                        logger.LogWarning($"Could not find {item.ProductId} {JsonConvert.SerializeObject(item.PullInstance.Timestamp)} {item.Id} {exists.FirstOrDefault()?.ReferenceId} {exists.Count}");
                    }
                    catch (System.Exception e)
                    {
                        logger.LogError(e, "sampling cassandra");
                    }
                }).ToList();

                await Task.WhenAll(tasks);
                if (stoppingToken.IsCancellationRequested)
                {
                    break;
                }
                if (i % 500 == 0)
                {
                    logger.LogInformation($"Cassandra check reached {i}");
                }
            }
            logger.LogInformation("done with cassandra sampling");

        }

        internal async Task MigrateFromMariadb(HypixelContext context, System.Threading.CancellationToken stoppingToken)
        {
            var maxTime = new DateTime(2022, 2, 1);
            var session = await GetSession();
            var table = GetSmalestTable(session);
            var sampleStatus = new StorageQuickStatus();
            var maxSelect = $"Select max(Timestamp) from {TABLE_NAME_SECONDS} where ProductId = '{DEFAULT_ITEM_TAG}' and Timestamp < '2022-02-17 16:05' and Timestamp > '2022-02-07'";
            var highestTime = session.Execute(maxSelect).FirstOrDefault()?.FirstOrDefault();
            Nullable<Int64> highestId = 1;
            int pullInstanceId = 1;
            Console.WriteLine($"max timestamp is {highestTime} {highestTime?.GetType()?.Name}");
            if (highestTime != null)
            {
                var minTime = ((DateTimeOffset)highestTime).Subtract(TimeSpan.FromSeconds(1));
                var maxRetTime = ((DateTimeOffset)highestTime).Add(TimeSpan.FromSeconds(1));
                var newest = await table.Where(t => t.ProductId == DEFAULT_ITEM_TAG && t.TimeStamp > minTime && t.TimeStamp < maxTime)
                            .FirstOrDefault().ExecuteAsync();
                highestId = newest?.ReferenceId;
                if (highestId == null)
                {
                    // lost migrationid 
                    var time = ((DateTimeOffset)highestTime).DateTime;
                    var fromDb = await context.BazaarPull.Where(b => b.Timestamp > minTime && b.Timestamp < maxRetTime)
                                    .FirstOrDefaultAsync();
                    Console.WriteLine(JsonConvert.SerializeObject(fromDb));
                    pullInstanceId = fromDb.Id;
                }
                Console.WriteLine(JsonConvert.SerializeObject(newest));
            }
            Console.WriteLine($"Starting migrating from " + highestId);
            var noEntries = false;
            if (pullInstanceId <= 1)
                try
                {
                    var data = context.BazaarPrices.Include(b => b.PullInstance).Where(b => b.Id == highestId).FirstOrDefault();
                    pullInstanceId = data.PullInstance.Id;
                    Console.WriteLine("retrieved pullIntanceId");
                }
                catch (Exception e)
                {
                    pullInstanceId = 4005005;
                    logger.LogError(e, "failed to retrieve pullInstance Id starting from " + pullInstanceId);
                }
            if (pullInstanceId > 1000)
                pullInstanceId--; // redo the last one to make sure none is lost
            Console.WriteLine($"Pull instance ref id " + pullInstanceId);
            while (!noEntries && !stoppingToken.IsCancellationRequested)
            {
                var start = pullInstanceId;
                pullInstanceId += 4;
                var end = pullInstanceId;
                var pulls = await context.BazaarPull
                        .Include(p => p.Products).ThenInclude(p => p.SellSummary)
                        .Include(p => p.Products).ThenInclude(p => p.BuySummery)
                        .Include(p => p.Products).ThenInclude(p => p.QuickStatus)
                        .Where(p => p.Id >= start && p.Id < end).AsNoTracking().ToListAsync();
                if (pulls.Count == 0)
                {
                    if (pullInstanceId == 540527)
                    {
                        pullInstanceId = 540558;
                        continue;
                    }
                    throw new Exception("none retrieved from mariadb, exiting " + (pullInstanceId - 1));
                }

                foreach (var pull in pulls)
                {
                    if (pull.Timestamp >= new DateTime(2022, 2, 17, 16, 9, 38, DateTimeKind.Utc))
                    {
                        Console.WriteLine("whooooo migration done");
                        noEntries = true;
                        return;
                    }
                    await AddEntry(pull, session);
                }
            }
            Console.WriteLine(highestTime);
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
            for (int i = 0; i < 3; i++)
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

            var session = await GetSession(null);

            var replication = new Dictionary<string, string>()
            {
                {"class", config["CASSANDRA:REPLICATION_CLASS"]},
                {"replication_factor", config["CASSANDRA:REPLICATION_FACTOR"]}
            };
            session.CreateKeyspaceIfNotExists("bazaar_quickstatus", replication);
            session.ChangeKeyspace("bazaar_quickstatus");

            /*session.Execute("drop table " + TABLE_NAME_HOURLY);
            session.Execute("drop table " + TABLE_NAME_DAILY);
            Console.WriteLine("dropped tables for migration");*/

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

        private static Table<AggregatedQuickStatus> GetDaysTable(ISession session)
        {
            return new Table<AggregatedQuickStatus>(session, new MappingConfiguration(), TABLE_NAME_DAILY);
        }

        private static Table<AggregatedQuickStatus> GetHoursTable(ISession session)
        {
            return new Table<AggregatedQuickStatus>(session, new MappingConfiguration(), TABLE_NAME_HOURLY);
        }

        private static Table<AggregatedQuickStatus> GetMinutesTable(ISession session)
        {
            return new Table<AggregatedQuickStatus>(session, new MappingConfiguration(), TABLE_NAME_MINUTES);
        }

        private static Table<StorageQuickStatus> GetSmalestTable(ISession session)
        {
            return new Table<StorageQuickStatus>(session, new MappingConfiguration(), TABLE_NAME_SECONDS);
        }

        public async Task AddEntry(BazaarPull pull, ISession session = null)
        {
            if (session == null)
                session = await GetSession();

            //session.CreateKeyspaceIfNotExists("bazaar_quickstatus");
            //session.ChangeKeyspace("bazaar_quickstatus");
            //var mapper = new Mapper(session);
            var table = GetSmalestTable(session);
            var inserts = pull.Products.Select(item =>
            {
                if (item.QuickStatus == null)
                    throw new NullReferenceException("Quickstatus can't be null " + item.ProductId);
                var flip = new StorageQuickStatus()
                {
                    TimeStamp = pull.Timestamp,
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
            }).ToList();

            currentState = inserts;

            Console.WriteLine($"inserting {pull.Timestamp}   at {DateTime.UtcNow}");
            await Task.WhenAll(inserts.Select(async status =>
            {
                var maxTries = 5;
                for (int i = 0; i < maxTries; i++)
                    try
                    {
                        var statement = table.Insert(status);
                        statement.SetConsistencyLevel(ConsistencyLevel.Quorum);
                        await session.ExecuteAsync(statement);
                        insertCount.Inc();
                        return;
                    }
                    catch (Exception e)
                    {
                        insertFailed.Inc();
                        logger.LogError(e, $"storing {status.ProductId} {status.TimeStamp} failed {i} times");
                        await Task.Delay(500 * i);
                        if (i >= maxTries - 1)
                            throw e;
                    }
            }));
            return;

            var loadedFlip = (await GetStatus("kevin", DateTime.Now - TimeSpan.FromMinutes(200), DateTime.Now + TimeSpan.FromSeconds(2))).First();
            //var loadedFlip = await mapper.FirstOrDefaultAsync<StorageQuickStatus>("SELECT * FROM StorageQuickStatus where ProductId = ? Order by Timestamp DESC", pull.Products.First().ProductId);
            //var loadedFlips = await mapper.Execut;
            //var loadedFlip = loadedFlips.First();
            Console.WriteLine(loadedFlip.TimeStamp);
            Console.WriteLine(loadedFlip.ProductId);
            Console.WriteLine(JsonConvert.SerializeObject(MessagePack.MessagePackSerializer.Deserialize<List<dev.SellOrder>>(loadedFlip.SerialisedSellOrders)));
            Console.WriteLine(JsonConvert.SerializeObject(loadedFlip.SerialisedSellOrders));

        }

        public async Task<ISession> GetSession(string keyspace = "bazaar_quickstatus")
        {
            if (_session != null)
                return _session;
            var cluster = Cluster.Builder()
                                .WithCredentials(config["CASSANDRA:USER"], config["CASSANDRA:PASSWORD"])
                                .AddContactPoints(config["CASSANDRA:HOSTS"].Split(","))
                                .Build();
            if (keyspace == null)
                return await cluster.ConnectAsync();
            _session = await cluster.ConnectAsync(keyspace);
            return _session;
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
            using var span = GlobalTracer.Instance.BuildSpan("cassandra").StartActive();
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
