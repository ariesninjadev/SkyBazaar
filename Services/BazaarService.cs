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

namespace Coflnet.Sky.SkyAuctionTracker.Services
{
    public class BazaarService
    {
        private const string TABLE_NAME_DAILY = "QuickStatusDaly";
        private const string TABLE_NAME_HOURLY = "QuickStatusHourly";
        private const string TABLE_NAME_MINUTES = "QuickStatusMin";
        private const string TABLE_NAME_SECONDS = "QuickStatusSeconds";

        private static bool ranCreate;
        private IConfiguration config;
        private ILogger<BazaarService> logger;

        private static Prometheus.Counter insertCount = Prometheus.Metrics.CreateCounter("sky_bazaar_status_insert", "How many inserts were made");
        private static Prometheus.Counter insertFailed = Prometheus.Metrics.CreateCounter("sky_bazaar_status_insert_failed", "How many inserts failed");

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

            session.CreateKeyspaceIfNotExists("bazaar_quickstatus");
            session.ChangeKeyspace("bazaar_quickstatus");

            // await session.ExecuteAsync(new SimpleStatement("DROP table Flip;"));
            Table<StorageQuickStatus> tenseconds = GetSmalestTable(session);
            await tenseconds.CreateIfNotExistsAsync();
            Table<AggregatedQuickStatus> minutes = GetMinutesTable(session);
            await minutes.CreateIfNotExistsAsync();
            Table<AggregatedQuickStatus> hours = GetHoursTable(session);
            await hours.CreateIfNotExistsAsync();
            Table<AggregatedQuickStatus> daily = GetDaysTable(session);
            await daily.CreateIfNotExistsAsync();

            try
            {
                session.Execute($"ALTER TABLE {TABLE_NAME_SECONDS} ADD referenceid BIGINT;");
            }
            catch (Exception e)
            {
                Console.WriteLine("referenceid exists already");
            }
            Console.WriteLine("there are this many booster cookies: " + JsonConvert.SerializeObject(session.Execute("Select count(*) from " + TABLE_NAME_SECONDS + " where ProductId = 'BOOSTER_COOKIE'").FirstOrDefault()));
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
                };
                return flip;
            });

            Console.WriteLine($"inserting {pull.Timestamp}   at {DateTime.Now}");
            await Task.WhenAll(inserts.Select(async status =>
            {
                for (int i = 0; i < 3; i++)
                    try
                    {
                        await session.ExecuteAsync(table.Insert(status));
                        insertCount.Inc();
                        return;
                    }
                    catch (Exception e)
                    {
                        insertFailed.Inc();
                        logger.LogError(e, $"storing { status.ProductId} { status.TimeStamp}");
                        await Task.Delay(1500);
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
            var cluster = Cluster.Builder()
                                .WithCredentials(config["CASSANDRA:USER"], config["CASSANDRA:PASSWORD"])
                                .AddContactPoints(config["CASSANDRA:HOST"])
                                .Build();
            if (keyspace == null)
                return await cluster.ConnectAsync();
            return await cluster.ConnectAsync(keyspace);
        }

        public async Task<IEnumerable<StorageQuickStatus>> GetStatus(string productId, DateTime start, DateTime end, int count = 1)
        {
            var session = await GetSession();
            var mapper = new Mapper(session);
            string tableName = GetTable(start, end);
            return await GetSmalestTable(session).Where(f => f.ProductId == productId && f.TimeStamp <= end && f.TimeStamp > start).Take(count).ExecuteAsync();
            var loadedFlip = await mapper.FetchAsync<StorageQuickStatus>("SELECT * FROM " + tableName
                + " where ProductId = ? and TimeStamp > ? and TimeStamp <= ? Order by Timestamp DESC", productId, start, end);

            return loadedFlip.ToList();
        }

        private static string GetTable(DateTime start, DateTime end)
        {
            var length = (end - start);
            var tableName = TABLE_NAME_DAILY; // one daily
            if (length < TimeSpan.FromDays(7.01f))
                tableName = TABLE_NAME_HOURLY; // 1 per 2 hours
            else if (length < TimeSpan.FromHours(12))
                tableName = TABLE_NAME_MINUTES; // 1 per 5 min
            else if (length < TimeSpan.FromHours(1))
                tableName = TABLE_NAME_SECONDS; // one every 10 seconds
            return tableName;
        }
    }
}
