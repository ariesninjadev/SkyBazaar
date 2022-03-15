using System.Threading;
using System.Threading.Tasks;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Confluent.Kafka;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Coflnet.Sky.SkyAuctionTracker.Controllers;
using dev;
using System.Linq;
using System.Collections.Generic;
using System;
using Cassandra;

namespace Coflnet.Sky.SkyAuctionTracker.Services
{

    public class BazaarBackgroundService : BackgroundService
    {
        private IServiceScopeFactory scopeFactory;
        private IConfiguration config;
        private ILogger<BazaarBackgroundService> logger;

        Prometheus.Counter consumeCounter = Prometheus.Metrics.CreateCounter("sky_bazaar_consume_counter", "How many message batches were consumed from kafka");

        public BazaarBackgroundService(
            IServiceScopeFactory scopeFactory, IConfiguration config, ILogger<BazaarBackgroundService> logger)
        {
            this.scopeFactory = scopeFactory;
            this.config = config;
            this.logger = logger;
        }
        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            return;
            await GetService().Create();
            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    await GetConsumeTask(stoppingToken);
                }
                catch (NoHostAvailableException e)
                {
                    logger.LogError(e, "");
                    logger.LogError(Newtonsoft.Json.JsonConvert.SerializeObject(e.Errors));
                }
                catch (Exception e)
                {
                    logger.LogError(e, "consume & insert");
                }
                logger.LogError("ended consumption");

            }
        }

        private Task GetConsumeTask(CancellationToken stoppingToken)
        {
            return Coflnet.Kafka.KafkaConsumer.ConsumeBatch<BazaarPull>(config["KAFKA_HOST"], config["TOPICS:BAZAAR"], async bazaar =>
            {
                consumeCounter.Inc();
                using var scope = scopeFactory.CreateScope();
                BazaarService service = scope.ServiceProvider.GetRequiredService<BazaarService>();
                using var session = await service.GetSession();
                System.Console.WriteLine($"retrieved batch {bazaar.Count()}, start processing");
                await Task.WhenAll(bazaar.Select(async (b) =>
                {
                    try
                    {
                        await service.AddEntry(b, session);
                    }
                    catch (System.Exception e)
                    {
                        logger.LogError(e, "saving");
                        throw e;
                    }
                }));
                await session.ShutdownAsync();
                await session.Cluster.ShutdownAsync();
            }, stoppingToken, "sky-bazaar-test", 5);
        }

        private BazaarService GetService()
        {
            return scopeFactory.CreateScope().ServiceProvider.GetRequiredService<BazaarService>();
        }
    }
}