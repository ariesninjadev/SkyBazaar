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

namespace Coflnet.Sky.SkyAuctionTracker.Services
{

    public class BazaarBackgroundService : BackgroundService
    {
        private IServiceScopeFactory scopeFactory;
        private IConfiguration config;
        private ILogger<BazaarBackgroundService> logger;

        public BazaarBackgroundService(
            IServiceScopeFactory scopeFactory, IConfiguration config, ILogger<BazaarBackgroundService> logger)
        {
            this.scopeFactory = scopeFactory;
            this.config = config;
            this.logger = logger;
        }
        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            var index = 0;
            await GetService().Create();


            var consumeTask = Coflnet.Kafka.KafkaConsumer.ConsumeBatch<BazaarPull>(config["KAFKA_HOST"], config["TOPICS:BAZAAR"], async bazaar =>
            {
                BazaarService service = GetService();
                foreach (var item in bazaar)
                {

                }
                await Task.WhenAll(bazaar.Select(async (b) =>
                {
                    try
                    {
                        await service.NewPull(index++, b);
                    }
                    catch (System.Exception e)
                    {
                        logger.LogError(e, "saving");
                    }
                }));
            }, stoppingToken, "sky-bazaar-test");

            await Task.WhenAny(consumeTask);
        }


        private BazaarService GetService()
        {
            return scopeFactory.CreateScope().ServiceProvider.GetRequiredService<BazaarService>();
        }
    }
}