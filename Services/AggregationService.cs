using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using System;
using Cassandra;
using Coflnet.Sky.Core;

namespace Coflnet.Sky.SkyAuctionTracker.Services
{
    public class AggregationService : BackgroundService
    {
        private IServiceScopeFactory scopeFactory;
        private IConfiguration config;
        private ILogger<BazaarBackgroundService> logger;

        public AggregationService(IServiceScopeFactory scopeFactory, IConfiguration config, ILogger<BazaarBackgroundService> logger)
        {
            this.scopeFactory = scopeFactory;
            this.config = config;
            this.logger = logger;
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            await MigrateFromMariadb(stoppingToken);
            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    await DoCycle();
                }
                catch (Exception e)
                {
                    logger.LogError(e, "aggreating");
                }
                await Task.Delay(300000);
            }
        }

        private async Task MigrateFromMariadb(CancellationToken stoppingToken)
        {
            using var scope = scopeFactory.CreateScope();
            using var context = scope.ServiceProvider.GetRequiredService<HypixelContext>();
            var bazaar = scope.ServiceProvider.GetRequiredService<BazaarService>();
            await bazaar.MigrateFromMariadb(context, stoppingToken);
        }

        private async Task<ISession> DoCycle()
        {
            await Task.Delay(6000);
            var service = GetService();
            var session = await service.GetSession();

            await service.Aggregate(session);
            return session;
        }

        private BazaarService GetService()
        {
            return scopeFactory.CreateScope().ServiceProvider.GetRequiredService<BazaarService>();
        }
    }
}