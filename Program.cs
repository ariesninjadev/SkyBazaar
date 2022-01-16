using System.Threading.Tasks;
using Coflnet.Sky.SkyAuctionTracker.Services;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.Hosting;

namespace Coflnet.Sky.SkyAuctionTracker
{
    public class Program
    {
        public static async Task Main(string[] args)
        {
            /*var order = new dev.SellOrder()
                            {
                                Amount = 5,
                                Orders = 2,
                                PricePerUnit = 3.2
                            };
            await new BazaarService(null).AddEntry(new dev.BazaarPull(){
                Timestamp = System.DateTime.Now,
                Products = new System.Collections.Generic.List<dev.ProductInfo>()
                {
                    new dev.ProductInfo()
                    {
                        ProductId = "kevin",
                        SellSummary = new System.Collections.Generic.List<dev.SellOrder>()
                        {
                            order,order,order,order,order,order,order,order,order,order
                        }
                    }
                }
            });*/
            CreateHostBuilder(args).Build().Run();
        }

        public static IHostBuilder CreateHostBuilder(string[] args) =>
            Host.CreateDefaultBuilder(args)
                .ConfigureWebHostDefaults(webBuilder =>
                {
                    webBuilder.UseStartup<Startup>();
                });
    }
}
