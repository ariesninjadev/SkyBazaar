using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;
using System;
using System.Linq;
using Microsoft.EntityFrameworkCore;
using System.Collections;
using System.Collections.Generic;
using Coflnet.Sky.SkyAuctionTracker.Services;
using dev;
using Coflnet.Sky.SkyBazaar.Models;

namespace Coflnet.Sky.SkyAuctionTracker.Controllers
{
    /// <summary>
    /// Main Controller handling tracking
    /// </summary>
    [ApiController]
    [Route("api/[controller]")]
    public class BazaarController : ControllerBase
    {
        private readonly BazaarService service;

        /// <summary>
        /// Creates a new instance of <see cref="BazaarController"/>
        /// </summary>
        /// <param name="service"></param>
        public BazaarController(BazaarService service)
        {
            this.service = service;
        }

        /// <summary>
        /// Gets the latest status for an item
        /// </summary>
        /// <param name="itemId"></param>
        /// <returns></returns>
        [Route("{itemId}/status")]
        [HttpGet]
        public async Task<SkyBazaar.Models.StorageQuickStatus> GetStatus(string itemId)
        {
            var entries = (await service.GetStatus(itemId, DateTime.UtcNow - TimeSpan.FromSeconds(520), DateTime.UtcNow, 1)).ToList();
            return entries.FirstOrDefault();
        }
        /// <summary>
        /// Gets the latest status for an item
        /// </summary>
        /// <param name="itemId"></param>
        /// <param name="start"></param>
        /// <param name="end"></param>
        /// <returns></returns>
        [Route("{itemId}/data")]
        [HttpGet]
        public async Task<IEnumerable<SkyBazaar.Models.AggregatedQuickStatus>> GetData(string itemId, DateTime start, DateTime end)
        {
            var entries = await service.GetStatus(itemId, start, end, 100);
            return entries;
        }

        /// <summary>
        /// Gets a snapshot of the order book at a specific time
        /// </summary>
        /// <param name="itemId"></param>
        /// <param name="time"></param>
        /// <returns></returns>
        [Route("{itemId}/snapshot")]
        [HttpGet]
        public async Task<SkyBazaar.Models.StorageQuickStatus> GetClosestTo(string itemId, DateTime time)
        {
            var entries = await service.GetStatus(itemId, time - TimeSpan.FromMinutes(1), time + TimeSpan.FromSeconds(9), 1);
            return entries.FirstOrDefault();
        }
        /// <summary>
        /// Gets the latest status for an item
        /// </summary>
        /// <param name="itemId"></param>
        /// <param name="start"></param>
        /// <param name="end"></param>
        /// <returns></returns>
        [Route("{itemId}/history")]
        [HttpGet]
        public async Task<IEnumerable<GraphResult>> GetHistoryGraph(string itemId, DateTime start, DateTime end)
        {
            var entries = await service.GetStatus(itemId, start, end, 500);
            var result = entries.ToList();

            Console.WriteLine(result.Count());

            return result.Select(a =>
            {
                return new GraphResult()
                {
                    Buy = a.BuyOrders.FirstOrDefault()?.PricePerUnit ?? a.BuyPrice,
                    MaxBuy = a.MaxBuy,
                    MinBuy = a.MinBuy,
                    MinSell = a.MinSell,
                    Sell = a.SellOrders.FirstOrDefault()?.PricePerUnit ?? a.SellPrice,
                    SellVolume = a.SellVolume,
                    BuyVolume = a.BuyVolume,
                    Timestamp = a.TimeStamp
                };
            });
        }
    }
}
