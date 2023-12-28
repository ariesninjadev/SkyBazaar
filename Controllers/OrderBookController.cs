using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Coflnet.Sky.SkyAuctionTracker.Services;
using Coflnet.Sky.SkyBazaar.Models;
using Confluent.Kafka;
using Microsoft.AspNetCore.Mvc;

namespace Coflnet.Sky.SkyAuctionTracker.Controllers
{
    /// <summary>
    /// OrderBook controller
    /// </summary>
    /// <returns></returns>
    /// 
    [ApiController]
    [Route("[controller]")]
    public class OrderBookController : ControllerBase
    {
        private readonly OrderBookService service;

        /// <summary>
        /// Creates a new instance of <see cref="OrderBookController"/>
        /// </summary>
        /// <param name="service"></param>
        public OrderBookController(OrderBookService service)
        {
            this.service = service;
        }

        /// <summary>
        /// Gets the order book for a specific item
        /// </summary>
        /// <param name="itemTag"></param>
        /// <returns></returns>
        [HttpGet]
        [Route("{itemTag}")]
        public async Task<OrderBook> GetOrderBook(string itemTag)
        {
            return await service.GetOrderBook(itemTag);
        }

        /// <summary>
        /// Adds an order to the order book
        /// </summary>
        /// <param name="order"></param>
        [HttpPost]
        public async Task AddOrder(OrderEntry order)
        {
            order.IsVerfified = false;
            await service.AddOrder(order);
        }

        /// <summary>
        /// Removes and order from the order book
        /// </summary>
        [HttpDelete]
        public async Task RemoveOrder(string itemTag, string userId, DateTime timestamp)
        {
            await service.RemoveOrder(itemTag, userId, timestamp);
        }
    }
}
