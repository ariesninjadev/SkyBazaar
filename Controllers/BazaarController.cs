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
        public BazaarController( BazaarService service)
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
            Console.WriteLine(itemId);
            var entries = (await service.GetStatus(itemId,DateTime.UtcNow - TimeSpan.FromSeconds(520), DateTime.UtcNow, 1)).ToList();
            foreach (var item in entries)
            {
                Console.WriteLine(" oooh " + item.TimeStamp);
            }
            Console.WriteLine(entries.Count());
            var result = entries.ToList().Take(5).ToList();
            
            Console.WriteLine(result.Count());
            
            return result.FirstOrDefault();
        }
    }
}
