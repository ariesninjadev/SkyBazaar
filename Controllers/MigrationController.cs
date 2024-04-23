using Microsoft.AspNetCore.Mvc;
using System;
using Coflnet.Sky.SkyAuctionTracker.Services;
using Microsoft.Extensions.DependencyInjection;

namespace Coflnet.Sky.SkyAuctionTracker.Controllers
{
    [ApiController]
    public class MigrationController : ControllerBase
    {
        IServiceProvider provider;

        public MigrationController(IServiceProvider provider)
        {
            this.provider = provider;
        }

        [Route("ready")]
        [HttpGet]
        public IActionResult Ready()
        {
            var migrationService = provider.GetService<MigrationService>();
            if(migrationService == null)
            {
                return Ok("success");
            }
            return migrationService.IsDone ? Ok("done") : StatusCode(503);
        }
    }
}
