using System;
using MessagePack;
using Newtonsoft.Json;

namespace Coflnet.Sky.SkyBazaar.Models
{
    /// <summary>
    /// Result element for historic price graph
    /// </summary>
    public class GraphResult
    {
        /// <summary>
        /// The biggest buy price in this aggregated timespan
        /// </summary>
        /// <value></value>
        [JsonProperty("maxBuy")]
        public float MaxBuy { get; set; }
        /// <summary>
        /// The biggest sell price in this aggregated timespan
        /// </summary>
        /// <value></value>
        [JsonProperty("maxSell")]
        public float MaxSell { get; set; }
        /// <summary>
        /// The smalest buy price in this aggregated timespan
        /// </summary>
        /// <value></value>
        [JsonProperty("minBuy")]
        public float MinBuy { get; set; }
        /// <summary>
        /// The smalest sell price in this aggregated timespan
        /// </summary>
        /// <value></value>
        [IgnoreMember]
        [JsonProperty("minSell")]
        public float MinSell { get; set; }
        /// <summary>
        /// The buy price at the start of this aggregated timespan
        /// </summary>
        /// <value></value>
        [JsonProperty("buy")]
        public double Buy { get; set; }
        /// <summary>
        /// The sell price at the start of this aggregated timespan
        /// </summary>
        /// <value></value>
        [JsonProperty("sell")]
        public double Sell { get; set; }
        /// <summary>
        /// The sell volume 
        /// </summary>
        /// <value></value>
        [JsonProperty("sellVol")]
        public long SellVolume { get; internal set; }
        /// <summary>
        /// The buy volume (count of transfared items)
        /// </summary>
        /// <value></value>
        [JsonProperty("buyVol")]
        public long BuyVolume { get; internal set; }
        /// <summary>
        /// The Date and time this element coresponds to
        /// </summary>
        /// <value></value>
        [JsonProperty("timestamp")]
        public DateTime Timestamp { get; internal set; }
    }
}