using System;
using MessagePack;
using Newtonsoft.Json;

namespace Coflnet.Sky.SkyBazaar.Models
{
    public class StorageQuickStatus
    {
        [Key(0)]
        [JsonProperty("productId")]
        [Cassandra.Mapping.Attributes.PartitionKey]
        public string ProductId { get; set; }
        [Key(1)]
        [JsonProperty("buyPrice")]
        public double BuyPrice { get; set; }
        [Key(2)]
        [JsonProperty("buyVolume")]
        public long BuyVolume { get; set; }
        [Key(3)]
        [JsonProperty("buyMovingWeek")]
        public long BuyMovingWeek { get; set; }
        [Key(4)]
        [JsonProperty("buyOrders")]
        public int BuyOrdersCount { get; set; }
        [Key(5)]
        [JsonProperty("sellPrice")]
        public double SellPrice { get; set; }
        [Key(6)]
        [JsonProperty("sellVolume")]
        public long SellVolume { get; set; }
        [Key(7)]
        [JsonProperty("sellMovingWeek")]
        public long SellMovingWeek { get; set; }
        [Key(8)]
        [JsonProperty("sellOrders")]
        public int SellOrdersCount { get; set; }
        [Key(9)]
        [JsonProperty("timestamp")]
        [Cassandra.Mapping.Attributes.ClusteringKey]
        public DateTime TimeStamp { get; set; }
        [IgnoreMember]
        [JsonProperty("buyOrdersSerialised")]
        public byte[] SerialisedBuyOrders { get; set; }
        [IgnoreMember]
        [JsonProperty("sellOrdersSerialised")]
        public byte[] SerialisedSellOrders { get; set; }
        [IgnoreMember]
        [JsonProperty("refId")]
        public long ReferenceId { get; set; }

        public StorageQuickStatus() { }

        public StorageQuickStatus(StorageQuickStatus status)
        {
            ProductId = status.ProductId;
            BuyPrice = status.BuyPrice;
            BuyVolume = status.BuyVolume;
            BuyMovingWeek = status.BuyMovingWeek;
            BuyOrdersCount = status.BuyOrdersCount;
            SellPrice = status.SellPrice;
            SellVolume = status.SellVolume;
            SellMovingWeek = status.SellMovingWeek;
            SellOrdersCount = status.SellOrdersCount;
            TimeStamp = status.TimeStamp;
            ReferenceId = status.ReferenceId;
            SerialisedBuyOrders = status.SerialisedBuyOrders;
            SerialisedSellOrders = status.SerialisedSellOrders;
        }
    }

    public class BuyOrder : Order
    {
        public BuyOrder() { }
    }

    public class SellOrder : Order
    {
        public SellOrder() { }
    }


    [MessagePackObject]
    public class Order
    {
        public Order() { }

        [IgnoreMember]
        public int Id { get; set; }
        [Key(0)]
        [JsonProperty("amount")]
        public int Amount { get; set; }
        [Key(1)]
        [JsonProperty("pricePerUnit")]
        public double PricePerUnit { get; set; }
        [Key(2)]
        [JsonProperty("orders")]
        public short Orders { get; set; }

        public bool ValueSame(Order order)
        {
            return Amount == order.Amount
                && PricePerUnit == order.PricePerUnit
                && Orders == order.Orders;
        }
    }
}