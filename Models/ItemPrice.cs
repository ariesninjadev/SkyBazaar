namespace Coflnet.Sky.SkyBazaar.Models
{
    public class ItemPrice
    {
        public string ProductId { get; set; }
        public double BuyPrice { get; set; }
        public double SellPrice { get; set; }
        public double GreatestBuy { get; set; }
        public double CheapestSell { get; set; }
    }
}
