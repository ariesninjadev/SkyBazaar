using System.Collections.Generic;

namespace Coflnet.Sky.SkyBazaar.Models;

public class OrderBook
{
    /// <summary>
    /// all buy orders (biggest is best)
    /// </summary>
    public List<OrderEntry> Buy { get; set; } = new();
    /// <summary>
    /// all sell orders (smallest is best)
    /// </summary>
    public List<OrderEntry> Sell { get; set; }= new();

    /// <summary>
    /// Returns true if there was sombebody outbid by the new order
    /// </summary>
    /// <param name="entry"></param>
    /// <param name="outbid"></param>
    /// <returns></returns>
    public bool TryGetOutbid(OrderEntry entry, out OrderEntry outbid)
    {
        outbid = null;
        if (entry.IsSell)
        {
            foreach (var item in Sell)
            {
                if (item.PricePerUnit > entry.PricePerUnit && item.UserId != null)
                {
                    outbid = item;
                    return true;
                }
            }
        }
        else
        {
            foreach (var item in Buy)
            {
                if (item.PricePerUnit < entry.PricePerUnit && item.UserId != null)
                {
                    outbid = item;
                    return true;
                }
            }
        }
        return false;
    }
}