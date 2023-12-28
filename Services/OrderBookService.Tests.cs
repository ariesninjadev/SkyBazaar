using NUnit.Framework;
using Moq;
using Cassandra;
using Coflnet.Sky.EventBroker.Client.Api;
using Coflnet.Sky.Items.Client.Api;
using Coflnet.Sky.SkyBazaar.Models;
using System.Threading.Tasks;
using System;
using System.Collections.Generic;
using Coflnet.Sky.EventBroker.Client.Model;
using System.Linq;

namespace Coflnet.Sky.SkyAuctionTracker.Services;

public class OrderBookServiceTests
{
    private class NoDbOrderBookService : OrderBookService
    {
        public OrderEntry LastOrder { get; private set; }
        public OrderEntry RemovedOrder { get; private set; }
        public NoDbOrderBookService(ISessionContainer service, IMessageApi messageApi, IItemsApi itemsApi) : base(service, messageApi, itemsApi)
        {
        }
        protected override Task InsertToDb(OrderEntry order)
        {
            LastOrder = order;
            return Task.CompletedTask;
        }
        protected override Task RemoveFromDb(OrderEntry order)
        {
            RemovedOrder = order;
            return Task.CompletedTask;
        }
    }
    private NoDbOrderBookService orderBookService;
    Mock<IItemsApi> itemsApiMock;
    Mock<IMessageApi> messageApiMock;

    [SetUp]
    public void Setup()
    {
        var container = new Mock<ISessionContainer>();
        container.SetupGet(c => c.Session).Returns(null as ISession);
        messageApiMock = new Mock<IMessageApi>();
        itemsApiMock = new Mock<IItemsApi>();
        orderBookService = new NoDbOrderBookService(container.Object, messageApiMock.Object, itemsApiMock.Object);
    }

    [Test]
    public async Task TriggerMessageTest()
    {
        itemsApiMock.Setup(i => i.ItemNamesGetAsync(0, default)).ReturnsAsync(new List<Items.Client.Model.ItemPreview>() { new() { Tag = "test", Name = "test" } });
        var order = new OrderEntry()
        {
            Amount = 1,
            IsSell = true,
            ItemId = "test",
            PlayerName = "test",
            PricePerUnit = 10,
            Timestamp = DateTime.UtcNow,
            UserId = "1"
        };
        await orderBookService.AddOrder(order);
        Assert.That(orderBookService.LastOrder, Is.EqualTo(order));
        var undercutOrder = new OrderEntry()
        {
            Amount = 1,
            IsSell = true,
            ItemId = "test",
            PlayerName = "test",
            PricePerUnit = 9,
            Timestamp = DateTime.UtcNow,
            UserId = "1"
        };
        await orderBookService.AddOrder(undercutOrder);

        messageApiMock.Verify(m => m.MessageSendUserIdPostAsync("1", It.Is<MessageContainer>(m => m.Message == "Your sell-order for 1x test has been undercut for 9 per unit"), 0, default), Times.Once);
    }

    [Test]
    public async Task TriggerMessageFromPull()
    {
        itemsApiMock.Setup(i => i.ItemNamesGetAsync(0, default)).ReturnsAsync(new List<Items.Client.Model.ItemPreview>() { new() { Tag = "test", Name = "test" } });
        var order = new OrderEntry()
        {
            Amount = 1,
            IsSell = true,
            ItemId = "test",
            PlayerName = "test",
            PricePerUnit = 10,
            Timestamp = DateTime.UtcNow,
            UserId = "1"
        };
        await orderBookService.AddOrder(order);
        await orderBookService.BazaarPull(new dev.BazaarPull()
        {
            Timestamp = DateTime.UtcNow,
            Products = new(){
                new (){
                    ProductId = order.ItemId,
                    SellSummary = new (){},
                    BuySummery = new (){
                        new (){
                            Amount = 1,
                            PricePerUnit = 9
                        }
                    }
                }
            }
        });

        messageApiMock.Verify(m => m.MessageSendUserIdPostAsync("1", It.Is<MessageContainer>(m => m.Message == "Your sell-order for 1x test has been undercut for 9 per unit"), 0, default), Times.Once);
    }

    [Test]
    public async Task OrderRemovedWhenFilled()
    {
        // removed when not present in bazaar pull (only higher price)
        var buyOrder = new OrderEntry()
        {
            Amount = 1,
            IsSell = false,
            ItemId = "test",
            PlayerName = "test",
            PricePerUnit = 10,
            Timestamp = DateTime.UtcNow,
            UserId = "1"
        };
        var sellOrder = new OrderEntry()
        {
            Amount = 1,
            IsSell = true,
            ItemId = "test",
            PlayerName = "test",
            PricePerUnit = 11,
            Timestamp = DateTime.UtcNow,
            UserId = "1"
        };
        await orderBookService.AddOrder(buyOrder);
        await orderBookService.AddOrder(sellOrder);
        var orderbook = await orderBookService.GetOrderBook(buyOrder.ItemId);
        Assert.That(orderbook.Buy.Count, Is.EqualTo(1));
        Assert.That(orderbook.Sell.Count, Is.EqualTo(1));
        await orderBookService.BazaarPull(new dev.BazaarPull()
        {
            Timestamp = DateTime.UtcNow,
            Products = new(){
                new (){
                    ProductId = buyOrder.ItemId,
                    SellSummary = new (){
                        new (){
                            Amount = 1,
                            PricePerUnit = 1
                        }
                    },
                    BuySummery = new (){
                        new (){
                            Amount = 1,
                            PricePerUnit = 100
                        }
                    }
                }
            }
        });
        // orders should be removed - new ones present
        Assert.That(orderbook.Buy.Count, Is.EqualTo(1));
        Assert.That(orderbook.Buy.First().PricePerUnit, Is.EqualTo(1));
        Assert.That(orderbook.Sell.Count, Is.EqualTo(1));
        Assert.That(orderbook.Sell.First().PricePerUnit, Is.EqualTo(100));
    }
}
