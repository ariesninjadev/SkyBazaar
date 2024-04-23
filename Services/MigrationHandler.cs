extern alias CoflCore;

using System;
using System.Collections;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Cassandra;
using Cassandra.Data.Linq;
using Cassandra.Mapping;
using Microsoft.Extensions.Logging;
using Prometheus;
using StackExchange.Redis;

namespace Coflnet.Sky.SkyAuctionTracker.Services;
#nullable enable
public class MigrationHandler<T>
{
    Func<Table<T>> oldTableFactory;
    Func<Table<T>> newTableFactory;
    ISession session;
    ILogger<MigrationHandler<T>> logger;
    private readonly ConnectionMultiplexer redis;
    Counter migrated;
    private int pageSize = 2000;

    public MigrationHandler(Func<Table<T>> oldTableFactory, ISession session, ILogger<MigrationHandler<T>> logger, ConnectionMultiplexer redis, Func<Table<T>> newTableFactory)
    {
        this.oldTableFactory = oldTableFactory;
        this.session = session;
        this.logger = logger;
        this.redis = redis;
        this.newTableFactory = newTableFactory;
    }

    public async Task Migrate(CancellationToken stoppingToken = default)
    {
        newTableFactory().CreateIfNotExists();
        var tableName = newTableFactory().Name;
        var prefix = $"cassandra_migration_{tableName}_";
        migrated = Metrics.CreateCounter($"{prefix}migrated", "The number of items migrated");
        var db = redis.GetDatabase();
        var pagingSateRedis = db.StringGet($"{prefix}paging_state");
        byte[]? pagingState;
        var offset = 0;
        IPage<T> page;
        if (!pagingSateRedis.IsNullOrEmpty)
        {
            pagingState = Convert.FromBase64String(pagingSateRedis!);
            page = await GetOldTable(pagingState);
        }
        else
        {
            page = await GetOldTable([]);
        }
        var fromRedis = db.StringGet($"{prefix}offset");
        if (!fromRedis.IsNullOrEmpty)
        {
            offset = int.Parse(fromRedis);
            logger.LogInformation("Resuming migration of {table} from {0}", tableName, offset);
        }
        var semaphore = new SemaphoreSlim(10);
        do
        {
            _ = Task.Run(async () =>
            {
                try
                {
                    await semaphore.WaitAsync();
                    var insertCount = await InsertBatch(prefix, db, offset, page);
                    Interlocked.Add(ref offset, insertCount);
                }
                catch (System.Exception e)
                {
                    logger.LogError(e, "Batch insert failed, ");
                    await Task.Delay(2000);
                    var insertCount = await InsertBatch(prefix, db, offset, page);
                    Interlocked.Add(ref offset, insertCount);
                    await Task.Delay(1000000);
                    throw;
                }
                finally
                {
                    semaphore.Release();
                }
            });
            pagingState = page.PagingState;
            logger.LogInformation("Migrated batch {0} of {table}", offset, tableName);
            await semaphore.WaitAsync();
            page = await GetOldTable(pagingState);
            semaphore.Release();
        } while (page != null && !stoppingToken.IsCancellationRequested);

        logger.LogInformation("Migration for {tableName} done", tableName);
    }

    private async Task<int> InsertBatch(string prefix, IDatabase db, int offset, IPage<T> page)
    {
        var batchToInsert = page;
        await Parallel.ForEachAsync(Batch(batchToInsert, 300), async (batch, c) =>
        {
            await InsertChunk(batch);
        });
        migrated.Inc(batchToInsert.Count);
        offset += batchToInsert.Count;
        db.StringSet($"{prefix}offset", offset);
        var queryState = page.PagingState;
        if (queryState != null)
        {
            db.StringSet($"{prefix}paging_state", Convert.ToBase64String(queryState));
        }

        return batchToInsert.Count;
    }

    private IEnumerable<IEnumerable<T>> Batch(IEnumerable<T> values, int batchSize)
    {
        var list = new List<T>(batchSize);
        foreach (var value in values)
        {
            list.Add(value);
            if (list.Count == batchSize)
            {
                yield return list;
                list = new List<T>(batchSize);
            }
        }

        if (list.Count > 0)
        {
            yield return list;
        }
    }

    private async Task InsertChunk(IEnumerable<T> batchToInsert)
    {
        var newTable = newTableFactory();
        var batchStatement = new BatchStatement();
        foreach (var score in batchToInsert)
        {
            batchStatement.Add(newTable.Insert(score));
        }
        await session.ExecuteAsync(batchStatement);
    }

    private async Task<IPage<T>> GetOldTable(byte[]? pagingState = null)
    {
        var query = oldTableFactory();
        query.SetPageSize(pageSize);
        query.SetAutoPage(false);
        if (pagingState == null)
            return null;
        if (pagingState.Length != 0)
            query.SetPagingState(pagingState);
        return await query.ExecutePagedAsync();
    }
}
