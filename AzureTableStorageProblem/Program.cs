using Azure.Core.Diagnostics;
using Azure.Data.Tables;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.Tracing;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace AzureTableStorageProblem
{
    class Program
    {
        static void Main(string[] args)
        {
            #region Setup
            var blobconnection = "Connectionstring to storage account";
            var path2data = @"Path to dataset.json";

            // Logging
            TableClientOptions options = new TableClientOptions()
            {
                Diagnostics =
                {
                    IsLoggingContentEnabled = true
                },
                Retry =
                {
                    MaxRetries = 0
                }
            };
            using Stream stream = new FileStream(
                "consolelog.log",
                FileMode.OpenOrCreate,
                FileAccess.Write,
                FileShare.Read);

            using StreamWriter streamWriter = new StreamWriter(stream)
            {
                AutoFlush = true
            };
            var logLock = new Object();
            using AzureEventSourceListener listener = new AzureEventSourceListener((args, message) =>
            {
                lock (logLock)
                {
                    streamWriter.Write(message);
                }
            }, EventLevel.LogAlways);

            //// Do HighLoadTest
            //HighLoadTest.Run(blobconnection);
            //return;


            // Setup storage account
            Console.WriteLine("Create connection and table");
            var conn = new TableClient(blobconnection, "futurePrices", options);
            conn.CreateIfNotExists();

            // Via Dataset
            var insertPrices = JsonConvert.DeserializeObject<List<PriceEntity>>(File.ReadAllText(path2data));

            // Simplied problem:
            Console.WriteLine("Insert testdata to table");
            // 1. Insert data
            BatchOperation(conn, insertPrices, TableTransactionActionType.UpsertReplace);

            // 2. Query data
            Console.WriteLine("Query data from table");
            var stopwatch = new Stopwatch();
            stopwatch.Start();
            var entries = conn.Query<PriceEntity>(item => item.Status == PriceStatus.approved.ToString() && item.DateFrom <= DateTime.UtcNow && DateTime.UtcNow <= item.DateTo && item.Type != PriceType.Customer.ToString()).ToList();
            stopwatch.Stop();
            Console.WriteLine("Succeeded to load  " + entries.Count + " entitites from db:");
            Console.WriteLine("Took: " + stopwatch.ElapsedMilliseconds + "ms (" + stopwatch.Elapsed.ToString("mm\\:ss\\.ff") + ") to load all entities\r\n");


            // 3. Delete data
            Console.WriteLine("Delete data from table");
            BatchOperation(conn, entries, TableTransactionActionType.Delete);



            // Insert data
            //Console.WriteLine("Insert testdata to table");
            //var insertPrices = JsonConvert.DeserializeObject<List<PriceEntity>>(File.ReadAllText(path2data));
            //var insertBatches = PriceDbHandler.GenerateBatches(insertPrices);
            //Parallel.ForEach(insertBatches, new ParallelOptions { MaxDegreeOfParallelism = 2000 }, (insertBatch) => // avoid iops limit -> max 2000 threads, since a batch could have up to 10 entries
            //{
            //    var batch = insertBatch.Value.Select(x => new TableTransactionAction(TableTransactionActionType.UpsertReplace, x));
            //    conn.SubmitTransaction(batch);
            //});
            #endregion

            #region Our problem
            //Console.WriteLine("Our problem starts");
            //// Load prices from db
            //var toDelete = conn.Query<PriceEntity>(item => item.Status == PriceStatus.approved.ToString() && item.DateFrom <= DateTime.UtcNow && DateTime.UtcNow <= item.DateTo && item.Type != PriceType.Customer.ToString());

            //// Generate our batches which should be deleted
            //var deleteBatches = PriceDbHandler.GenerateBatches(toDelete.Distinct());

            //Parallel.ForEach(deleteBatches, new ParallelOptions { MaxDegreeOfParallelism = 2000 }, futureBatch =>
            //{
            //    var deleteOperation = new List<TableTransactionAction>();
            //    foreach (var entity in futureBatch.Value)
            //    {
            //        try
            //        {
            //            deleteOperation.Add(new TableTransactionAction(TableTransactionActionType.Delete, entity));
            //        }
            //        catch (Exception ex)
            //        {
            //            Console.WriteLine("Problem with fetching entity: " + entity.PartitionKey + " || " + entity.RowKey);
            //            Console.WriteLine(ex);
            //        }
            //    }
            //    try
            //    {
            //        if (deleteOperation.Count != 0)
            //            conn.SubmitTransaction(deleteOperation);
            //    }
            //    catch (Exception ex)
            //    {
            //        Console.WriteLine("Problem with deleting batch: " + JsonConvert.SerializeObject(futureBatch));
            //        Console.WriteLine(ex);
            //    }
            //});
            #endregion
        }

        static void BatchOperation(TableClient conn, List<PriceEntity> entities, TableTransactionActionType action)
        {
            Console.WriteLine("Start to process batches");
            var stopwatch = new Stopwatch();

            var batches = entities.GroupBy(x => x.PartitionKey).ToList();
            stopwatch.Start();
            Parallel.ForEach(batches, new ParallelOptions { MaxDegreeOfParallelism = 2000 }, (batchEntries) =>
            {
                var batch = batchEntries.Select(x => new TableTransactionAction(action, x));
                try
                {
                    conn.SubmitTransaction(batch);
                }
                catch (Exception e)
                {
                    Console.WriteLine("Problem with batch: " + action.ToString());
                    Console.WriteLine(e);
                }
            });

            stopwatch.Stop();

            Console.WriteLine("Succeeded to " + action.ToString() + " batches:");
            Console.WriteLine(entities.Count + " entities in " + batches.Count + " batches");
            Console.WriteLine("Took: " + stopwatch.ElapsedMilliseconds + "ms (" + stopwatch.Elapsed.ToString("mm\\:ss\\.ff") + ")  to process all batches");
            Console.WriteLine("That are ~" + stopwatch.ElapsedMilliseconds / batches.Count + "ms per batch operation\r\n");
        }
    }
}
