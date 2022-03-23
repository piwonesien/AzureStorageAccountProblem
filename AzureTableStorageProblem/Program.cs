using Azure.Data.Tables;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
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

            // Setup storage account
            Console.WriteLine("Create connection and table");
            var conn = new TableClient(blobconnection, "futurePrices");
            conn.CreateIfNotExists();

            // Insert data
            Console.WriteLine("Insert testdata to table");
            var insertPrices = JsonConvert.DeserializeObject<List<PriceEntity>>(File.ReadAllText(path2data));
            var insertBatches = PriceDbHandler.GenerateBatches(insertPrices);
            Parallel.ForEach(insertBatches, new ParallelOptions { MaxDegreeOfParallelism = 2000 }, (insertBatch) => // avoid iops limit -> max 2000 threads, since a batch could have up to 10 entries
            {
                var batch = insertBatch.Value.Select(x => new TableTransactionAction(TableTransactionActionType.UpsertReplace, x));
                conn.SubmitTransaction(batch);
            });
            #endregion

            #region Our problem
            Console.WriteLine("Our problem starts");
            // Load prices from db
            var toDelete = conn.Query<PriceEntity>().
                Where(item => item.Status == PriceStatus.approved.ToString() && item.DateFrom <= DateTime.UtcNow && DateTime.UtcNow <= item.DateTo && item.Type != PriceType.Customer.ToString());

            // Generate our batches which should be deleted
            var deleteBatches = PriceDbHandler.GenerateBatches(toDelete.Distinct());

            Parallel.ForEach(deleteBatches, new ParallelOptions { MaxDegreeOfParallelism = 2000 }, futureBatch =>
            {
                var deleteOperation = new List<TableTransactionAction>();
                foreach (var entity in futureBatch.Value)
                {
                    try
                    {
                        // Re-query the entity to make sure, that it is really in the database - if the table would work normally we don't need to fetch the entity again
                        var curEntity = conn.Query<PriceEntity>(x => x.PartitionKey == entity.PartitionKey && x.RowKey == entity.RowKey).FirstOrDefault();
                        if (curEntity != null)
                            deleteOperation.Add(new TableTransactionAction(TableTransactionActionType.Delete, curEntity));
                        else
                            Console.WriteLine("Entity not found: " + entity.PartitionKey + " || " + entity.RowKey);
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine("Problem with fetching entity: " + entity.PartitionKey + " || " + entity.RowKey);
                        Console.WriteLine(ex);
                    }
                }
                try
                {
                    if (deleteOperation.Count != 0)
                        conn.SubmitTransaction(deleteOperation);
                }
                catch (Exception ex)
                {
                    Console.WriteLine("Problem with deleting batch: " + JsonConvert.SerializeObject(futureBatch));
                    Console.WriteLine(ex);
                }
            });
            #endregion


        }
    }
}
