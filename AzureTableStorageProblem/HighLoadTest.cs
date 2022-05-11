using Azure.Data.Tables;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AzureTableStorageProblem
{
    public static class HighLoadTest
    {
        public static void Run(string connectionstring)
        {
            Console.WriteLine(DateTime.Now + " Do highperformance import");
            var conn = new TableClient(connectionstring, "highperformance");
            conn.CreateIfNotExists();

            // Inserting 1 million datasets
            var stopwatch = new Stopwatch();
            stopwatch.Start();
            Parallel.For(0, 100000000, new ParallelOptions { MaxDegreeOfParallelism = 2000 }, (i) =>
            {
                if (i % 10000 == 0)
                    Console.WriteLine(DateTime.Now + " Current status: " + i);

                var partitionkey = PriceEntity.RandomString(10);
                var entities = new List<PriceEntity>();
                for (var j = 0; j < 10; j++)
                    entities.Add(PriceEntity.CreateRandomPrice(partitionkey, j.ToString()));

                var batch = entities.Select(x => new TableTransactionAction(TableTransactionActionType.UpsertMerge, x));
                try
                {
                    conn.SubmitTransaction(batch);
                }
                catch (Exception e)
                {
                    Console.WriteLine("Problem with batch");
                    Console.WriteLine(e);
                }
            });
            stopwatch.Stop();
            Console.WriteLine(DateTime.Now + " Succeeded to import 1 000 000 000 entitites to db:");
            Console.WriteLine(DateTime.Now + " Took: " + stopwatch.ElapsedMilliseconds + "ms (" + stopwatch.Elapsed.ToString("mm\\:ss\\.ff") + ") to load all entities\r\n");

        }
    }
}
