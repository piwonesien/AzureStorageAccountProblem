using Azure;
using Azure.Data.Tables;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace AzureTableStorageProblem
{
    public class PriceEntity : ITableEntity
    {
        public string MdmId { get; set; }
        public DateTime DateFrom { get; set; }
        public DateTime DateTo { get; set; }
        public double Price { get; set; }
        public PriceType PriceType { get; set; }
        public string Type { get; set; }
        public string Runpath { get; set; }
        public string CustomerId { get; set; }
        public string PriceSource { get; set; }
        public string Status { get; set; }
        //public DateTime Freigegeben_am { get; set; } = new DateTime(1900, 1, 1);
        public string Kommentar_Ablehnung { get; set; }
        public string SupplierItemNumber { get; set; }
        public string PartitionKey { get; set; }
        public string RowKey { get; set; }
        public DateTimeOffset? Timestamp { get; set; }
        public ETag ETag { get; set; }
        

        public static PriceEntity CreateRandomPrice (string PartitionKey, string index)
        {
            var random = new Random();
            return new PriceEntity
            {
                PartitionKey = PartitionKey,
                RowKey = RandomString(15) + index,
                DateFrom = DateTime.UtcNow,
                DateTo = DateTime.UtcNow,
                Price = random.NextDouble(),
                PriceType = PriceType.BasicPriceLevel0,
                Runpath = RandomString(30),
                CustomerId = RandomString(15),
            };
        }

        public static string RandomString(int length)
        {
            var random = new Random();
            const string chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
            return new string(Enumerable.Repeat(chars, length)
              .Select(s => s[random.Next(s.Length)]).ToArray());
        }
    }
}
