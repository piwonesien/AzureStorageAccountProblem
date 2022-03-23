using Azure;
using Azure.Data.Tables;
using System;
using System.Collections.Generic;
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
        public DateTime Freigegeben_am { get; set; } = new DateTime(1900, 1, 1);
        public string Kommentar_Ablehnung { get; set; }
        public string SupplierItemNumber { get; set; }
        public string PartitionKey { get; set; }
        public string RowKey { get; set; }
        public DateTimeOffset? Timestamp { get; set; }
        public ETag ETag { get; set; }
    }
}
