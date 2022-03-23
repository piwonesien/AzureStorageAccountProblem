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

        /// <summary>
        /// This method sets the partition and rowkey for this element based on the target table.
        /// This method has to be called before insert entity in DB!
        /// </summary>
        /// <param name="table"></param>
        public void SetIdentifierByTable(PriceTable table)
        {
            if (PriceType == PriceType.None && !String.IsNullOrWhiteSpace(Type))
                PriceType = (PriceType)Enum.Parse(typeof(PriceType), Type, true);

            PartitionKey = MdmId;
            RowKey = PriceType.ToString();
            Type = PriceType.ToString();
            switch (table)
            {
                case PriceTable.Archive:
                case PriceTable.FuturePrice:
                case PriceTable.OfferPrice:
                    // Customer prices are also in Future/Offer/Archive table until they are not released
                    if (PriceType == PriceType.Customer)
                        RowKey += "_" + CustomerId;

                    RowKey += "_" + DateFrom.ToString("yyyy-MM-dd") + "_" + DateTo.ToString("yyyy-MM-dd");
                    break;
                case PriceTable.CurrentPrice:
                    // No additional information
                    break;
                case PriceTable.CustomerPrice:
                    RowKey += "_" + CustomerId;
                    break;
                default:
                case PriceTable.None:
                    throw new ArgumentException("This is not a valide table!");
            }
        }

        /// <summary>
        /// This method checks, if a price entity is valide
        /// </summary>
        /// <returns>
        /// Tuple(valide, errorlist):
        /// - valide: bool
        /// - errorList: List<string> containing error messages
        /// </returns>
        public (bool valide, List<string> errors) IsValide()
        {
            var errors = new List<string>();

            if (String.IsNullOrWhiteSpace(MdmId))
                errors.Add("MdmId is empty");
            if (String.IsNullOrWhiteSpace(Runpath))
                errors.Add("RunPath is empty");
            if (Price <= 0)
                errors.Add("Price is null or unparseable");
            if (PriceType == PriceType.None)
                errors.Add("Invalide PriceType");
            if (String.IsNullOrWhiteSpace(CustomerId) && PriceType == PriceType.Customer)
                errors.Add("CustomerId is empty for a customer price");
            if (DateFrom == DateTime.MinValue)
                errors.Add("DateFrom is invalide");
            if (DateTo == DateTime.MinValue)
                errors.Add("DateFrom is invalide");
            if (DateFrom > DateTo)
                errors.Add("DateFrom is greater than DateTo");
            if (String.IsNullOrWhiteSpace(RowKey) || String.IsNullOrWhiteSpace(PartitionKey))
                errors.Add("Identifiers are not set");

            return (errors.Count == 0, errors);
        }
    }
}
