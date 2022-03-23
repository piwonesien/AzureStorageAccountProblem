using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.Linq;
using System.Net;
using Azure.Data.Tables;

namespace AzureTableStorageProblem
{
    public enum PriceTable
    {
        None,
        CurrentPrice,
        FuturePrice,
        OfferPrice,
        CustomerPrice,
        Archive,
        PriceApprovalStatus
    }

    public enum PriceType
    {
        None,
        BasicPriceLevel0,
        BasicPriceLevel1,
        BasicPriceLevel2,
        BasicPriceLevel3,
        ConditionLevel1,
        ConditionLevel2,
        ConditionLevel3,
        ConditionLevel4,
        ConditionLevel5,
        ConditionLevel6,
        ConditionLevel7,
        ConditionLevel8,
        ConditionLevel9,
        ConditionLevel10,
        VgV,
        Customer
    }

    public enum PriceStatus
    {
        approved,
        declined,
        undefined
    }

    public static class PriceDbHandler
    {
        public static TableClient GetTableConnection(string ConnectionString, PriceTable table)
        {
            string tableName;

            switch (table)
            {
                case PriceTable.Archive:
                    tableName = "Archive";
                    break;
                case PriceTable.CurrentPrice:
                    tableName = "CurrentPrices";
                    break;
                case PriceTable.CustomerPrice:
                    tableName = "CustomerPrices";
                    break;
                case PriceTable.FuturePrice:
                    tableName = "FuturePrices";
                    break;
                case PriceTable.OfferPrice:
                    tableName = "OfferPrices";
                    break;
                case PriceTable.PriceApprovalStatus:
                    tableName = "PriceApprovalStatus";
                    break;
                case PriceTable.None:
                default:
                    throw new ArgumentException("This is not a valide table");
            }

            return new TableClient(ConnectionString, tableName);
        }

        public static Dictionary<PriceTable, TableClient> GetConnections()
        {
            var DbConnectionString = Environment.GetEnvironmentVariable("BlobConnection");
            return PriceDbHandler.GetAllTableConnections(DbConnectionString);
        }

        /// <summary>
        /// This method returns all table connections in a dict
        /// </summary>
        /// <param name="ConnectionString"></param>
        /// <returns></returns>
        public static Dictionary<PriceTable, TableClient> GetAllTableConnections(string ConnectionString)
        {
            return new Dictionary<PriceTable, TableClient>
            {
                { PriceTable.Archive, GetTableConnection(ConnectionString, PriceTable.Archive) },
                { PriceTable.CurrentPrice, GetTableConnection(ConnectionString, PriceTable.CurrentPrice) },
                { PriceTable.CustomerPrice, GetTableConnection(ConnectionString, PriceTable.CustomerPrice) },
                { PriceTable.FuturePrice, GetTableConnection(ConnectionString, PriceTable.FuturePrice) },
                { PriceTable.OfferPrice, GetTableConnection(ConnectionString, PriceTable.OfferPrice) },
                { PriceTable.PriceApprovalStatus, GetTableConnection(ConnectionString, PriceTable.PriceApprovalStatus) },

            };
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="entities"></param>
        /// <returns></returns>
        public static Dictionary<string, List<PriceEntity>> GenerateBatches(IEnumerable<PriceEntity> entities, string runpath = null)
        {
            var batches = new Dictionary<string, List<PriceEntity>>();

            foreach (var entity in entities ?? Enumerable.Empty<PriceEntity>())
            {
                // Update runpath if required
                if (!String.IsNullOrWhiteSpace(runpath))
                    entity.Runpath = runpath;

                if (batches.ContainsKey(entity.MdmId))
                    batches[entity.MdmId].Add(entity);
                else
                    batches.Add(entity.MdmId, new List<PriceEntity> { entity });
            };

            return batches;
        }
    }
}
