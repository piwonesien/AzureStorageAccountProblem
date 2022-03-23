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
