using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using System.Web.Http;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using Microsoft.Azure.Documents.Linq;
using Newtonsoft.Json.Linq;

namespace ArrayContainsError.Controllers
{
    public class ValuesController : ApiController
    {
        // GET api/values
        public async Task<IEnumerable<string>> Get()
        {
            var results = new List<string>();
            results.Add(Environment.Is64BitProcess ? "64-bit" : "32-bit");
            try
            {

                var url = Environment.GetEnvironmentVariable("DOCUMENT_DB_URL");
                var key = Environment.GetEnvironmentVariable("DOCUMENT_DB_KEY");
                var databaseId = Environment.GetEnvironmentVariable("DOCUMENT_DB_DATABASE_ID");
                var collectionId = Environment.GetEnvironmentVariable("DOCUMENT_DB_TENANT_ID");

                var parsedUrl = new Uri(url);
                var client = new DocumentClient(parsedUrl, key, new ConnectionPolicy
                {
                    ConnectionMode = ConnectionMode.Direct,
                    ConnectionProtocol = Protocol.Tcp
                });
                var database = client.CreateDatabaseQuery()
                    .Where(d => d.Id == databaseId)
                    .AsEnumerable()
                    .First();

                var partitionResolver = new DoNothingPartitionResolver(database, client, collectionId);
                client.PartitionResolvers[database.SelfLink] = partitionResolver;

                var queryable = client.CreateDocumentQuery<JObject>(
                    database.SelfLink,
                    new SqlQuerySpec(
                        @"SELECT VALUE {id:r.id,tenantId:r.tenantId,userId:r.userId,name:r.name,type:r.type,subType:r.subType,simVersion:r.simVersion,creationDate:r.creationDate,modifiedDate:r.modifiedDate,properties:r.properties,data:IS_OBJECT(r.data)?{errorMessages:r.data.errorMessages,jobCount:r.data.jobCount,dispatchedJobCount:r.data.dispatchedJobCount,completedJobCount:r.data.completedJobCount,succeededJobCount:r.data.succeededJobCount,succeededComputeCredits:(IS_DEFINED(r.data.succeededComputeCredits) ? r.data.succeededComputeCredits : (IS_DEFINED(r.data.succeededSimulationCount) ? r.data.succeededSimulationCount : (r.data.jobCount > 1 AND r.data.succeededJobCount > 0 AND r.data.completedJobCount = r.data.jobCount ? r.data.succeededJobCount - 1 : r.data.succeededJobCount))),succeededStorageCredits:(IS_DEFINED(r.data.succeededStorageCredits) ? r.data.succeededStorageCredits : (IS_DEFINED(r.data.succeededSimulationCount) ? r.data.succeededSimulationCount : (r.data.jobCount > 1 AND r.data.succeededJobCount > 0 AND r.data.completedJobCount = r.data.jobCount ? r.data.succeededJobCount - 1 : r.data.succeededJobCount))),seed:r.data.seed,isTransient:r.data.isTransient,executionTimeSeconds:r.data.executionTimeSeconds,studyType:r.data.studyType,studyState:r.data.studyState,sources:r.data.sources}:undefined,supportSession:IS_OBJECT(r.supportSession)?{isOpen:r.supportSession.isOpen,modifiedDate:r.supportSession.modifiedDate,modifiedTenantId:r.supportSession.modifiedTenantId,modifiedUserId:r.supportSession.modifiedUserId}:undefined}FROM root r WHERE r.tenantId=@tenantId AND r.type='study'AND r.subType='definition'AND NOT(r.data.isTransient ?? false) AND (ARRAY_CONTAINS(r.data.sources, { name: @p3 }, true)) ORDER BY r['creationDate'] DESC",
                        new SqlParameterCollection
                        {
                            new SqlParameter("@tenantId", "a4ed02d5506c4237a58d520e151f74be"),
                            new SqlParameter("@type", "study"),
                            new SqlParameter("@subType", "definition"),
                            new SqlParameter("@p3", "Test"),
                        }),
                    new FeedOptions {MaxItemCount = 3});

                var query = queryable.AsDocumentQuery();

                while (query.HasMoreResults)
                {
                    var next = await query.ExecuteNextAsync<JObject>();

                    foreach (var item in next)
                    {
                        results.Add(item.Value<string>("name"));
                    }
                }
            }
            catch (Exception t)
            {
                results.Add(t.ToString());
            }

            return results;
        }
    }

    public class DoNothingPartitionResolver : IPartitionResolver
    {

        private readonly IReadOnlyList<string> collectionLinks;

        public DoNothingPartitionResolver(Database database, DocumentClient client, string collectionId)
        {
            var collection = client.CreateDocumentCollectionQuery(database.SelfLink)
                .Where(c => c.Id == collectionId)
                .AsEnumerable()
                .First();
            this.collectionLinks = new List<string> { collection.SelfLink }.AsReadOnly();
        }

        public object GetPartitionKey(object document)
        {
            return null;
        }

        public string ResolveForCreate(object partitionKey)
        {
            return this.collectionLinks[0];
        }

        public IEnumerable<string> ResolveForRead(object partitionKey)
        {
            return this.collectionLinks;
        }
    }
}
