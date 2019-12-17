using EventHubSample.Model;
using Kusto.Data;
using Kusto.Data.Common;
using Kusto.Ingest;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace EventHubSender
{
  public class DirectIngestion
  {
    private const string EventHubConnectionString = "Endpoint=sb://ehtest-dg.servicebus.windows.net/;SharedAccessKeyName=AUTHOR;SharedAccessKey=OhJyrMYske/bTu223Vf7vwYKWjQkrvb7UGPQVxR7YLk=;EntityPath=main";
    private const string EventHubName = "main";
    private const string StorageContainerName = "object-storage";
    private const string StorageAccountName = "activitystorage";
    private const string StorageAccountKey = "vLGTGdWszgmwuf6l58LuYkQfVEwNecJuag/KF/H96aQK3p5NkM6EJybKtvI4wpa5F8/mnSQCw52OieXlSPuapQ==";
    private static string StorageConnectionString = $"DefaultEndpointsProtocol=https;AccountName={StorageAccountName};AccountKey={StorageAccountKey}";

    public DirectIngestion()
    { }

    public static async Task Run(int numMessagesToSend = 100000, bool direct = true)
    {
      var tenantId = "1a59e398-83d8-4052-aec9-74a7d6461c5e";
      //var user = "gdimauro@codearchitects.com";
      //var password = "******";
      var clientid = "24c6b350-9b47-4e49-b794-7861cf0ce116";
      var clientsecret = "=BYPyhmHf0efu7[-zuoOLGtsT=xcO6v3";
      var database = "trillsample";

      var rand = new Random((int)DateTime.Now.Ticks);

      var m1 = new List<MeasureT1>();
      var m3 = new List<MeasureT3>();
      var m4 = new List<MeasureT4>();

      var j1 = new StringBuilder();
      var j3 = new StringBuilder();
      var j4 = new StringBuilder();

      if (direct)
      {
        var ingestUri = "Data Source=https://trillsample.westeurope.kusto.windows.net;Initial Catalog=trillsample";

        var ingestConnectionStringBuilder = new KustoConnectionStringBuilder(ingestUri)
        {
          FederatedSecurity = true,
          InitialCatalog = database,
#if false
          UserID = user,
          Password = password,
#else
          ApplicationClientId = clientid,
          ApplicationKey = clientsecret,
#endif
          Authority = tenantId
        };

        using (var ingestClient = KustoIngestFactory.CreateDirectIngestClient(ingestConnectionStringBuilder))
        {
          for (var i = 1; i < numMessagesToSend; i++)
          {
            var uri = (1002030 + i).ToString();
            var min = 200 + ((float)rand.Next(100, 500) / 10.0f);
            var now = DateTime.Now;
            var value = 23f + (float)rand.Next(10) / 10f;
            j1.Append(JsonConvert.SerializeObject(Program.CreateMeasureT1(rand, i, uri, min, now, value))).Append("\n");
            j3.Append(JsonConvert.SerializeObject(Program.CreateMeasureT3(i, uri, now, value))).Append("\n");
            j4.Append(JsonConvert.SerializeObject(Program.CreateMeasureT4(rand, uri, now))).Append("\n");

            if (i % 5000 == 0)
            {
              Console.Title = $"{i}/{numMessagesToSend}";
              m1 = new List<MeasureT1>();
              m3 = new List<MeasureT3>();
              m4 = new List<MeasureT4>();

              using (var s1 = new MemoryStream(Encoding.ASCII.GetBytes(j1.ToString())))
              using (var s3 = new MemoryStream(Encoding.ASCII.GetBytes(j1.ToString())))
              using (var s4 = new MemoryStream(Encoding.ASCII.GetBytes(j1.ToString())))
              {
                Console.Write("+");
                var props = new KustoQueuedIngestionProperties(database, "MeasuresT1") { Format = DataSourceFormat.json, JSONMappingReference = "MapMeasureT1" };
                var result = Task.Run(() => ingestClient.IngestFromStream(s1, props, leaveOpen: true));

                props = new KustoQueuedIngestionProperties(database, "MeasuresT3") { Format = DataSourceFormat.json, JSONMappingReference = "MapMeasureT3" };
                result = ingestClient.IngestFromStreamAsync(s3, props, leaveOpen: true);

                props = new KustoQueuedIngestionProperties(database, "MeasuresT4") { Format = DataSourceFormat.json, JSONMappingReference = "MapMeasureT4" };
                result = ingestClient.IngestFromStreamAsync(s4, props, leaveOpen: true);
                Console.Write("-");
              }
            }
          }
        }
      }
      else
      {
        var storageAccount = CloudStorageAccount.Parse(StorageConnectionString);
        var blobClient = storageAccount.CreateCloudBlobClient();
        var blobContainer = blobClient.GetContainerReference("uploads");
        blobContainer.CreateIfNotExistsAsync().Wait();
        var ingestUri = "https://ingest-trillsample.westeurope.kusto.windows.net/";

        var kustoConnectionStringBuilder = new KustoConnectionStringBuilder(ingestUri)
        {
          FederatedSecurity = true,
          InitialCatalog = database,
#if false
          UserID = user,
          Password = password,
#else
          ApplicationClientId = clientid,
          ApplicationKey = clientsecret,
#endif
          Authority = tenantId
        };

        for (var i = 1; i < numMessagesToSend; i++)
        {
          var uri = (1002030 + 10000000 + i).ToString();
          var min = 200 + ((float)rand.Next(100, 500) / 10.0f);
          var now = DateTime.Now;
          var value = 23f + (float)rand.Next(10) / 10f;
          j1.Append(JsonConvert.SerializeObject(Program.CreateMeasureT1(rand, i, uri, min, now, value))).Append("\n");
          j3.Append(JsonConvert.SerializeObject(Program.CreateMeasureT3(i, uri, now, value))).Append("\n");
          j4.Append(JsonConvert.SerializeObject(Program.CreateMeasureT4(rand, uri, now))).Append("\n");

          if (i % 5000 == 0)
          {
            Console.Title = $"{i}/{numMessagesToSend}";
            var blockBlob1 = blobContainer.GetBlockBlobReference($"MeasuresT1-{i}");
            using (var stream = new StreamWriter(await blockBlob1.OpenWriteAsync()))
              await stream.WriteAsync(j1);

            var blockBlob3 = blobContainer.GetBlockBlobReference($"MeasuresT3-{i}");
            using (var stream = new StreamWriter(await blockBlob3.OpenWriteAsync()))
              await stream.WriteAsync(j3);

            var blockBlob4 = blobContainer.GetBlockBlobReference($"MeasuresT4-{i}");
            using (var stream = new StreamWriter(await blockBlob4.OpenWriteAsync()))
              await stream.WriteAsync(j4);

            var adHocPolicy = new SharedAccessBlobPolicy()
            {
              SharedAccessExpiryTime = DateTime.UtcNow.AddHours(24),
              Permissions = SharedAccessBlobPermissions.Read | SharedAccessBlobPermissions.Delete
            };

            Console.WriteLine("+");
            using (var ingestClient = KustoIngestFactory.CreateQueuedIngestClient(kustoConnectionStringBuilder))
            {
              var props = new KustoQueuedIngestionProperties(database, "MeasuresT1") { Format = DataSourceFormat.json, JSONMappingReference = "MapMeasureT1" };
              await ingestClient.IngestFromStorageAsync($"{blockBlob1.Uri.AbsoluteUri}{blockBlob1.GetSharedAccessSignature(adHocPolicy).ToString()}", props, new StorageSourceOptions { DeleteSourceOnSuccess = true });

              props = new KustoQueuedIngestionProperties(database, "MeasuresT3") { Format = DataSourceFormat.json, JSONMappingReference = "MapMeasureT3" };
              await ingestClient.IngestFromStorageAsync($"{blockBlob3.Uri.AbsoluteUri}{blockBlob3.GetSharedAccessSignature(adHocPolicy).ToString()}", props, new StorageSourceOptions { DeleteSourceOnSuccess = true });

              props = new KustoQueuedIngestionProperties(database, "MeasuresT4") { Format = DataSourceFormat.json, JSONMappingReference = "MapMeasureT4" };
              await ingestClient.IngestFromStorageAsync($"{blockBlob4.Uri.AbsoluteUri}{blockBlob4.GetSharedAccessSignature(adHocPolicy).ToString()}", props, new StorageSourceOptions { DeleteSourceOnSuccess = true });
            }
            Console.WriteLine("-");
          }
        }
      }
    }
  }
}
