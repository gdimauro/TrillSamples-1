// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Threading.Tasks;
using Microsoft.Azure.EventHubs;
using Microsoft.Azure.EventHubs.Processor;

namespace EventHubReceiver
{
  public sealed class Program
  {
    private const string EventHubConnectionString = "Endpoint=sb://ehtest-dg.servicebus.windows.net/;SharedAccessKeyName=AUTHOR;SharedAccessKey=OhJyrMYske/bTu223Vf7vwYKWjQkrvb7UGPQVxR7YLk=;EntityPath=main";
    private const string EventHubName = "main";
    private const string StorageContainerName = "object-storage";
    private const string StorageAccountName = "activitystorage";
    private const string StorageAccountKey = "vLGTGdWszgmwuf6l58LuYkQfVEwNecJuag/KF/H96aQK3p5NkM6EJybKtvI4wpa5F8/mnSQCw52OieXlSPuapQ==";

    internal static readonly string StorageConnectionString = $"DefaultEndpointsProtocol=https;AccountName={StorageAccountName};AccountKey={StorageAccountKey}";

    public static void Main(string[] args)
    {
      MainAsync(args).GetAwaiter().GetResult();
    }

    private static async Task MainAsync(string[] args)
    {
      Console.WriteLine("Registering EventProcessor...");

      var eventProcessorHost = new EventProcessorHost(
          EventHubName,
          PartitionReceiver.DefaultConsumerGroupName,
          EventHubConnectionString,
          StorageConnectionString,
          StorageContainerName);

      // Registers the Event Processor Host and starts receiving messages
      await eventProcessorHost.RegisterEventProcessorAsync<EventProcessor>();

      Console.WriteLine("Receiving. Press enter key to stop worker.");
      Console.ReadLine();

      // Disposes of the Event Processor Host
      await eventProcessorHost.UnregisterEventProcessorAsync();
    }
  }
}
