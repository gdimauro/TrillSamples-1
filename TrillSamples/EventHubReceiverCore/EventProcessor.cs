// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Reactive.Linq;
using System.Reactive.Subjects;
using System.Runtime.Serialization;
using System.Threading.Tasks;
using Microsoft.Azure.EventHubs;
using Microsoft.Azure.EventHubs.Processor;
using Microsoft.StreamProcessing;
#if USE_BLOB_STORAGE
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
#endif
using Newtonsoft.Json;
using static BinarySerializer;

namespace EventHubReceiver
{
  /// <summary>
  /// Event processor for Trill query with state
  /// </summary>
  public sealed class EventProcessor : IEventProcessor
  {
    private static readonly TimeSpan CheckpointInterval = TimeSpan.FromSeconds(10);
    private static readonly string StorageConnectionString = Program.StorageConnectionString;

    private Stopwatch checkpointStopWatch;
#if USE_BLOB_STORAGE
    private CloudBlobContainer checkpointContainer;
#endif
    private Subject<StreamEvent<SampleEvent>> input;
    private QueryContainer queryContainer;
    private Microsoft.StreamProcessing.Process queryProcess;
    private int ReceivedMessageCount = 0;

    /// <summary>
    /// Close processor for partition
    /// </summary>
    /// <param name="context"></param>
    /// <param name="reason"></param>
    /// <returns></returns>
    public Task CloseAsync(PartitionContext context, CloseReason reason)
    {
      Console.WriteLine($"Processor Shutting Down. Partition '{context.PartitionId}', Reason: '{reason}'.");
      return Task.CompletedTask;
    }

    /// <summary>
    /// Open processor for partition
    /// </summary>
    /// <param name="context"></param>
    /// <returns></returns>
    public Task OpenAsync(PartitionContext context)
    {
      Config.ForceRowBasedExecution = true;

      this.checkpointStopWatch = new Stopwatch();
      this.checkpointStopWatch.Start();

#if USE_BLOB_STORAGE
      var storageAccount = CloudStorageAccount.Parse(StorageConnectionString);
      var blobClient = storageAccount.CreateCloudBlobClient();
      this.checkpointContainer = blobClient.GetContainerReference("checkpoints");
      this.checkpointContainer.CreateIfNotExistsAsync().Wait();

      var blockBlob = this.checkpointContainer.GetBlockBlobReference($"{context.Lease.PartitionId}-{context.Lease.SequenceNumber}");
      if (blockBlob.ExistsAsync().Result)
      {
        Console.WriteLine($"Restoring query from EH checkpoint {context.Lease.SequenceNumber}");
        var stream = blockBlob.OpenReadAsync().GetAwaiter().GetResult();
        CreateQuery();
        try
        {
          this.queryProcess = this.queryContainer.Restore(stream);
        }
        catch
        {
          Console.WriteLine($"Unable to restore from checkpoint, starting clean");
          CreateQuery();
          this.queryProcess = this.queryContainer.Restore();
        }
      }
      else
#endif
      {
        Console.WriteLine($"Clean start of query");
        CreateQuery();
        this.queryProcess = this.queryContainer.Restore();
      }

      Console.WriteLine($"SimpleEventProcessor initialized. Partition: '{context.PartitionId}', " +
          $"Lease SeqNo: '{context.Lease.SequenceNumber}'");
      return Task.CompletedTask;
    }

    /// <summary>
    /// Process errors
    /// </summary>
    /// <param name="context"></param>
    /// <param name="error"></param>
    /// <returns></returns>
    public Task ProcessErrorAsync(PartitionContext context, Exception error)
    {
      Console.WriteLine($"Error on Partition: {context.PartitionId}, Error: {error.Message}");
      return Task.CompletedTask;
    }

    /// <summary>
    /// Process events
    /// </summary>
    /// <param name="context"></param>
    /// <param name="messages"></param>
    /// <returns></returns>
    public Task ProcessEventsAsync(PartitionContext context, IEnumerable<EventData> messages)
    {
      long lastSeq = 0;
      Console.Title = (ReceivedMessageCount += messages.Count()).ToString();
      Console.Write(".");
      foreach (var eventData in messages)
      {
        var message = BinarySerializer.DeserializeStreamEventSampleEvent(eventData.Body.ToArray());
        lastSeq = eventData.SystemProperties.SequenceNumber;
        //Task.Delay(5).Wait();
        //this.input.OnNext(message);
      }

      if (this.checkpointStopWatch.Elapsed > TimeSpan.FromSeconds(10))
      {
        Console.WriteLine("Taking checkpoint");
#if USE_BLOB_STORAGE
        var storageAccount = CloudStorageAccount.Parse(StorageConnectionString);
        var blobClient = storageAccount.CreateCloudBlobClient();
        CloudBlobContainer container = blobClient.GetContainerReference("checkpoints");
        var blockBlob = container.GetBlockBlobReference(context.PartitionId + "-" + lastSeq);
        CloudBlobStream blobStream = blockBlob.OpenWriteAsync().GetAwaiter().GetResult();
        Console.WriteLine($"writing blob: ${((CloudBlockBlob)blockBlob).Name}");
        this.queryProcess.Checkpoint(blobStream);
        blobStream.Flush();
        blobStream.Close();
#endif
        return context
            .CheckpointAsync()
            .ContinueWith(t => DeleteOlderCheckpoints(context.PartitionId + "-" + lastSeq));
      }

      return Task.CompletedTask;
    }

    /// <summary>
    /// Create query and register subscriber
    /// </summary>
    private void CreateQuery()
    {
      this.queryContainer = new QueryContainer();
      this.input = new Subject<StreamEvent<SampleEvent>>();
      var inputStream = this.queryContainer.RegisterInput(
          this.input,
          DisorderPolicy.Drop(),
          FlushPolicy.FlushOnPunctuation,
          PeriodicPunctuationPolicy.Time(1));
      var query = inputStream.AlterEventDuration(StreamEvent.InfinitySyncTime).Count();
      var async = this.queryContainer.RegisterOutput(query);
      async
        // .Where(e => e.IsStart)
        .ForEachAsync(o => Console.WriteLine($"{o}"));
    }

    /// <summary>
    /// Delete checkpoints other than specified last checkpoint file
    /// </summary>
    /// <param name="checkpointFile"></param>
    /// <returns></returns>
    private Task DeleteOlderCheckpoints(string checkpointFile)
    {
#if USE_BLOB_STORAGE
      var storageAccount = CloudStorageAccount.Parse(StorageConnectionString);
      var blobClient = storageAccount.CreateCloudBlobClient();
      CloudBlobContainer container = blobClient.GetContainerReference("checkpoints");
      var token = default(BlobContinuationToken);
      for (
        var result = container.ListBlobsSegmentedAsync(null, true, BlobListingDetails.None, 500, null, null, null).Result;
        result.Results.Count() > 0;
        result = container.ListBlobsSegmentedAsync(null, true, BlobListingDetails.None, 500, token, null, null).Result
        )
      {
        token = result.ContinuationToken;
        foreach (var blob in result.Results)
        {
          if (((CloudBlockBlob)blob).Name != checkpointFile)
          {
            Console.WriteLine($"deleting blob: ${((CloudBlockBlob)blob).Name}");
            ((CloudBlockBlob)blob).DeleteIfExistsAsync().Wait();
          }
        }
        if (token == null)
          break;
      }
      this.checkpointStopWatch.Restart();
#endif
      return Task.CompletedTask;
    }
  }
}