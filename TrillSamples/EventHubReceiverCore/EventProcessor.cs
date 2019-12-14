﻿// *********************************************************************
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
using EventHubSample.Model;
using Microsoft.Azure.EventHubs;
using Microsoft.Azure.EventHubs.Processor;
using Microsoft.StreamProcessing;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
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
    private CloudBlobContainer checkpointContainer;
    private Subject<StreamEvent<Measure>> input;
    private QueryContainer queryContainer;
    private Microsoft.StreamProcessing.Process queryProcess;

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

    static long __count = 0;
    static DateTime LastTime = DateTime.UtcNow;

    /// <summary>
    /// Process events
    /// </summary>
    /// <param name="context"></param>
    /// <param name="messages"></param>
    /// <returns></returns>
    public Task ProcessEventsAsync(PartitionContext context, IEnumerable<EventData> messages)
    {
      EventProcessor.__count += messages.Count();

      if (DateTime.UtcNow > LastTime.AddSeconds(2))
      {
        LastTime = DateTime.UtcNow;
        Console.Title = $"total: {EventProcessor.__count} offset: {context.ToString()} count: {messages.Count()}";
      }

#if true
      context.CheckpointAsync();
#else
      long lastSeq = 0;
      foreach (var eventData in messages)
      {
        var message = BinarySerializer.DeserializeStreamEventSampleEvent<Measure>(eventData.Body.ToArray());
        lastSeq = eventData.SystemProperties.SequenceNumber;
        Task.Delay(5).Wait();
        this.input.OnNext(message);
        //else if (message.Payload.Data is MeasureT3)
        //{
        //  var serializer = Newtonsoft.Json.JsonSerializer.Create();
        //  var value = JsonConvert.DeserializeObject<StreamEvent<Measure<MeasureT3>>>(JsonConvert.SerializeObject(message));
        //  this.input.OnNext(value);
        //}
        //else if (message.Payload.Data is MeasureT4)
        //  this.input.OnNext((StreamEvent<Measure>)(object)(StreamEvent<Measure<MeasureT4>>)(object)message);
      }

      if (this.checkpointStopWatch.Elapsed > TimeSpan.FromSeconds(10))
      {
        Console.WriteLine("Taking checkpoint");
        var storageAccount = CloudStorageAccount.Parse(StorageConnectionString);
        var blobClient = storageAccount.CreateCloudBlobClient();
        CloudBlobContainer container = blobClient.GetContainerReference("checkpoints");
        var blockBlob = container.GetBlockBlobReference(context.PartitionId + "-" + lastSeq);
        CloudBlobStream blobStream = blockBlob.OpenWriteAsync().GetAwaiter().GetResult();
        this.queryProcess.Checkpoint(blobStream);
        blobStream.Flush();
        blobStream.Close();

        return context
            .CheckpointAsync()
            .ContinueWith(t => DeleteOlderCheckpoints(context.PartitionId + "-" + lastSeq));
      }
#endif
      return Task.CompletedTask;
    }

    /// <summary>
    /// Create query and register subscriber
    /// </summary>
    private void CreateQuery()
    {
      this.queryContainer = new QueryContainer();
      this.input = new Subject<StreamEvent<Measure>>();
      var inputStream = this.queryContainer.RegisterInput<Measure>(
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
      var storageAccount = CloudStorageAccount.Parse(StorageConnectionString);
      var blobClient = storageAccount.CreateCloudBlobClient();
      CloudBlobContainer container = blobClient.GetContainerReference("checkpoints");
      var results = new List<IListBlobItem>();
      var result = container.ListBlobsSegmentedAsync(null).Result;
      var token = result.ContinuationToken;
      foreach (var blob in container.ListBlobsSegmentedAsync(token).Result.Results)
      {
        if (((CloudBlockBlob)blob).Name != checkpointFile)
        {
          ((CloudBlockBlob)blob).DeleteIfExistsAsync().Wait();
        }
      }
      this.checkpointStopWatch.Restart();
      return Task.CompletedTask;
    }
  }
}