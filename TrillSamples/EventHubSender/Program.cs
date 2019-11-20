// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Threading.Tasks;
using Microsoft.Azure.EventHubs;
using Microsoft.StreamProcessing;
using static BinarySerializer;

namespace EventHubSender
{
  public sealed class Program
  {
    private const string EventHubConnectionString = "Endpoint=sb://ehtest-dg.servicebus.windows.net/;SharedAccessKeyName=AUTHOR;SharedAccessKey=OhJyrMYske/bTu223Vf7vwYKWjQkrvb7UGPQVxR7YLk=;EntityPath=main";
    private const string EventHubName = "main";
    private static EventHubClient eventHubClient;

    public static void Main(string[] args)
    {
      MainAsync(args).GetAwaiter().GetResult();
    }

    private static async Task MainAsync(string[] args)
    {
      var connectionStringBuilder = new EventHubsConnectionStringBuilder(EventHubConnectionString)
      {
        EntityPath = EventHubName
      };

      eventHubClient = EventHubClient.CreateFromConnectionString(connectionStringBuilder.ToString());

      await SendMessagesToEventHub(100);

      await eventHubClient.CloseAsync();

      Console.WriteLine("Press any key to exit.");
      Console.ReadLine();
    }

    private static int Add<T>(EventDataBatch batch, StreamEvent<T> msg)
    {
      int retval = 0;
      if (!batch.TryAdd(new EventData(BinarySerializer.Serialize(msg))))
      {
        Console.WriteLine($"Sending #{batch.Count} messages");
        retval = batch.Count;
        eventHubClient.SendAsync(batch).Wait();
        batch = eventHubClient.CreateBatch(new BatchOptions
        {
          MaxMessageSize = 16 * 1024,
          PartitionKey = "default"
        });
        if (!batch.TryAdd(new EventData(BinarySerializer.Serialize(msg))))
          throw new OverflowException("batch size to low");
      }
      return retval;
    }

    // Creates an Event Hub client and sends 100 messages to the event hub.
    private static async Task SendMessagesToEventHub(int numMessagesToSend)
    {
      var rand = new Random((int)DateTime.Now.Ticks);
      var proc = System.Diagnostics.Process.GetCurrentProcess();
      int messageCount = 0;
      var totalMessages = 0;
      var nextTotalMessages = 1000;

      var batch = eventHubClient.CreateBatch(new BatchOptions
      {
        MaxMessageSize = 1046528,
        PartitionKey = "default"
      });
#if false

      for (var key = 0; key < 100; key++)
      {
        for (var value = 0; value < 10; value++)
        {
          try
          {
            var message = StreamEvent.CreateStart(DateTime.UtcNow.Ticks, new SampleEvent { EventGroupKey = $"{key}", EventValue = value });
            Console.WriteLine($"adding message #{++messageCount}: {message}");
            if (!batch.TryAdd(new EventData(BinarySerializer.Serialize(message))))
            {
              Console.WriteLine($"Sending #{batch.Count} messages");
              totalMessages += batch.Count;
              await eventHubClient.SendAsync(batch);
              batch = eventHubClient.CreateBatch(new BatchOptions
              {
                MaxMessageSize = 16 * 1024,
                PartitionKey = "default"
              });
              if (!batch.TryAdd(new EventData(BinarySerializer.Serialize(message))))
                throw new OverflowException("batch size to low");
            }
          }
          catch (Exception exception)
          {
            Console.WriteLine($"{DateTime.Now} > Exception: {exception.Message}");
          }
        }
      }
#else
      for (var i = 0; i < 10000; i++)
      {
        var uri = (1002030 + i).ToString();
        var min = 200 + ((float)rand.Next(100, 500) / 10.0f);
        var now = DateTime.Now;
        var value = 23f + (float)rand.Next(10) / 10f;

        var mt1 = new MeasureT1
        {
          id = i % 15,
          Min = min,
          Max = min + (float)rand.Next(100, 500) / 10f,
          Std = 45f + (float)rand.Next(10) / 10f,
          Time = now,
          nSample = 1000,
          Mean = value,
          URI = uri
        };

        var mt3 = new MeasureT3
        {
          URI = uri,
          Time = now,
          Event = i % 2 == 0 ? "CARICO_PEZZI" : "FINE_CICLO",
          Value = value
        };

        var mt4 = new MeasureT4
        {
          URI = uri,
          Time = now,
          AW = (Byte)(rand.Next(4) == 2 ? 1 : 0),
          Flag = (Byte)(rand.Next(4) <= 2 ? 1 : 0)
        };

        try
        {
          var m1 = StreamEvent.CreateStart(DateTime.UtcNow.Ticks, mt1);
          var m3 = StreamEvent.CreateStart(DateTime.UtcNow.Ticks, mt3);
          var m4 = StreamEvent.CreateStart(DateTime.UtcNow.Ticks, mt4);

          Console.Write(".");
          totalMessages += Add(batch, m1);
          totalMessages += Add(batch, m3);
          totalMessages += Add(batch, m4);
          Console.Title = totalMessages.ToString();
          if (totalMessages >= nextTotalMessages)
          {
            nextTotalMessages = totalMessages + 1000;
            Console.WriteLine($"Total Messages = {totalMessages}");
          }
        }
        catch (Exception exception)
        {
          Console.WriteLine($"{DateTime.Now} > Exception: {exception.Message}");
        }
      }
#endif
      Console.WriteLine($"Sending #{batch.Count} messages");
      totalMessages += batch.Count;
      await eventHubClient.SendAsync(batch);
      Console.WriteLine($"Sent total of #{totalMessages} #{messageCount} messages");
    }
  }
}
