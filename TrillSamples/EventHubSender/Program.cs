// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Threading.Tasks;
using EventHubSample.Model;
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

      await SendMessagesToEventHub(1000000);

      await eventHubClient.CloseAsync();

      Console.WriteLine("Press any key to exit.");
      Console.ReadLine();
    }

    private static int Add<T>(ref EventDataBatch batch, T msg)
    {
      int retval = 0;
      var data = new EventData(BinarySerializer.Serialize(msg));
      data.Properties.Add("Table", typeof(T).Name);
      data.Properties.Add("Format", "json");
      data.Properties.Add("IngestionMappingReference", $"Map{typeof(T).Name}");
      //var back = BinarySerializer.DeserializeStreamEventSampleEvent(data);
      if (!batch.TryAdd(data))
      {
        Console.WriteLine($"Sending #{batch.Count} messages");
        retval = batch.Count;
        eventHubClient.SendAsync(batch).Wait();
        batch = eventHubClient.CreateBatch(new BatchOptions
        {
          MaxMessageSize = 1046528,
          PartitionKey = "default"
        });
        if (!batch.TryAdd(data))
          throw new OverflowException("batch size to low");
      }
      return retval;
    }

    private static int Add<T>(ref EventDataBatch batch, StreamEvent<T> msg)
    {
      int retval = 0;
      var data = BinarySerializer.Serialize(msg);
      var back = BinarySerializer.DeserializeStreamEventSampleEvent(data);
      var eventData = new EventData(data);
      if (!batch.TryAdd(eventData))
      {
        Console.WriteLine($"Sending #{batch.Count} messages");
        retval = batch.Count;
        eventHubClient.SendAsync(batch).Wait();
        batch = eventHubClient.CreateBatch(new BatchOptions
        {
          MaxMessageSize = 1046528,
          PartitionKey = "default"
        });
        if (!batch.TryAdd(new EventData(data)))
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
      for (var i = 0; i < numMessagesToSend; i++)
      {
        var uri = (1002030 + i).ToString();
        var min = 200 + ((float)rand.Next(100, 500) / 10.0f);
        var now = DateTime.Now;
        var value = 23f + (float)rand.Next(10) / 10f;

        var mt1 = new MeasureT1
        {
          URI = uri,
          Time = now,
          id = i % 15,
          Min = min,
          Max = min + (float)rand.Next(100, 500) / 10f,
          Std = 45f + (float)rand.Next(10) / 10f,
          nSample = 1000,
          Mean = value
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
          //var m1 = StreamEvent.CreateStart(DateTime.UtcNow.Ticks, mt1);
          //var m3 = StreamEvent.CreateStart(DateTime.UtcNow.Ticks, mt3);
          //var m4 = StreamEvent.CreateStart(DateTime.UtcNow.Ticks, mt4);

          Console.Write(".");
          //totalMessages += Add(ref batch, m1);
          //totalMessages += Add(ref batch, m3);
          //totalMessages += Add(ref batch, m4);

          totalMessages += Add(ref batch, mt1);
          totalMessages += Add(ref batch, mt3);
          totalMessages += Add(ref batch, mt4);


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
      Console.WriteLine($"Flushing #{batch.Count} messages");
      totalMessages += batch.Count;
      await eventHubClient.SendAsync(batch);
      Console.WriteLine($"Sent total of #{totalMessages} #{messageCount} messages");
    }
  }
}
