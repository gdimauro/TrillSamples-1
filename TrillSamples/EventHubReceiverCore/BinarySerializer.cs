// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.IO;
using System.Runtime.Serialization;
using System.Text;
using System.Xml;
using Microsoft.StreamProcessing;
using Newtonsoft.Json;

public static class BinarySerializer
{

  public struct SampleEvent
  {
    public long syncTime;
    public long otherTime;
    public string EventGroupKey { get; set; }
    public int EventValue { get; set; }

    public override string ToString()
    {
      return $"{syncTime} {otherTime} {EventGroupKey} {EventValue}";
    }
  }

  public static byte[] Serialize(StreamEvent<SampleEvent> message)
  {
    message.Payload.syncTime = message.IsEnd ? message.EndTime : message.StartTime;
    message.Payload.otherTime = message.IsEnd ? message.StartTime : message.EndTime;
    var result = JsonConvert.SerializeObject(message.Payload);
    return Encoding.UTF7.GetBytes(result);
  }

  public static StreamEvent<SampleEvent> DeserializeStreamEventSampleEvent(byte[] data)
  {
    var strData = Encoding.UTF7.GetString(data);
    strData = strData.Substring(strData.IndexOf("{\""));
    var payload = default(SampleEvent);
    try
    {
      payload = JsonConvert.DeserializeObject<SampleEvent>(strData);
    }
    catch
    {
      Console.Write("x");
    }
    return new StreamEvent<SampleEvent>(payload.syncTime, payload.otherTime, payload);
  }

  public static byte[] Serialize<T>(StreamEvent<T> message)
  {
    var result = JsonConvert.SerializeObject(message.Payload);
    return Encoding.UTF7.GetBytes(result);
  }

  public static StreamEvent<T> DeserializeStreamEventSampleEvent<T>(byte[] data)
  {
    var strData = Encoding.UTF7.GetString(data);
    strData = strData.Substring(strData.IndexOf("{\""));
    var payload = default(T);
    try
    {
      payload = JsonConvert.DeserializeObject<T>(strData);
    }
    catch
    {
      Console.Write("x");
    }
    return new StreamEvent<T>(0, 0, payload);
  }

}