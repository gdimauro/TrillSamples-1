using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Text;

namespace EventHubSample.Model
{
  public interface IMeasure
  {
    public string URI { get; set; }
    public DateTime Time { get; set; }
    //public MeasureData Data { get; set; }
  }

  public class MeasureData { }

  [JsonObject]
  public class Measure : IMeasure
  {
    /// <summary>
    /// è il codice univoco di identificazione della risorsa che genera la misura, può essere
    /// visto come un ID combinato ed è formato da 8 numeri in sequenza secondo lo schema
    /// XX-YYY-ZZZ dove XX è relativo alla linea che si sta considerando, YYY al test bench, ZZZ
    /// alla stazione.
    /// </summary>
    [JsonProperty("uri")]
    public string URI { get; set; }
    /// <summary>
    /// contiene il timestamp dell’ultima misura effettuata
    /// </summary>
    [JsonProperty("timeStamp")]
    public DateTime Time { get; set; }
    //[JsonProperty("data")]
    //public MeasureData Data { get; set; }
  }

  //public class Measure<T> : Measure
  //    where T : MeasureData
  //{
  //  [JsonIgnore]
  //  public T ExtraData
  //  {
  //    get
  //    {
  //      return (T)this.Data;
  //    }
  //    set
  //    {
  //      this.Data = value;
  //    }
  //  }
  //}

  public class MeasureT1 : Measure
  {
    /// <summary>
    /// contiene la media sulle nSample misure
    /// </summary>
    [JsonProperty("mean")]
    public float Mean { get; set; }
    /// <summary>
    /// contiene il valore della deviazione standard delle misure
    /// </summary>
    [JsonProperty("std")]
    public float Std { get; set; }
    /// <summary>
    /// valore minimo sul campione di misure
    /// </summary>
    [JsonProperty("min")]
    public float Min { get; set; }
    /// <summary>
    /// valore massimo sul campione di misure
    /// </summary>
    [JsonProperty("max")]
    public float Max { get; set; }
    /// <summary>
    /// è il numero di campioni valutati
    /// </summary>
    [JsonProperty("nSample")]
    public int nSample { get; set; }
    /// <summary>
    /// id della variabile
    /// </summary>
    [JsonProperty("id")]
    public int id { get; set; }
  }

  public class MeasureT3 : Measure
  {
    /// <summary>
    /// stringa relativa all’evento che si è generato
    /// </summary>
    [JsonProperty("event")]
    public string Event { get; set; }
    /// <summary>
    /// valore legato all’evento
    /// </summary>
    [JsonProperty("val")]
    public float Value { get; set; }
  }

  /// <summary>
  /// Misure Allarme
  /// Rappresentano i dati relativi a warning e agli allarmi.La variabile è gestita ad eventi, quando
  /// un allarme o warning cambia stato(da true a false o viceversa), viene inviato un dato.
  /// </summary>
  public class MeasureT4 : Measure
  {
    /// <summary>
    /// Allarme/warning
    /// </summary>
    [JsonProperty("aw")]
    public byte AW { get; set; }
    /// <summary>
    /// Codice dell’errore
    /// </summary>
    [JsonProperty("code")]
    public string Code { get; set; }
    /// <summary>
    /// Fornisce l’informazione sul fatto che l’allarme sia attivo o no - aw: flag per allarme/warning
    /// </summary>
    [JsonProperty("flag")]
    public byte Flag { get; set; }
  }
}
