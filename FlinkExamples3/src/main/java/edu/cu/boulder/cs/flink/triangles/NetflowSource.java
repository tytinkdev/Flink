package edu.cu.boulder.cs.flink.triangles;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;

import java.time.Instant;
import java.util.Random;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
/**
 * This creates netflows from a pool of IPs.
 */
/*

  This code is used to create artificial netflows. Unneccesary!  

public class NetflowSource extends RichParallelSourceFunction<Netflow> {

  
  /// The number of netflows to create.
  private int numEvents;

  /// How many netflows created.
  private int currentEvent;

  /// How many unique IPs in the pool
  private int numIps;

  /// Used to create the netflows with randomly selected ips
  private Random random;

  /// Time from start of run
  private double time = 0;

  /// The time in seconds between netflows
  private double increment = 0.1;

  /// When the first netflow was created
  private double t1;

  public int samGeneratedId;
  public int label;
  public double timeSeconds;
  public String parseDate;
  public String dateTimeString;
  public String protocol;
  public String protocolCode;
  public String sourceIp;
  public String destIp;
  public int sourcePort;
  public int destPort;
  public String moreFragments;
  public int countFragments;
  public double durationSeconds;
  public long srcPayloadBytes;
  public long destPayloadBytes;
  public long srcTotalBytes;
  public long destTotalBytes;
  public long firstSeenSrcPacketCount;
  public long firstSeenDestPacketCount;
  public int recordForceOut; 

  
   *
   * @param numEvents The number of netflows to produce.
   * @param numIps The number of ips in the pool.
   * @param rate The rate of netflows produced in netflows/s.
   
  public NetflowSource(int numEvents, int numIps, double rate)
  {
    this.numEvents = numEvents;
    currentEvent = 0;
    this.numIps = numIps;
    this.increment = 1 / rate;
    //this.random = new Random();

    label = 0;
    parseDate = "parseDate";
    dateTimeString = "dateTimeString";
    protocol = "protocol";
    protocolCode = "protocolCode";
    sourcePort = 80;
    destPort = 80;
    moreFragments = "moreFragments";
    countFragments = 0;
    durationSeconds = 0.1;
    srcPayloadBytes = 10;
    destPayloadBytes = 10;
    srcTotalBytes = 15;
    destTotalBytes = 15;
    firstSeenSrcPacketCount = 5;
    firstSeenDestPacketCount =5;
    recordForceOut = 0;
  }
*/

public class NetflowSource extends RichParallelSourceFunction <Netflow> {
  BufferedReader csvReader;

  public NetflowSource(String filename)
  {
    this.csvReader = new BufferedReader(new FileReader(fileToParse));
  }

  @Override
  public void run(SourceContext<Netflow> out) throws Exception
  {
    RuntimeContext context = getRuntimeContext();
    int taskId = context.getIndexOfThisSubtask();
    this.csvReader = new BufferedReader(new FileReader(fileToParse));
    String line = "";

    while(line = csvReader.readLine() !=null) {
      String[] data = line.split(",");
      Long timeSeconds = Long.parseLong(data[0]);
      Long t = timeSeconds
      String sourceIp = data[10];
      String destIp = data[11];
      Netflow netflow = new Netflow(timeSeconds, sourceIp, destIp);
      out.collectWithTimestamp(netflow, t);
      out.emitWatermark(new Watermark(t));
    }
  }

  @Override
  public void cancel()
  {
    //this.currentEvent = numEvents;
  }
}
