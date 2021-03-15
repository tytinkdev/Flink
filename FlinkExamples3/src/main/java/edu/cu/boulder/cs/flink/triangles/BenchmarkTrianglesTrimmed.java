package edu.cu.boulder.cs.flink.triangles;

import org.apache.commons.cli.*;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * This file looks at the temporal triangle query without all the overhead
 * of the netflow fields.  The tmpoeral triangle query only involves
 * three fields, the source (sourceIP for netflows), the target 
 * (destIP for netflows), and the timestamp.  This version of the 
 * temporal triangle query creates data structures with just those fields.
 * That way we can compare to the version using full netflows to understand
 * what the overhead is for representing all the fields of the netflow
 * and tossing them around during the computation.
 */
public class BenchmarkTrianglesTrimmed {

  /**
   * A class to represent an edge.  It needs three things, a source,
   * a target, and a timestamp.
   */
  private static class EdgeWithTime
  {
    public String source;
    public String target;
    public double timeSeconds; 

    public EdgeWithTime(String source, String target, double timeSeconds)
    {
      this.source = source;
      this.target = target;
      this.timeSeconds = timeSeconds;
    }

    public String toString()
    {
      String str = source + ", " + target + ", " + timeSeconds;
      return str;
    }
  }


  /**
   * Class to grab the source of the edge.  Used by the dataflow below
   * to join edges together to form a triad.
   */
  private static class SourceKeySelector 
    implements KeySelector<EdgeWithTime, String>
  {
    @Override
    public String getKey(EdgeWithTime edge) {
      return edge.source;
    }
  }

  /**
   * Class to grab the destination of the edge.  Used by the data pipelin
   * below to join edges together to form a triad.
   */
  private static class DestKeySelector 
    implements KeySelector<EdgeWithTime, String>
  {
    @Override
    public String getKey(EdgeWithTime edge) {
      return edge.target;
    }
  }

  /**
   * Key selector that returns a tuple with the target of the edge
   * followed by the source of the edge.
   */
  private static class LastEdgeKeySelector 
    implements KeySelector<EdgeWithTime, Tuple2<String, String>>
  {
    @Override
    public Tuple2<String, String> getKey(EdgeWithTime e1)
    {
      return new Tuple2<String, String>(e1.target, e1.source);
    }
  }

  /**
   * Key selector that returns a tuple with the source of the first edge and the
   * destination of the second edge.
   */
  private static class TriadKeySelector 
    implements KeySelector<Triad, Tuple2<String, String>>
  {
    @Override
    public Tuple2<String, String> getKey(Triad triad)
    {
      return new Tuple2<String, String>(triad.e1.source, triad.e2.target);
    }
  }


  private static class Netflow2EdgeWithTime 
    implements MapFunction<Netflow, EdgeWithTime>
  {
    @Override
    public EdgeWithTime map(Netflow inNetflow) throws Exception {
      EdgeWithTime edge = new EdgeWithTime(inNetflow.sourceIp, inNetflow.destIp, inNetflow.timeSeconds);
      return edge;
    }
  }

  /**
   * A triad is two edges connected with a common vertex.  The common
   * vertex is not enforced by this class, but with the logic defined
   * in the dataflow where Triads are formed with:
   * sourceTarget.join(sourceTarget)
   *     .where(new DestKeySelector())
   *     .equalTo(new SourceKeySelector())
   */
  private static class Triad
  {
    EdgeWithTime e1;
    EdgeWithTime e2;

    public Triad(EdgeWithTime e1, EdgeWithTime e2) {
      this.e1 = e1;
      this.e2 = e2;
    }

    public String toString()
    {
      String str = e1.toString() + " " + e2.toString();
      return str;
    }
  }

  /**
   * A triangle is three edes where vertex A->B->C->A.
   * The topological and temporal constraints are again handled
   * by the data flow defined below.
   */
  private static class Triangle
  {
    EdgeWithTime e1;
    EdgeWithTime e2;
    EdgeWithTime e3;

    public Triangle(EdgeWithTime e1, EdgeWithTime e2, EdgeWithTime e3)
    {
      this.e1 = e1;
      this.e2 = e2;
      this.e3 = e3;
    }

    public String toString()
    {
      String str = e1.toString() + " " + e2.toString() + " " + e3.toString();
      return str;
    }
  }

 
  /**
   * Joins two edges together to form triads.
   */ 
  private static class EdgeJoiner 
    implements FlatJoinFunction<EdgeWithTime, EdgeWithTime, Triad>
  {
    private double queryWindow;

    public EdgeJoiner(double queryWindow)
    {
      this.queryWindow = queryWindow;
    }

    @Override
    public void join(EdgeWithTime e1, EdgeWithTime e2, Collector<Triad> out)
    {
      if (e1.timeSeconds < e2.timeSeconds) {
        if (e2.timeSeconds - e1.timeSeconds <= queryWindow) {
          out.collect(new Triad(e1, e2));
        }
      }
    }
  }

  /**
   * Joins a Triad with and edge to form a triangle.
   */
  private static class TriadJoiner 
    implements FlatJoinFunction<Triad, EdgeWithTime, Triangle>
  {
    private double queryWindow;

    public TriadJoiner(double queryWindow)
    {
      this.queryWindow = queryWindow;
    }

    @Override
    public void join(Triad triad, EdgeWithTime e3, Collector<Triangle> out)
    {
      if (triad.e2.timeSeconds < e3.timeSeconds) {
        if (e3.timeSeconds - triad.e1.timeSeconds <= queryWindow) {
          out.collect(new Triangle(triad.e1, triad.e2, e3));
        }
      }
    }
  }

  /**
   * Maps an instance of a triangle to the value 1.  This is used
   * to count the triangles in a map-reduce operation.
   */
  private static class TriangleMapper 
    implements MapFunction<Triangle, Integer>
  {
    @Override
    public Integer map(Triangle triangle) throws Exception {
      return new Integer(1);
    }
  }

  /**
   * A reduce operation that combines two integers by adding them.  This
   * is used in a map-reduce operation to count the triangles.
   */
  private static class CountTriangles implements ReduceFunction<Integer>
  {
    @Override
    public Integer reduce(Integer n1, Integer n2)
    {
      return n1 + n2;
    }

  }


  public static void main(String[] args) throws Exception {

    Options options = new Options();
    Option numNetflowsOption = new Option("nn", "numNetflows", true,
        "Number of netflows to create per source.");
    Option numIpsOption = new Option("nip", "numIps", true,
        "Number of ips in the pool.");
    Option windowSizeMsOption = new Option("wms", "windowSizeMs",  true,
        "The window size in milliseconds");
    Option slideSizeMsOption = new Option("sms", "slideSizeMs", true,
        "The size of the slide in milliseconds");
    Option rateOption = new Option("r", "rate", true,
        "The rate that netflows are generated.");
    Option numSourcesOption = new Option("ns", "numSources", true,
        "The number of netflow sources.");
    Option queryWindowOption = new Option("qw", "queryWindow", true,
        "The length of the query in seconds.");
    Option outputFileOption = new Option("out", "outputFile", true,
        "Where the output should go.");

    numNetflowsOption.setRequired(true);
    numIpsOption.setRequired(true);
    windowSizeMsOption.setRequired(true);
    slideSizeMsOption.setRequired(true);
    rateOption.setRequired(true);
    numSourcesOption.setRequired(true);
    queryWindowOption.setRequired(true);
    outputFileOption.setRequired(true);

    options.addOption(numNetflowsOption);
    options.addOption(numIpsOption);
    options.addOption(windowSizeMsOption);
    options.addOption(slideSizeMsOption);
    options.addOption(rateOption);
    options.addOption(numSourcesOption);
    options.addOption(queryWindowOption);
    options.addOption(outputFileOption);

    CommandLineParser parser = new DefaultParser();
    HelpFormatter formatter = new HelpFormatter();
    CommandLine cmd = null;

    try {
      cmd = parser.parse(options, args);
    } catch (ParseException e) {
      System.out.println(e.getMessage());
      formatter.printHelp("utility-name", options);

      System.exit(1);
    }

    int numEvents = Integer.parseInt(cmd.getOptionValue("numNetflows"));
    int numIps = Integer.parseInt(cmd.getOptionValue("numIps"));
    long windowSizeMs = Long.parseLong(cmd.getOptionValue("windowSizeMs"));
    long slideSizeMs = Long.parseLong(cmd.getOptionValue("slideSizeMs"));
    double rate = Double.parseDouble(cmd.getOptionValue("rate"));
    int numSources = Integer.parseInt(cmd.getOptionValue("numSources"));
    double queryWindow = Double.parseDouble(cmd.getOptionValue("queryWindow"));
    String outputFile = cmd.getOptionValue("outputFile");

    // get the execution environment
    final StreamExecutionEnvironment env = 
      StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(numSources);
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

    NetflowSource netflowSource = new NetflowSource(numEvents, numIps, rate);
    DataStreamSource<Netflow> netflows = env.addSource(netflowSource);

    DataStream<EdgeWithTime> sourceTarget = 
      netflows.map(new Netflow2EdgeWithTime());
    
    DataStream<Triangle> triangles = sourceTarget.join(sourceTarget)
        .where(new DestKeySelector())
        .equalTo(new SourceKeySelector())
        .window(SlidingEventTimeWindows.of(Time.milliseconds(windowSizeMs),
                                           Time.milliseconds(slideSizeMs)))
        .apply(new EdgeJoiner(queryWindow))
        .join(sourceTarget)
        .where(new TriadKeySelector())
        .equalTo(new LastEdgeKeySelector())
        .window(SlidingEventTimeWindows.of(Time.milliseconds(windowSizeMs),
            Time.milliseconds(slideSizeMs)))
        .apply(new TriadJoiner(queryWindow));

    SingleOutputStreamOperator<Integer> result = 
        triangles.map(new TriangleMapper())
        .timeWindowAll(Time.milliseconds(windowSizeMs),
            Time.milliseconds(slideSizeMs))
        .reduce(new CountTriangles());

    result.writeAsText(outputFile).setParallelism(1);
    env.execute();
  }
}
