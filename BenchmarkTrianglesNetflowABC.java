package edu.cu.boulder.cs.flink.triangles;

import org.apache.commons.cli.*;
import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * This benchmarks finding triangles, A->B, B->C, C->A, where the times of
 * the edges are strictly increasing.  The edges are netflows, and the
 * netflow representation is kept throughout (similar to the SAM
 * implementation).
 *
 * The approach is to use an interval join
 * (https://ci.apache.org/projects/flink/flink-docs-release-1.6/dev/stream/operators/joining.html#interval-join)
 * which allows you to specify an interval of time from an event.
 * The interval join is applied between the netflow stream and itself to create
 * triads (two edges), and then between the triads and the netflow stream to
 * find the triangles.
 */
public class BenchmarkTrianglesNetflow {

  /**
   * Class to grab the source of the edge.  Used by the dataflow below
   * to join edges together to form a triad.
   */
  private static class SourceKeySelector 
    implements KeySelector<Netflow, String>
  {
    @Override
    public String getKey(Netflow edge) {
      return edge.sourceIp;
    }
  }

  /**
   * Class to grab the destination of the edge.  Used by the data pipelin
   * below to join edges together to form a triad.
   */
  private static class DestKeySelector 
    implements KeySelector<Netflow, String>
  {
    @Override
    public String getKey(Netflow edge) {
      return edge.destIp;
    }
  }

  /**
   * Key selector that returns a tuple with the target of the edge
   * followed by the source of the edge.
   */
  private static class LastEdgeKeySelector 
    implements KeySelector<Netflow, Tuple2<String, String>>
  {
    @Override
    public Tuple2<String, String> getKey(Netflow e1)
    {
      return new Tuple2<String, String>(e1.destIp, e1.sourceIp);
    }
  }

  /**
   * A triad is two edges connected with a common vertex.  The common
   * vertex is not enforced by this class, but with the logic defined
   * in the dataflow. 
   */

  private static class Triad
  {
    Netflow e1;
    Netflow e2;

    public Triad(Netflow e1, Netflow e2) {
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
   * Key selector that returns a tuple with the source of the first edge and the
   * destination of the second edge.
   */
  private static class TriadKeySelector 
    implements KeySelector<Triad, Tuple2<String, String>>
  {
    @Override
    public Tuple2<String, String> getKey(Triad triad)
    {
      return new Tuple2<String, String>(triad.e1.sourceIp, triad.e2.destIp);
    }
  }

  /**
   * A triangle is three edes where vertex A->B->C->A.
   * The topological and temporal constraints are again handled
   * by the data flow defined below.
   */
  private static class Triangle
  {
    Netflow e1;
    Netflow e2;
    Netflow e3;

    public Triangle(Netflow e1, Netflow e2, Netflow e3)
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
    extends ProcessJoinFunction<Netflow, Netflow, Triad>
  {
    private double queryWindow;

    public EdgeJoiner(double queryWindow)
    {
      this.queryWindow = queryWindow;
    }

    @Override
    public void processElement(Netflow e1, Netflow e2, Context ctx, Collector<Triad> out)
    {
      if (e1.timeSeconds < e2.timeSeconds) {
        if (e2.timeSeconds - e1.timeSeconds <= queryWindow) {
          out.collect(new Triad(e1, e2));
        }
      }
    }
  }

  private static class TriadJoiner extends ProcessJoinFunction<Triad, Netflow, Triangle>
  {
    private double queryWindow;

    public TriadJoiner(double queryWindow)
    {
      this.queryWindow = queryWindow;
    }

    @Override
    public void processElement(Triad triad, Netflow e3, Context ctx, Collector<Triangle> out)
    {
      if (triad.e2.timeSeconds < e3.timeSeconds) {
        if (e3.timeSeconds - triad.e1.timeSeconds <= queryWindow) {
          out.collect(new Triangle(triad.e1, triad.e2, e3));
        }
      }
    }
  }

  public static void main(String[] args) throws Exception {

    Options options = new Options();
    Option parseFile = new Option("file", "parseFile", true,
      "Input the CSV for a netflow file");
    /*Option numNetflowsOption = new Option("nn", "numNetflows", true,
        "Number of netflows to create per source.");
    Option numIpsOption = new Option("nip", "numIps", true,
        "Number of ips in the pool.");
    Option rateOption = new Option("r", "rate", true,
        "The rate that netflows are generated.");
    Option numSourcesOption = new Option("ns", "numSources", true,
        "The number of netflow sources.");*/
    Option queryWindowOption = new Option("qw", "queryWindow", true,
        "The length of the query in seconds.");
    Option outputFileOption = new Option("out", "outputFile", true,
        "Where the output should go.");
    Option outputNetflowOption = new Option("net", "outputNetflow", true,
        "Where the netflows should go (optional).");
    Option outputTriadOption = new Option("triad", "outputTriads", true,
        "Where the triads should go (optional).");

    parseFile.setRequired(true);
    queryWindowOption.setRequired(true);
    outputFileOption.setRequired(true);

    options.addOption(parseFile);
    options.addOption(queryWindowOption);
    options.addOption(outputFileOption);
    options.addOption(outputNetflowOption);
    options.addOption(outputTriadOption);

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

    String fileToParse = cmd.getOptionValue("parseFile");
    double queryWindow = Double.parseDouble(cmd.getOptionValue("queryWindow"));
    String outputFile = cmd.getOptionValue("outputFile");
    String outputNetflowFile = cmd.getOptionValue("outputNetflow");
    String outputTriadFile = cmd.getOptionValue("outputTriads");


    // get the execution environment
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(numSources);
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

    // Get a stream of netflows from the NetflowSource
    NetflowSource netflowSource = new NetflowSource(numEvents, numIps, rate);
    DataStreamSource<Netflow> netflows = env.addSource(netflowSource);

    // If specified, we write out the raw data we see to a file.
    if (outputNetflowFile != null) {
      netflows.writeAsText(outputNetflowFile, FileSystem.WriteMode.OVERWRITE);
    }

    // Transforms the netflows into a stream of triads
    DataStream<Triad> triads = netflows
        //.keyBy(new DestKeySelector())
        //.intervalJoin(netflows.keyBy(new SourceKeySelector()))
        .keyBy(new SourceKeySelector())
        .intervalJoin(netflows.keyBy(new DestKeySelector()))
        .between(Time.milliseconds(0), Time.milliseconds((long) queryWindow * 1000))
        .process(new EdgeJoiner(queryWindow));

    // If specified, we spit out the triads we found to disk.
    if (outputTriadFile != null) {
      triads.writeAsText(outputTriadFile, FileSystem.WriteMode.OVERWRITE);
    }

    // Transforms the stream of triads into triangles.
    DataStream<Triangle> triangles = triads
        .keyBy(new TriadKeySelector())
        .intervalJoin(netflows.keyBy(new LastEdgeKeySelector()))
        .between(Time.milliseconds(0), Time.milliseconds((long) queryWindow * 1000))
        .process(new TriadJoiner(queryWindow));

    // Write the triangles we found to disk.
    triangles.writeAsText(outputFile, FileSystem.WriteMode.OVERWRITE);
    env.execute();
  }

}
