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
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

public class BenchmarkTemporalTriangles {


  private static class SourceKeySelector 
    implements KeySelector<TemporalEdge, Integer>
  {
    @Override
    public Integer getKey(TemporalEdge edge) {
      return edge.source;
    }
  }

  private static class DestKeySelector implements KeySelector<TemporalEdge, Integer>
  {
    @Override
    public Integer getKey(TemporalEdge edge) {
      return edge.target;
    }
  }

  private static class LastEdgeKeySelector implements KeySelector<TemporalEdge, Tuple2<Integer, Integer>>
  {
    @Override
    public Tuple2<Integer, Integer> getKey(TemporalEdge e1)
    {
      return new Tuple2<Integer, Integer>(e1.target, e1.source);
    }
  }

  private static class Triad
  {
    TemporalEdge e1;
    TemporalEdge e2;

    public Triad(TemporalEdge e1, TemporalEdge e2) {
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
  private static class TriadKeySelector implements KeySelector<Triad, Tuple2<Integer, Integer>>
  {
    @Override
    public Tuple2<Integer, Integer> getKey(Triad triad)
    {
      return new Tuple2<Integer, Integer>(triad.e1.source, triad.e2.target);
    }
  }

  private static class Triangle
  {
    TemporalEdge e1;
    TemporalEdge e2;
    TemporalEdge e3;

    public Triangle(TemporalEdge e1, TemporalEdge e2, TemporalEdge e3)
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

  private static class EdgeJoiner extends ProcessJoinFunction<TemporalEdge, TemporalEdge, Triad>
  {
    private double queryWindow;

    public EdgeJoiner(double queryWindow)
    {
      this.queryWindow = queryWindow;
    }

    @Override
    public void processElement(TemporalEdge e1, TemporalEdge e2, Context ctx, Collector<Triad> out)
    {
      if (e1.time < e2.time) {
        if (e2.time - e1.time <= queryWindow) {
          out.collect(new Triad(e1, e2));
        }
      }
    }
  }

  /*private static class EdgeJoiner implements FlatJoinFunction<TemporalEdge, TemporalEdge, Triad>
  {
    private double queryWindow;

    public EdgeJoiner(double queryWindow)
    {
      this.queryWindow = queryWindow;
    }

    @Override
    public void join(TemporalEdge e1, TemporalEdge e2, Collector<Triad> out)
    {
      if (e1.time < e2.time) {
        if (e2.time - e1.time <= queryWindow) {
          out.collect(new Triad(e1, e2));
        }
      }
    }
  }*/

  private static class TriadJoiner implements FlatJoinFunction<Triad, TemporalEdge, Triangle>
  {
    private double queryWindow;

    public TriadJoiner(double queryWindow)
    {
      this.queryWindow = queryWindow;
    }

    @Override
    public void join(Triad triad, TemporalEdge e3, Collector<Triangle> out)
    {
      if (triad.e2.time < e3.time) {
        if (e3.time - triad.e1.time <= queryWindow) {
          out.collect(new Triangle(triad.e1, triad.e2, e3));
        }
      }
    }
  }

  private static class TriangleMapper implements MapFunction<Triangle, Integer>
  {
    @Override
    public Integer map(Triangle triangle) throws Exception {
      return new Integer(1);
    }
  }

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
    Option numTemporalEdgesOption = new Option("nn", "numEdges", true,
        "Number of edges to create per source.");
    Option numIpsOption = new Option("v", "numVertices", true,
        "Number of vertices in the graph.");
    Option windowSizeMsOption = new Option("wms", "windowSizeMs",  true,
        "The window size in milliseconds");
    Option slideSizeMsOption = new Option("sms", "slideSizeMs", true,
        "The size of the slide in milliseconds");
    Option rateOption = new Option("r", "rate", true,
        "The rate that edges are generated.");
    Option numSourcesOption = new Option("ns", "numSources", true,
        "The number of edge sources.");
    Option queryWindowOption = new Option("qw", "queryWindow", true,
        "The length of the query in seconds.");
    Option outputFileOption = new Option("otri", "outputFile", true,
        "Where the output should go.");
    Option outputTemporalEdgeOption = new Option("oedges", "outputEdges", true,
        "Where the edges should go (optional).");
    Option outputTriadOption = new Option("otriads", "outputTriads", true,
        "Where the triads should go (optional).");

    numTemporalEdgesOption.setRequired(true);
    numIpsOption.setRequired(true);
    windowSizeMsOption.setRequired(true);
    slideSizeMsOption.setRequired(true);
    rateOption.setRequired(true);
    numSourcesOption.setRequired(true);
    queryWindowOption.setRequired(true);
    outputFileOption.setRequired(true);

    options.addOption(numTemporalEdgesOption);
    options.addOption(numIpsOption);
    options.addOption(windowSizeMsOption);
    options.addOption(slideSizeMsOption);
    options.addOption(rateOption);
    options.addOption(numSourcesOption);
    options.addOption(queryWindowOption);
    options.addOption(outputFileOption);
    options.addOption(outputTemporalEdgeOption);
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

    int numEvents = Integer.parseInt(cmd.getOptionValue("numEdges"));
    int numIps = Integer.parseInt(cmd.getOptionValue("numVertices"));
    long windowSizeMs = Long.parseLong(cmd.getOptionValue("windowSizeMs"));
    long slideSizeMs = Long.parseLong(cmd.getOptionValue("slideSizeMs"));
    double rate = Double.parseDouble(cmd.getOptionValue("rate"));
    int numSources = Integer.parseInt(cmd.getOptionValue("numSources"));
    double queryWindow = Double.parseDouble(cmd.getOptionValue("queryWindow"));
    String outputFile = cmd.getOptionValue("outputFile");
    String outputTemporalEdgeFile = cmd.getOptionValue("outputEdges");
    String outputTriadFile = cmd.getOptionValue("outputTriads");


    // get the execution environment
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(numSources);
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

    TemporalEdgeSource edgeSource = new TemporalEdgeSource(numEvents, numIps, rate);
    DataStreamSource<TemporalEdge> edges = env.addSource(edgeSource);

    if (outputTemporalEdgeFile != null) {
      edges.writeAsText(outputTemporalEdgeFile, FileSystem.WriteMode.OVERWRITE);
    }

    DataStream<Triad> triads = edges
        .keyBy(new DestKeySelector())
        .intervalJoin(edges.keyBy(new SourceKeySelector()))
        .between(Time.milliseconds(0), Time.milliseconds((long) queryWindow * 1000))
        .process(new EdgeJoiner(queryWindow));
    //DataStream<Triad> triads = edges.join(edges)
    //    .where(new DestKeySelector())
    //    .equalTo(new SourceKeySelector())
    //    .window(SlidingEventTimeWindows.of(Time.milliseconds(windowSizeMs),
    //        Time.milliseconds(slideSizeMs)))
    //    .apply(new EdgeJoiner(queryWindow));
    //DataStream<Triad> triads = edges.join(edges)
    //    .where(new DestKeySelector())
    //    .equalTo(new SourceKeySelector())
    //    .window(TumblingEventTimeWindows.of(Time.milliseconds(windowSizeMs)))
    //    .apply(new EdgeJoiner(queryWindow));


    if (outputTriadFile != null) {
      triads.writeAsText(outputTriadFile, FileSystem.WriteMode.OVERWRITE);
    }

    DataStream<Triangle> triangles = triads
        .join(edges)
        .where(new TriadKeySelector())
        .equalTo(new LastEdgeKeySelector())
        .window(SlidingEventTimeWindows.of(Time.milliseconds(windowSizeMs),
            Time.milliseconds(slideSizeMs)))
        .apply(new TriadJoiner(queryWindow));


    triangles.writeAsText(outputFile, FileSystem.WriteMode.OVERWRITE);
    //SingleOutputStreamOperator<Integer> result = triangles.map(new TriangleMapper())
    //    .timeWindowAll(Time.milliseconds(windowSizeMs),
    //        Time.milliseconds(slideSizeMs))
    //    .reduce(new CountTriangles());

    //result.writeAsText(outputFile).setParallelism(1);
    env.execute();
  }

}
