package edu.cu.boulder.cs.flink.triangles;

/**
 * This class represents a netflow.  The fields came from the VAST Challenge
 * 2013: Mini-Challenge 3 dataset and the format they used.  
 * http://vacommunity.org/VAST+Challenge+2013%3A+Mini-Challenge+3 
 * There are different formats for netflows, version 5 and version 9
 * are the most popular.  To represent those formats, likely another
 * class is needed.
 *
 * The samGeneratedId and the label fields came from the Streaming
 * Analytics Machine (github/elgood/SAM).  These don't come from
 * the netflow representation but are used by SAM internally.  I added
 * them here to compare directly with the SAM implementation.
 */
public class Netflow {
  public double timeSeconds;
  public String sourceIp;
  public String destIp;

  public Netflow(double timeSeconds,
                 String sourceIp,
                 String destIp)
  {
    this.timeSeconds = timeSeconds;
    this.sourceIp = sourceIp;
    this.destIp = destIp;
  }

  /**
   * Converts the netflow to a string.  This is mostly for debugging.
   * I only print the time, source ip and dest ip because those are the
   * fields that matter for the temporal triangle query.
   */
  public String toString()
  {
    return timeSeconds + ", " + sourceIp + ", " + destIp;
  }
}
