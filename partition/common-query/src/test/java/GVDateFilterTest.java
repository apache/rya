/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Map.Entry;

import org.apache.hadoop.io.Text;
import org.junit.Test;

import ss.cloudbase.core.iterators.GMDenIntersectingIterator;
import ss.cloudbase.core.iterators.filter.general.GVDateFilter;

import cloudbase.core.client.Connector;
import cloudbase.core.client.Scanner;
import cloudbase.core.client.TableNotFoundException;
import cloudbase.core.data.Key;
import cloudbase.core.data.Range;
import cloudbase.core.data.Value;
import cloudbase.core.iterators.FilteringIterator;
import cloudbase.core.security.Authorizations;

/**
 *
 * @author rashah
 */
public class GVDateFilterTest
{

  private Connector cellLevelConn;
  private Connector serializedConn;
  private static final String TABLE = "partition";
  private static final Authorizations AUTHS = new Authorizations("ALPHA,BETA,GAMMA".split(","));



  protected Connector getSerializedConnector()
  {
    if (serializedConn == null)
    {
      serializedConn = SampleGVData.initConnector();
      SampleGVData.writeDenSerialized(serializedConn, SampleGVData.sampleData());
    }
    return serializedConn;
  }



  protected Scanner getSerializedScanner()
  {
    Connector c = getSerializedConnector();
    try
    {
      return c.createScanner(TABLE, AUTHS);
    }
    catch (TableNotFoundException e)
    {
      return null;
    }
  }

  protected Scanner setUpGVDFFilter(Scanner s, String timesta)
  {
    try
    {
  
      s.setScanIterators(50, FilteringIterator.class.getName(), "gvdf");
      s.setScanIteratorOption("gvdf", "0", GVDateFilter.class.getName());
      s.setScanIteratorOption("gvdf", "0." + GVDateFilter.OPTIONInTimestamp, timesta);

    }
    catch (IOException e)
    {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    return s;
  }

  protected String checkSerialized(Scanner s)
  {
    StringBuilder sb = new StringBuilder();
    boolean first = true;
    for (Entry<Key, Value> e : s)
    {

      if (!first)
      {
        sb.append(",");
      }
      else
      {
        first = false;
      }

      String colq = e.getKey().getColumnQualifier().toString();

      sb.append(colq);
    }
    return sb.toString();
  }


  @Test
  public void testNoResults()
  {

    Scanner s = setUpGVDFFilter(getSerializedScanner(), "2008-03-03T20:44:28.633Z");
    s.setRange(new Range());

    assertTrue(checkSerialized(s).equals(""));
  }


  @Test
  public void testOneResult()
  {

    Scanner s = setUpGVDFFilter(getSerializedScanner(), "2011-03-03T20:44:28.633Z");
    s.setRange(new Range());

    System.out.println(checkSerialized(s));

    assertTrue(checkSerialized(s).equals("03"));
  }

  @Test
  public void testTwoResults()
  {

    Scanner s = setUpGVDFFilter(getSerializedScanner(), "2009-03-03T20:44:28.633Z");
    s.setRange(new Range());

    assertTrue(checkSerialized(s).equals("04,01"));
  }

    @Test
  public void testThreeResults()
  {

    Scanner s = setUpGVDFFilter(getSerializedScanner(), "2010-03-01T20:44:28.633Z");
    s.setRange(new Range());

    assertTrue(checkSerialized(s).equals("04,01,03"));
  }

  @Test
  public void testDummyTest()
  {
    assertTrue(true);
  }

}
