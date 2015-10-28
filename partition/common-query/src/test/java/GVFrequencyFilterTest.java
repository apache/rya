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
import ss.cloudbase.core.iterators.filter.general.GVFrequencyFilter;

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
public class GVFrequencyFilterTest
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

  protected Scanner setUpGVDFFilter(Scanner s, String Frequency)
  {
    try
    {
      s.clearScanIterators();
  
      s.setScanIterators(50, FilteringIterator.class.getName(), "gvff");
      s.setScanIteratorOption("gvff", "0", GVFrequencyFilter.class.getName());
      s.setScanIteratorOption("gvff", "0." + GVFrequencyFilter.OPTIONFrequency, Frequency);

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

      //System.out.println(e.getKey()+"\t"+e.getValue());

      sb.append(colq);
    }
    return sb.toString();
  }

  @Test
  public void testNoMatch()
  {

    Scanner s = setUpGVDFFilter(getSerializedScanner(), "2000000000");
    s.setRange(new Range());

    assertTrue(checkSerialized(s).isEmpty());
  }

  @Test
  public void testSingleMatch()
  {
    Scanner s = setUpGVDFFilter(getSerializedScanner(), "1500000000");
    s.setRange(new Range());

    assertTrue(checkSerialized(s).equals("01"));
  }


  @Test
  public void testDoubleMatch()
  {
    Scanner s = setUpGVDFFilter(getSerializedScanner(), "1200000000");
    s.setRange(new Range());

    assertTrue(checkSerialized(s).equals("01,03"));
  }

  @Test
  public void testDummyTest()
  {
    assertTrue(true);
  }

}
