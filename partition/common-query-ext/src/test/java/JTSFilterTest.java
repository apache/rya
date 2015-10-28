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
import ss.cloudbase.core.iterators.filter.jts.JTSFilter;

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
public class JTSFilterTest
{

  private Connector cellLevelConn;
  private Connector serializedConn;
  private static final String TABLE = "partition";
  private static final Authorizations AUTHS = new Authorizations("ALPHA,BETA,GAMMA".split(","));



  protected Connector getSerializedConnector()
  {
    if (serializedConn == null)
    {
      serializedConn = SampleJTSData.initConnector();
      SampleJTSData.writeDenSerialized(serializedConn, SampleJTSData.sampleData());
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

  protected Scanner setUpJTSFilter(Scanner s, String latitude, String longitude, boolean change_name)
  {
    try
    {
  
      s.setScanIterators(50, FilteringIterator.class.getName(), "gvdf");
      s.setScanIteratorOption("gvdf", "0", JTSFilter.class.getName());
      s.setScanIteratorOption("gvdf", "0." + JTSFilter.OPTIONCenterPointLat, latitude);
      s.setScanIteratorOption("gvdf", "0." + JTSFilter.OPTIONCenterPointLon, longitude);
      if (change_name)
          s.setScanIteratorOption("gvdf", "0." + JTSFilter.OPTIONGeometryKeyName, "beam-footprint");


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
    //London is in niether - 51°30'0.00"N   0° 7'0.00"W
    String latitude = "51.5";
    String longitude = "0.11";

    Scanner s = setUpJTSFilter(getSerializedScanner(), latitude, longitude, false);
    s.setRange(new Range());

//    System.out.println("{" + checkSerialized(s) + "}");
    assertTrue(checkSerialized(s).isEmpty());
  }


  @Test
  public void testOneResultAmerica()
  {
    //This is North America
    //Points  39°44'21.00"N 104°59'3.00"W (Denver) are in the footprint
    String latitude = "33";
    String longitude = "-93.0";

    Scanner s = setUpJTSFilter(getSerializedScanner(), latitude, longitude, false);
    s.setRange(new Range());

    System.out.println("{" + checkSerialized(s) + "}");
    assertTrue(checkSerialized(s).equals("02"));
  }


  @Test
  public void testOneResultAustralia()
  {
    //This is Australia
    //Points like 22S 135E are in the beam
    String latitude = "-9";
    String longitude = "100.0";

    Scanner s = setUpJTSFilter(getSerializedScanner(), latitude, longitude, false);
    s.setRange(new Range());

    System.out.println("{" + checkSerialized(s) + "}");
    assertTrue(checkSerialized(s).equals("01"));
  }

  @Test
  public void testOneResultHawaii()
  {
    // -164 40 - somewhere near hawaii

    //This is Australia
    //Points like 22S 135E are in the beam
    String latitude = "40";
    String longitude = "-164.0";

    Scanner s = setUpJTSFilter(getSerializedScanner(), latitude, longitude, true);
    s.setRange(new Range());

    System.out.println("{" + checkSerialized(s) + "}");
    assertTrue(checkSerialized(s).equals("03"));
  }


  @Test
  public void testDummyTest()
  {
    assertTrue(true);
  }

}
