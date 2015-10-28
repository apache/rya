/*
 * This is a filter for some basic Geo Functionality for data stored in a WKT format
 */
package ss.cloudbase.core.iterators.filter.jts;

import ss.cloudbase.core.iterators.filter.CBConverter;

import java.util.Map;

import org.apache.log4j.Logger;

import cloudbase.core.data.Key;
import cloudbase.core.data.Value;
import cloudbase.core.iterators.filter.Filter;

import com.vividsolutions.jts.io.WKTReader;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.CoordinateSequence;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.impl.CoordinateArraySequenceFactory;

/**
 * @author Raju Shah
 */
public class JTSFilter implements Filter
{

  private static final Logger logger = Logger.getLogger(JTSFilter.class);
  /** The string that indicates the key name in the row value. */
  public static final String OPTIONGeometryKeyName = "GeometryKeyName";
  protected String GeometryKeyName = "geometry-contour";
  /** The string that is the centerpoint - Latitude. */
  public static final String OPTIONCenterPointLat = "latitude";
  protected String CenterPointLat = "0.0";
  /** The string that is the centerpoint - Longitude. */
  public static final String OPTIONCenterPointLon = "longitude";
  protected String CenterPointLon = "0.0";
  public static final String OPTIONBeamIDName = "BeamID";
  protected String BeamIDKeyName = "beam-globalviewid";
  /** The string that is the centerpoint - Latitude. */
  /** The compare type for the geometric point **/
  protected Point p = null;
  CBConverter cbconvertor = new CBConverter();

  /**
   * Whether or not to accept this key/value entry. A map of row keys and values is parsed and then sent off to the process function to be evaluated.
   * @param key The cloudbase entry key
   * @param value The cloudbase entry value
   * @return True if the entry should be included in the results, false otherwise
   */
  @Override
  public boolean accept(Key CBKey, Value CBValue)
  {
    boolean return_value = false;
    Map<String, String> CBRecord = cbconvertor.toMap(CBKey, CBValue);

    String s = (String) CBRecord.get(GeometryKeyName);

    // I expect the field to exist
    if ((s == null) || (s.length() < 1))
    {
      return return_value;
    }

    // If the object cotains the word POLYGON or MULTIPOLYGON then it should be good
    if (s.contains("POLYGON"))
    {
      //convert that string into a geometry
      WKTReader reader = new WKTReader();
      try
      {
        Geometry WKTgeometry = reader.read(s);

        //See if the two geometries overlap
        return_value = p.coveredBy(WKTgeometry);
      }
      catch (Exception e)
      {
          try
          {
            String beamid = (String) CBRecord.get(BeamIDKeyName);
            logger.debug("Bad Beam ID ["+beamid + "]");
            //See if the two geometries overlap
          }
          catch (Exception ex)
          {
          }

        //logger.error(e, e);
        return return_value;
      }
    }
    else
    {
      String start_s = "SDO_ORDINATE_ARRAY(";
      int start_index = s.indexOf(start_s);
      if (start_index != -1)
      {
        start_index += start_s.length();

        int end_index = s.indexOf(")", start_index);

        if (end_index == -1)
        {
          return false;
        }
        s = s.substring(start_index, end_index);
        //System.out.println("{" + s + "}");

        //remove every other ,
        // want to search for -70.838, 39.967, and replace with -70.838 39.967,
        start_index = 1;
        end_index = s.length();
        while ((start_index < (end_index - 1)) && (start_index > 0))
        {
          start_index = s.indexOf(",", start_index);
          char[] temp = s.toCharArray();
          temp[start_index] = ' ';
          s = new String(temp);
          //skip the next one
          start_index = s.indexOf(",", start_index) + 1;
        }
        //System.out.println("<" + s + ">");

        //convert that string into a geometry
        WKTReader reader = new WKTReader();
        try
        {
          Geometry WKTgeometry = reader.read("POLYGON((" + s + "))");

          //See if the two geometries overlap
          return_value = p.coveredBy(WKTgeometry);
        }
        catch (Exception e)
        {
          //logger.error(e, e);
          return return_value;
        }
      }
    }
    return return_value;
  }

  @Override
  public void init(Map<String, String> options)
  {
    cbconvertor.init(options);

    GeometryKeyName = options.get(OPTIONGeometryKeyName);
    if (GeometryKeyName == null || GeometryKeyName.length() == 0)
    {
      GeometryKeyName = "geometry-contour";
    }


    CenterPointLat = options.get(OPTIONCenterPointLat);
    if (CenterPointLat == null || CenterPointLat.length() == 0)
    {
      CenterPointLat = "0.0";
    }


    CenterPointLon = options.get(OPTIONCenterPointLon);
    if (CenterPointLon == null || CenterPointLon.length() == 0)
    {
      CenterPointLon = "0.0";
    }

    BeamIDKeyName = options.get(OPTIONBeamIDName);
    if (BeamIDKeyName == null || BeamIDKeyName.length() == 0)
    {
      BeamIDKeyName = "beam-globalviewid";
    }

    Double CenterPointLatD = Double.parseDouble(CenterPointLat);
    Double CenterPointLonD = Double.parseDouble(CenterPointLon);

    Coordinate[] coordinates =
    {
      new Coordinate(CenterPointLonD, CenterPointLatD)
    };

    CoordinateSequence cs = CoordinateArraySequenceFactory.instance().create(coordinates);
    GeometryFactory gf = new GeometryFactory();

    p = new Point(cs, gf);
  }
}
