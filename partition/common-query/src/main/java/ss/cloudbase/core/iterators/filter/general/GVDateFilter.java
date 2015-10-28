/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package ss.cloudbase.core.iterators.filter.general;

import ss.cloudbase.core.iterators.filter.CBConverter;

import java.util.Map;

import org.apache.log4j.Logger;

import cloudbase.core.data.Key;
import cloudbase.core.data.Value;
import cloudbase.core.iterators.filter.Filter;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
 * This filter will take an incoming frequency and match that to a range
 * contained within the cloudbase record
 *
 * @author Raju Shah
 */
public class GVDateFilter implements Filter
{

  private static final Logger LOG = Logger.getLogger(GVDateFilter.class);
  /** The string that indicates the key name in the row value. */
  public static final String OPTIONInTimestamp = "InDate";
  protected String TimeStamp_S = "2011-03-03 20:44:28.633";
  protected Timestamp TimeStamp_T = Timestamp.valueOf(TimeStamp_S);
  public static final String OPTIONGVTimeStartField = "date-start";
  protected String DateStartField = "date-start";
  public static final String OPTIONGVTimeEndField = "date-end";
  protected String DateEndField = "date-end";
  public static final String OPTIONRBActive = "RBCurrentlyActive";
  protected String RBActive = "version";
  CBConverter cbconvertor = new CBConverter();

  public long GetUSecFromString(String Time_S)
  {
    long return_value = 0;
    Date d = null;
    Calendar c = Calendar.getInstance();
    SimpleDateFormat df_long = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
    SimpleDateFormat df_med = new SimpleDateFormat("yyyy-MM-dd");

    try
    {
      d = df_long.parse(Time_S);
    }
    catch (Exception e)
    {
      try
      {
        d = df_med.parse(Time_S);
      }
      catch (Exception e1)
      {
        System.out.println("Don't like it [" + Time_S + "]");
        return return_value;
      }
    }
    c.setTime(d);
    return_value = c.getTimeInMillis();

    return return_value;
  }

  /**
   * Whether or not to accept this key/value entry. A map of row keys and values is parsed and then sent off to the process function to be evaluated.
   * @param key The cloudbase entry key
   * @param value The cloudbase entry value
   * @return True if the entry should be included in the results, false otherwise
   */
  @Override
  public boolean accept(Key CBKey, Value CBValue)
  {
    LOG.trace("accept");

    boolean return_value = false;

    Map<String, String> CBRecord = cbconvertor.toMap(CBKey, CBValue);

    // Get the Date Strings
    String sStart = (String) CBRecord.get(DateStartField);
    Timestamp tStart = new Timestamp(0);
    String sEnd = (String) CBRecord.get(DateEndField);
    Timestamp tEnd = new Timestamp(0);

    //Get Active Strings
    String rbActive = (String) CBRecord.get(RBActive);

    //LOGIC
    //1) If The signal is NOT ACTIVE (I.E. the active flag is specified and off) PUNT
    if ( ((rbActive != null) && rbActive.equals("0")) )
    {
      return return_value;
    }
    //1) Remaining signals are either specified ACTIVE or NOT INDICATED


    //LOGIC
    //2) Next check if both start and end are specified, then it must be inbetween
    if ((sStart != null) && (sEnd != null))
    {
      tStart.setTime(GetUSecFromString(sStart));
      tEnd.setTime(GetUSecFromString(sEnd));
      if (tStart.before(TimeStamp_T) && TimeStamp_T.before(tEnd))
      {
        return_value = true;
      }
      return return_value;
    }


    //LOGIC
    //3) If the start date is specified then just check against start date
    if (sStart != null)
    {
      tStart.setTime(GetUSecFromString(sStart));
      if (tStart.before(TimeStamp_T))
      {
        return_value = true;
      }
      return return_value;
    }

    //LOGIC
    //4) Return false for all others - Start Date must be present


    return return_value;
  }

  @Override
  public void init(Map<String, String> options)
  {
    LOG.trace("init");
    cbconvertor.init(options);

    DateStartField = options.get(OPTIONGVTimeStartField);
    if (DateStartField == null || DateStartField.length() == 0)
    {
      DateStartField = "date-start";
    }


    DateEndField = options.get(OPTIONGVTimeEndField);
    if (DateEndField == null || DateEndField.length() == 0)
    {
      DateEndField = "date-end";
    }


    TimeStamp_S = options.get(OPTIONInTimestamp);
    if (TimeStamp_S == null || TimeStamp_S.length() == 0)
    {
      TimeStamp_S = "2011-03-03T20:44:28.633Z";
    }
    TimeStamp_T.setTime(GetUSecFromString(TimeStamp_S));


    LOG.debug("Creating Time Filter, does  " + TimeStamp_S + " = " + TimeStamp_T.toString());
  }
}
