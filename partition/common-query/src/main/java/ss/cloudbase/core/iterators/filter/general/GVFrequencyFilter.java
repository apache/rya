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

/**
 * This filter will take an incoming frequency and match that to a range
 * contained within the cloudbase record
 *
 * @author Raju Shah
 */
public class GVFrequencyFilter implements Filter
{

  private static final Logger LOG = Logger.getLogger(GVFrequencyFilter.class);
  /** The string that indicates the key name in the row value. */
  public static final String OPTIONFrequency = "frequency";
  protected String Frequency_S = "0.0";
  protected Double Frequency_D = Double.parseDouble(Frequency_S);
  // Initially the values in Global Vision are just Center Freq and BW
  // On the second revision we may change that to the actual ranges so 
  // the numerical computations below can be optimized out.  Then we can just use the normal OGC filters
  //public static final String OPTIONGVFrequencyStart = "Frequency_Start";
  //public static final String OPTIONGVFrequencyEnd   = "Frequency_End";
  public static final String OPTIONGVCenterFrequency = "frequency";
  public static final String OPTIONGVBandwidth = "bandwidth";
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
    LOG.trace("Accept");

    boolean return_value = false;
    Map<String, String> CBRecord = cbconvertor.toMap(CBKey, CBValue);

    try
    {
      String s1 = (String) CBRecord.get(OPTIONGVCenterFrequency);
      String s2 = (String) CBRecord.get(OPTIONGVBandwidth);

      Double d1 = Double.parseDouble(s1);
      Double d2 = Double.parseDouble(s2);

      if (((d1 - (0.5 * d2)) <= Frequency_D) && (Frequency_D <= (d1 + (0.5 * d2))))
      {
        return_value = true;
      }

    }
    catch (Exception e)
    {
      return_value = false;
    }

    return return_value;
  }

  @Override
  public void init(Map<String, String> options)
  {
    LOG.trace("Init");

    cbconvertor.init(options);

    Frequency_S = options.get(OPTIONFrequency);
    if (Frequency_S == null || Frequency_S.length() == 0)
    {
      Frequency_S = "0.0";
    }


    Frequency_D = Double.parseDouble(Frequency_S);
  }
}
