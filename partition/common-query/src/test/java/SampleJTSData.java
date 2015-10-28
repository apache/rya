
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.io.Text;

import cloudbase.core.client.BatchWriter;
import cloudbase.core.client.CBException;
import cloudbase.core.client.CBSecurityException;
import cloudbase.core.client.Connector;
import cloudbase.core.client.Instance;
import cloudbase.core.client.MultiTableBatchWriter;
import cloudbase.core.client.TableExistsException;
import cloudbase.core.client.TableNotFoundException;
import cloudbase.core.client.mock.MockInstance;
import cloudbase.core.data.Mutation;
import cloudbase.core.security.Authorizations;

// For use in testing the Date Filter and Frequency Filter classes
public class SampleJTSData
{

  public static int NUM_PARTITIONS = 2;


  public static Connector initConnector()
  {
    Instance instance = new MockInstance();

    try
    {
      Connector connector = instance.getConnector("root", "password".getBytes());

      // set up table
      connector.tableOperations().create("partition");

      // set up root's auths
      connector.securityOperations().changeUserAuthorizations("root", new Authorizations("ALPHA,BETA,GAMMA".split(",")));

      return connector;
    }
    catch (CBException e)
    {
      e.printStackTrace();
    }
    catch (CBSecurityException e)
    {
      e.printStackTrace();
    }
    catch (TableExistsException e)
    {
      e.printStackTrace();
    }

    return null;
  }

  public static Collection<Map<String, String>> sampleData()
  {
    List<Map<String, String>> list = new ArrayList<Map<String, String>>();
    Map<String, String> item;

    item = new HashMap<String, String>();
    item.put("geometry-contour",  "SDO_GEOMETRY(2007, 8307, NULL, SDO_ELEM_INFO_ARRAY(1, 1003, 1), SDO_ORDINATE_ARRAY(91.985, -12.108, 94.657, -12.059, 98.486, -11.988, 101.385, -12.296, 102.911, -12.569, 103.93, -12.852, 105.005, -12.531, 106.37, -12.204, 108.446, -11.503, 109.585, -10.88, 110.144, -10.207, 108.609, -9.573, 106.05, -8.535, 104.145, -7.606, 102.191, -7.522, 99.522, -7.691, 97.64, -7.606, 95.482, -7.947, 94.546, -8.084, 92.465, -8.605, 90.554, -9.366, 90.197, -10.436, 89.84, -11.729, 90.554, -12.175, 91.985, -12.108))");
    item.put("beam-name",    "OPTUS D1 Ku-BAND NATIONAL A & B AUSTRALIA Downlink");
    list.add(item);
    //This is Australia
    //Points like 22S 135E are in the beam

    //This one is like GV
    item = new HashMap<String, String>();
    item.put("beam-name",  "AMC 1 Ku-BAND ZONAL NORTH AMERICA Down HV");
    item.put("geometry-contour",   "SDO_GEOMETRY(2007, 8307, NULL, SDO_ELEM_INFO_ARRAY(1, 1003, 1), SDO_ORDINATE_ARRAY(-70.838, 39.967, -70.506, 40.331, -70.698, 41.679, -71.179, 42.401, -71.578, 42.38, -72.994, 42.924, -74.353, 43.242, -75.715, 43.26, -77.318, 42.981, -78.684, 42.774, -80.05, 42.491, -82.005, 42.517, -83.608, 42.312, -84.977, 41.805, -86.58, 41.525, -88.127, 41.02, -89.731, 40.741, -90.905, 41.582, -92.264, 41.9, -93.861, 42.147, -95.411, 41.341, -96.257, 40.076, -97.222, 38.737, -98.011, 37.17, -98.031, 35.593, -97.691, 34.312, -96.875, 33.25, -97.307, 31.904, -97.916, 30.561, -98.702, 29.295, -99.134, 27.949, -98.14, 26.884, -97.205, 25.821, -95.842, 25.803, -94.42, 25.784, -92.876, 26.064, -91.277, 26.043, -90.085, 26.553, -88.729, 26.01, -87.38, 24.941, -86.031, 23.797, -84.616, 23.253, -83.256, 23.01, -81.887, 23.517, -80.866, 24.555, -80.254, 26.124, -79.642, 27.693, -78.444, 28.728, -77.486, 29.542, -76.463, 30.805, -76.088, 32.377, -75.656, 33.723, -76.051, 35.305, -75.442, 36.649, -74.426, 37.386, -73.228, 38.422, -72.032, 39.232, -70.838, 39.967))");
    list.add(item);
    //This is North America
    //Points  39°44'21.00"N 104°59'3.00"W (Denver) are in the footprint

    item = new HashMap<String, String>();
    item.put("beam-name",  "testa");
    item.put("beam-footprint",   "MULTIPOLYGON (((-169.286 40.431, -164.971 39.992, -155.397 38.482, -146.566 36.233, -136.975 32.539, -128.124 27.742, -121.946 24.548, -116.849 21.339, -112.156 17.479, -109.391 14.206, -107.301 11.715, -105.274 9.477, -103.443 8.229, -102.108 7.7, -99.109 7.428, -96.681 7.745, -93.894 8.843, -89.917 11.687, -85.953 15.017, -81.148 17.266, -78.145 17.986, -75.582 17.887, -68.1 17.987, -64.696 18.493, -61.445 19.38, -60.094 20.288, -59.315 21.564, -57.026 26.51, -55.089 30.962, -53.59 33.657, -52.495 34.691, -50.468 36.204, -46.146 38.672, -41.684 40.663, -37.914 42.055, -33.806 43.082, -27.523 44.149, -21.645 44.96, -16.578 45.406, -13.807 45.771, -14.929 50.108, -16.186 53.919, -17.051 56.0, -18.388 58.824, -19.861 61.567, -21.807 64.188, -23.104 65.742, -25.28 67.904, -27.699 69.823, -28.955 70.728, -32.415 72.768, -34.968 73.998, -38.468 75.309, -48.292 73.025, -56.545 71.12, -64.023 70.474, -72.753 70.357, -78.41 70.827, -80.466 71.093, -82.412 71.876, -83.02 72.944, -83.175 74.04, -82.493 74.782, -82.412 75.552, -82.697 76.778, -84.041 78.398, -86.316 81.078, -104.098 80.819, -110.861 80.482, -115.73 80.17, -120.936 79.669, -125.84 79.176, -126.696 79.02, -134.316 77.732, -139.505 76.478, -144.823 74.826, -148.231 73.417, -151.517 71.687, -153.87 70.165, -154.536 69.672, -155.868 68.678, -156.482 68.098, -158.281 66.421, -159.716 64.804, -160.996 63.126, -161.878 61.786, -163.046 59.875, -164.369 57.254, -165.563 54.479, -166.73 51.089, -167.811 47.267, -168.581 44.041, -169.286 40.431)), ((-171.333 23.244, -171.523 18.894, -170.127 18.986, -161.559 18.555, -156.977 18.134, -153.574 18.116, -151.108 18.324, -149.947 18.45, -149.018 18.957, -148.515 19.822, -148.524 20.914, -149.018 21.766, -149.947 22.272, -152.185 23.054, -155.563 23.434, -158.075 23.75, -160.272 24.034, -162.184 24.008, -163.514 23.99, -164.595 23.976, -166.52 23.687, -169.159 23.18, -171.333 23.244)))");
    list.add(item);
// this point should be in there...
    // -164 40 - somewhere near hawaii

    item = new HashMap<String, String>();
    item.put("beam-name",  "testb");
    item.put("beam-footprint",   "POLYGON ((-140.153 34.772, -140.341 33.272, -137.024 33.026, -132.723 32.369, -130.947 31.916, -128.664 31.225, -125.293 29.612, -121.813 27.871, -118.699 25.892, -115.589 23.79, -112.593 21.875, -109.136 19.335, -106.939 16.701, -105.006 14.97, -104.195 14.407, -103.049 13.659, -100.363 12.717, -98.063 12.288, -94.299 11.612, -90.825 11.097, -87.997 11.584, -86.815 12.109, -86.163 12.893, -85.014 14.342, -83.804 15.788, -82.104 16.998, -80.413 17.269, -78.005 16.574, -76.181 16.531, -74.65 16.68, -73.552 17.392, -72.957 18.3, -72.917 19.651, -73.526 21.325, -74.913 23.018, -76.036 24.519, -76.159 26.428, -75.741 28.447, -74.257 30.072, -72.771 31.331, -70.517 34.328, -69.638 36.04, -68.624 39.467, -68.015 41.851, -67.607 43.501, -67.548 45.528, -67.586 47.308, -68.601 49.066, -69.868 50.07, -71.621 50.778, -73.285 50.888, -74.9 50.926, -76.994 50.975, -79.332 50.846, -81.066 50.887, -83.842 51.136, -86.569 51.016, -87.95 50.864, -90.831 50.563, -94.27 50.644, -98.068 50.733, -102.937 51.032, -106.455 51.484, -109.973 51.936, -114.119 52.402, -117.363 53.031, -119.899 53.276, -123.243 53.539, -127.017 54.427, -130.519 55.431, -133.643 56.058, -134.826 56.279, -135.354 55.029, -135.792 53.864, -136.168965072136 52.8279962761917, -136.169 52.828, -136.169497186166 52.8264970826432, -136.192 52.763, -136.556548517884 51.6453176911637, -136.703232746756 51.2152965828266, -136.781220290925 50.9919311116929, -136.793 50.959, -136.80274055379 50.9259886895048, -136.992 50.295, -137.200898649547 49.5808675274021, -137.202 49.581, -137.200962495599 49.5806459535167, -137.360714473458 49.0197683891632, -137.459 48.677, -137.462166719028 48.6649126473121, -137.471 48.634, -137.515105536699 48.4619710228524, -137.74710368039 47.5528216167105, -137.793718522461 47.3758260237407, -137.854 47.152, -137.977773277882 46.6610808974241, -138.044 46.403, -138.330834102374 45.1674736036557, -138.365 45.019, -138.38180854655 44.9421315900087, -138.449801069917 44.6389849661384, -138.485 44.484, -138.497077239724 44.4262941289417, -138.536 44.25, -138.622787032392 43.8206200438395, -138.743816168807 43.232032787661, -138.981390224617 42.0843314825185, -138.989 42.048, -138.990605533614 42.0389442888447, -138.991 42.037, -138.997785044232 41.9994454595406, -139.004 41.969, -139.035645873997 41.7890661698517, -139.061212567475 41.6462082823816, -139.428 39.584, -139.673 38.073, -139.713116752585 37.8001474769807, -139.766 37.457, -139.764942047737 37.4567768906428, -139.898 36.573, -139.897723683259 36.5729429963606, -139.986 35.994, -140.04777653037 35.5462970502163, -140.094 35.232, -140.090797568766 35.2315144621917, -140.153 34.772))");
    list.add(item);



    //London is in niether - 51°30'0.00"N   0° 7'0.00"W
    return list;
  }


  public static void writeDenSerialized(Connector connector, Collection<Map<String, String>> data)
  {
    // write sample data
    MultiTableBatchWriter mtbw = connector.createMultiTableBatchWriter(200000, 10000, 1);
    try
    {
      BatchWriter writer;
      if (mtbw != null)
      {
        writer = mtbw.getBatchWriter("partition");
      }
      else
      {
        writer = connector.createBatchWriter("partition", 200000, 10000, 1);
      }
      int count = 0;
      Mutation m;
      for (Map<String, String> object : data)
      {
        count++;
        String id = (count < 10 ? "0" + count : "" + count);
        Text partition = new Text("" + (count % NUM_PARTITIONS));

        StringBuilder value = new StringBuilder();
        boolean first = true;
        for (Entry<String, String> entry : object.entrySet())
        {
          if (!first)
          {
            value.append("\u0000");
          }
          else
          {
            first = false;
          }
          value.append(entry.getKey());
          value.append("\uFFFD");
          value.append(entry.getValue());

          // write the general index mutation
          m = new Mutation(partition);
          m.put("index", entry.getValue() + "\u0000" + id, "");
          writer.addMutation(m);

          // write the specific index mutation
          m = new Mutation(partition);
          m.put("index", entry.getKey() + "//" + entry.getValue() + "\u0000" + id, "");
          writer.addMutation(m);
        }

        // write the event mutation
        m = new Mutation(partition);
        m.put("event", id, value.toString());
        writer.addMutation(m);
      }
      writer.close();
    }
    catch (CBException e)
    {
      e.printStackTrace();
    }
    catch (CBSecurityException e)
    {
      e.printStackTrace();
    }
    catch (TableNotFoundException e)
    {
      e.printStackTrace();
    }
  }
}
