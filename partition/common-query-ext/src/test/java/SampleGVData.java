
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
public class SampleGVData
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
    item.put("a",  "a");
    item.put("b",  "b");

    //This one is like RB
    item.put("date-start",  "2009-01-01");
    item.put("date-end",    "2011-02-24");
    item.put("date-update", "2011-02-24T00:00:00Z");
    item.put("frequency",  "1250000000");
    item.put("bandwidth",   "500000000");
    item.put("version",     "1");
    list.add(item);

    item = new HashMap<String, String>();
    item.put("a",  "a");
    item.put("b",  "b");
    list.add(item);

    //This one is like GV
    item = new HashMap<String, String>();
    item.put("a",  "a");
    item.put("b",  "b");
    item.put("date-start",  "2010-01-01");
    item.put("date-update", "2010-01-23");
    item.put("frequency",  "1150000000");
    item.put("bandwidth",   "300000000");
    list.add(item);

    item = new HashMap<String, String>();
    item.put("a",  "a");
    item.put("b",  "b");
    item.put("date-start",  "2009-01-01");
    item.put("date-end",    "2011-02-24");
    item.put("date-update", "2008-01-23");
    list.add(item);

    item = new HashMap<String, String>();
    item.put("a",  "a");
    item.put("b",  "b");
    list.add(item);

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
