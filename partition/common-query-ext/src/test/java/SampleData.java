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


public class SampleData {
	public static int NUM_PARTITIONS = 2;
	public static int NUM_SAMPLES = 10;
	
	public static Connector initConnector() {
		Instance instance = new MockInstance();
		
		try {
			Connector connector = instance.getConnector("root", "password".getBytes());
			
			// set up table
			connector.tableOperations().create("partition");
			connector.tableOperations().create("provenance");
			
			// set up root's auths
			connector.securityOperations().changeUserAuthorizations("root", new Authorizations("ALPHA,BETA,GAMMA".split(",")));
			
			return connector;
		} catch (CBException e) {
			e.printStackTrace();
		} catch (CBSecurityException e) {
			e.printStackTrace();
		} catch (TableExistsException e) {
			e.printStackTrace();
		}
		
		return null;
	}
	
	public static Collection<Map<String, String>> sampleData() {
		List<Map<String, String>> list = new ArrayList<Map<String, String>>();
		Map<String, String> item;
		
		for (int i = 0; i < NUM_SAMPLES; i++) {
			item = new HashMap<String, String>();
			for (int j = 0; j < 5; j++) {
				item.put("field" + j , new String(new char[] {(char) ('A' + ((j + i) % 26))}));
			}
			list.add(item);
		}
		return list;
	}
	
	public static void writeDenCellLevel(Connector connector, Collection<Map<String, String>> data) {
		// write sample data
		MultiTableBatchWriter mtbw = connector.createMultiTableBatchWriter(200000, 10000, 1);
		try {
			BatchWriter writer;
			if (mtbw != null) {
				writer = mtbw.getBatchWriter("partition");
			} else {
				writer = connector.createBatchWriter("partition", 200000, 10000, 1);
			}
			int count = 0;
			Mutation m;
			for (Map<String, String> object: data) {
				count++;
				String id = (count < 10 ? "0" + count: "" + count);
				Text partition = new Text("" + (count % NUM_PARTITIONS));
				
				// write dummy record
				m = new Mutation(partition);
				m.put("event", id, "");
				writer.addMutation(m);
				
				for (Entry<String, String> entry: object.entrySet()) {
					// write the event mutation
					m = new Mutation(partition);
					m.put("event", id + "\u0000" + entry.getKey(), entry.getValue());
					writer.addMutation(m);
					
					// write the general index mutation
					m = new Mutation(partition);
					m.put("index", entry.getValue() + "\u0000" + id, "");
					writer.addMutation(m);
					
					// write the specific index mutation
					m = new Mutation(partition);
					m.put("index", entry.getKey() + "//" + entry.getValue() + "\u0000" + id, "");
					writer.addMutation(m);
				}
			}
			writer.close();
		} catch (CBException e) {
			e.printStackTrace();
		} catch (CBSecurityException e) {
			e.printStackTrace();
		} catch (TableNotFoundException e) {
			e.printStackTrace();
		}
	}
	
	public static void writeDenSerialized(Connector connector, Collection<Map<String, String>> data) {
		// write sample data
		MultiTableBatchWriter mtbw = connector.createMultiTableBatchWriter(200000, 10000, 1);
		try {
			BatchWriter writer;
			if (mtbw != null) {
				writer = mtbw.getBatchWriter("partition");
			} else {
				writer = connector.createBatchWriter("partition", 200000, 10000, 1);
			}
			int count = 0;
			Mutation m;
			for (Map<String, String> object: data) {
				count++;
				String id = (count < 10 ? "0" + count: "" + count);
				Text partition = new Text("" + (count % NUM_PARTITIONS));
				
				StringBuilder value = new StringBuilder();
				boolean first = true;
				for (Entry<String, String> entry: object.entrySet()) {
					if (!first) {
						value.append("\u0000");
					} else {
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
		} catch (CBException e) {
			e.printStackTrace();
		} catch (CBSecurityException e) {
			e.printStackTrace();
		} catch (TableNotFoundException e) {
			e.printStackTrace();
		}
	}
	
	public static void writeDenProvenance(Connector connector) {
		// write sample data
		MultiTableBatchWriter mtbw = connector.createMultiTableBatchWriter(200000, 10000, 1);
		try {
			BatchWriter writer;
			if (mtbw != null) {
				writer = mtbw.getBatchWriter("provenance");
			} else {
				writer = connector.createBatchWriter("provenance", 200000, 10000, 1);
			}
			Mutation m;
			for (int sid = 1; sid <= 2; sid++) {
				for (int time = 1; time <= 3; time++) {
					for (int uuid = 1; uuid <= (6 + 2 * time); uuid++) {
						m = new Mutation(new Text("sid" + sid));
						m.put("time" + time, "uuid-" + Integer.toHexString(uuid), "");
						writer.addMutation(m);
					}
				}
			}
			writer.close();
		} catch (CBException e) {
			e.printStackTrace();
		} catch (CBSecurityException e) {
			e.printStackTrace();
		} catch (TableNotFoundException e) {
			e.printStackTrace();
		}
	}
	
	public static void writeMinIndexes(Connector connector) {
		// write sample data
		MultiTableBatchWriter mtbw = connector.createMultiTableBatchWriter(200000, 10000, 1);
		try {
			BatchWriter writer;
			if (mtbw != null) {
				writer = mtbw.getBatchWriter("partition");
			} else {
				writer = connector.createBatchWriter("partition", 200000, 10000, 1);
			}
			Mutation m;
			for (int i = 1; i <= NUM_SAMPLES; i++) {
				m = new Mutation(new Text("" + (i % NUM_PARTITIONS)));
				
				String id = (i < 10 ? "0" + i: "" + i);
				
				m.put("index", "z_" + id + "_rdate\u0000" + id, "");
				writer.addMutation(m);
			}
			writer.close();
		} catch (CBException e) {
			e.printStackTrace();
		} catch (CBSecurityException e) {
			e.printStackTrace();
		} catch (TableNotFoundException e) {
			e.printStackTrace();
		}
	}
}
