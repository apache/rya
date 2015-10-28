package dss.webservice.itr.test;

import java.util.Map;

import org.apache.hadoop.io.Text;

import cloudbase.core.client.BatchWriter;
import cloudbase.core.client.CBException;
import cloudbase.core.client.CBSecurityException;
import cloudbase.core.client.Connector;
import cloudbase.core.client.MultiTableBatchWriter;
import cloudbase.core.client.TableNotFoundException;
import cloudbase.core.data.Mutation;
import cloudbase.core.data.Value;
import cloudbase.core.security.ColumnVisibility;
import dss.webservice.itr.Test;

public class AddTestRecords implements Test {

	@Override
	public void runTest(Map<String, String> request, Connector connector, String table, String auths) {
		MultiTableBatchWriter mtbw = connector.createMultiTableBatchWriter(200000000, 500, 4);
		try {
			BatchWriter writer = mtbw.getBatchWriter(table);
			Mutation m = new Mutation(new Text("elint//rdate:79899179//geokey:20223312022200"));
			m.put(new Text("event"), new Text("02eacfa1-b548-11df-b72e-002219501672"), new ColumnVisibility(new Text("U&FOUO")), new Value("uuid~event\uFFFD02eacfa1-b548-11df-b72e-002219501672\u0000date\uFFFD20100820\u0000time~dss\uFFFD010226.000\u0000technology\uFFFDelint\u0000feedName\uFFFDParserBinarySpSigFlat\u0000systemName\uFFFDSP\u0000pddg\uFFFDBJ\u0000latitude\uFFFD46.79429069085071\u0000longitude\uFFFD9.852863417535763\u0000altitude\uFFFD1841.0\u0000geoerror~semimajor\uFFFD3709.1270902747297\u0000geoerror~semiminor\uFFFD1896.9438653491684\u0000geoerror~tilt\uFFFD68.68795738630202\u0000frequency\uFFFD\u0000cenot_elnot\uFFFD008LJ\u0000datetime\uFFFD2010-08-20T01:02:26.000Z".getBytes()));
			
			writer.addMutation(m);
			mtbw.flush();
			mtbw.close();
		} catch (CBException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (CBSecurityException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (TableNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
