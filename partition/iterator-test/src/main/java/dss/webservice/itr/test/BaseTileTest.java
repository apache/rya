package dss.webservice.itr.test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import cloudbase.core.client.BatchScanner;
import cloudbase.core.client.Connector;
import cloudbase.core.data.Key;
import cloudbase.core.data.Range;
import cloudbase.core.data.Value;
import cloudbase.core.security.Authorizations;
import dss.webservice.itr.Test;

public class BaseTileTest implements Test {
	private static final Logger logger = Logger.getLogger(BaseTileTest.class);
	
	String comboIndexTable = "index_v2";
	String type = "hpcp";
	
	@Override
	public void runTest(Map<String, String> request, Connector connector, String table, String auths) {
		if (!request.containsKey("dates")) {
			logger.warn("No 'dates' parameter supplied. e.g. dates=20100720,20100721...");
			return;
		}
		
		if (request.containsKey("type")) {
			type = request.get("type");
		}
		
		String[] dates = request.get("dates").split(",");		
		
		List<Long> comboTimes = new ArrayList<Long>();
		List<Long> partTimes = new ArrayList<Long>();
		List<Long> comboCounts = new ArrayList<Long>();
		List<Long> partCounts = new ArrayList<Long>();
		List<String> errors = new ArrayList<String>();
		try {
			for (String date: dates) {
				long rdate = 99999999 - Long.parseLong(date);
				for (int g = 0; g < 8; g++) {
					String begin = type + "//rdate:" + rdate + "//geokey:" + g;
					String end   = type + "//rdate:" + rdate + "//geokey:" + (g+1);
					long count = 0;
					Set<Range> ranges = new HashSet<Range>();

					logger.info("Running test for " + begin + " ...");
					// run combo index test
					BatchScanner reader = connector.createBatchScanner(table, new Authorizations(auths.split(",")), 30);
					ranges.add(new Range(new Key(new Text(begin)), true, new Key(new Text(end)), false));
					
					reader.setRanges(ranges);
					long start = System.currentTimeMillis();
					for (Entry<Key, Value> entry: reader) {
						count++;
					}
					comboTimes.add(System.currentTimeMillis() - start);
					comboCounts.add(count);
					
					logger.info("\tC count=" + count + " time=" + comboTimes.get(comboTimes.size() - 1) + " ms");
					
					count = 0;
					
					// run partition index test
//					reader = connector.createBatchScanner(table, new Authorizations(auths.split(",")), 30);
//					
//					reader.setScanIterators(3, SortedRangeIterator.class.getName(), "ri");
//					reader.setScanIteratorOption("ri", SortedRangeIterator.OPTION_LOWER_BOUND, begin.replace("geokey", "geoKey"));
//					reader.setScanIteratorOption("ri", SortedRangeIterator.OPTION_UPPER_BOUND, end.replace("geokey", "geoKey"));
//					
//					ranges.clear();
//					ranges.add(new Range(new Key(new Text("date:" + date)), true, new Key(new Text("date:" + date + "z")), false));
//					reader.setRanges(ranges);
//					
//					start = System.currentTimeMillis();
//					for (Entry<Key, Value> entry: reader) {
//						count++;
//					}
//					partTimes.add(System.currentTimeMillis() - start);
//					partCounts.add(count);
//					
//					if (count != comboCounts.get(comboCounts.size() - 1)) {
//						String msg = "Counts differed for " + begin + " C: " + comboCounts.get(comboCounts.size() - 1) + " P: " + count; 
//						logger.warn(msg);
//						errors.add(msg);
//					}
//					logger.info("\tP count=" + count + " time=" + partTimes.get(partTimes.size() - 1) + " ms");
				}
			}
			
			logger.info("********************* RESULTS *********************");
			logger.info("Tested all 0 level tiles on " + type + " for " + request.get("dates"));
			//logger.info("This is a test of SortedRangeIterator performance");
			
			double comboSum = 0, partSum = 0;
			for (int i = 0; i < comboTimes.size(); i++) {
				comboSum += comboTimes.get(i);
				//partSum += partTimes.get(i);
			}
			
			logger.info("Average C Time: " + (comboSum / comboTimes.size()) + " ms");
			//logger.info("Average P Time: " + (partSum / partTimes.size()) + " ms");
			
			comboSum = 0; 
			partSum = 0;
			
			for (int i = 0; i < comboCounts.size(); i++) {
				comboSum += comboCounts.get(i);
				//partSum += partCounts.get(i);
			}
			
			logger.info("Average C Count: " + (comboSum / comboCounts.size()));
			//logger.info("Average P Count: " + (partSum / partCounts.size()));
			
			if (errors.size() > 0) {
				logger.warn("ERRORS!!!:");
				for (String e: errors) {
					logger.warn(e);
				}
			}
		} catch (Exception e) {
			logger.error(e, e);
		}
	}
}
