package dss.webservice.itr.test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import ss.cloudbase.core.iterators.ConversionIterator;
import cloudbase.core.client.BatchScanner;
import cloudbase.core.client.Connector;
import cloudbase.core.data.Key;
import cloudbase.core.data.Range;
import cloudbase.core.data.Value;
import cloudbase.core.security.Authorizations;
import dss.webservice.itr.Test;

public class ConversionTest implements Test {
	private static final Logger logger = Logger.getLogger(ConversionTest.class);
	
	String comboIndexTable = "index_v3";
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
		
		int threads = 5;
		if (request.containsKey("threads")) {
			threads = Integer.parseInt(request.remove("threads"));
		}
		
		String[] dates = request.get("dates").split(",");		
		
		List<Long> baseTimes = new ArrayList<Long>();
		List<Long> convertTimes = new ArrayList<Long>();
		List<Long> baseCounts = new ArrayList<Long>();
		List<Long> convertCounts = new ArrayList<Long>();
		List<String> errors = new ArrayList<String>();
		
		List<Value> values = new ArrayList<Value>();
		
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
					BatchScanner reader = connector.createBatchScanner(table, new Authorizations(auths.split(",")), threads);
					ranges.add(new Range(new Key(new Text(begin)), true, new Key(new Text(end)), false));
					
					reader.setRanges(ranges);
					values.clear();
					long start = System.currentTimeMillis();
					for (Entry<Key, Value> entry: reader) {
						values.add(entry.getValue());
						count++;
					}
					baseTimes.add(System.currentTimeMillis() - start);
					baseCounts.add(count);
					
					logger.info("\tBase    count=" + count + " time=" + baseTimes.get(baseTimes.size() - 1) + " ms");
					
					count = 0;
					for (Value value: values) {
						logger.info("\t"  + value.toString());
						count++;
						if (count == 2) {
							break;
						}
					}
					
					count = 0;
					values.clear();
					
					reader = connector.createBatchScanner(table, new Authorizations(auths.split(",")), threads);
					ranges.add(new Range(new Key(new Text(begin)), true, new Key(new Text(end)), false));
					
					reader.setScanIterators(50, ConversionIterator.class.getName(), "ci");
					reader.setScanIteratorOption("ci", ConversionIterator.OPTION_CONVERSIONS, ConversionIterator.encodeConversions(new String[] {
						"frequency / 1000000"
					}));
					
					reader.setRanges(ranges);
					values.clear();
					start = System.currentTimeMillis();
					for (Entry<Key, Value> entry: reader) {
						values.add(entry.getValue());
						count++;
					}
					
					convertTimes.add(System.currentTimeMillis() - start);
					convertCounts.add(count);
					
					logger.info("\tConvert count=" + count + " time=" + convertTimes.get(convertTimes.size() - 1) + " ms");
					
					count = 0;
					for (Value value: values) {
						logger.info("\t"  + value.toString());
						count++;
						if (count == 2) {
							break;
						}
					}
				}
			}
			
			logger.info("********************* RESULTS *********************");
			logger.info("Tested all 0 level tiles on " + type + " for " + request.get("dates"));
			logger.info("This is a test of ConversionIterator performance");
			
			double baseSum = 0, convertSum = 0;
			for (int i = 0; i < baseTimes.size(); i++) {
				baseSum += baseTimes.get(i);
				convertSum += convertTimes.get(i);
			}
			
			logger.info("Average Base    Time: " + (baseSum / baseTimes.size()) + " ms");
			logger.info("Average Convert Time: " + (convertSum / convertTimes.size()) + " ms");
			
			baseSum = 0; 
			convertSum = 0;
			
			for (int i = 0; i < baseCounts.size(); i++) {
				baseSum += baseCounts.get(i);
				convertSum += convertCounts.get(i);
			}
			
			logger.info("Average Base    Count: " + (baseSum / baseCounts.size()));
			logger.info("Average Convert Count: " + (convertSum / convertCounts.size()));
			
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
