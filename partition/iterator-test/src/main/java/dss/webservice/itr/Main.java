package dss.webservice.itr;

import java.io.File;
import java.util.HashSet;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;
import java.util.TreeMap;
import java.util.Map.Entry;

import org.apache.hadoop.io.Text;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import ss.cloudbase.core.iterators.CellLevelFilteringIterator;
import ss.cloudbase.core.iterators.CellLevelRecordIterator;
import ss.cloudbase.core.iterators.ConversionIterator;
import ss.cloudbase.core.iterators.GMDenIntersectingIterator;
import ss.cloudbase.core.iterators.SortedMinIterator;
import ss.cloudbase.core.iterators.SortedRangeIterator;
import ss.cloudbase.core.iterators.filter.ogc.OGCFilter;
import cloudbase.core.client.BatchScanner;
import cloudbase.core.client.Connector;
import cloudbase.core.client.ZooKeeperInstance;
import cloudbase.core.data.Key;
import cloudbase.core.data.PartialKey;
import cloudbase.core.data.Range;
import cloudbase.core.data.Value;
import cloudbase.core.iterators.FilteringIterator;
import cloudbase.core.iterators.filter.RegExFilter;
import cloudbase.core.security.Authorizations;

public class Main {
	private static final Logger logger = Logger.getLogger(Main.class);
	
	private static String CB_INSTANCE = "INSTANCENAME"; // INSERT INSTANCE NAME
	private static String ZK_SERVERS = "r02sv22:2181,r03sv23:2181,r04sv22:2181,r05sv23:2181";
	private static String CB_USER = "user"; // SET USERNAME
	private static String CB_PASS = "pass"; // SET PASSWORD
	private static String CB_AUTH = "U,FOUO";
	private static String CB_TABLE = "partition_gi";
	
	public static void main(String[] args) {
		Map<String,String> request = new TreeMap<String, String>();
		
		int itrLevel = 50;
		
		for (String pair: args) {
			String[] parts = pair.split("[=]");
			if (parts.length == 1) {
				request.put(parts[0], parts[0]);
			} else if (parts.length == 2) {
				request.put(parts[0], parts[1]);
			}
		}
		
		BatchScanner reader = null;
		
		String filter = request.remove("filter");
		String terms = request.remove("terms");
		String ranges = request.remove("ranges");
		String partition = request.remove("partition");
		String rangeFamily = request.remove("rangeFamily");
		String prefix = request.remove("prefix");
		String index = request.remove("index");
		String test = request.remove("test");
		String testKey = request.remove("testKey");
		String convert = request.remove("convert");
		String grep = request.remove("grep");
		int print = -1;
		
		try {
			print = Integer.parseInt(request.remove("print"));
		} catch (NumberFormatException e) {
			print = 0;
		}
	
		boolean dryRun = request.remove("dryRun") != null;
		boolean debug = request.remove("debug") != null;
		boolean startInclusive = request.remove("start") != null;
		boolean endInclusive = request.remove("end") != null;
		boolean nodoc = request.remove("nodoc") != null;
		boolean multiDoc = request.remove("multiDoc") != null;
		boolean aggregate = request.remove("aggregate") != null;
		
		int threads = 5;
		if (request.containsKey("threads")) {
			threads = Integer.parseInt(request.remove("threads"));
		}
		
		if (partition != null) {
			partition = partition.replace(".", "\u0000");
		}
		
		if (index != null) {
			index = index.replace(':', '=');
		}
		
		if (testKey != null) {
			testKey = testKey.replace(".", "\u0000");
		}
		
		if (request.containsKey("c")) {
			CB_INSTANCE = request.remove("c");
		}
		
		if (request.containsKey("z")) {
			ZK_SERVERS = request.remove("z");
		}
		
		if (request.containsKey("u")) {
			CB_USER = request.remove("u");
		}
		
		if (request.containsKey("p")) {
			CB_PASS = request.remove("p");
		}
		
		if (request.containsKey("s")) {
			CB_AUTH = request.remove("s");
		}
		
		if (request.containsKey("t")) {
			CB_TABLE = request.remove("t");
		}
		
		logger.info("Cloudbase Connection: ");
		logger.info("\tc (instance):\t" + CB_INSTANCE);
		logger.info("\tz (zk servers):\t" + ZK_SERVERS);
		logger.info("\tu (user):\t" + CB_USER);
		logger.info("\tp (pass):\t" + CB_PASS);
		logger.info("\ts (auths):\t" + CB_AUTH);
		logger.info("\tt (table):\t" + CB_TABLE);
		
		logger.info("Query Parameters:");
		logger.info("\tindex:\t\t" + index);
		logger.info("\tfilter:\t\t" + filter);
		logger.info("\tterms:\t\t" + terms);
		logger.info("\tgrep:\t\t" + grep);
		logger.info("\tprefix:\t\t" + prefix);
		logger.info("\tranges:\t\t" + ranges);
		logger.info("\trangeFamily:\t" + rangeFamily);
		logger.info("\tpartition:\t" + partition);
		logger.info("\tstartInc:\t" + startInclusive);
		logger.info("\tendInc:\t\t" + endInclusive);
		logger.info("\tthreads:\t" + threads);
		logger.info("\tprint:\t\t" + print);
		logger.info("\tdryRun:\t\t" + dryRun);
		logger.info("\tdebug:\t\t" + debug);
		logger.info("\ttestKey:\t" + testKey);
		logger.info("\tmultiDoc:\t" + multiDoc);
		logger.info("\taggregate:\t" + aggregate);
		logger.info("\tconvert:\t" + convert);
		
		logger.info("Unknown Parameters: ");
		for (Entry<String,String> entry: request.entrySet()) {
			logger.info("\t" + entry.getKey() + ":\t\t" + entry.getValue());
		}
		
		if (debug) {
			// set the cloudbase logging to trace
			Logger.getLogger("cloudbase").setLevel(Level.TRACE);
		}
		
		boolean iteratorSet = false;
		
		try {
			ZooKeeperInstance zk = new ZooKeeperInstance(CB_INSTANCE, ZK_SERVERS);
			Connector connector = new Connector(zk, CB_USER, CB_PASS.getBytes());
			if (test != null) {
				Test t = (Test) Class.forName("dss.webservice.itr.test." + test).newInstance();
				t.runTest(request, connector, CB_TABLE, CB_AUTH);
				logger.info("done.");
				System.exit(0);
			}
			reader = connector.createBatchScanner(CB_TABLE, new Authorizations(CB_AUTH.split(",")), threads);
	
			Set<Range> partitionRanges = new HashSet<Range>();
			if (partition != null) {
				partition = partition.replace(".", "\u0000");
				Key startKey = null;
				Key endKey = null;
				if (partition.contains(",")) {
					startKey = new Key(new Text(partition.split(",")[0]));
					endKey = new Key(new Text(partition.split(",")[1]));
				} else {
					startKey = new Key(new Text(partition));
					endKey = startKey.followingKey(PartialKey.ROW);
				}
				
				Range range = new Range(startKey, true, endKey, false);
				if (testKey != null) {
					Key kTest = new Key(new Text(testKey));
					if (range.contains(kTest)) {
						logger.info("Key " + kTest + " is in the current range");
					} else {
						logger.info("Key " + kTest + " is not in the current range");
					}
				}
				partitionRanges.add(range);
			} else {
				partitionRanges.add(new Range());
			}
	
			if (terms != null && terms.trim().length() > 0) {
				String[] parts = terms.trim().split(",");
				if (parts.length == 1) {
					logger.info("Creating range iterator from '" + parts[0] + "' to '" + parts[0] + "\\u0000'.");
					reader.setScanIterators(itrLevel++, SortedRangeIterator.class.getName(), "ri");
					reader.setScanIteratorOption("ri", SortedRangeIterator.OPTION_DOC_COLF, "event");
					reader.setScanIteratorOption("ri", SortedRangeIterator.OPTION_COLF, "index");
					reader.setScanIteratorOption("ri", SortedRangeIterator.OPTION_LOWER_BOUND, parts[0]);
					reader.setScanIteratorOption("ri", SortedRangeIterator.OPTION_UPPER_BOUND, parts[0] + "\u0000");
					reader.setScanIteratorOption("ri", SortedRangeIterator.OPTION_MULTI_DOC, "" + multiDoc);
					iteratorSet = true;
				} else if (parts.length > 1) {
					logger.info("Creating intersecting iterator from all terms");
					Text[] t = new Text[parts.length];
					for (int i = 0; i < parts.length; i++) {
						if (parts[i].startsWith("range")) {
							parts[i] = parts[i].replace("_", "\u0000");
						}
						
						t[i] = new Text(parts[i]);
						logger.info("Adding Term: " + parts[i]);
					}

					reader.setScanIterators(itrLevel++, GMDenIntersectingIterator.class.getName(), "ii");
					reader.setScanIteratorOption("ii", GMDenIntersectingIterator.docFamilyOptionName, "event");
					reader.setScanIteratorOption("ii", GMDenIntersectingIterator.indexFamilyOptionName, "index");
					reader.setScanIteratorOption("ii", GMDenIntersectingIterator.columnFamiliesOptionName, GMDenIntersectingIterator.encodeColumns(t));
					reader.setScanIteratorOption("ii", GMDenIntersectingIterator.OPTION_MULTI_DOC, "" + multiDoc);
					iteratorSet = true;
				}
			} else if (ranges != null && ranges.trim().length() > 0) {
				// set up a range iterator
				logger.info("Creating range iterator on " + (rangeFamily != null ? rangeFamily: "index") + " for all ranges startInclusive: " + startInclusive + " endInclusive: " + endInclusive);
				String[] parts = ranges.trim().split(",");
				if (parts.length > 1 && parts.length % 2 == 0) {
//					reader.setScanIterators(itrLevel++, RangeIterator.class.getName(), "ri");
//					reader.setScanIteratorOption("ri", RangeIterator.OPTION_INDEX_COLF, rangeFamily != null ? rangeFamily: "index");
//					reader.setScanIteratorOption("ri", RangeIterator.OPTION_START_INCLUSIVE, "" + startInclusive);
//					reader.setScanIteratorOption("ri", RangeIterator.OPTION_END_INCLUSIVE, "" + endInclusive);
//					reader.setScanIteratorOption("ri", RangeIterator.OPTION_RANGES, RangeIterator.encodeRanges(parts));
					
					reader.setScanIterators(itrLevel++, SortedRangeIterator.class.getName(), "ir");
					reader.setScanIteratorOption("ir", SortedRangeIterator.OPTION_COLF, rangeFamily != null ? rangeFamily: "index");
					reader.setScanIteratorOption("ir", SortedRangeIterator.OPTION_START_INCLUSIVE, "" + startInclusive);
					reader.setScanIteratorOption("ir", SortedRangeIterator.OPTION_END_INCLUSIVE, "" + endInclusive);
					reader.setScanIteratorOption("ir", SortedRangeIterator.OPTION_LOWER_BOUND, parts[0]);
					reader.setScanIteratorOption("ir", SortedRangeIterator.OPTION_UPPER_BOUND, parts[1]);
					reader.setScanIteratorOption("ir", SortedRangeIterator.OPTION_MULTI_DOC, "" + multiDoc);
					iteratorSet = true;
				} else {
					throw new RuntimeException("A start and end range must be given for each range");
				}
			} else if (index != null && index.trim().length() > 0 && partition != null) {
				// look for an index on a partition
				
				// get out the ranges and add the index colf and term colq
				Range r = partitionRanges.iterator().next();
				Key start = new Key (r.getStartKey().getRow(), new Text("index"), new Text(index));
				Key end = new Key (r.getStartKey().getRow(), new Text("index"), new Text(index + "\uFFFD"));
				partitionRanges.clear();
				partitionRanges.add(new Range(start, true, end, false));
				iteratorSet = true;
				
			} else if (prefix != null && prefix.trim().length() > 0) {
				logger.info("Setting a min iterator on " + prefix);
				reader.setScanIterators(itrLevel++, SortedMinIterator.class.getName(), "mi");
				reader.setScanIteratorOption("mi", SortedMinIterator.OPTION_PREFIX, prefix);
				reader.setScanIteratorOption("mi", SortedMinIterator.OPTION_MULTI_DOC, "" + multiDoc);
				iteratorSet = true;
			}
			
			if (aggregate) {
				reader.setScanIterators(itrLevel++, CellLevelRecordIterator.class.getName(), "aggregator");
			}
			
			if (filter != null && filter.trim().length() > 0) {
				logger.info("Creating filtering iterator from filter in " + filter);
				Scanner scanner = new Scanner(new File(filter));
				
				filter = "";
				while (scanner.hasNextLine()) {
					filter += scanner.nextLine().trim(); 
				}
				
				// set up a filtering iterator
				logger.info("Filer = " + filter);
				
				if (multiDoc && !aggregate) {
					reader.setScanIterators(itrLevel++, CellLevelFilteringIterator.class.getName(), "fi");
					reader.setScanIteratorOption("fi", CellLevelFilteringIterator.OPTION_FILTER, filter);
				} else {
					reader.setScanIterators(itrLevel++, FilteringIterator.class.getName(), "fi");
					reader.setScanIteratorOption("fi", "0", OGCFilter.class.getName());
					reader.setScanIteratorOption("fi", "0." + OGCFilter.OPTION_FILTER, filter);
//					reader.setScanIteratorOption("fi", "1", RegExFilter.class.getName());
//					reader.setScanIteratorOption("fi", "1." + RegExFilter.ROW_REGEX, "theRegex");
				}
				iteratorSet = true;
			}
			
			if (convert != null && convert.trim().length() > 0) {
				convert = convert.replaceAll("_", " ");
				String[] conversions = convert.split(",");
				reader.setScanIterators(itrLevel++, ConversionIterator.class.getName(), "ci");
				reader.setScanIteratorOption("ci", ConversionIterator.OPTION_CONVERSIONS, ConversionIterator.encodeConversions(conversions));
				reader.setScanIteratorOption("ci", ConversionIterator.OPTION_MULTI_DOC, "" + (multiDoc && ! aggregate));
			}
			
			logger.info("Setting range to: " + partitionRanges.iterator().next());
			reader.setRanges(partitionRanges);
			
			if (!iteratorSet) {
				reader.fetchColumnFamily(new Text("event"));
			}
			if (!dryRun) {
				long start = System.currentTimeMillis();
				int count = 0;
				String id = null;
				for (Entry<Key, Value> entry: reader) {
					count++;
					if (print == -1 || count <= print) {
						String text = entry.getKey() + "\t" + entry.getValue();
						
						if ((grep != null && text.contains(grep)) || grep == null) {
							logger.info(text);
						}
					}
				}
				reader.close();
				logger.info("Time: " + (System.currentTimeMillis() - start) + " ms");
				logger.info("Count: " + count);
			} else if (!iteratorSet) {
				logger.info("No iterator was set from the provided parameters (and I'm not doing a full table scan... so there).");
			} else {
				logger.info("Dry run complete.");
			}
			logger.info("Done");
			System.exit(0);
		} catch (Exception e) {
			logger.error(e, e);
			System.exit(1);
		}
	}
}
