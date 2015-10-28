import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.io.Text;
import org.junit.Test;

import ss.cloudbase.core.iterators.CellLevelFilteringIterator;
import ss.cloudbase.core.iterators.CellLevelRecordIterator;
import ss.cloudbase.core.iterators.ConversionIterator;
import ss.cloudbase.core.iterators.GMDenIntersectingIterator;
import ss.cloudbase.core.iterators.SortedMinIterator;
import ss.cloudbase.core.iterators.SortedRangeIterator;
import ss.cloudbase.core.iterators.UniqueIterator;
import ss.cloudbase.core.iterators.filter.CBConverter;
import cloudbase.core.client.Connector;
import cloudbase.core.client.Scanner;
import cloudbase.core.client.TableNotFoundException;
import cloudbase.core.data.Key;
import cloudbase.core.data.PartialKey;
import cloudbase.core.data.Range;
import cloudbase.core.data.Value;
import cloudbase.core.security.Authorizations;

public class IteratorTest {
	private Connector cellLevelConn;
	private Connector serializedConn;
	
	private static final String TABLE = "partition";
	private static final Authorizations AUTHS = new Authorizations("ALPHA,BETA,GAMMA".split(","));
	
	public IteratorTest() {
		
	}
	
	protected Connector getCellLevelConnector() {
		if (cellLevelConn == null) {
			cellLevelConn = SampleData.initConnector();
			SampleData.writeDenCellLevel(cellLevelConn, SampleData.sampleData());
		}
		return cellLevelConn;
	}
	
	protected Connector getSerializedConnector() {
		if (serializedConn == null) {
			serializedConn = SampleData.initConnector();
			SampleData.writeDenSerialized(serializedConn, SampleData.sampleData());
			SampleData.writeDenProvenance(serializedConn);
			SampleData.writeMinIndexes(serializedConn);
		}
		return serializedConn;
	}
	
	protected Scanner getProvenanceScanner() {
		Connector c = getSerializedConnector();
		try {
			return c.createScanner("provenance", AUTHS);
		} catch (TableNotFoundException e) {
			return null;
		}
	}
	
	protected Scanner getCellLevelScanner() {
		Connector c = getCellLevelConnector();
		try {
			return c.createScanner(TABLE, AUTHS);
		} catch (TableNotFoundException e) {
			return null;
		}
	}
	
	protected Scanner getSerializedScanner() {
		Connector c = getSerializedConnector();
		try {
			return c.createScanner(TABLE, AUTHS);
		} catch (TableNotFoundException e) {
			return null;
		}
	}
	
	protected Scanner setUpIntersectingIterator(Scanner s, Text[] terms, boolean multiDoc) {
		try {
			s.setScanIterators(50, GMDenIntersectingIterator.class.getName(), "ii");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		s.setScanIteratorOption("ii", GMDenIntersectingIterator.indexFamilyOptionName, "index");
		s.setScanIteratorOption("ii", GMDenIntersectingIterator.docFamilyOptionName, "event");
		s.setScanIteratorOption("ii", GMDenIntersectingIterator.OPTION_MULTI_DOC, "" + multiDoc);
		s.setScanIteratorOption("ii", GMDenIntersectingIterator.columnFamiliesOptionName, GMDenIntersectingIterator.encodeColumns(terms));
		return s;
	}
	
	protected String checkSerialized(Scanner s) {
		StringBuilder sb = new StringBuilder();
		boolean first = true;
		for (Entry<Key, Value> e: s) {
			if (!first) {
				sb.append(",");
			} else {
				first = false;
			}
			
			String colq = e.getKey().getColumnQualifier().toString();
			
			sb.append(colq);
		}
		return sb.toString();
	}
	
	protected String checkCellLevel(Scanner s) {
		StringBuilder sb = new StringBuilder();
		boolean first = true;
		for (Entry<Key, Value> e: s) {
			String colq = e.getKey().getColumnQualifier().toString();
			int i = colq.indexOf("\u0000");
			if (i > -1) {
				if (!first) {
					sb.append(",");
				} else {
					first = false;
				}
				sb.append(colq.substring(0, i));
				sb.append(".");
				sb.append(colq.substring(i + 1));
				sb.append("=");
				sb.append(e.getValue().toString());
			}
		}
		return sb.toString();
	}
	
	@Test
	public void testSerializedSingleDuplicate() {
		Text[] terms = new Text[] {
			new Text("A"),
			new Text("A")
		};
		
		String test = "01";
		Scanner s = setUpIntersectingIterator(getSerializedScanner(), terms, false);
		s.setRange(new Range());
		assertTrue(test.equals(checkSerialized(s)));
	}
	
	@Test
	public void testCellLevelSingleDuplicate() {
		Text[] terms = new Text[] {
			new Text("A"),
			new Text("A")
		};
		String test = "01.field0=A,01.field1=B,01.field2=C,01.field3=D,01.field4=E";
		Scanner s = setUpIntersectingIterator(getCellLevelScanner(), terms, true);
		s.setRange(new Range());
		assertTrue(test.equals(checkCellLevel(s)));
	}
	
	@Test
	public void testSerializedTwoTerms() {
		Text[] terms = new Text[] {
			new Text("C"),
			new Text("D")
		};
		// all the evens will come first
		String test = "02,01,03";
		Scanner s = setUpIntersectingIterator(getSerializedScanner(), terms, false);
		s.setRange(new Range());
		assertTrue(test.equals(checkSerialized(s)));
	}
	
	@Test
	public void testCellLevelTwoTerms() {
		Text[] terms = new Text[] {
			new Text("C"),
			new Text("D")
		};
		
		String test = "02.field0=B,02.field1=C,02.field2=D,02.field3=E,02.field4=F,"
			+ "01.field0=A,01.field1=B,01.field2=C,01.field3=D,01.field4=E,"
			+ "03.field0=C,03.field1=D,03.field2=E,03.field3=F,03.field4=G";
		Scanner s = setUpIntersectingIterator(getCellLevelScanner(), terms, true);
		s.setRange(new Range());
		assertTrue(test.equals(checkCellLevel(s)));
	}
	
	@Test
	public void testSerializedTwoTermsWithRange() {
		Text[] terms = new Text[] {
			new Text("C"),
			new Text("D")
		};
		
		String test = "02";
		Scanner s = setUpIntersectingIterator(getSerializedScanner(), terms, false);
		s.setRange(new Range(new Key(new Text("0")), true, new Key(new Text("1")), false));
		assertTrue(test.equals(checkSerialized(s)));
	}
	
	@Test
	public void testCellLevelTwoTermsWithRange() {
		Text[] terms = new Text[] {
			new Text("C"),
			new Text("D")
		};
		
		String test = "02.field0=B,02.field1=C,02.field2=D,02.field3=E,02.field4=F";
		Scanner s = setUpIntersectingIterator(getCellLevelScanner(), terms, true);
		s.setRange(new Range(new Key(new Text("0")), true, new Key(new Text("1")), false));
		assertTrue(test.equals(checkCellLevel(s)));
	}
	
	@Test
	public void testSerializedSingleRange() {
		Text[] terms = new Text[] {
			new Text(GMDenIntersectingIterator.getRangeTerm("index", "A", true, "B", true)),
			new Text(GMDenIntersectingIterator.getRangeTerm("index", "A", true, "B", true))
		};
		
		String test = "02,01";
		Scanner s = setUpIntersectingIterator(getSerializedScanner(), terms, false);
		s.setRange(new Range());
		assertTrue(test.equals(checkSerialized(s)));
	}
	
	@Test
	public void testSerializedMultiRange() {
		Text[] terms = new Text[] {
			new Text(GMDenIntersectingIterator.getRangeTerm("index", "A", true, "B", true)),
			new Text(GMDenIntersectingIterator.getRangeTerm("index", "B", true, "C", true))
		};
		
		String test = "02,01";
		Scanner s = setUpIntersectingIterator(getSerializedScanner(), terms, false);
		s.setRange(new Range());
		assertTrue(test.equals(checkSerialized(s)));
	}
	
	@Test
	public void testSerializedTermAndRange() {
		Text[] terms = new Text[] {
			new Text("B"),
			new Text(GMDenIntersectingIterator.getRangeTerm("index", "A", true, "E", true))
		};
		
		String test = "02,01";
		Scanner s = setUpIntersectingIterator(getSerializedScanner(), terms, false);
		s.setRange(new Range());
		assertTrue(test.equals(checkSerialized(s)));
	}
	
	protected Scanner setUpSortedRangeIterator(Scanner s, boolean multiDoc) {
		try {
			s.setScanIterators(50, SortedRangeIterator.class.getName(), "ri");
			s.setScanIteratorOption("ri", SortedRangeIterator.OPTION_COLF, "index");
			s.setScanIteratorOption("ri", SortedRangeIterator.OPTION_DOC_COLF, "event");
			s.setScanIteratorOption("ri", SortedRangeIterator.OPTION_LOWER_BOUND, "A");
			s.setScanIteratorOption("ri", SortedRangeIterator.OPTION_UPPER_BOUND, "C");
			s.setScanIteratorOption("ri", SortedRangeIterator.OPTION_START_INCLUSIVE, "true");
			s.setScanIteratorOption("ri", SortedRangeIterator.OPTION_END_INCLUSIVE, "true");
			s.setScanIteratorOption("ri", SortedRangeIterator.OPTION_MULTI_DOC, "" + multiDoc);
			return s;
		} catch (IOException e) {
			e.printStackTrace();
			return null;
		}
	}
	
	@Test
	public void testSerializedSortedRangeIterator() {
		Scanner s = setUpSortedRangeIterator(getSerializedScanner(), false);
		String test = "02,01,03";
		s.setRange(new Range());
		assertTrue(test.equals(checkSerialized(s)));
	}
	
	@Test
	public void testCellLevelSortedRangeIterator() {
		Scanner s = setUpSortedRangeIterator(getCellLevelScanner(), true);
		String test = "02.field0=B,02.field1=C,02.field2=D,02.field3=E,02.field4=F,"
			+ "01.field0=A,01.field1=B,01.field2=C,01.field3=D,01.field4=E,"
			+ "03.field0=C,03.field1=D,03.field2=E,03.field3=F,03.field4=G";
		s.setRange(new Range());
		assertTrue(test.equals(checkCellLevel(s)));
	}
	
	@Test
	public void testUniqueIterator() {
		Scanner s = getProvenanceScanner();
		try {
			s.setScanIterators(50, UniqueIterator.class.getName(), "skipper");
			Key start = new Key(new Text("sid1"));
			s.setRange(new Range(start, start.followingKey(PartialKey.ROW)));
			
			int count = 0;
			for (Entry<Key, Value> e: s) {
				count++;
			}
			
			assertEquals(count, 3);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	protected Scanner setUpConversionIterator(Scanner s) {
		String[] conversions = new String[] {
			"field0 + 10",
			"field1 - 10",
			"field2 * 10",
			"field3 / 10",
			"field4 % 10"
		};
		
		try {
			s.setScanIterators(50, ConversionIterator.class.getName(), "ci");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		s.setScanIteratorOption("ci", ConversionIterator.OPTION_CONVERSIONS, ConversionIterator.encodeConversions(conversions));
		Key start = new Key(new Text("1"), new Text("event"), new Text("01"));
		s.setRange(new Range(start, true, start.followingKey(PartialKey.ROW_COLFAM_COLQUAL), false));
		
		return s;
	}
	
	@Test
	public void testConversionIteratorSerialized() {
		Scanner s = getSerializedScanner();
		s = setUpConversionIterator(s);
		
		CBConverter c = new CBConverter();
		
		boolean test = true;
		Map<String, Double> expected = new HashMap<String, Double>();
		
		expected.put("field0", 20.0);
		expected.put("field1", 1.0);
		expected.put("field2", 120.0);
		expected.put("field3", 1.3);
		expected.put("field4", 4.0);
		
		Map<String, String> record;
		
		for (Entry<Key, Value> e: s) {
			record = c.toMap(e.getKey(), e.getValue());
			
			for (Entry<String, String> pair: record.entrySet()) {
				test = test && expected.get(pair.getKey()).equals(new Double(Double.parseDouble(record.get(pair.getKey()))));
			}
		}
		
		assertTrue(test);
	}
	
	@Test
	public void testConversionIteratorCellLevel() {
		Scanner s = getCellLevelScanner();
		s = setUpConversionIterator(s);
		s.setScanIteratorOption("ci", ConversionIterator.OPTION_MULTI_DOC, "true");
		
		boolean test = true;
		Map<String, Double> expected = new HashMap<String, Double>();
		
		expected.put("field0", 20.0);
		expected.put("field1", 1.0);
		expected.put("field2", 120.0);
		expected.put("field3", 1.3);
		expected.put("field4", 4.0);
		
		for (Entry<Key, Value> e: s) {
			String field = getField(e.getKey());
			if (field != null) {
				test = test && expected.get(field).equals(new Double(Double.parseDouble(e.getValue().toString())));
			}
		}
		
		assertTrue(test);
	}
	
	protected String getField(Key key) {
		String colq = key.getColumnQualifier().toString();
		int start = colq.indexOf("\u0000");
		if (start == -1) {
			return null;
		}
		
		int end = colq.indexOf("\u0000", start + 1);
		if (end == -1) {
			end = colq.length();
		}
		
		return colq.substring(start + 1, end);
	}
	
	@Test
	public void testCellLevelOGCFilter() {
		Scanner s = getCellLevelScanner();
		s.fetchColumnFamily(new Text("event"));
		
		try {
			s.setScanIterators(60, CellLevelFilteringIterator.class.getName(), "fi");
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		s.setScanIteratorOption("fi", CellLevelFilteringIterator.OPTION_FILTER, "<PropertyIsBetween><PropertyName>field0</PropertyName>"
			+ "<LowerBoundary><Literal>A</Literal></LowerBoundary>"
			+ "<UpperBoundary><Literal>C</Literal></UpperBoundary>"
			+ "</PropertyIsBetween>");
		
		String test = "02.field0=B,02.field1=C,02.field2=D,02.field3=E,02.field4=F,"
			+ "01.field0=A,01.field1=B,01.field2=C,01.field3=D,01.field4=E,"
			+ "03.field0=C,03.field1=D,03.field2=E,03.field3=F,03.field4=G";
		assertTrue(test.equals(checkCellLevel(s)));
	}
	
	@Test
	public void testMultiLevelIterator() {
		Scanner s = getCellLevelScanner();
		Text[] terms = new Text[] {
			new Text("C"),
			new Text("D")
		};
		
		s = setUpIntersectingIterator(s, terms, true);
		
		try {
			s.setScanIterators(60, CellLevelFilteringIterator.class.getName(), "fi");
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		s.setScanIteratorOption("fi", CellLevelFilteringIterator.OPTION_FILTER, "<PropertyIsEqualTo><PropertyName>field0</PropertyName>"
			+ "<Literal>A</Literal>"
			+ "</PropertyIsEqualTo>");
		
		String test = "01.field0=A,01.field1=B,01.field2=C,01.field3=D,01.field4=E";
		assertTrue(test.equals(checkCellLevel(s)));
	}
	
	@Test
	public void testMultiLevelIterator2() {
		Scanner s = getCellLevelScanner();
		s = setUpSortedRangeIterator(s, true);
		try {
			s.setScanIterators(60, CellLevelFilteringIterator.class.getName(), "fi");
		} catch (IOException e) {
			e.printStackTrace();
		}
		s.setScanIteratorOption("fi", CellLevelFilteringIterator.OPTION_FILTER, "<PropertyIsEqualTo><PropertyName>field0</PropertyName>"
			+ "<Literal>A</Literal>"
			+ "</PropertyIsEqualTo>");
		
		String test = "01.field0=A,01.field1=B,01.field2=C,01.field3=D,01.field4=E";
		assertTrue(test.equals(checkCellLevel(s)));
	}
	
	@Test
	public void testCellLevelRecordIterator() {
		Scanner s = getCellLevelScanner();
		s = setUpSortedRangeIterator(s, true);
		try {
			s.setScanIterators(60, CellLevelRecordIterator.class.getName(), "recordItr");
		} catch (IOException e) {
			e.printStackTrace();
		}
		
//		for (Entry<Key, Value> e: s) {
//			String v = e.getValue().toString();
//			v = v.replaceAll("\\u0000", ",");
//			v = v.replaceAll("\\uFFFD", "=");
//			System.out.println(e.getKey() + "\t" + v);
//		}
		String test = "02,01,03";
		assertTrue(test.equals(checkSerialized(s)));
	}
	
	@Test
	public void testIntersectionWithoutDocLookup() {
		Text[] terms = new Text[] {
			new Text("C"),
			new Text("D")
		};
		// all the evens will come first
		String test = "\u000002,\u000001,\u000003";
		Scanner s = setUpIntersectingIterator(getSerializedScanner(), terms, false);
		s.setScanIteratorOption("ii", GMDenIntersectingIterator.OPTION_DOC_LOOKUP, "false");
		s.setRange(new Range());
		assertTrue(test.equals(checkSerialized(s)));
	}
	
	@Test
	public void testSimpleNot() {
		Text[] terms = new Text[] {
			new Text("B"),
			new Text("F")
		};
		
		boolean[] nots = new boolean[] {
			false,
			true
		};
		
		String test="01";
		Scanner s = setUpIntersectingIterator(getSerializedScanner(), terms, false);
		s.setScanIteratorOption("ii", GMDenIntersectingIterator.notFlagOptionName, GMDenIntersectingIterator.encodeBooleans(nots));
		s.setRange(new Range());
		
		assertTrue(test.equals(checkSerialized(s)));
	}
	
	@Test
	public void testRangeNot() {
		Text[] terms = new Text[] {
			new Text("B"),
			new Text(GMDenIntersectingIterator.getRangeTerm("index", "F", true, "H", true))
		};
		
		boolean[] nots = new boolean[] {
			false,
			true
		};
		
		String test = "01";
		Scanner s = setUpIntersectingIterator(getSerializedScanner(), terms, false);
		s.setScanIteratorOption("ii", GMDenIntersectingIterator.notFlagOptionName, GMDenIntersectingIterator.encodeBooleans(nots));
		s.setRange(new Range());
		
		assertTrue(test.equals(checkSerialized(s)));
	}
	
	@Test
	public void testMinIteratorOnLastKeys() {
		Scanner s = getSerializedScanner();
		try {
			s.setScanIterators(50, SortedMinIterator.class.getName(), "min");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		s.setScanIteratorOption("min", SortedMinIterator.OPTION_PREFIX, "z");
		s.setRange(new Range());
		
		String test = "02,04,06,08,10,01,03,05,07,09";
		assertTrue(test.equals(checkSerialized(s)));
	}
}
