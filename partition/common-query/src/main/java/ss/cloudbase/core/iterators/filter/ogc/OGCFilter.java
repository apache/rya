package ss.cloudbase.core.iterators.filter.ogc;

import java.io.IOException;
import java.io.StringReader;
import java.util.HashMap;
import java.util.Map;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.log4j.Logger;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import ss.cloudbase.core.iterators.filter.ogc.operation.IOperation;

import cloudbase.core.data.Key;
import cloudbase.core.data.Value;
import cloudbase.core.iterators.filter.Filter;
import cloudbase.start.classloader.CloudbaseClassLoader;

/**
 * The OGCFilter class provides a basic implementation of the 
 * <a href="http://www.opengeospatial.org/standards/filter">OGC Filter Encoding specification</a>. 
 * This allows for arbitrary queries to be passed via XML and executed in a distributed fashion across tablet servers. The following
 * code sets up a basic FilteringIterator that uses this filter (note that this jar must be present in each
 * of the tablet servers' classpaths to work):
 * 
 * <code>
 * <pre>
 * cloudbase.core.client.Scanner reader;
 * // set up the reader ...
 *  
 * // we're going to parse a row formated like key:value|key:value|key:value and return all of the rows
 * // where the key "city" starts with "new"
 * String filter = "&lt;PropertyIsLike&gt;&lt;PropertyName&gt;city&lt;/PropertyName&gt;&lt;Literal&gt;new*&lt;/Literal&gt;&lt;/PropertyIsLike&gt;";
 * 
 * reader.setScanIterators(50, FilteringIterator.class.getName(), "myIterator");
 * reader.setScanIteratorOption("myIterator", "0", OGCFilter.class.getName());
 * reader.setScanIteratorOption("myIterator", "0." + OGCFilter.OPTION_PAIR_DELIMITER, "\\|");
 * reader.setScanIteratorOption("myIterator", "0." + OGCFilter.OPTION_VALUE_DELIMITER, ":");
 * reader.setScanIteratorOption("myIterator", "0." + OGCFilter.OPTION_FILTER, filter);
 * <pre>
 * </code>
 *  
 * @author William Wall
 */
public class OGCFilter implements Filter {
	private static final Logger logger = Logger.getLogger(OGCFilter.class);
	
	/** The string that separates the key/value pairs in the row value. */
	public static final String OPTION_PAIR_DELIMITER = "pairDelimiter";
	
	/** The string that separates the key and the value(s) within a pair in the row value. */
	public static final String OPTION_VALUE_DELIMITER = "valueDelimiter";
	
	/** The OGC Filter Encoding XML as a string */
	public static final String OPTION_FILTER = "filter";
	
	/** Specifies the column name for the column family. If this option is not included, the column family is not included in the row. */
	public static final String OPTION_COLF_NAME = "colfName";
	
	/** Specifies the column name for the column qualifier. If this option is not included, the column qualifier is not included in the row. */
	public static final String OPTION_COLQ_NAME = "colqName";
	
	/** Specifies the compare type for the filter. Defaults to "auto". **/
	public static final String OPTION_COMPARE_TYPE = "compareType";
	
	public static final String TYPE_NUMERIC = "numeric";
	
	public static final String TYPE_STRING = "string";
	
	public static final String TYPE_AUTO = "auto";
	
	/** Contains the pair delimiter provided through the <code>OPTION_PAIR_DELIMITER</code> option. */
	protected String pairDelimiter;
	
	/** Contains the value delimiter provided through the <code>OPTION_VALUE_DELIMITER</code> option. */
	protected String valueDelimiter;
	
	/** Contains the column family column name provided through the <code>OPTION_COLF_NAME</code> option. */
	protected String colfName;
	
	/** Contains the column qualifier column name provided through the <code>OPTION_COLQ_NAME</code> option. */
	protected String colqName;
	
	/** The root operation of the query tree **/
	protected IOperation root;
	
	/** The compare type for the query tree **/
	protected String compareType;

	/**
	 * Whether or not to accept this key/value entry. A map of row keys and values is parsed and then sent off to the process function to be evaluated.
	 * @param key The cloudbase entry key
	 * @param value The cloudbase entry value
	 * @return True if the entry should be included in the results, false otherwise
	 */
	@Override
	public boolean accept(Key key, Value value) {
		if (root != null) {
			Map<String, String> row = getRow(key, value);
			if (root != null) {
				return root.execute(row);
			}
		}
		return false;
	}
	
	public boolean accept(Map<String, String> record) {
		if (root != null) {
			return root.execute(record);
		}
		return false;
	}
	
	/**
	 * Parses the cloudbase value into a map of key/value pairs. If the <code>OPTION_COLF_NAME</code> 
	 * or <code>OPTION_COLQ_NAME</code> options were used, then they will also be added to the row map. 
	 * By default, pairs are delimited by the first unicode character ("\u0000") and values by the last unicode
	 * character ("\uFFFD"). See the <code>OPTION_PAIR_DELIMITER</code> and <code>OPTION_VALUE_DELIMITER</code> 
	 * options to change these values.
	 * 
	 * @param cbKey The cloudbase entry key
	 * @param cbValue The cloudbase entry value
	 * @return A map that represents this row
	 */
	protected Map<String, String> getRow(Key cbKey, Value cbValue) {
		//TODO: This should really be replaced by CBValueFormatter.parse(value.toString()), but I'm hesitant to require
		// more jars (common-data and google-collections) to be in the cloudbase/lib directory. Also, what do we do with
		// a field with multiple values? Should we just assume that if any value in that field matches then the row
		// matches? Or should they all have to match? 
		
		String value = cbValue.toString();
		Map<String, String> row = new HashMap<String, String>();
		
		if (colfName != null) {
			row.put(colfName, cbKey.getColumnFamily().toString());
		}
		if (colqName != null) {
			row.put(colqName, cbKey.getColumnQualifier().toString());
		}
		
		// Determine the start/end of the value.
		int valueStartIndex = 0;
		int valueEndIndex = value.length();
		
		int vLen = valueDelimiter.length();
		int fLen = pairDelimiter.length();

		// Parse each of the values from the row value.
		while (valueStartIndex < valueEndIndex) {
			int vIndex = value.indexOf(valueDelimiter, valueStartIndex);
	
			// If an "equals" sign was found, parse the key and value.
			if (vIndex != -1) {
				String key = value.substring(valueStartIndex, vIndex).trim();
				int v = value.indexOf(valueDelimiter, vIndex + vLen);
				if (v == -1) {
					v = valueEndIndex;
				}
				int f = value.indexOf(pairDelimiter, vIndex + vLen);
				if (f == -1) {
					f = valueEndIndex;
				}
				
				int fIndex = Math.min(f,v);
				String val = value.substring(vIndex + 1, fIndex).trim();
				valueStartIndex = f;
				valueStartIndex += fLen;
				row.put(key, val);
			}
		}
		
		return row;
	}

	@Override
	public void init(Map<String, String> options) {
		pairDelimiter = options.get(OPTION_PAIR_DELIMITER);
		if (pairDelimiter == null || pairDelimiter.length() == 0) {
			pairDelimiter = "\u0000";
		}
		
		valueDelimiter = options.get(OPTION_VALUE_DELIMITER);
		if (valueDelimiter == null || valueDelimiter.length() == 0) {
			valueDelimiter = "\uFFFD";
		}
		
		compareType = options.get(OPTION_COMPARE_TYPE);
		if (compareType == null || compareType.length() == 0) {
			compareType = TYPE_AUTO;
		}
		
		colfName = options.get(OPTION_COLF_NAME);
		colqName = options.get(OPTION_COLQ_NAME);
		
		try {
			DocumentBuilder builder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
			InputSource is = new InputSource();
			is.setCharacterStream(new StringReader(options.get(OPTION_FILTER)));
			Document doc = builder.parse(is);
			Node filter = doc.getDocumentElement();
			if (filter.getNodeName().equalsIgnoreCase("filter")) {
				filter = filter.getFirstChild();
			}
			root = createOperationTree(filter);
		} catch (IOException e) {
			logger.error(e,e);
		} catch (SAXException e) {
			logger.error(e,e);
		} catch (ParserConfigurationException e) {
			logger.error(e,e);
		}
	}
	
	/**
	 * Creates the operation tree from the filter XML
	 * @param node The filter XML node to parse
	 * @return The root IOperation
	 */
	protected IOperation createOperationTree(Node node) {
		try {
			// instantiate the operation and initialize it
			Class<? extends IOperation> clazz = CloudbaseClassLoader.loadClass(IOperation.class.getPackage().getName() + "." + node.getNodeName(), IOperation.class);
			IOperation op = clazz.newInstance();
			op.init(node, compareType);
			return op;
		} catch (ClassNotFoundException e) {
			logger.warn("Operation not supported: " + node.getNodeName());
		} catch (InstantiationException e) {
			logger.error(e,e);
		} catch (IllegalAccessException e) {
			logger.error(e,e);
		}
		return null;
	}
}
