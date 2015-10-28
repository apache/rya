/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package ss.cloudbase.core.iterators.filter;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.log4j.Logger;

import cloudbase.core.data.Key;
import cloudbase.core.data.Value;

/**
 * 
 * @author rashah
 */
public class CBConverter {

	/** The string that separates the key/value pairs in the row value. */
	public static final String OPTION_PAIR_DELIMITER = "pairDelimiter";
	/**
	 * The string that separates the key and the value(s) within a pair in the
	 * row value.
	 */
	public static final String OPTION_VALUE_DELIMITER = "valueDelimiter";
	/**
	 * Contains the pair delimiter provided through the
	 * <code>OPTION_PAIR_DELIMITER</code> option.
	 */
	protected String pairDelimiter = "\u0000";
	/**
	 * Contains the value delimiter provided through the
	 * <code>OPTION_VALUE_DELIMITER</code> option.
	 */
	protected String valueDelimiter = "\uFFFD";
	private static Logger LOG = Logger.getLogger(CBConverter.class);

	public CBConverter() {
	}

	public Map<String, String> toMap(Key CBKey, Value CBValue) {
		LOG.trace("Convert");

		Map<String, String> return_value = new HashMap<String, String>();

		String value = CBValue.toString();

		// Determine the start/end of the value.
		int valueStartIndex = 0;
		int valueEndIndex = value.length();

		int vLen = valueDelimiter.length();
		int fLen = pairDelimiter.length();
		LOG.debug(vLen + ", " + fLen + ", CBValue = " + CBValue.toString());
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

				int fIndex = Math.min(f, v);
				String val = value.substring(vIndex + 1, fIndex).trim();
				valueStartIndex = f;
				valueStartIndex += fLen;
				return_value.put(key, val);
				LOG.debug("Key {" + key + "} Value {" + val + "}");
			}
		}

		return return_value;
	}
	
	public Value toValue(Map<String, String> record) {
		StringBuilder sb = new StringBuilder();
		boolean first = true;
		
		for (Entry<String, String> e: record.entrySet()) {
			if (first) {
				first = false;
			} else {
				sb.append(pairDelimiter);
			}
			sb.append(e.getKey());
			sb.append(valueDelimiter);
			sb.append(e.getValue());
		}
		
		return new Value(sb.toString().getBytes());
	}

	public void init(Map<String, String> options) {
		LOG.trace("Init");

		pairDelimiter = options.get(OPTION_PAIR_DELIMITER);
		if (pairDelimiter == null || pairDelimiter.length() == 0) {
			pairDelimiter = "\u0000";
		}

		valueDelimiter = options.get(OPTION_VALUE_DELIMITER);
		if (valueDelimiter == null || valueDelimiter.length() == 0) {
			valueDelimiter = "\uFFFD";
		}
	}
}
