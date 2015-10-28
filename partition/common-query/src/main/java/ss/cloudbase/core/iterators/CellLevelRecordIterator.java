package ss.cloudbase.core.iterators;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.Text;

import ss.cloudbase.core.iterators.filter.CBConverter;

import cloudbase.core.data.Key;
import cloudbase.core.data.Range;
import cloudbase.core.data.Value;
import cloudbase.core.iterators.IteratorEnvironment;
import cloudbase.core.iterators.SkippingIterator;
import cloudbase.core.iterators.SortedKeyValueIterator;

public class CellLevelRecordIterator extends SkippingIterator {
	public static final String OPTION_FIELD_END = "fieldEnd";
	public static final String OPTION_MULTIPLE_DELIMITER = "multipleDelimiter";
	
	protected String multipleDelimiter = ",";
	
	protected Key topKey;
	protected Value topValue;
	protected String fieldEnd = "@";
	protected String docId = null;
	protected CBConverter converter = new CBConverter();
	
	@Override
	public SortedKeyValueIterator<Key, Value> deepCopy(IteratorEnvironment env) {
		CellLevelRecordIterator itr = new CellLevelRecordIterator();
		itr.setSource(this.getSource().deepCopy(env));
		itr.fieldEnd = this.fieldEnd;
		return itr;
	}
	
	@Override
	public void init(SortedKeyValueIterator<Key, Value> source, Map<String, String> options, IteratorEnvironment env) throws IOException {
		super.init(source, options, env);
        converter.init(options);
		if (options.containsKey(OPTION_FIELD_END)) {
			fieldEnd = options.get(OPTION_FIELD_END);
		}
		
		if (options.containsKey(OPTION_MULTIPLE_DELIMITER)) {
			multipleDelimiter = options.get(OPTION_MULTIPLE_DELIMITER);
		}
	}

	@Override
	public void next() throws IOException {
		consume();
	}

	@Override
	public boolean hasTop() {
		return getSource().hasTop() || topKey != null || topValue != null;
	}

	@Override
	public Key getTopKey() {
		return topKey;
	}

	@Override
	public Value getTopValue() {
		return topValue;
	}
	
	protected String getDocId(Key key) {
		String colq = key.getColumnQualifier().toString();
		int i = colq.indexOf("\u0000");
		if (i == -1) {
			i = colq.length();	
		}
		return colq.substring(0, i);
	}

	protected Key buildTopKey(Key key, String docId) {
		return new Key(key.getRow(), key.getColumnFamily(), new Text(docId), key.getColumnVisibility(), key.getTimestamp());
	}
	
	protected String getField(Key key, Value value) {
		String colq = key.getColumnQualifier().toString();
		int i = colq.indexOf("\u0000");
		if (i == -1) {
			return null;
		}
		
		int j = colq.indexOf(fieldEnd, i + 1);
		if (j == -1) {
			j = colq.length();
		}
		
		return colq.substring(i + 1, j);
	}
	
	protected String getValue(Key key, Value value) {
		return value.toString();
	}
	
	protected Key getRecordStartKey(Key key, String docId) {
		return new Key(key.getRow(), key.getColumnFamily(), new Text(docId));
	}
	
	protected Key getRecordEndKey(Key key, String docId) {
		return new Key(key.getRow(), key.getColumnFamily(), new Text(docId + "\u0000\uFFFD"));
	}

	@Override
	protected void consume() throws IOException {
		// build the top key
		if (getSource().hasTop()) {
			docId = getDocId(getSource().getTopKey());
			topKey = buildTopKey(getSource().getTopKey(), docId);
			
			Range range = new Range(
				getRecordStartKey(getSource().getTopKey(), docId),
				true,
				getRecordEndKey(getSource().getTopKey(), docId),
				true
			);
			
			Map<String, String> record = new HashMap<String, String>();
			while (getSource().hasTop() && range.contains(getSource().getTopKey())) {
				String field = getField(getSource().getTopKey(), getSource().getTopValue());
				if (field != null) {
					if (record.get(field) == null) {
						record.put(field, getValue(getSource().getTopKey(), getSource().getTopValue()));
					} else {
						record.put(field, record.get(field) + multipleDelimiter + getValue(getSource().getTopKey(), getSource().getTopValue()));
					}
				}
				getSource().next();
			}
			
			topValue = converter.toValue(record);  
		} else {
			topKey = null;
			topValue = null;
		}
	}
}
