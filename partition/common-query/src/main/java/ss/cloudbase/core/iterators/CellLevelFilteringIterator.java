package ss.cloudbase.core.iterators;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.Text;

import ss.cloudbase.core.iterators.filter.ogc.OGCFilter;
import cloudbase.core.data.ByteSequence;
import cloudbase.core.data.Key;
import cloudbase.core.data.Range;
import cloudbase.core.data.Value;
import cloudbase.core.iterators.IteratorEnvironment;
import cloudbase.core.iterators.SortedKeyValueIterator;
import cloudbase.core.iterators.WrappingIterator;

public class CellLevelFilteringIterator extends WrappingIterator {
	private static final Collection<ByteSequence> EMPTY_SET = Collections.emptySet();
	
	/** The OGC Filter string **/
	public static final String OPTION_FILTER = "filter";
	
	/** The character or characters that defines the end of the field in the column qualifier. Defaults to '@' **/
	public static final String OPTION_FIELD_END = "fieldEnd";
	
	protected SortedKeyValueIterator<Key, Value> checkSource;
	
	protected Map<String, Boolean> cache = new HashMap<String, Boolean>();
	
	protected OGCFilter filter;
	
	protected String fieldEnd = "@";

	public CellLevelFilteringIterator() {}
	
	public CellLevelFilteringIterator(CellLevelFilteringIterator other, IteratorEnvironment env) {
		setSource(other.getSource().deepCopy(env));
		checkSource = other.checkSource.deepCopy(env);
		cache = other.cache;
		fieldEnd = other.fieldEnd;
	}
	
	@Override
	public CellLevelFilteringIterator deepCopy(IteratorEnvironment env) {
		return new CellLevelFilteringIterator(this, env);
	}

	@Override
	public void init(SortedKeyValueIterator<Key, Value> source, Map<String, String> options, IteratorEnvironment env) throws IOException {
		super.init(source, options, env);
		if (source instanceof GMDenIntersectingIterator) {
			checkSource = ((GMDenIntersectingIterator) source).docSource.deepCopy(env);
		} else if (source instanceof SortedRangeIterator) {
			checkSource = ((SortedRangeIterator) source).docSource.deepCopy(env);
		} else {
			checkSource = source.deepCopy(env);
		}
		filter = new OGCFilter();
		filter.init(options);
		
		if (options.containsKey(OPTION_FIELD_END)) {
			fieldEnd = options.get(OPTION_FIELD_END);
		}
	}

	@Override
	public void next() throws IOException {
		getSource().next();
		findTop();
	}
	
	protected String getDocId(Key key) {
		String colq = key.getColumnQualifier().toString();
		int i = colq.indexOf("\u0000");
		if (i == -1) {
			i = colq.length();	
		}
		return colq.substring(0, i);
	}
	
	protected Key getRecordStartKey(Key key, String docId) {
		return new Key(key.getRow(), key.getColumnFamily(), new Text(docId + "\u0000"));
	}
	
	protected Key getRecordEndKey(Key key, String docId) {
		return new Key(key.getRow(), key.getColumnFamily(), new Text(docId + "\u0000\uFFFD"));
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
	
	protected void findTop() throws IOException {
		boolean goodKey;
		String docId;
		Map<String, String> record = new HashMap<String, String>();
		
		while (getSource().hasTop()) {
			docId = getDocId(getSource().getTopKey());
			
			// if the document is in the cache, then we have already scanned it
			if (cache.containsKey(docId)) {
				goodKey = cache.get(docId);
			} else {
				// we need to scan the whole record into a map and evaluate the filter
				
				// seek the check source to the beginning of the record
				Range range = new Range(
					getRecordStartKey(getSource().getTopKey(), docId),
					true,
					getRecordEndKey(getSource().getTopKey(), docId),
					true
				);
				
				checkSource.seek(range, EMPTY_SET, false);
				
				// read in the record to the map
				record.clear();
				while (checkSource.hasTop()) {
					String field = getField(checkSource.getTopKey(), checkSource.getTopValue());
					if (field != null) {
						record.put(field, getValue(checkSource.getTopKey(), checkSource.getTopValue()));
					}
					checkSource.next();
				}
				
				// evaluate the filter
				goodKey = filter.accept(record);
				
				// cache the result so that we don't do this for every cell
				cache.put(docId, goodKey);
			}
			
			if (goodKey==true)
				return;
			getSource().next();
		}
	}

	@Override
	public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
		getSource().seek(range, columnFamilies, inclusive);
		findTop();
	}
}
