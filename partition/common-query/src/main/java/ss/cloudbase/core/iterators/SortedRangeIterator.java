package ss.cloudbase.core.iterators;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import cloudbase.core.data.ArrayByteSequence;
import cloudbase.core.data.ByteSequence;
import cloudbase.core.data.Key;
import cloudbase.core.data.PartialKey;
import cloudbase.core.data.Range;
import cloudbase.core.data.Value;
import cloudbase.core.iterators.IteratorEnvironment;
import cloudbase.core.iterators.SortedKeyValueIterator;

/**
 * <code>SortedRangeIterator</code> uses the insertion sort functionality of <code>IntersectionRange</code>
 * to store off document keys rather than term keys.
 *  
 * @author William Wall (wawall)
 */
public class SortedRangeIterator extends IntersectionRange {
	private static final Logger logger = Logger.getLogger(SortedRangeIterator.class);
	
	/** Use this option to set the document column family. Defaults to "event". **/
	public static final String OPTION_DOC_COLF = "docColf";
	
	/** 
	 * Use this option to retrieve all the documents that match the UUID rather than just the first. This 
	 * is commonly used in cell-level security models that use the column-qualifier like this:
	 * UUID \0 field1 [] value
	 * UUID \0 securedField [ALPHA] secretValue
	 **/
	public static final String OPTION_MULTI_DOC = "multiDoc";
	
	/** The source document iterator **/
	protected SortedKeyValueIterator<Key, Value> docSource;
	
	/** The document column family. Defaults to "event". **/
	protected Text docColf;
	protected Value docValue;
	
	protected boolean nextId = false;
	protected Range docRange = null;
	protected boolean multiDoc;
	
	protected Set<ByteSequence> docColfSet;
		
	@Override
	public void next() throws IOException {
		if (multiDoc && nextId) {
			docSource.next();
			
			// check to make sure that the docSource top is less than our max key
			if (docSource.hasTop() && docRange.contains(docSource.getTopKey())) {
				topKey = docSource.getTopKey();
				docValue = docSource.getTopValue();
				return;
			}
		}
		
		super.next();
		
		// if we're looking for multiple documents in the doc source, then
		// set the max key for our range check
		if (topKey != null) {
			Text row = topKey.getRow();
			Text colf = topKey.getColumnFamily();
			if (multiDoc) {
				docRange = new Range(
					new Key (row, colf, new Text(topKey.getColumnQualifier().toString())),
					true,
					new Key (row, colf, new Text(topKey.getColumnQualifier().toString() + "\u0000\uFFFD")),
					true
				);
			} else {
				docRange = new Range(new Key (row, colf, new Text(topKey.getColumnQualifier().toString())),true, null, false);
			}
		}
		
		nextId = false;
		getDocument();
	}

	@Override
	public Value getTopValue() {
		return docValue;
	}

	@Override
	protected Key buildOutputKey(Key key) {
		// we want to build the document key as the output key
		return new Key(currentPartition, docColf, new Text(getDocID(key)));
	}

	protected void getDocument() throws IOException {
		// look up the document value
		if (topKey != null) {
			docSource.seek(docRange, docColfSet, true);
			
			if (docSource.hasTop() && docRange.contains(docSource.getTopKey())) {
				// found it!
				topKey = docSource.getTopKey();
				docValue = docSource.getTopValue();
				nextId = true;
			} else {
				// does not exist or user had auths that could see the index but not the event
				logger.warn("Document: " + topKey + " does not exist or user had auths for " + colf + " but not " + docColf);
				docValue = IteratorConstants.emptyValue;
			}
		}
	}

	@Override
	public void init(SortedKeyValueIterator<Key, Value> source, Map<String, String> options, IteratorEnvironment env) throws IOException {
		super.init(source, options, env);
		docSource = source.deepCopy(env);
		if (options.containsKey(OPTION_DOC_COLF)) {
			docColf = new Text(options.get(OPTION_DOC_COLF));
		} else {
			docColf = new Text("event");
		}
		
		if (options.containsKey(OPTION_MULTI_DOC)) {
			multiDoc = Boolean.parseBoolean(options.get(OPTION_MULTI_DOC));
		} else {
			multiDoc = false;
		}
		
		docColfSet = Collections.singleton((ByteSequence) new ArrayByteSequence(docColf.getBytes(), 0, docColf.getLength()));
	}
}
