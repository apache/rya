// Dear Cloudbase,
// 		Use protected fields/methods as much as possible in APIs.
// 		Love,
//			Will

// since the IntersectingIterator/FamilyIntersectingIterator classes are stingy with their fields, we have to use
// the exact same package name to get at currentPartition and currentDocID
package ss.cloudbase.core.iterators;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import ss.cloudbase.core.iterators.IntersectingIterator.TermSource;

import cloudbase.core.data.ArrayByteSequence;
import cloudbase.core.data.ByteSequence;
import cloudbase.core.data.Key;
import cloudbase.core.data.Range;
import cloudbase.core.data.Value;
import cloudbase.core.iterators.IteratorEnvironment;
import cloudbase.core.iterators.SortedKeyValueIterator;

/**
 * This class is a copy of FamilyIntersectingIterator with a few minor changes. It assumes a table structure like the following:
 * <table>
 * <tr><th>Row</th><th>Column Family</th><th>Column Qualifier</th><th>Value</th></tr>
 * <tr><td>Partition1</td><td>event</td><td>UUID</td><td>The record value</td></tr>
 * <tr><td>Partition1</td><td>index</td><td>term\u0000UUID</td><td></td></tr>
 * </table>
 * 
 * @author William Wall
 *
 */
public class GMDenIntersectingIterator extends IntersectingIterator {
	private static final Logger logger = Logger.getLogger(GMDenIntersectingIterator.class);
	
	public static final Text DEFAULT_INDEX_COLF = new Text("i");
	public static final Text DEFAULT_DOC_COLF = new Text("e");
	
	public static final String indexFamilyOptionName = "indexFamily";
	public static final String docFamilyOptionName = "docFamily";
	
	protected static Text indexColf = DEFAULT_INDEX_COLF;
	protected static Text docColf = DEFAULT_DOC_COLF;
	protected static Set<ByteSequence> indexColfSet;
	protected static Set<ByteSequence> docColfSet;
	
	protected static final byte[] nullByte = {0};
	
	protected SortedKeyValueIterator<Key,Value> docSource;
	
	/** 
	 * Use this option to retrieve all the documents that match the UUID rather than just the first. This 
	 * is commonly used in cell-level security models that use the column-qualifier like this:
	 * UUID \0 field1 [] value
	 * UUID \0 securedField [ALPHA] secretValue
	 **/
	public static final String OPTION_MULTI_DOC = "multiDoc";
	
	/**
	 * Use this option to turn off document lookup.
	 */
	public static final String OPTION_DOC_LOOKUP = "docLookup";
	
	protected boolean multiDoc = false;
	protected boolean doDocLookup = true;
	protected Range docRange = null;
	protected boolean nextId = false;
	
	@Override
	public void init(SortedKeyValueIterator<Key, Value> source, Map<String, String> options, IteratorEnvironment env) throws IOException {
		if (options.containsKey(indexFamilyOptionName))
			indexColf = new Text(options.get(indexFamilyOptionName));
		if (options.containsKey(docFamilyOptionName))
			docColf = new Text(options.get(docFamilyOptionName));
		docSource = source.deepCopy(env);
		indexColfSet = Collections.singleton((ByteSequence)new ArrayByteSequence(indexColf.getBytes(),0,indexColf.getLength()));
		
		if (options.containsKey(OPTION_MULTI_DOC)) {
			multiDoc = Boolean.parseBoolean(options.get(OPTION_MULTI_DOC));
		}
		
		if (options.containsKey(OPTION_DOC_LOOKUP)) {
			doDocLookup = Boolean.parseBoolean(options.get(OPTION_DOC_LOOKUP));
		}
		
		if (!doDocLookup) {
			// it makes no sense to turn on multiDoc if doDocLookup is off
			multiDoc = false;
		}
		
		// remove any range terms
		Text[] originalTerms = decodeColumns(options.get(columnFamiliesOptionName));
		boolean[] originalBooleans = decodeBooleans(options.get(notFlagOptionName));
		
		List<Text> terms = new ArrayList<Text>();
		List<Boolean> termBooleans = new ArrayList<Boolean>();
		List<Text> ranges = new ArrayList<Text>();
		List<Boolean> rangeBooleans = new ArrayList<Boolean>();
		
		boolean boolsExist = originalBooleans != null && originalBooleans.length == originalTerms.length;
		
		for (int i = 0; i < originalTerms.length; i++) {
			if (isRangeTerm(originalTerms[i])) {
				ranges.add(originalTerms[i]);
				if (boolsExist) {
					rangeBooleans.add(originalBooleans[i]);
				} else {
					rangeBooleans.add(false);
				}
			} else {
				terms.add(originalTerms[i]);
				
				if (boolsExist) {
					termBooleans.add(originalBooleans[i]);
				} else {
					termBooleans.add(false);
				}
			}
		}
		
		boolean[] bools = new boolean[termBooleans.size()];
		for (int i = 0; i < termBooleans.size(); i++) {
			bools[i] = termBooleans.get(i).booleanValue();
		}
		
		boolean[] rangeBools = new boolean[rangeBooleans.size()];
		for (int i = 0; i < rangeBooleans.size(); i++) {
			rangeBools[i] = rangeBooleans.get(i).booleanValue();
		}
		
		// put the modified term/boolean lists back in the options
		
		if (terms.size() < 2) {
			// the intersecting iterator will choke on these, so we'll set it up ourselves
			if (terms.size() == 1) {
				sources = new TermSource[1];
				sources[0] = new TermSource(source, terms.get(0));
			}
		} else {
			options.put(columnFamiliesOptionName, encodeColumns(terms.toArray(new Text[terms.size()])));
			if (termBooleans.size() > 0) {
				options.put(notFlagOptionName, encodeBooleans(bools));
			}
			
			super.init(source, options, env);
		}
		
		// add the range terms
		if (ranges.size() > 0) {
			
			TermSource[] localSources;
			
			int offset = 0;
			if (sources != null) {
				localSources = new TermSource[sources.length + ranges.size()];
				
				// copy array
				for (int i = 0; i < sources.length; i++) {
					localSources[i] = sources[i];
				}
				
				offset = sources.length;
			} else {
				localSources = new TermSource[ranges.size()];
			}
			
			for (int i = 0; i < ranges.size(); i++) {
				IntersectionRange ri = new IntersectionRange();
				ri.init(source.deepCopy(env), getRangeIteratorOptions(ranges.get(i)), env);
				localSources[i + offset] = new TermSource(ri, ri.getOutputTerm(), rangeBools[i]);
			}
			
			sources = localSources;
		}
		
		sourcesCount = sources.length;
		
		if (sourcesCount < 2) {
			throw new IOException("GMDenIntersectingIterator requires two or more terms");
		}
		
		docColfSet = Collections.singleton((ByteSequence)new ArrayByteSequence(docColf.getBytes(),0,docColf.getLength()));
	}

	@Override
	protected Key buildKey(Text partition, Text term, Text docID) {
		Text colq = new Text(term);
		colq.append(nullByte, 0, 1);
		colq.append(docID.getBytes(), 0, docID.getLength());
		return new Key(partition, indexColf, colq);
	}

	@Override
	protected Key buildKey(Text partition, Text term) {
		Text colq = new Text(term);
		return new Key(partition, indexColf, colq);
	}

	@Override
	protected Text getTerm(Key key) {
		if (indexColf.compareTo(key.getColumnFamily().getBytes(),0,indexColf.getLength())< 0) {
		 // We're past the index column family, so return a term that will sort lexicographically last.
		 // The last unicode character should suffice
		 return new Text("\uFFFD");
		}
		Text colq = key.getColumnQualifier();
		int zeroIndex = colq.find("\0");
		Text term = new Text();
		term.set(colq.getBytes(),0,zeroIndex);
		return term;
	}

	@Override
	public SortedKeyValueIterator<Key, Value> deepCopy(IteratorEnvironment env) {
		GMDenIntersectingIterator newItr = new GMDenIntersectingIterator();
		if(sources != null) {
		    newItr.sourcesCount = sourcesCount;
			newItr.sources = new TermSource[sourcesCount];
			for(int i = 0; i < sourcesCount; i++) {
				newItr.sources[i] = new TermSource(sources[i].iter.deepCopy(env), sources[i].term);
			}
		}
		newItr.currentDocID = currentDocID;
		newItr.currentPartition = currentPartition;
		newItr.docRange = docRange;
		newItr.docSource = docSource.deepCopy(env);
		newItr.inclusive = inclusive;
		newItr.multiDoc = multiDoc;
		newItr.nextId = nextId;
		newItr.overallRange = overallRange;
		return newItr;
	}
	
	@Override
	public void seek(Range range, Collection<ByteSequence> seekColumnFamilies, boolean inclusive) throws IOException {
		super.seek(range, indexColfSet, true);
		
	}
	
	@Override
	protected Text getDocID(Key key) {
		Text colq = key.getColumnQualifier();
		int firstZeroIndex = colq.find("\0");
		if (firstZeroIndex < 0) {
			throw new IllegalArgumentException("bad docid: "+key.toString());
		}
		Text docID = new Text();
		try {
			docID.set(colq.getBytes(),firstZeroIndex+1, colq.getBytes().length - firstZeroIndex - 1);
		} catch (ArrayIndexOutOfBoundsException e) {
			throw new IllegalArgumentException("bad indices for docid: "+key.toString()+" "+firstZeroIndex +" " + (colq.getBytes().length - firstZeroIndex - 1));			
		}
		return docID;
	}
	
	protected Key buildStartKey() {
		return new Key(currentPartition, docColf, currentDocID);
	}
	
	protected Key buildEndKey() {
		if (multiDoc) {
			return new Key(currentPartition, docColf, new Text(currentDocID.toString() + "\u0000\uFFFD"));
		}
		return null;
	}
	
	@Override
	public void next() throws IOException {
		if (multiDoc && nextId) {
			docSource.next();
			
			// check to make sure that the docSource top is less than our max key
			if (docSource.hasTop() && docRange.contains(docSource.getTopKey())) {
				topKey = docSource.getTopKey();
				value = docSource.getTopValue();
				return;
			}
		}
		
		nextId = false;
		super.next();
	}

	@Override
	protected void advanceToIntersection() throws IOException {
		super.advanceToIntersection();
		
		if (topKey==null || !doDocLookup)
			return;
		
		if (logger.isTraceEnabled()) logger.trace("using top key to seek for doc: "+topKey.toString());
		docRange = new Range(buildStartKey(), true, buildEndKey(), false);
		docSource.seek(docRange, docColfSet, true);
		logger.debug("got doc key: "+docSource.getTopKey().toString());
		if (docSource.hasTop()&& docRange.contains(docSource.getTopKey())) {
			value = docSource.getTopValue();
		}
		logger.debug("got doc value: "+value.toString());
		
		if (docSource.hasTop()) {
			if (multiDoc && topKey != null) {
				nextId = true;
			}
			topKey = docSource.getTopKey();
		}
	}

	
	public boolean isRangeTerm(Text term) {
		return term.toString().startsWith("range\u0000");
	}

	protected Map<String, String> getRangeIteratorOptions(Text config) {
		// we want the keys from Range Iterators to look like this:
		// range|colf|lower|includeLower|upper|includeUpper
		// e.g. range|geo|21332|true|21333|false
		
		// and we'll output a key like this:
		// partition index:geo\0UUID ...
		
		
		String[] range = config.toString().split("\u0000");
		Map<String, String> options = new HashMap<String, String>();
		options.put(IntersectionRange.OPTION_COLF, range[1]);
		options.put(IntersectionRange.OPTION_OUTPUT_TERM, range[1]);
		options.put(IntersectionRange.OPTION_LOWER_BOUND, range[2]);
		options.put(IntersectionRange.OPTION_START_INCLUSIVE, range[3]);
		options.put(IntersectionRange.OPTION_UPPER_BOUND, range[4]);
		options.put(IntersectionRange.OPTION_END_INCLUSIVE, range[5]);
		options.put(IntersectionRange.OPTION_OUTPUT_COLF, indexColf.toString());
		return options;
	}
	
	/**
	 * Builds a range term for use with the IntersectingIterator
	 * @param colf The column family to search
	 * @param start The start of the range
	 * @param includeStart Whether the start of the range is inclusive or not
	 * @param end The end of the range 
	 * @param includeEnd Whether the end of the range is inclusive or not
	 * @return A String formatted for use as a term a GMDenIntersectingIterator
	 */
	public static String getRangeTerm(String colf, String start, boolean includeStart, String end, boolean includeEnd) {
		StringBuilder sb = new StringBuilder();
		sb.append("range\u0000");
		sb.append(colf).append("\u0000");
		sb.append(start).append("\u0000");
		sb.append(includeStart ? "true": "false").append("\u0000");
		sb.append(end).append("\u0000");
		sb.append(includeEnd ? "true": "false").append("\u0000");
		return sb.toString();
	}
}
