package ss.cloudbase.core.iterators;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.hadoop.io.Text;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import cloudbase.core.client.CBException;
import cloudbase.core.data.ArrayByteSequence;
import cloudbase.core.data.ByteSequence;
import cloudbase.core.data.Key;
import cloudbase.core.data.PartialKey;
import cloudbase.core.data.Range;
import cloudbase.core.data.Value;
import cloudbase.core.iterators.IteratorEnvironment;
import cloudbase.core.iterators.SortedKeyValueIterator;

/**
 * When attempting to intersect a term which is a range (lowerval <= x <= upperval), the entire range
 * must first be scanned so that the document keys can be sorted before passing them up to the 
 * intersecting iterator of choice. 
 * 
 * @author William Wall (wawall)
 */
public class IntersectionRange implements SortedKeyValueIterator<Key, Value>{
	private static final Logger logger = Logger.getLogger(IntersectionRange.class);
	
	public static final String OPTION_OUTPUT_COLF = "outputColf";
	public static final String OPTION_OUTPUT_TERM = "outputTerm";
	public static final String OPTION_COLF = "columnFamily";
	public static final String OPTION_LOWER_BOUND = "lower";
	public static final String OPTION_UPPER_BOUND = "upper";
	public static final String OPTION_DELIMITER = "delimiter";
	public static final String OPTION_START_INCLUSIVE = "startInclusive";
	public static final String OPTION_END_INCLUSIVE = "endInclusive";
	public static final String OPTION_TEST_OUTOFMEM = "testOutOfMemory";
	
	protected SortedKeyValueIterator<Key, Value> source;
	protected Text colf = null;
	protected Text lower = null;
	protected Text upper = null;
	protected String delimiter = null;
	protected String outputTerm = null;
	protected Text outputColf = null;
	protected Text currentPartition = null;
	protected boolean startInclusive = true;
	protected boolean endInclusive = false;
	protected boolean testOutOfMemory = false;
	
	protected Key topKey = null;
	
	protected Iterator<Key> itr;
	protected boolean sortComplete = false;
	protected Range overallRange;
	protected SortedSet<Key> docIds = new TreeSet<Key>();
	protected static Set<ByteSequence> indexColfSet;
	
	@Override
	public SortedKeyValueIterator<Key, Value> deepCopy(IteratorEnvironment env) {
		return new IntersectionRange(this, env);
	}
	
	public IntersectionRange() {
		logger.setLevel(Level.ALL);
	}
	
	public IntersectionRange(IntersectionRange other, IteratorEnvironment env) {
		source = other.source.deepCopy(env);
		colf = other.colf;
		lower = other.lower;
		upper = other.upper;
		delimiter = other.delimiter;
		outputColf = other.outputColf;
		outputTerm = other.outputTerm;
		currentPartition = other.currentPartition;
		startInclusive = other.startInclusive;
		endInclusive = other.endInclusive;
		topKey = other.topKey;
		docIds.addAll(other.docIds);
		itr = docIds.iterator();
		sortComplete = other.sortComplete;
		overallRange = other.overallRange;
	}
	
	public Text getOutputTerm() {
		return new Text(outputTerm);
	}
	
	public Text getOutputColumnFamily() {
		return outputColf;
	}

	@Override
	public Key getTopKey() {
		return topKey;
	}

	@Override
	public Value getTopValue() {
		return IteratorConstants.emptyValue;
	}

	@Override
	public boolean hasTop() {
		try {
			if (topKey == null) next();
		} catch (IOException e) {
			
		}
		
		return topKey != null;
	}
	
	protected String getDocID(Key key) {
		try {
			String s = key.getColumnQualifier().toString();
			int start = s.indexOf("\u0000") + 1;
			int end = s.indexOf("\u0000", start);
			if (end == -1) {
				end = s.length();
			}
			return s.substring(start, end);
		} catch (Exception e) {
			
		}
		return null;
	}
	
	protected Text getTerm(Key key) {
		try {
			Text colq = key.getColumnQualifier(); 
			Text term = new Text();
			term.set(colq.getBytes(), 0, colq.find("\0"));
			return term;
		} catch (Exception e) {
		}
		return null;
	}
	
	protected Text getPartition(Key key) {
		return key.getRow();
	}
	
	protected Text getFollowingPartition(Key key) {
		return key.followingKey(PartialKey.ROW).getRow();
	}
	
	@Override
	public void init(SortedKeyValueIterator<Key, Value> source, Map<String, String> options, IteratorEnvironment env) throws IOException {
		if (options.containsKey(OPTION_LOWER_BOUND)) {
			lower = new Text(options.get(OPTION_LOWER_BOUND));
		} else {
			lower = new Text("\u0000");
		}
		
		if (options.containsKey(OPTION_UPPER_BOUND)) {
			upper = new Text(options.get(OPTION_UPPER_BOUND));
		} else {
			upper = new Text("\u0000");
		}
		
		if (options.containsKey(OPTION_DELIMITER)) {
			delimiter = options.get(OPTION_DELIMITER);
		} else {
			delimiter = "\u0000";
		}
		
		if (options.containsKey(OPTION_COLF)) {
			colf = new Text(options.get(OPTION_COLF));
		} else {
			colf = new Text("index");
		}
		
		if (options.containsKey(OPTION_OUTPUT_COLF)) {
			outputColf = new Text(options.get(OPTION_OUTPUT_COLF));
		} else {
			outputColf = colf;
		}
		
		if (options.containsKey(OPTION_START_INCLUSIVE)) {
			startInclusive = Boolean.parseBoolean(options.get(OPTION_START_INCLUSIVE));
		}
		
		if (options.containsKey(OPTION_END_INCLUSIVE)) {
			endInclusive = Boolean.parseBoolean(options.get(OPTION_END_INCLUSIVE));
		}
		
		if (options.containsKey(OPTION_TEST_OUTOFMEM)) {
			testOutOfMemory = Boolean.parseBoolean(options.get(OPTION_TEST_OUTOFMEM));
		}
		
		outputTerm = options.get(OPTION_OUTPUT_TERM);
		this.source = source;
		
		indexColfSet = Collections.singleton((ByteSequence) new ArrayByteSequence(colf.getBytes(),0,colf.getLength()));
	}
	
	/**
	 * Sets up the document/record IDs in a sorted structure. 
	 * @throws IOException
	 * @throws CBException 
	 */
	protected void setUpDocIds() throws IOException {
		int count = 0;
		try {
			if (testOutOfMemory) {
				throw new OutOfMemoryError();
			}
			
			long start = System.currentTimeMillis();
			if (source.hasTop()) {
				docIds.clear();
				currentPartition = getPartition(source.getTopKey());
				while (currentPartition != null) {
					Key lowerKey = new Key(currentPartition, colf, lower);
					try {
						source.seek(new Range(lowerKey, true, null, false), indexColfSet, true);
					} catch (IllegalArgumentException e) {
						// the range does not overlap the overall range? quit
						currentPartition = null;
						break;
					}
					
					// if we don't have a value then quit
					if (!source.hasTop()) {
						currentPartition = null;
						break;
					}
					
					Key top;
					while(source.hasTop()) {
						top = source.getTopKey();
						
						if (overallRange != null && overallRange.getEndKey() != null) {
							// see if we're past the end of the partition range
							int endCompare = overallRange.getEndKey().compareTo(top, PartialKey.ROW);
							if ((!overallRange.isEndKeyInclusive() && endCompare <= 0) || endCompare < 0) {
								// we're done
								currentPartition = null;
								break;
							}
						}
						
						// make sure we're still in the right partition
						if (currentPartition.compareTo(getPartition(top)) < 0) {
							currentPartition.set(getPartition(top));
							break;
						}
						
						// make sure we're still in the right column family
						if (colf.compareTo(top.getColumnFamily()) < 0) {
							// if not, then get the next partition
							currentPartition = getFollowingPartition(top);
							break;
						}
						
						Text term = getTerm(top);
						int lowerCompare = term.compareTo(lower);
						int upperCompare = term.compareTo(upper);
						
						// if we went past the upper bound, jump to the next partition
						if ((endInclusive && upperCompare > 0) || (!endInclusive && upperCompare >= 0)) {
							currentPartition = getFollowingPartition(top);
							break;
						} else if ((startInclusive && lowerCompare >= 0) || (!startInclusive && lowerCompare > 0)) {
							// if the term is lexicographically between the upper and lower bounds,
							// then add the doc ID
							docIds.add(buildOutputKey(top));
							count++;
						}
						source.next();
						
						// make sure we check to see if we're at the end before potentially seeking back
						if (!source.hasTop()) {
							currentPartition = null;
							break;
						}
					}
				}
				itr = docIds.iterator();
				sortComplete = true;
				logger.debug("setUpDocIds completed for " + lower + "<=" + colf + "<=" + upper + " in " + (System.currentTimeMillis() - start) + " ms. Count = " + count);
			} else {
				logger.warn("There appear to be no records on this tablet");
			}
		} catch (OutOfMemoryError e) {
			logger.warn("OutOfMemory error: Count = " + count);
			throw new IOException("OutOfMemory error while sorting keys");
		}
	}
	
	protected Key buildOutputKey(Key key) {
		String id = getDocID(key);
		return new Key(currentPartition, outputColf, new Text((outputTerm != null ? outputTerm: colf.toString()) + "\u0000" +id));
	}

	@Override
	public void next() throws IOException {
		if (itr != null && itr.hasNext()) {
			topKey = itr.next();
		} else {
			topKey = null;
		}
	}

	@Override
	public void seek(Range range, Collection<ByteSequence> colfs, boolean inclusive) throws IOException {
		if (!sortComplete) {
			overallRange = range;
			source.seek(range, colfs, inclusive);
			setUpDocIds();
		}
		
		if (range.getStartKey() != null) {
			while (hasTop() && topKey.compareTo(range.getStartKey(), PartialKey.ROW_COLFAM_COLQUAL) < 0) {
				next();
			}
		} else {
			next();
		}
	}
}
