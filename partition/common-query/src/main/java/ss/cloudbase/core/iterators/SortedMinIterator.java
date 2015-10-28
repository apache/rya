package ss.cloudbase.core.iterators;

import java.io.IOException;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import cloudbase.core.data.Key;
import cloudbase.core.data.PartialKey;
import cloudbase.core.data.Range;
import cloudbase.core.data.Value;
import cloudbase.core.iterators.IteratorEnvironment;
import cloudbase.core.iterators.SortedKeyValueIterator;

/**
 * Iterates over the minimum value of every term with the given prefix and parts delimeter. If, for example, you
 * wanted to find each person's last known position, you would set up the following index:
 * 
 * We want the last date instead of the first, so we'll use reverseDate in our index
 * partitionX index:&lt;prefix&gt;_&lt;personID&gt;_&lt;reverseDate&gt;.&lt;recordID&gt;
 * 
 * (where "." is actually "\u0000")
 * 
 * <code>SortedMinIterator</code> initially seeks to index:prefix in the first partition. From there, it grabs the record
 * as the "document" and then seeks to index:<whatever-the-term-was-up-to-last-delimiter> + "\uFFFD" (last unicode
 * character), which then puts it at the next persion ID in our example.
 * 
 * NOTE that this iterator gives a unique result per tablet server. You may have to process the results to determine
 * the true minimum value.
 * 
 * @author William Wall (wawall)
 */
public class SortedMinIterator extends SortedRangeIterator {
	private static final Logger logger = Logger.getLogger(SortedMinIterator.class);
	
	/** 
	 * The option to supply a prefix to the term combination. Defaults to "min"
	 */
	public static final String OPTION_PREFIX = "prefix";
	
	/**
	 * The delimiter for the term (note that this is and must be different than the delimiter between the term and record ID). Defaults to "_"
	 */
	public static final String OPTION_PARTS_DELIMITER = "partsDelimiter";
	
	protected String prefix = "min";
	protected String partsDelimiter = "_";
	protected boolean firstKey = true;
	protected String lastPart = null;
	
	@Override
	public void init(SortedKeyValueIterator<Key, Value> source, Map<String, String> options, IteratorEnvironment env) throws IOException {
		super.init(source, options, env);
		
		prefix = options.get(OPTION_PREFIX);
		String s = options.get(OPTION_PARTS_DELIMITER);
		partsDelimiter = s != null ? s: "_";
		//TODO: make sure prefix and partsDelimeter is set
		lower = new Text(prefix);
	}
	
	protected String getPrefix(Key key) {
		String s = key.getColumnQualifier().toString();
		int i = s.indexOf(partsDelimiter);
		if (i > 0) {
			return s.substring(0, i + partsDelimiter.length());
		}
		return null;
	}
	
	protected String getPart(Key key) {
		String s = key.getColumnQualifier().toString();
		int i = s.lastIndexOf(partsDelimiter);
		if (i > 0) {
			return s.substring(0, i + 1);
		}
		return null;
	}

	@Override
	protected void setUpDocIds() throws IOException {
		int count = 0;
		try {
			if (testOutOfMemory) {
				throw new OutOfMemoryError();
			}
			
			long start = System.currentTimeMillis();
			if (source.hasTop()) {
				SortedSet<Key> docIds = new TreeSet<Key>();
				currentPartition = getPartition(source.getTopKey());
				while (currentPartition != null) {
					// seek to the prefix (aka lower)
					Key lowerKey = new Key(currentPartition, colf, lower);
					source.seek(new Range(lowerKey, true, null, false), indexColfSet, true);
					
					// if we don't have a value then quit
					if (!source.hasTop()) {
						currentPartition = null;
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
						
						// make sure we're still in the index prefix
						String p = getPrefix(top);
						String part = getPart(top);
						
						if (p != null && p.startsWith(prefix)) {
							if (part != null) {
								if (!part.equals(lastPart)) {
									// if the part (e.g. "lastPosition_personId_") is different, then it's valid
									lastPart = part;
									docIds.add(buildOutputKey(top));
									count++;
								}
								
								// seek to the next part
								lowerKey = new Key(currentPartition, colf, new Text(part + "\uFFFD"));
								source.seek(new Range(lowerKey, true, null, false), indexColfSet, true);
							}
						} else {
							// we're done in this partition
							currentPartition = getFollowingPartition(top);
							break;
						}
						
						// make sure we check to see if we're at the end before potentially seeking back
						if (!source.hasTop()) {
							currentPartition = null;
							break;
						}
					}
				}
				itr = docIds.iterator();
				sortComplete = true;
				logger.debug("setUpDocIds completed in " + (System.currentTimeMillis() - start) + " ms. Count = " + count);
			} else {
				logger.warn("There appear to be no records on this tablet");
			}
		} catch (OutOfMemoryError e) {
			logger.warn("OutOfMemory error: Count = " + count);
			throw new IOException("OutOfMemory error while sorting keys");
		}
	}
}
