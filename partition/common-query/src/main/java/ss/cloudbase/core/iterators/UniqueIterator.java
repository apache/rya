package ss.cloudbase.core.iterators;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;

import cloudbase.core.data.ByteSequence;
import cloudbase.core.data.Key;
import cloudbase.core.data.PartialKey;
import cloudbase.core.data.Range;
import cloudbase.core.data.Value;
import cloudbase.core.iterators.IteratorEnvironment;
import cloudbase.core.iterators.SkippingIterator;
import cloudbase.core.iterators.SortedKeyValueIterator;
import cloudbase.core.iterators.WrappingIterator;

/**
 * This iterator gets unique keys by the given depth. The depth defaults to PartialKey.ROW_COLFAM.
 * 
 * @author William Wall
 */
public class UniqueIterator extends WrappingIterator {
	public static final String OPTION_DEPTH = "depth";
	private static final Collection<ByteSequence> EMPTY_SET = Collections.emptySet();
	protected PartialKey depth;
	protected Range range;
	protected Key lastKey = null;
	
	public UniqueIterator() {}
	
	public UniqueIterator(UniqueIterator other) {
		this.depth = other.depth;
		this.range = other.range;
		this.lastKey = other.lastKey;
	}
	
	@Override
	public void next() throws IOException {
		consume();
	}

	protected void consume() throws IOException {
		if (lastKey != null) {
			int count = 0;
			// next is way faster, so we'll try doing that 10 times before seeking 
			while (getSource().hasTop() && getSource().getTopKey().compareTo(lastKey, depth) == 0 && count < 10) {
				getSource().next();
				count++;
			}
			if (getSource().hasTop() && getSource().getTopKey().compareTo(lastKey, depth) == 0) {
				reseek(getSource().getTopKey().followingKey(depth));
			}
		}
		
		if (getSource().hasTop()) {
			lastKey = getSource().getTopKey();
		}
	}
	
	protected void reseek(Key key) throws IOException {
		if (range.afterEndKey(key)) {
			range = new Range(range.getEndKey(), true, range.getEndKey(), range.isEndKeyInclusive());
		} else {
			range = new Range(key, true, range.getEndKey(), range.isEndKeyInclusive());
		}
		getSource().seek(range, EMPTY_SET, false);
	}

	
	@Override
	public void init(SortedKeyValueIterator<Key, Value> source, Map<String, String> options, IteratorEnvironment env) throws IOException {
		super.init(source, options, env);
		
		if (options.containsKey(OPTION_DEPTH)) {
			depth = PartialKey.getByDepth(Integer.parseInt(options.get(OPTION_DEPTH)));
		} else {
			depth = PartialKey.ROW_COLFAM;
		}
	}

	@Override
	public SortedKeyValueIterator<Key, Value> deepCopy(IteratorEnvironment env) {
		UniqueIterator u = new UniqueIterator(this);
		u.setSource(getSource().deepCopy(env));
		return u;
	}
	
	@Override
	public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
		this.range = range;
		getSource().seek(range, columnFamilies, inclusive);
		consume();
	}
}
