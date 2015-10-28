package mvm.rya.indexing;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import mvm.rya.indexing.accumulo.Md5Hash;
import mvm.rya.indexing.accumulo.StatementSerializer;

import org.apache.accumulo.core.data.Value;
import org.apache.commons.codec.binary.StringUtils;
import org.apache.hadoop.io.Text;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.impl.ContextStatementImpl;
import org.openrdf.model.impl.StatementImpl;
import org.openrdf.model.impl.URIImpl;

import com.google.common.collect.Constraints;

/**
 * Store and format the various temporal index keys.
 * Row Keys are in these two forms, where [x] denotes x is optional:
 * 		rowkey = contraintPrefix datetime
 * 		rowkey = datetime 0x/00 uniquesuffix
 * 		contraintPrefix = 0x/00 hash([subject][predicate])
 * 		uniquesuffix = some bytes to make it unique, like hash(statement).
 * 
 * The instance is in one of two modes depending on the constructor:
 * 		storage mode  -- construct with a triple statement, get an iterator of keys to store.  
 * 		query mode	  -- construct with a statement and query constraints, get the key prefix to search.
 * 
 * this has the flavor of an immutable object
 * This is independent of the underlying database engine
 * 
 * @author David.Lotts
 *
 */
public class KeyParts implements Iterable<KeyParts> { 
    	private static final String CQ_S_P_AT = "spo";
    	private static final String CQ_P_AT = "po";
    	private static final String CQ_S_AT = "so";
    	private static final String CQ_O_AT = "o";
    	public static final String CQ_BEGIN = "begin";
    	public static final String CQ_END = "end";
        
        public static final byte[] HASH_PREFIX = new byte[] {0};
        public static final byte[] HASH_PREFIX_FOLLOWING = new byte[] {1};

		public final Text cf;
		public final Text cq;
		public final Text constraintPrefix; // subject and/or predicate
		final Text storeKey; // subject and/or predicate
		final private TemporalInstant instant;
		final private Statement statement;
		final private boolean queryMode;
		KeyParts(Text constraintPrefix, TemporalInstant instant, String cf, String cq) {
			this.queryMode = true; // query mode
			this.storeKey = null;
			this.statement = null;
			this.constraintPrefix = constraintPrefix;
			this.instant = instant;
			this.cf = new Text(cf);
			this.cq = new Text(cq);
		}
		
		/**
		 * this is the value to index.
		 * @return
		 */
		public Value getValue() {
			assert statement!=null;
			return new Value(StringUtils.getBytesUtf8(StatementSerializer.writeStatement(statement)));
		}

		public KeyParts(Statement statement, TemporalInstant instant2) {
			this.queryMode = false; // store mode
			this.storeKey = null;
			this.constraintPrefix = null;
			this.statement = statement;
			this.instant = instant2;
			this.cf = null;
			this.cq = null;
		}

		private KeyParts(Text keyText, Text cf, Text cq, Statement statement) {
			this.queryMode = false; // store mode
			this.constraintPrefix = null;
			this.statement = statement;
			this.instant = null;
			this.storeKey = keyText;
			this.cf = cf;
			this.cq = cq;
		}
		
		@Override
		public Iterator<KeyParts> iterator() {
			final String[] strategies = new String[] {    
					CQ_O_AT, CQ_S_P_AT, CQ_P_AT, CQ_S_AT
				} ;  // CQ_END?
			assert !queryMode : "iterator for queryMode is not immplemented" ;  
			if (queryMode)
				return null;

			// if (!queryMode)
			return new Iterator<KeyParts>() {
				int nextStrategy = 0;

				@Override
				public boolean hasNext() {
					return nextStrategy < strategies.length;
				}

				@Override
				public KeyParts next() {
					assert(statement!=null);
					Text keyText = new Text();
					// increment++ the next strategy AFTER getting the value
					switch (nextStrategy++) {
					case 0: // index o+hash(p+s)
						assert (CQ_O_AT.equals(strategies[0]));
						keyText = new Text(instant.getAsKeyBytes());
						KeyParts.appendUniqueness(statement, keyText);
						return new KeyParts(keyText, new Text(StatementSerializer.writeContext(statement)), new Text(CQ_O_AT), statement);
					case 1:// index hash(s+p)+o
						assert (CQ_S_P_AT.equals(strategies[1]));
						KeyParts.appendSubjectPredicate(statement, keyText);
						KeyParts.appendInstant(instant, keyText);
						// appendUniqueness -- Not needed since it is already unique.
						return new KeyParts(keyText, new Text(StatementSerializer.writeContext(statement)), new Text(CQ_S_P_AT), statement);
					case 2: // index hash(p)+o
						assert (CQ_P_AT.equals(strategies[2]));
						KeyParts.appendPredicate(statement, keyText);
						KeyParts.appendInstant(instant, keyText);
						KeyParts.appendUniqueness(statement, keyText);
						return new KeyParts(keyText, new Text(StatementSerializer.writeContext(statement)), new Text(CQ_P_AT), statement);
					case 3: // index hash(s)+o
						assert (CQ_S_AT.equals(strategies[3]));
						KeyParts.appendSubject(statement, keyText);
						KeyParts.appendInstant(instant, keyText);
						KeyParts.appendUniqueness(statement, keyText);
						return new KeyParts(keyText, new Text(StatementSerializer.writeContext(statement)), new Text(CQ_S_AT), statement);
					}
					throw new Error("Next passed end?  No such nextStrategy="+(nextStrategy-1));

				}

				@Override
				public void remove() {
					throw new Error("Remove not Implemented.");
				}
			};
		}
		
		public byte[] getStoreKey() {
			assert !queryMode : "must be in store Mode, store keys are not initialized.";
			return this.storeKey.copyBytes();
		}

		/**
		 * Query key is the prefix plus the datetime, but no uniqueness at the end.
		 * @return the row key for range queries.
		 */
	public Text getQueryKey() {
		return getQueryKey(this.instant);
	};

	/**
	 * Query key is the prefix plus the datetime, but no uniqueness at the end.
	 * 
	 * @return the row key for range queries.
	 */
	public Text getQueryKey(TemporalInstant theInstant) {
		assert queryMode : "must be in query Mode, query keys are not initialized.";
		Text keyText = new Text();
		if (constraintPrefix != null)
			appendBytes(constraintPrefix.copyBytes(), keyText);
		appendInstant(theInstant, keyText);
		return keyText;
	};

		@Override
		public String toString() {
			return "KeyParts [contraintPrefix=" + toHumanString(constraintPrefix) + ", instant=" + toHumanString(instant.getAsKeyBytes()) + ", cf=" + cf + ", cq=" + cq + "]";
		}
	    private static void appendSubject(Statement statement, Text keyText) {
	        Value statementValue = new Value(StatementSerializer.writeSubject(statement).getBytes());
	        byte[] hashOfValue = uniqueFromValueForKey(statementValue);
	        appendBytes(HASH_PREFIX, keyText); // prefix the hash with a zero byte. 
	        appendBytes(hashOfValue, keyText);
		}

		private static void appendPredicate(Statement statement, Text keyText) {
	        Value statementValue = new Value(StringUtils.getBytesUtf8(StatementSerializer.writePredicate(statement)));
	        byte[] hashOfValue = uniqueFromValueForKey(statementValue);
	        appendBytes(HASH_PREFIX, keyText); // prefix the hash with a zero byte. 
	        appendBytes(hashOfValue, keyText);
		}

		private static void appendInstant(TemporalInstant instant, Text keyText) {
			byte[] bytes = instant.getAsKeyBytes();
	        appendBytes(bytes, keyText);
		}

		private static void appendSubjectPredicate(Statement statement, Text keyText) {
	        Value statementValue = new Value(StringUtils.getBytesUtf8(StatementSerializer.writeSubjectPredicate(statement)));
	        byte[] hashOfValue = uniqueFromValueForKey(statementValue);
	        appendBytes(HASH_PREFIX, keyText); // prefix the hash with a zero byte. 
	        appendBytes(hashOfValue, keyText);
		}

		/**
		 * Append any byte array to a row key.
		 * @param bytes append this
		 * @param keyText text to append to
		 */
		private static void appendBytes(byte[] bytes, Text keyText) {
			keyText.append(bytes, 0, bytes.length);
		}

		/**
	     * Get a collision unlikely hash string and append to the key, 
	     * so that if two keys have the same value, then they will be the same,
	     * if two different values that occur at the same time there keys are different.
	     * If the application uses a very large number of statements at the exact same time,
	     * the md5 value might be upgraded to for example sha-1 to avoid collisions.
	     * @param statement
	     * @param keyText
	     */
	    public static void appendUniqueness(Statement statement, Text keyText) {
	        keyText.append(HASH_PREFIX, 0, 1);   // delimiter
	        Value statementValue = new Value(StringUtils.getBytesUtf8(StatementSerializer.writeStatement(statement)));
	        byte[] hashOfValue = Md5Hash.md5Binary(statementValue);
	        keyText.append(hashOfValue, 0, hashOfValue.length);
	    }
	    /**
	     * Get a collision unlikely hash string to append to the key, 
	     * so that if two keys have the same value, then they will be the same,
	     * if two different values that occur at the same time there keys are different.
	     * @param value
	     * @return
	     */
	    private static byte[] uniqueFromValueForKey(Value value) {
	        return Md5Hash.md5Binary(value);
	    }

		/**
		 * List all the index keys to find for any query.  Set the strategy via the column qualifier, ex: CQ_S_P_AT.
		 * Column Family (CF) is the context/named-graph.
		 * @param queryInstant
		 * @param contraints
		 * @return
		 */
		static public List<KeyParts> keyPartsForQuery(TemporalInstant queryInstant, StatementContraints contraints) {
			List<KeyParts> keys = new LinkedList<KeyParts>();
			URI urlNull = new URIImpl("urn:null");
			Resource currentContext = contraints.getContext();
			boolean hasSubj = contraints.hasSubject();
			if (contraints.hasPredicates()) {
				for (URI nextPredicate : contraints.getPredicates()) {
					Text contraintPrefix  = new Text();
					Statement statement = new ContextStatementImpl(hasSubj ? contraints.getSubject() : urlNull, nextPredicate, urlNull, contraints.getContext());
					if (hasSubj)
						appendSubjectPredicate(statement, contraintPrefix);
					else
						appendPredicate(statement, contraintPrefix);
					keys.add(new KeyParts(contraintPrefix, queryInstant, (currentContext==null)?"":currentContext.toString(), hasSubj?CQ_S_P_AT:CQ_P_AT  ));
				}
			}
			else if (contraints.hasSubject()) { // and no predicates
				Text contraintPrefix = new Text();
				Statement statement = new StatementImpl(contraints.getSubject(), urlNull, urlNull);
				appendSubject(statement, contraintPrefix);
				keys.add( new KeyParts(contraintPrefix, queryInstant, (currentContext==null)?"":currentContext.toString(), CQ_S_AT) );
			}
			else {
				// No constraints except possibly a context/named-graph, handled by the CF
				 keys.add( new KeyParts(null, queryInstant, (currentContext==null)?"":currentContext.toString(), CQ_O_AT) );
			}
			return keys;
		}
	    /**
	     * convert a non-utf8 byte[] and text and value to string and show unprintable bytes as {xx} where x is hex.
	     * @param value
	     * @return Human readable representation.
	     */
		public static String toHumanString(Value value) {
			return toHumanString(value==null?null:value.get());
		}
		public static String toHumanString(Text text) {
			return toHumanString(text==null?null:text.copyBytes());
		}
		public static String toHumanString(byte[] bytes) {
			if (bytes==null) 
				return "{null}";
			StringBuilder sb = new StringBuilder();
			for (byte b : bytes) {
				if ((b > 0x7e) || (b < 32)) {
					sb.append("{");
					sb.append(Integer.toHexString( b & 0xff )); // Lop off the sign extended ones.
					sb.append("}");
				} else if (b == '{'||b == '}') { // Escape the literal braces.
					sb.append("{");
					sb.append((char)b);
					sb.append("}");
				} else
					sb.append((char)b);
			}
			return sb.toString();
		}
		
	}