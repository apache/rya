/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.rya.accumulo.documentIndex;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.ArrayByteSequence;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.tabletserver.thrift.TabletClientService;
import org.apache.accumulo.core.util.TextUtil;
import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

/**
 * This iterator facilitates document-partitioned indexing. It involves grouping a set of documents together and indexing those documents into a single row of
 * an Accumulo table. This allows a tablet server to perform boolean AND operations on terms in the index.
 * 
 * The table structure should have the following form:
 * 
 * row: shardID, colfam: term, colqual: docID
 * 
 * When you configure this iterator with a set of terms (column families), it will return only the docIDs that appear with all of the specified terms. The
 * result will have an empty column family, as follows:
 * 
 * row: shardID, colfam: (empty), colqual: docID
 * 
 * This iterator is commonly used with BatchScanner or AccumuloInputFormat, to parallelize the search over all shardIDs.
 * 
 * This iterator will *ignore* any columnFamilies passed to {@link #seek(Range, Collection, boolean)} as it performs intersections over terms. Extending classes
 * should override the {@link TermSource#seekColfams} in their implementation's {@link #init(SortedKeyValueIterator, Map, IteratorEnvironment)} method.
 * 
 * README.shard in docs/examples shows an example of using the IntersectingIterator.
 */
public class DocumentIndexIntersectingIterator implements SortedKeyValueIterator<Key,Value> {
  
    
    
    
  protected Text nullText = new Text();
  
  protected Text getRow(Key key) {
    return key.getRow();
  }
  
  protected Text getTerm(Key key) {
    return key.getColumnFamily();
  }
  
  protected Text getTermCond(Key key) {
    return key.getColumnQualifier();
  }
  
  protected Key buildKey(Text row, TextColumn column) {
      return new Key(row, (column.getColumnFamily() == null) ? nullText: column.getColumnFamily(), column.getColumnQualifier());
  }
  
  protected Key buildKey(Text row, Text term) {
    return new Key(row, (term == null) ? nullText : term);
  }
  
  protected Key buildKey(Text row, Text term, Text termCond) {
    return new Key(row, (term == null) ? nullText : term, termCond);
  }
  
  protected Key buildFollowRowKey(Key key, Text term, Text termCond) {
    return new Key(getRow(key.followingKey(PartialKey.ROW)),(term == null) ? nullText : term, termCond);
  }
  
    protected static final Logger log = Logger.getLogger(DocumentIndexIntersectingIterator.class);

    public static class TermSource {
        public SortedKeyValueIterator<Key, Value> iter;
        public Text term;
        public Text termCond;
        public Collection<ByteSequence> seekColfams;
        public TextColumn column;
        public boolean isPrefix;
        public Key top ;
        public Key next ;
        public Text currentCQ;
        private boolean seeked = false;

        public TermSource(TermSource other) {
         
            this.iter = other.iter;
            this.term = other.term;
            this.termCond = other.termCond;
            this.seekColfams = other.seekColfams;
            this.column = other.column;
            this.top = other.top;
            this.next = other.next;
            this.currentCQ = other.currentCQ;
            this.isPrefix = other.isPrefix;
        }


        public TermSource(SortedKeyValueIterator<Key, Value> iter, TextColumn column) {
           
            this.iter = iter;
            this.column = column;
            this.term = column.getColumnFamily();
            this.termCond = column.getColumnQualifier();
            this.currentCQ = new Text(emptyByteArray);
            this.seekColfams = Collections.<ByteSequence> singletonList(new ArrayByteSequence(term
                    .getBytes(), 0, term.getLength()));
           
        }
        
        
        
        public void seek(Range r) throws IOException {

            if (seeked) {
 
                if (next != null && !r.beforeStartKey(next)) {
                    if (next.getColumnFamily().equals(term)) {
                        this.updateTop();
                    }
                } else if (iter.hasTop()) {
                    iter.seek(r, seekColfams, true);
                    this.updateTopNext();
                } else {
                    top = null;
                    next = null;
                
                }
            } else {

                iter.seek(r, seekColfams, true);
                this.updateTopNext();
                seeked = true;
            }

        }
        
        
        public void next() throws IOException {

            this.updateTop();
        }
        
        public void updateTop() throws IOException {

            top = next;
            if (next != null) {
                iter.next();
                if (iter.hasTop()) {
                    next = iter.getTopKey();
                } else {
                    next = null;
                }
            }

        }
        
        public void updateTopNext() throws IOException {

            if (iter.hasTop()) {
                top = iter.getTopKey();
            } else {
                top = null;
                next = null;
                return;
            }
            
            iter.next();
            
            if(iter.hasTop()) {
                next = iter.getTopKey();
            } else {
                next = null;
            }
        }
        
        public boolean hasTop() {
            return top != null;
        }
        

        public String getTermString() {
            return (this.term == null) ? new String("Iterator") : this.term.toString();
        }
    }
  
  TermSource[] sources;
  int sourcesCount = 0;
  Range overallRange;
  
  // query-time settings
  protected Text currentRow = null;
  protected Text currentTermCond = new Text(emptyByteArray);
  static final byte[] emptyByteArray = new byte[0];
  
  protected Key topKey = null;
  protected Value value = new Value(emptyByteArray);
  protected String ctxt = null;
  protected boolean hasContext = false;
  protected boolean termCondSet = false;
  
  public DocumentIndexIntersectingIterator() {}
  
  @Override
  public SortedKeyValueIterator<Key,Value> deepCopy(IteratorEnvironment env) {
      //log.info("Calling deep copy on " + this);
    return new DocumentIndexIntersectingIterator(this, env);
  }
  
  private DocumentIndexIntersectingIterator(DocumentIndexIntersectingIterator other, IteratorEnvironment env) {
    if (other.sources != null) {
      sourcesCount = other.sourcesCount;
      sources = new TermSource[sourcesCount];
      for (int i = 0; i < sourcesCount; i++) {
        sources[i] = new TermSource(other.sources[i].iter.deepCopy(env), other.sources[i].column);
      }
    }
  }
  
  @Override
  public Key getTopKey() {

    return topKey;
  }
  
  @Override
  public Value getTopValue() {
    // we don't really care about values
    return value;
  }
  
  @Override
  public boolean hasTop() {
    return currentRow != null;
  }

    // precondition: currentRow is not null
    private boolean seekOneSource(int sourceID) throws IOException {
        // find the next key in the appropriate column family that is at or
        // beyond the cursor (currentRow, currentCQ)
        // advance the cursor if this source goes beyond it
        // return whether we advanced the cursor

        // within this loop progress must be made in one of the following forms:
        // - currentRow or currentCQ must be increased
        // - the given source must advance its iterator
        // this loop will end when any of the following criteria are met
        // - the iterator for the given source is pointing to the key
        // (currentRow, columnFamilies[sourceID], currentCQ)
        // - the given source is out of data and currentRow is set to null
        // - the given source has advanced beyond the endRow and currentRow is
        // set to null
        boolean advancedCursor = false;
        
      
        
        

        while (true) {
            
//            if(currentRow.toString().equals(s)) {
//                log.info("Source id is " + sourceID);
//                if (sources[sourceID].top != null) {
//                    log.info("Top row is " + getRow(sources[sourceID].top));
//                    log.info("Top cq is " + getTermCond(sources[sourceID].top));
//                }
//                if (sources[sourceID].next != null) {
//                    log.info("Next row is " + getRow(sources[sourceID].next));
//                    log.info("Next termCond is " + getTermCond(sources[sourceID].next));
//                }
//            }
            
            if (sources[sourceID].hasTop() == false) {
                currentRow = null;
                // setting currentRow to null counts as advancing the cursor
                return true;
            }
            // check if we're past the end key
            int endCompare = -1;
            // we should compare the row to the end of the range

            if (overallRange.getEndKey() != null) {
                endCompare = overallRange.getEndKey().getRow().compareTo(sources[sourceID].top.getRow());
                if ((!overallRange.isEndKeyInclusive() && endCompare <= 0) || endCompare < 0) {
                    currentRow = null;
                    // setting currentRow to null counts as advancing the cursor
                    return true;
                }
            }
            

            
            int rowCompare = currentRow.compareTo(getRow(sources[sourceID].top));
            // check if this source is already at or beyond currentRow
            // if not, then seek to at least the current row
            
        
            
            if (rowCompare > 0) {
                // seek to at least the currentRow
                Key seekKey = buildKey(currentRow, sources[sourceID].term);
                sources[sourceID].seek(new Range(seekKey, true, null, false));
              
                continue;
            }
            // check if this source has gone beyond currentRow
            // if so, advance currentRow
            if (rowCompare < 0) {
                currentRow.set(getRow(sources[sourceID].top));
                //log.info("Current row is " + currentRow);
                advancedCursor = true;
                continue;
            }
            // we have verified that the current source is positioned in
            // currentRow
            // now we must make sure we're in the right columnFamily in the
            // current row
            // Note: Iterators are auto-magically set to the correct
            // columnFamily

            if (sources[sourceID].column.isValid()) {
                
                boolean isPrefix = false;
                boolean contextEqual = false;
                String tempContext = "";
                
                int termCompare;

                String[] cQ = getTermCond(sources[sourceID].top).toString().split("\u0000");
                tempContext = cQ[0];

                if (!hasContext && ctxt == null) {
                    ctxt = cQ[0];
                }

                contextEqual = ctxt.equals(cQ[0]);

                String s1 = sources[sourceID].termCond.toString();
                String s2 = cQ[1] + "\u0000" + cQ[2];

                if (sources[sourceID].isPrefix) {
                    isPrefix = s2.startsWith(s1 + "\u0000");
                } else {
                    isPrefix = s2.startsWith(s1);
                }

                termCompare = (contextEqual && isPrefix) ? 0 : (ctxt + "\u0000" + s1).compareTo(cQ[0] + "\u0000" + s2);

                // if(currentRow.toString().equals(s)) {
                // log.info("Term compare is " + termCompare);
                // }
           
                // check if this source is already on the right columnFamily
                // if not, then seek forwards to the right columnFamily
                if (termCompare > 0) {
                    Key seekKey = buildKey(currentRow, sources[sourceID].term, new Text(ctxt + 
                            "\u0000" + sources[sourceID].termCond.toString()));
                    sources[sourceID].seek(new Range(seekKey, true, null, false));
                 
                    continue;
                }
                // check if this source is beyond the right columnFamily
                // if so, then seek to the next row
                if (termCompare < 0) {
                    // we're out of entries in the current row, so seek to the
                    // next one
                    
                    if (endCompare == 0) {
                        // we're done
                        currentRow = null;
                        // setting currentRow to null counts as advancing the
                        // cursor
                        return true;
                    }
                    
                    
                    
                    //advance to next row if context set - all entries in given row exhausted
                    if (hasContext || tempContext.length() == 0) {
                        Key seekKey = buildFollowRowKey(sources[sourceID].top, sources[sourceID].term,
                                new Text(ctxt + "\u0000" + sources[sourceID].termCond.toString()));
                        sources[sourceID].seek(new Range(seekKey, true, null, false));
                    } else {
                        
                        if(contextEqual && !isPrefix) {
                            Key seekKey = buildKey(currentRow, sources[sourceID].term, new Text(ctxt + "\u0001"));
                            sources[sourceID].seek(new Range(seekKey, true, null, false));
                            if(sources[sourceID].top != null) {
                                ctxt = getTermCond(sources[sourceID].top).toString().split("\u0000")[0];
                            } 
                        } else {
                            Key seekKey = buildKey(currentRow, sources[sourceID].term, new Text(tempContext + 
                                    "\u0000" + sources[sourceID].termCond.toString()));
                            sources[sourceID].seek(new Range(seekKey, true, null, false));
                            if(sources[sourceID].top != null) {
                                ctxt = getTermCond(sources[sourceID].top).toString().split("\u0000")[0];
                            } 
                        }
                        
                    }
                    
                    
//                    if(currentRow.toString().equals(s)) {
//                        log.info("current term cond is " + currentTermCond);
//                        
//                    }
                    
             
                    continue;
                }
            }
         
            
            
         
            
            
            
            
            
            
            //set currentTermCond -- gets appended to end of currentKey column qualifier
            //used to determine which term iterator to advance when a new iterator is created
            
            sources[sourceID].currentCQ.set(getTermCond(sources[sourceID].top));
            
            if (sources[sourceID].next != null) {
                        
                //is hasContext, only consider sourceID with next having designated context
                //otherwise don't set currentTermCond
                if (!termCondSet && hasContext) {
                    if (sources[sourceID].next.getRow().equals(currentRow)
                            && sources[sourceID].next.getColumnQualifier().toString()
                                    .startsWith(ctxt + "\u0000" + sources[sourceID].termCond.toString())) {
                        currentTermCond.set(new Text(Integer.toString(sourceID)));
                        termCondSet = true;
                    }
                } else if(!termCondSet){
                    String[] cq = getTermCond(sources[sourceID].next).toString().split("\u0000");
                    
                    //set currentTermCond with preference given to sourceID having next with same context
                    //otherwise set currentTermCond sourceID with next having termCond as prefix
                    if (sources[sourceID].next.getRow().equals(currentRow)) {
                        if (sources[sourceID].next.getColumnQualifier().toString()
                                .startsWith(ctxt + "\u0000" + sources[sourceID].termCond.toString())) {
                            currentTermCond.set(new Text(Integer.toString(sourceID)));
                            termCondSet = true;
                        } else if ((cq[1] + "\u0000" + cq[2]).startsWith(sources[sourceID].termCond.toString())) {
                            currentTermCond.set(new Text(Integer.toString(sourceID)));
                        }
                    }
                }
            } 
       
           
            break;
        }

        return advancedCursor;
    }
  
  @Override
  public void next() throws IOException {
    if (currentRow == null) {
      return;
    }
   
    
    
    if(currentTermCond.getLength() != 0) {
        
        int id = Integer.parseInt(currentTermCond.toString());

        sources[id].next();
        currentTermCond.set(emptyByteArray);
        termCondSet = false;
        if(sources[id].top != null && !hasContext) {
            ctxt = getTermCond(sources[id].top).toString().split("\u0000")[0];
        }
        advanceToIntersection();
        return;
    }
    
    sources[0].next();
    if(sources[0].top != null && !hasContext) {
        ctxt = getTermCond(sources[0].top).toString().split("\u0000")[0];
    }
    advanceToIntersection();
  }
  
  protected void advanceToIntersection() throws IOException {
    boolean cursorChanged = true;
    while (cursorChanged) {
      // seek all of the sources to at least the highest seen column qualifier in the current row
      cursorChanged = false;
      for (int i = 0; i < sourcesCount; i++) {
//          log.info("New sourceID is " + i);
        if (currentRow == null) {
          topKey = null;
          return;
        }
        if (seekOneSource(i)) {
          currentTermCond.set(emptyByteArray); 
          termCondSet = false;
          cursorChanged = true;
          break;
        }
      }
    }
    String cq = "";
    for(int i = 0; i < sourcesCount; i++) {
        cq = cq + sources[i].currentCQ.toString() + DocIndexIteratorUtil.DOC_ID_INDEX_DELIM;
    }
    
        if (currentTermCond.getLength() == 0) {
            topKey = buildKey(currentRow, nullText, new Text(cq + -1));
        } else {
            topKey = buildKey(currentRow, nullText, new Text(cq + currentTermCond.toString()));
        }
  }
  
  public static String stringTopKey(SortedKeyValueIterator<Key,Value> iter) {
    if (iter.hasTop())
      return iter.getTopKey().toString();
    return "";
  }
  
  private static final String columnOptionName = "columns";
  private static final String columnPrefix = "prefixes";
  private static final String context = "context";
  
  
  
  protected static String encodeColumns(TextColumn[] columns) {
      StringBuilder sb = new StringBuilder();
      for (int i = 0; i < columns.length; i++) {
        sb.append(new String(Base64.encodeBase64(TextUtil.getBytes(columns[i].getColumnFamily()))));
        sb.append('\n');
        sb.append(new String(Base64.encodeBase64(TextUtil.getBytes(columns[i].getColumnQualifier()))));
        sb.append('\u0001');
      }
      return sb.toString();
    }
    
  
  
  protected static TextColumn[] decodeColumns(String columns) {
      String[] columnStrings = columns.split("\u0001");
      TextColumn[] columnTexts = new TextColumn[columnStrings.length];
      for (int i = 0; i < columnStrings.length; i++) {
        String[] columnComponents = columnStrings[i].split("\n");
        columnTexts[i] = new TextColumn(new Text(Base64.decodeBase64(columnComponents[0].getBytes())), 
                new Text(Base64.decodeBase64(columnComponents[1].getBytes())));
      }
      return columnTexts;
    }
  
 
  
  
  
  /**
   * @param context
   * @return encoded context
   */
  protected static String encodeContext(String context) {
 
    return new String(Base64.encodeBase64(context.getBytes()));
  }
  
 
  
  /**
   * @param context
   * @return decoded context
   */
    protected static String decodeContext(String context) {

        if (context == null) {
            return null;
        } else {
            return new String(Base64.decodeBase64(context.getBytes()));
        }
    }
  
  
  
  
  
  protected static String encodeBooleans(boolean[] prefixes) {
      byte[] bytes = new byte[prefixes.length];
      for (int i = 0; i < prefixes.length; i++) {
        if (prefixes[i])
          bytes[i] = 1;
        else
          bytes[i] = 0;
      }
      return new String(Base64.encodeBase64(bytes));
    }
    
    /**
     * @param flags
     * @return decoded flags
     */
    protected static boolean[] decodeBooleans(String prefixes) {
      // return null of there were no flags
      if (prefixes == null)
        return null;
      
      byte[] bytes = Base64.decodeBase64(prefixes.getBytes());
      boolean[] bFlags = new boolean[bytes.length];
      for (int i = 0; i < bytes.length; i++) {
        if (bytes[i] == 1)
          bFlags[i] = true;
        else
          bFlags[i] = false;
      }
      return bFlags;
    }
  
  
  
  
  
  
  
  
  @Override
  public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
    TextColumn[] terms = decodeColumns(options.get(columnOptionName));
    boolean[] prefixes = decodeBooleans(options.get(columnPrefix));
    ctxt = decodeContext(options.get(context));
    
    if(ctxt != null) {
        hasContext = true;
    }
  
   
    
    if (terms.length < 2) {
      throw new IllegalArgumentException("IntersectionIterator requires two or more columns families");
    }
    
    sources = new TermSource[terms.length];
    sources[0] = new TermSource(source, terms[0]);
    for (int i = 1; i < terms.length; i++) {
        //log.info("For decoded column " + i + " column family is " + terms[i].getColumnFamily() + " and qualifier is " + terms[i].getColumnQualifier());
      sources[i] = new TermSource(source.deepCopy(env), terms[i]);
      sources[i].isPrefix = prefixes[i];
    }
    sourcesCount = terms.length;
  }
  
    @Override
    public void seek(Range range, Collection<ByteSequence> seekColumnFamilies, boolean inclusive) throws IOException {
        overallRange = new Range(range);
        currentRow = new Text();
        currentTermCond.set(emptyByteArray);
        termCondSet = false;


        
//       log.info("Calling seek with range " + range);

        // seek each of the sources to the right column family within the row
        // given by key
       
        Key sourceKey;

        if (rangeCqValid(range)) {
            
            String[] cqInfo = cqParser(range.getStartKey().getColumnQualifier());
            int id = Integer.parseInt(cqInfo[1]);
            

            
            if (id >= 0) {
                for (int i = 0; i < sourcesCount; i++) {

                    if (i == id) {
                        sourceKey = buildKey(getRow(range.getStartKey()), sources[i].term, new Text(cqInfo[0]));
                        sources[i].seek(new Range(sourceKey, true, null, false));
                        sources[i].next();
                        if(!hasContext && sources[i].hasTop()) {
                            ctxt = getTermCond(sources[i].top).toString().split("\u0000")[0];
                        }
                    } else {
                        sourceKey = buildKey(getRow(range.getStartKey()), sources[i].term);
                        sources[i].seek(new Range(sourceKey, true, null, false));
                    }
                }
            } else {
                

                for (int i = 0; i < sourcesCount; i++) {
                    sourceKey = buildKey(getRow(range.getStartKey()), sources[i].term, range.getStartKey()
                            .getColumnQualifier());
                    sources[i].seek(new Range(sourceKey, true, null, false));
                }
            }
                
            
        } else {
            
//            log.info("Range is invalid.");
            for (int i = 0; i < sourcesCount; i++) {

                if (range.getStartKey() != null) {

                    sourceKey = buildKey(getRow(range.getStartKey()), sources[i].term);

                    // Seek only to the term for this source as a column family
                    sources[i].seek(new Range(sourceKey, true, null, false));
                } else {
                    // Seek only to the term for this source as a column family

                    sources[i].seek(range);
                }
            }
        }
        
        advanceToIntersection();

    }
    
    
    private String[] cqParser(Text cq) {
        
        String cQ = cq.toString();
        String[] cqComponents = cQ.split(DocIndexIteratorUtil.DOC_ID_INDEX_DELIM);
        int id = -1;
        String[] valPos = new String[2];
        

        
        
        if(cqComponents.length > 1) {
            id = Integer.parseInt(cqComponents[cqComponents.length-1]);
            if (id >= 0) {
                valPos[0] = cqComponents[id].toString();
                valPos[1] = "" + id;
            } else {
                valPos[0] = cqComponents[0].toString();
                valPos[1] = "" + id;
            }
        } else {
            valPos[0] = cq.toString();
            valPos[1] = "" + -1;
        }
        
        return valPos;
       
    }
    
  
  private boolean rangeCqValid(Range range) {
      return (range.getStartKey() != null) && (range.getStartKey().getColumnQualifier() != null);
  }
  
  
  
  public void addSource(SortedKeyValueIterator<Key,Value> source, IteratorEnvironment env, TextColumn column) {
    // Check if we have space for the added Source
    if (sources == null) {
      sources = new TermSource[1];
    } else {
      // allocate space for node, and copy current tree.
      // TODO: Should we change this to an ArrayList so that we can just add() ? - ACCUMULO-1309
      TermSource[] localSources = new TermSource[sources.length + 1];
      int currSource = 0;
      for (TermSource myTerm : sources) {
        // TODO: Do I need to call new here? or can I just re-use the term? - ACCUMULO-1309
        localSources[currSource] = new TermSource(myTerm);
        currSource++;
      }
      sources = localSources;
    }
    sources[sourcesCount] = new TermSource(source.deepCopy(env), column);
    sourcesCount++;
  }
  
  /**
   * Encode the columns to be used when iterating.
   * 
   * @param cfg
   * @param columns
   */
  public static void setColumnFamilies(IteratorSetting cfg, TextColumn[] columns) {
    if (columns.length < 2)
      throw new IllegalArgumentException("Must supply at least two terms to intersect");
    
    boolean[] prefix = new boolean[columns.length];
    
    for(int i = 0; i < columns.length; i++) {
        prefix[i] = columns[i].isPrefix();
    }
    
    
    
    cfg.addOption(DocumentIndexIntersectingIterator.columnPrefix, DocumentIndexIntersectingIterator.encodeBooleans(prefix));
    cfg.addOption(DocumentIndexIntersectingIterator.columnOptionName, DocumentIndexIntersectingIterator.encodeColumns(columns));
  }
  
  
  
  
  
  public static void setContext(IteratorSetting cfg, String context) {
     
      cfg.addOption(DocumentIndexIntersectingIterator.context, DocumentIndexIntersectingIterator.encodeContext(context));
      
    }
  
  
  
  
  
  
  
  
  
  
  
  
  
}
