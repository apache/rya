package mvm.mmrts.rdf.partition.query.evaluation.select.utils;

import cloudbase.core.data.Key;
import cloudbase.core.data.Value;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.io.ByteStreams;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;

import java.util.*;

import static mvm.mmrts.rdf.partition.PartitionConstants.DOC;
import static mvm.mmrts.rdf.partition.PartitionConstants.VALUE_FACTORY;
import static mvm.mmrts.rdf.partition.utils.RdfIO.readStatement;

/**
 * This iterator will seek forward in the underlying BatchScanner Iterator and group
 * statements with the same subject.  This guards against the fact that the BatchScanner can return
 * statements out of order.
 * <br/>
 * TODO: Not the best solution.
 * Class DocumentIterator
 * Date: Aug 29, 2011
 * Time: 4:09:16 PM
 */
public class DocumentIterator implements Iterator<List<Statement>> {

    public static final int BATCH_SIZE = 1000;

    private int batchSize = BATCH_SIZE; //will hold up to 100 subject documents
    /**
     * TODO: Check performance against other multi maps
     */
    private ListMultimap<Resource, Statement> documents = ArrayListMultimap.create();
    //TODO: Hate having to keep track of this, expensive to constantly check the "contains"
    /**
     * We keep track of a queue of subjects, so that the first one in will most likely have all of its document
     * in our batch before popping. This assumes also that the documents won't get larger than 1000 at the most.
     */
    private LinkedList<Resource> subjects = new LinkedList<Resource>();

    private Iterator<Map.Entry<Key, Value>> iter;
    private boolean hasNext = true;

    public DocumentIterator(Iterator<Map.Entry<Key, Value>> iter) {
        this(iter, BATCH_SIZE);
    }

    public DocumentIterator(Iterator<Map.Entry<Key, Value>> iter, int batchSize) {
        this.iter = iter;
        this.batchSize = batchSize;
        fillDocumentMap();
    }

    protected void fillDocumentMap() {
        try {
            while ((documents.size() < batchSize) && statefulHasNext()) {
                Statement stmt = nextStatement();
                Resource subj = stmt.getSubject();
                documents.put(subj, stmt);
                if (!subjects.contains(subj))
                    subjects.add(subj);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    protected boolean statefulHasNext() {
        hasNext = iter.hasNext() && hasNext;
        return hasNext;
    }

    protected Statement nextStatement() throws Exception {
        Map.Entry<Key, Value> entry = iter.next();
        Key key = entry.getKey();
        if (DOC.equals(key.getColumnFamily()))
            return readStatement(ByteStreams.newDataInput(key.getColumnQualifier().getBytes()), VALUE_FACTORY);
        else
            return readStatement(ByteStreams.newDataInput(key.getColumnQualifier().getBytes()), VALUE_FACTORY, false);
    }

    @Override
    public boolean hasNext() {
        fillDocumentMap();
        return documents.size() > 0;
    }

    @Override
    public List<Statement> next() {
        fillDocumentMap();
        if (subjects.size() > 0) {
            Resource subject = subjects.pop();
            subjects.remove(subject);
            List<Statement> doc = documents.removeAll(subject);
            System.out.println(doc);
            return doc;
        }
        return null;
    }

    @Override
    public void remove() {
        this.next();
    }
}
