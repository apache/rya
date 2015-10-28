package mvm.mmrts.rdf.partition.query.evaluation.select;

import cloudbase.core.data.Key;
import cloudbase.core.data.Value;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Iterators;
import com.google.common.collect.Multimap;
import com.google.common.collect.PeekingIterator;
import com.google.common.io.ByteStreams;
import info.aduna.iteration.CloseableIteration;
import mvm.mmrts.rdf.partition.utils.RdfIO;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.impl.StatementImpl;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.algebra.Var;
import org.openrdf.query.algebra.evaluation.QueryBindingSet;
import ss.cloudbase.core.iterators.filter.CBConverter;

import java.util.*;

import static mvm.mmrts.rdf.partition.PartitionConstants.*;
import static mvm.mmrts.rdf.partition.utils.RdfIO.readStatement;

/**
 * Class SelectAllIterator
 * Date: Jul 18, 2011
 * Time: 12:01:25 PM
 */
public abstract class SelectIterator implements CloseableIteration<BindingSet, QueryEvaluationException> {

    protected PeekingIterator<Map.Entry<Key, Value>> iter;
    protected BindingSet bindings;
    protected CBConverter converter = new CBConverter();

    private boolean hasNext = true;

    public SelectIterator(BindingSet bindings, Iterator<Map.Entry<Key, Value>> iter) {
        this.bindings = bindings;
        this.iter = Iterators.peekingIterator(iter);
        converter.init(Collections.singletonMap(CBConverter.OPTION_VALUE_DELIMITER, VALUE_DELIMITER));
    }

    @Override
    public void close() throws QueryEvaluationException {

    }

    @Override
    public boolean hasNext() throws QueryEvaluationException {
        return statefulHasNext();
    }

    protected boolean statefulHasNext() {
        hasNext = iter.hasNext() && hasNext;
        return hasNext;
    }

    protected List<Statement> nextDocument() throws QueryEvaluationException {
        try {
            Map.Entry<Key, Value> entry = iter.peek();
            Key key = entry.getKey();
            Value value = entry.getValue();

            if (value.getSize() == 0) {
                //not an aggregate document
                return nextNonAggregateDocument();
//                return Collections.singletonList(RdfIO.readStatement(ByteStreams.newDataInput(key.getColumnQualifier().getBytes()), VALUE_FACTORY, true));
            }

            List<Statement> document = new ArrayList<Statement>();

            org.openrdf.model.Value subj = RdfIO.readValue(ByteStreams.newDataInput(key.getColumnQualifier().getBytes()), VALUE_FACTORY, FAMILY_DELIM);
            Map<String, String> map = converter.toMap(entry.getKey(), value);
            for (Map.Entry<String, String> e : map.entrySet()) {
                String predObj = e.getKey();
                String[] split = predObj.split(FAMILY_DELIM_STR);
                document.add(new StatementImpl((Resource) subj, VALUE_FACTORY.createURI(split[0]), RdfIO.readValue(ByteStreams.newDataInput(split[1].getBytes()), VALUE_FACTORY, FAMILY_DELIM)));
            }
            iter.next();
            return document;
        } catch (Exception e) {
            throw new QueryEvaluationException("Error retrieving document", e);
        }
    }

//    protected List<Statement> nextDocument() throws QueryEvaluationException {
//        try {
//            List<? extends Map.Entry<Key, Value>> entryList = iter.next();
//            List<Statement> document = new ArrayList();
//            for (Map.Entry<Key, Value> keyValueEntry : entryList) {
//                Statement stmt = null;
//                Key key = keyValueEntry.getKey();
//                if (DOC.equals(key.getColumnFamily()))
//                    stmt = readStatement(ByteStreams.newDataInput(key.getColumnQualifier().getBytes()), VALUE_FACTORY);
//                else
//                    stmt = readStatement(ByteStreams.newDataInput(key.getColumnQualifier().getBytes()), VALUE_FACTORY, false);
//                document.add(stmt);
//            }
//            return document;
//        } catch (Exception e) {
//            throw new QueryEvaluationException(e);
//        }
//    }

//    protected List<Statement> nextDocument() throws QueryEvaluationException {
//        return documentIter.next();
//    }

    protected List<Statement> nextNonAggregateDocument() throws QueryEvaluationException {
        try {
            List<Statement> document = new ArrayList<Statement>();
            if (!statefulHasNext())
                return document;
            Statement stmt = peekNextStatement();
            if (stmt == null)
                return document;

            Resource subject = stmt.getSubject();
            Resource current = subject;
            document.add(stmt);
            while ((current.equals(subject) && statefulHasNext())) {
                advance();
                current = subject;
                stmt = peekNextStatement();
                if (stmt != null) {
                    subject = stmt.getSubject();
                    if (subject.equals(current))
                        document.add(stmt);
                } else
                    subject = null;
            }
//            System.out.println(document);
            return document;
        } catch (Exception e) {
            throw new QueryEvaluationException(e);
        }
    }

//    protected Statement nextStatement() throws Exception {
//        List<Map.Entry<Key, Value>> entryList = iter.next();
//        for (Map.Entry<Key, Value> keyValueEntry : entryList) {
//
//        }
//        Map.Entry<Key, Value> entry = iter.next();
//        Key key = entry.getKey();
//        if (DOC.equals(key.getColumnFamily()))
//            return readStatement(ByteStreams.newDataInput(key.getColumnQualifier().getBytes()), VALUE_FACTORY);
//        else
//            return readStatement(ByteStreams.newDataInput(key.getColumnQualifier().getBytes()), VALUE_FACTORY, false);
//    }

    protected Statement peekNextStatement() throws Exception {
        if (!statefulHasNext())
            return null;
        Map.Entry<Key, Value> entry = iter.peek();
        Key key = entry.getKey();
        if (DOC.equals(key.getColumnFamily()))
            return readStatement(ByteStreams.newDataInput(key.getColumnQualifier().getBytes()), VALUE_FACTORY);
        else
            return readStatement(ByteStreams.newDataInput(key.getColumnQualifier().getBytes()), VALUE_FACTORY, false);
    }

    protected void advance() throws Exception {
        iter.next();
    }

    @Override
    public void remove() throws QueryEvaluationException {
        iter.next();
    }

    protected BindingSet populateBindingSet(Statement st, List<Map.Entry<Var, Var>> predObjVar) {
        QueryBindingSet result = new QueryBindingSet(bindings);
        for (Map.Entry<Var, Var> entry : predObjVar) {
            Var predVar = entry.getKey();
            Var objVar = entry.getValue();
            if (predVar != null && !result.hasBinding(predVar.getName()))
                result.addBinding(predVar.getName(), st.getPredicate());
            if (objVar != null && !result.hasBinding(objVar.getName()))
                result.addBinding(objVar.getName(), st.getObject());
        }
        return result;
    }

    protected List<QueryBindingSet> populateBindingSet(List<Statement> document, Var subjVar, List<Map.Entry<Var, Var>> predObjVar) {
        //convert document to a multimap
        Multimap<URI, Statement> docMap = ArrayListMultimap.create();
        for (Statement st : document) {
            docMap.put(st.getPredicate(), st);
        }

        List<QueryBindingSet> results = new ArrayList<QueryBindingSet>();
        QueryBindingSet bs0 = new QueryBindingSet(bindings);
//        QueryBindingSet result = new QueryBindingSet(bindings);

        if (document.size() > 0) {
            Statement stmt = document.get(0);
            if (subjVar != null && !bs0.hasBinding(subjVar.getName())) {
                bs0.addBinding(subjVar.getName(), stmt.getSubject());
            }
        }
        results.add(bs0);

//        for (Statement st : document) {
        for (Map.Entry<Var, Var> entry : predObjVar) {
            Var predVar = entry.getKey();
            Var objVar = entry.getValue();

//                if (predVar.hasValue() && !st.getPredicate().equals(predVar.getValue()))
//                    continue;
            if (predVar == null || !predVar.hasValue())
                continue;
            Collection<Statement> predSts = docMap.get((URI) predVar.getValue());

//            if (predVar != null && !result.hasBinding(predVar.getName()))
//                result.addBinding(predVar.getName(), st.getPredicate());
//            if (objVar != null && !result.hasBinding(objVar.getName()))
//                result.addBinding(objVar.getName(), st.getObject());

            populateBindingSets(results, predVar, objVar, predSts);
        }
//        }
        return results;
    }

    private void populateBindingSets(List<QueryBindingSet> results, Var predVar, Var objVar, Collection<Statement> stmts) {
        if (predVar == null || objVar == null || stmts == null || stmts.size() == 0)
            return;

        List<QueryBindingSet> copyOf = new ArrayList<QueryBindingSet>(results);

        int i = copyOf.size();
        int j = 0;
        for (Iterator<Statement> iter = stmts.iterator(); iter.hasNext();) {
            Statement st = iter.next();
            int k = 0;
            for (QueryBindingSet result : results) {
                if (!result.hasBinding(predVar.getName()) || k >= i) {
                    String name = predVar.getName();
                    org.openrdf.model.Value val = st.getPredicate();
                    addBinding(result, name, val);
                }
                if (!result.hasBinding(objVar.getName()) || k >= i)
                    addBinding(result, objVar.getName(), st.getObject());
                k++;
            }

            i = copyOf.size() + j * copyOf.size();
            j++;

            if (iter.hasNext()) {
                //copy results
                for (QueryBindingSet copy : copyOf) {
                    results.add(new QueryBindingSet(copy));
                }
            }

        }
    }

    private void addBinding(QueryBindingSet result, String name, org.openrdf.model.Value val) {
        if (result.hasBinding(name))
            result.removeBinding(name);
        result.addBinding(name, val);
    }

}
