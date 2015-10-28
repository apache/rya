package mvm.mmrts.rdf.partition;

import cloudbase.core.client.BatchWriter;
import cloudbase.core.client.Connector;
import cloudbase.core.client.Scanner;
import cloudbase.core.client.admin.TableOperations;
import cloudbase.core.data.Key;
import cloudbase.core.data.Mutation;
import cloudbase.core.data.Range;
import cloudbase.core.security.ColumnVisibility;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import info.aduna.iteration.CloseableIteration;
import mvm.mmrts.rdf.partition.converter.ContextColVisConverter;
import mvm.mmrts.rdf.partition.iterators.NamespaceIterator;
import mvm.mmrts.rdf.partition.query.evaluation.FilterTimeIndexVisitor;
import mvm.mmrts.rdf.partition.query.evaluation.PartitionEvaluationStrategy;
import mvm.mmrts.rdf.partition.query.evaluation.SubjectGroupingOptimizer;
import mvm.mmrts.rdf.partition.shard.ShardValueGenerator;
import mvm.mmrts.rdf.partition.utils.ContextsStatementImpl;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.openrdf.model.*;
import org.openrdf.query.Binding;
import org.openrdf.query.BindingSet;
import org.openrdf.query.Dataset;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.algebra.QueryRoot;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.impl.EmptyBindingSet;
import org.openrdf.sail.SailException;
import org.openrdf.sail.helpers.SailConnectionBase;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;

import static mvm.mmrts.rdf.partition.PartitionConstants.*;
import static mvm.mmrts.rdf.partition.utils.RdfIO.writeStatement;
import static mvm.mmrts.rdf.partition.utils.RdfIO.writeValue;

/**
 * Class PartitionConnection
 * Date: Jul 6, 2011
 * Time: 4:40:49 PM
 * <p/>
 * Ingest:
 * Triple ->
 * - <subject> <shard>:
 * - <shard> event:<subject>\0<predicate>\0<object>
 * - <shard> index:<predicate>\1<object>\0
 * <p/>
 * Namespace ->
 * - <prefix> ns:<namespace>
 */
public class PartitionConnection extends SailConnectionBase {

    private PartitionSail sail;
    private BatchWriter writer;
    private BatchWriter shardTableWriter;   //MMRTS-148
    
    private Multimap<Resource, ContextsStatementImpl> statements = HashMultimap.create(10000, 10);


    public PartitionConnection(PartitionSail sailBase) throws SailException {
        super(sailBase);
        this.sail = sailBase;
        this.initialize();
    }

    protected void initialize() throws SailException {
        try {
            Connector connector = sail.connector;
            String table = sail.table;
            String shardTable = sail.shardTable;

            //create these tables if they do not exist
            TableOperations tableOperations = connector.tableOperations();
            boolean tableExists = tableOperations.exists(table);
            if (!tableExists)
                tableOperations.create(table);

            tableExists = tableOperations.exists(shardTable);
            if(!tableExists)
                tableOperations.create(shardTable);

            writer = connector.createBatchWriter(table, 1000000l, 60000l, 10);
            shardTableWriter = connector.createBatchWriter(shardTable, 1000000l, 60000l, 10);
        } catch (Exception e) {
            throw new SailException(e);
        }
    }

    @Override
    protected void closeInternal() throws SailException {
        try {
            writer.close();
            shardTableWriter.close();
        } catch (Exception e) {
            throw new SailException(e);
        }
    }

    @Override
    protected CloseableIteration<? extends BindingSet, QueryEvaluationException> evaluateInternal(TupleExpr tupleExpr, Dataset dataset, BindingSet bindingSet, boolean b) throws SailException {
//        throw new UnsupportedOperationException("Query not supported");

        if (!(tupleExpr instanceof QueryRoot))
            tupleExpr = new QueryRoot(tupleExpr);

        try {
            Configuration queryConf = populateConf(bindingSet);
            //timeRange filter check
            tupleExpr.visit(new FilterTimeIndexVisitor(queryConf));

            (new SubjectGroupingOptimizer(queryConf)).optimize(tupleExpr, dataset, bindingSet);
            PartitionTripleSource source = new PartitionTripleSource(this.sail, queryConf);

            PartitionEvaluationStrategy strategy = new PartitionEvaluationStrategy(
                    source, dataset);

            return strategy.evaluate(tupleExpr, EmptyBindingSet.getInstance());
        } catch (Exception e) {
            throw new SailException(e);
        }

    }

    protected Configuration populateConf(BindingSet bs) {
        Configuration conf = new Configuration(this.sail.conf);

        for (String bname : bs.getBindingNames()) {
            conf.set(bname, bs.getValue(bname).stringValue());
        }
        Binding start = bs.getBinding(START_BINDING);
        if (start != null)
            conf.setLong(START_BINDING, Long.parseLong(start.getValue().stringValue()));

        Binding end = bs.getBinding(END_BINDING);
        if (end != null)
            conf.setLong(END_BINDING, Long.parseLong(end.getValue().stringValue()));

        Binding timePredicate = bs.getBinding(TIME_PREDICATE);
        if (timePredicate != null)
            conf.set(TIME_PREDICATE, timePredicate.getValue().stringValue());

        Binding timeType = bs.getBinding(TIME_TYPE_PROP);
        if (timeType != null)
            conf.set(TIME_TYPE_PROP, timeType.getValue().stringValue());
        else if (timePredicate != null)
            conf.set(TIME_TYPE_PROP, TimeType.XMLDATETIME.name()); //default to xml datetime

        return conf;
    }

    @Override
    protected CloseableIteration<? extends Resource, SailException> getContextIDsInternal() throws SailException {
        throw new UnsupportedOperationException("Contexts not supported");
    }

    @Override
    protected CloseableIteration<? extends Statement, SailException> getStatementsInternal(Resource resource, URI uri, Value value, boolean b, Resource... resources) throws SailException {
        throw new UnsupportedOperationException("Query not supported");
    }

    @Override
    protected long sizeInternal(Resource... resources) throws SailException {
        throw new UnsupportedOperationException("Size operation not supported");
    }

    @Override
    protected void startTransactionInternal() throws SailException {
        // no transaction support as of yet
    }

    @Override
    protected void commitInternal() throws SailException {
        try {
            ShardValueGenerator gen = sail.generator;
            ContextColVisConverter contextColVisConverter = sail.contextColVisConverter;
            Map<Resource, Collection<ContextsStatementImpl>> map = statements.asMap();
            for (Map.Entry<Resource, Collection<ContextsStatementImpl>> entry : map.entrySet()) {
                Resource subject = entry.getKey();
                byte[] subj_bytes = writeValue(subject);
                String shard = gen.generateShardValue(subject);
                Text shard_txt = new Text(shard);
                Collection<ContextsStatementImpl> stmts = entry.getValue();

                /**
                 * Triple - >
                 *- < subject ><shard >:
                 *- < shard > event:<subject >\0 < predicate >\0 < object >
                 *- < shard > index:<predicate >\1 < object >\0
                 */
                Mutation m_subj = new Mutation(shard_txt);
                for (ContextsStatementImpl stmt : stmts) {
                    Resource[] contexts = stmt.getContexts();
                    ColumnVisibility vis = null;
                    if (contexts != null && contexts.length > 0 && contextColVisConverter != null) {
                        vis = contextColVisConverter.convertContexts(contexts);
                    }

                    if (vis != null) {
                        m_subj.put(DOC, new Text(writeStatement(stmt, true)), vis, EMPTY_VALUE);
                        m_subj.put(INDEX, new Text(writeStatement(stmt, false)), vis, EMPTY_VALUE);
                    } else {
                        m_subj.put(DOC, new Text(writeStatement(stmt, true)), EMPTY_VALUE);
                        m_subj.put(INDEX, new Text(writeStatement(stmt, false)), EMPTY_VALUE);
                    }
                }

                /**
                 * TODO: Is this right?
                 * If the subject does not have any authorizations specified, then anyone can access it.
                 * But the true authorization check will happen at the predicate/object level, which means that
                 * the set returned will only be what the person is authorized to see.  The shard lookup table has to
                 * have the lowest level authorization all the predicate/object authorizations; otherwise,
                 * a user may not be able to see the correct document.   
                 */
                Mutation m_shard = new Mutation(new Text(subj_bytes));
                m_shard.put(shard_txt, EMPTY_TXT, EMPTY_VALUE);
                shardTableWriter.addMutation(m_shard);

                writer.addMutation(m_subj);
            }

            writer.flush();
            shardTableWriter.flush();
            statements.clear();
        } catch (Exception e) {
            throw new SailException(e);
        }
        finally {
        }
    }

    @Override
    protected void rollbackInternal() throws SailException {
        statements.clear();
    }

    @Override
    protected void addStatementInternal(Resource subject, URI predicate, Value object, Resource... contexts) throws SailException {
        statements.put(subject, new ContextsStatementImpl(subject, predicate, object, contexts));
    }

    @Override
    protected void removeStatementsInternal(Resource resource, URI uri, Value value, Resource... contexts) throws SailException {
        throw new UnsupportedOperationException("Remove not supported as of yet");
    }

    @Override
    protected void clearInternal(Resource... resources) throws SailException {
        throw new UnsupportedOperationException("Clear with context not supported as of yet");
    }

    @Override
    protected CloseableIteration<? extends Namespace, SailException> getNamespacesInternal() throws SailException {
        return new NamespaceIterator(sail.connector, sail.table);
    }

    @Override
    protected String getNamespaceInternal(String prefix) throws SailException {
        try {
            Scanner scanner = sail.connector.createScanner(sail.table, ALL_AUTHORIZATIONS);
            scanner.setRange(new Range(new Text(prefix)));
            scanner.fetchColumnFamily(NAMESPACE);
            Iterator<Map.Entry<Key, cloudbase.core.data.Value>> iter = scanner.iterator();
            if (iter != null && iter.hasNext())
                return iter.next().getKey().getColumnQualifier().toString();
        } catch (Exception e) {
            throw new SailException(e);
        }
        return null;
    }

    @Override
    protected void setNamespaceInternal(String prefix, String namespace) throws SailException {
        /**
         * Namespace ->
         * - <prefix> <namespace>:
         */

        try {
            Mutation m = new Mutation(new Text(prefix));
            m.put(NAMESPACE, new Text(namespace), EMPTY_VALUE);
            writer.addMutation(m);
        } catch (Exception e) {
            throw new SailException(e);
        }
    }

    @Override
    protected void removeNamespaceInternal
            (String
                    s) throws SailException {
        throw new UnsupportedOperationException("Namespace remove not supported");
    }

    @Override
    protected void clearNamespacesInternal
            () throws SailException {
        throw new UnsupportedOperationException("Namespace Clear not supported");
    }

}
