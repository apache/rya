package mvm.mmrts.rdf.partition;

import info.aduna.iteration.CloseableIteration;
import mvm.mmrts.rdf.partition.query.evaluation.ShardSubjectLookupStatementIterator;
import mvm.mmrts.rdf.partition.query.operators.ShardSubjectLookup;
import org.apache.hadoop.conf.Configuration;
import org.openrdf.model.*;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.algebra.evaluation.TripleSource;

/**
 * Class PartitionTripleSource
 * Date: Jul 18, 2011
 * Time: 10:45:06 AM
 */
public class PartitionTripleSource implements TripleSource {
    private PartitionSail sail;
    private Configuration configuration;

    public PartitionTripleSource(PartitionSail sail, Configuration configuration) {
        this.sail = sail;
        this.configuration = configuration;
    }

    @Override
    public CloseableIteration<? extends Statement, QueryEvaluationException> getStatements(Resource resource, URI uri, Value value, Resource... resources) throws QueryEvaluationException {
        return null;  
    }

    public CloseableIteration<BindingSet, QueryEvaluationException> getStatements(ShardSubjectLookup lookup,
                                                                                           BindingSet bindings, Resource... contexts) throws QueryEvaluationException {
        return new ShardSubjectLookupStatementIterator(sail, lookup, bindings, configuration);
    }

    @Override
    public ValueFactory getValueFactory() {
        return PartitionConstants.VALUE_FACTORY;
    }
}
