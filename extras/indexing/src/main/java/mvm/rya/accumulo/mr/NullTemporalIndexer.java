package mvm.rya.accumulo.mr;

import info.aduna.iteration.CloseableIteration;

import java.io.IOException;
import java.util.Collection;
import java.util.Set;

import mvm.rya.accumulo.experimental.AbstractAccumuloIndexer;
import mvm.rya.api.domain.RyaStatement;
import mvm.rya.api.domain.RyaURI;
import mvm.rya.indexing.StatementContraints;
import mvm.rya.indexing.TemporalIndexer;
import mvm.rya.indexing.TemporalInstant;
import mvm.rya.indexing.TemporalInterval;

import org.apache.hadoop.conf.Configuration;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.query.QueryEvaluationException;

/**
 * Temporal Indexer that does nothing, like when disabled.
 *
 */
public class NullTemporalIndexer extends AbstractAccumuloIndexer implements TemporalIndexer {

    @Override
    public String getTableName() {

        return null;
    }

    @Override
    public void storeStatement(RyaStatement statement) throws IOException {

        
    }

    @Override
    public Configuration getConf() {

        return null;
    }

    @Override
    public void setConf(Configuration arg0) {

        
    }

    @Override
    public CloseableIteration<Statement, QueryEvaluationException> queryInstantEqualsInstant(TemporalInstant queryInstant,
            StatementContraints contraints) throws QueryEvaluationException {

        return null;
    }

    @Override
    public CloseableIteration<Statement, QueryEvaluationException> queryInstantBeforeInstant(TemporalInstant queryInstant,
            StatementContraints contraints) throws QueryEvaluationException {

        return null;
    }

    @Override
    public CloseableIteration<Statement, QueryEvaluationException> queryInstantAfterInstant(TemporalInstant queryInstant,
            StatementContraints contraints) throws QueryEvaluationException {

        return null;
    }

    @Override
    public CloseableIteration<Statement, QueryEvaluationException> queryInstantBeforeInterval(TemporalInterval givenInterval,
            StatementContraints contraints) throws QueryEvaluationException {

        return null;
    }

    @Override
    public CloseableIteration<Statement, QueryEvaluationException> queryInstantAfterInterval(TemporalInterval givenInterval,
            StatementContraints contraints) throws QueryEvaluationException {

        return null;
    }

    @Override
    public CloseableIteration<Statement, QueryEvaluationException> queryInstantInsideInterval(TemporalInterval givenInterval,
            StatementContraints contraints) throws QueryEvaluationException {

        return null;
    }

    @Override
    public CloseableIteration<Statement, QueryEvaluationException> queryInstantHasBeginningInterval(TemporalInterval queryInterval,
            StatementContraints contraints) throws QueryEvaluationException {

        return null;
    }

    @Override
    public CloseableIteration<Statement, QueryEvaluationException> queryInstantHasEndInterval(TemporalInterval queryInterval,
            StatementContraints contraints) throws QueryEvaluationException {

        return null;
    }

    @Override
    public CloseableIteration<Statement, QueryEvaluationException> queryIntervalEquals(TemporalInterval query,
            StatementContraints contraints) throws QueryEvaluationException {

        return null;
    }

    @Override
    public CloseableIteration<Statement, QueryEvaluationException> queryIntervalBefore(TemporalInterval query,
            StatementContraints contraints) throws QueryEvaluationException {

        return null;
    }

    @Override
    public CloseableIteration<Statement, QueryEvaluationException> queryIntervalAfter(TemporalInterval query, StatementContraints contraints)
            throws QueryEvaluationException {

        return null;
    }

    @Override
    public Set<URI> getIndexablePredicates() {

        return null;
    }
}
