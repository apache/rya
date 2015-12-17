package mvm.rya.accumulo.mr;

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
