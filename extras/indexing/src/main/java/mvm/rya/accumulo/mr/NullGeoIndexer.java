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
import java.util.Set;

import mvm.rya.accumulo.experimental.AbstractAccumuloIndexer;
import mvm.rya.api.domain.RyaStatement;
import mvm.rya.indexing.GeoIndexer;
import mvm.rya.indexing.StatementContraints;

import org.apache.hadoop.conf.Configuration;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.query.QueryEvaluationException;

import com.vividsolutions.jts.geom.Geometry;

public class NullGeoIndexer extends AbstractAccumuloIndexer implements GeoIndexer {

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
    public CloseableIteration<Statement, QueryEvaluationException> queryEquals(Geometry query, StatementContraints contraints) {

        return null;
    }

    @Override
    public CloseableIteration<Statement, QueryEvaluationException> queryDisjoint(Geometry query, StatementContraints contraints) {

        return null;
    }

    @Override
    public CloseableIteration<Statement, QueryEvaluationException> queryIntersects(Geometry query, StatementContraints contraints) {

        return null;
    }

    @Override
    public CloseableIteration<Statement, QueryEvaluationException> queryTouches(Geometry query, StatementContraints contraints) {

        return null;
    }

    @Override
    public CloseableIteration<Statement, QueryEvaluationException> queryCrosses(Geometry query, StatementContraints contraints) {

        return null;
    }

    @Override
    public CloseableIteration<Statement, QueryEvaluationException> queryWithin(Geometry query, StatementContraints contraints) {

        return null;
    }

    @Override
    public CloseableIteration<Statement, QueryEvaluationException> queryContains(Geometry query, StatementContraints contraints) {

        return null;
    }

    @Override
    public CloseableIteration<Statement, QueryEvaluationException> queryOverlaps(Geometry query, StatementContraints contraints) {

        return null;
    }

    @Override
    public Set<URI> getIndexablePredicates() {

        return null;
    }

}
