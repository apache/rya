package mvm.rya.accumulo.mr;

/*
 * #%L
 * mvm.rya.indexing.accumulo
 * %%
 * Copyright (C) 2014 Rya
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

import info.aduna.iteration.CloseableIteration;

import java.io.IOException;
import java.util.Set;

import mvm.rya.accumulo.experimental.AbstractAccumuloIndexer;
import mvm.rya.api.domain.RyaStatement;
import mvm.rya.indexing.FreeTextIndexer;
import mvm.rya.indexing.StatementContraints;

import org.apache.hadoop.conf.Configuration;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.query.QueryEvaluationException;

public class NullFreeTextIndexer extends AbstractAccumuloIndexer implements FreeTextIndexer {

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
    public CloseableIteration<Statement, QueryEvaluationException> queryText(String query, StatementContraints contraints)
            throws IOException {
        return null;
    }

    @Override
    public Set<URI> getIndexablePredicates() {
        return null;
    }

}
