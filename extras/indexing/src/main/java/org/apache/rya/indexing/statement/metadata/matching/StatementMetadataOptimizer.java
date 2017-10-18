package org.apache.rya.indexing.statement.metadata.matching;
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

import com.google.common.base.Optional;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.rya.api.RdfTripleStoreConfiguration;
import org.apache.rya.indexing.external.matching.*;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.Dataset;
import org.eclipse.rdf4j.query.algebra.TupleExpr;

import static com.google.common.base.Preconditions.checkNotNull;

public class StatementMetadataOptimizer extends AbstractExternalSetOptimizer<StatementMetadataNode<?>>implements Configurable {

    private StatementMetadataExternalSetMatcherFactory factory = new StatementMetadataExternalSetMatcherFactory();
    private RdfTripleStoreConfiguration conf;
    public boolean init = false;
    public StatementMetadataExternalSetProvider provider;

    public StatementMetadataOptimizer() {}
    
    public StatementMetadataOptimizer(RdfTripleStoreConfiguration conf) {
        setConf(conf);
    }

    @Override
    public final void setConf(Configuration conf) {
        checkNotNull(conf);
        if (!init) {
            try {
                this.conf = (RdfTripleStoreConfiguration) conf;
                provider = new StatementMetadataExternalSetProvider(this.conf);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            init = true;
        }
    }
    
    @Override
    public Configuration getConf() {
        return conf;
    }
    
    @Override
    public void optimize(TupleExpr tupleExpr, Dataset dataset, BindingSet bindings) {
        if(!init) {
            throw new IllegalStateException("Optimizer has not been properly initialized.  Configuration must be set to initialize this class.");
        }
        super.optimize(tupleExpr, dataset, bindings);
    }

    @Override
    protected ExternalSetMatcher<StatementMetadataNode<?>> getMatcher(QuerySegment<StatementMetadataNode<?>> segment) {
        return factory.getMatcher(segment);
    }

    @Override
    protected ExternalSetProvider<StatementMetadataNode<?>> getProvider() {
        return provider;
    }

    @Override
    protected Optional<QueryNodeListRater> getNodeListRater(QuerySegment<StatementMetadataNode<?>> segment) {
        return Optional.of(new BasicRater(segment.getOrderedNodes()));
    }

}
