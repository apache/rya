package org.apache.rya.rdftriplestore.inference;

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



import org.apache.rya.api.RdfCloudTripleStoreConfiguration;
import org.apache.rya.rdftriplestore.utils.FixedStatementPattern;
import org.apache.rya.rdftriplestore.utils.TransitivePropertySP;
import org.apache.rya.rdftriplestore.utils.FixedStatementPattern;
import org.apache.rya.rdftriplestore.utils.TransitivePropertySP;
import org.openrdf.query.algebra.Join;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.algebra.Union;
import org.openrdf.query.algebra.Var;
import org.openrdf.query.algebra.helpers.QueryModelVisitorBase;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Class AbstractInferVisitor
 * Date: Mar 14, 2012
 * Time: 5:33:01 PM
 */
public class AbstractInferVisitor extends QueryModelVisitorBase {

    static Var EXPANDED = new Var("infer-expanded");

    boolean include = true;

    RdfCloudTripleStoreConfiguration conf;
    InferenceEngine inferenceEngine;

    public AbstractInferVisitor(RdfCloudTripleStoreConfiguration conf, InferenceEngine inferenceEngine) {
        checkNotNull(conf, "Configuration cannot be null");
        checkNotNull(inferenceEngine, "Inference Engine cannot be null");
        this.conf = conf;
        this.inferenceEngine = inferenceEngine;
    }

    @Override
    public void meet(StatementPattern sp) throws Exception {
        if (!include) {
            return;
        }
        if (sp instanceof FixedStatementPattern || sp instanceof TransitivePropertySP || sp instanceof DoNotExpandSP) {
            return;   //already inferred somewhere else
        }
        final Var predVar = sp.getPredicateVar();
        //we do not let timeRange preds be inferred, not good
        if (predVar == null || predVar.getValue() == null
//                || RdfCloudTripleStoreUtils.getTtlValueConverter(conf, (URI) predVar.getValue()) != null
                ) {
            return;
        }
        meetSP(sp);
    }

    protected void meetSP(StatementPattern sp) throws Exception {

    }

    @Override
    public void meet(Union node) throws Exception {
//        if (!(node instanceof InferUnion))
        super.meet(node);
    }

    @Override
    public void meet(Join node) throws Exception {
        if (!(node instanceof InferJoin)) {
            super.meet(node);
        }
    }

    public RdfCloudTripleStoreConfiguration getConf() {
        return conf;
    }

    public void setConf(RdfCloudTripleStoreConfiguration conf) {
        this.conf = conf;
    }

    public InferenceEngine getInferenceEngine() {
        return inferenceEngine;
    }

    public void setInferenceEngine(InferenceEngine inferenceEngine) {
        this.inferenceEngine = inferenceEngine;
    }
}
