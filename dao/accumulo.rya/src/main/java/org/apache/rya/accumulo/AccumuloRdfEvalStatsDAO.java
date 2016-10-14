package org.apache.rya.accumulo;

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



import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.rya.api.RdfCloudTripleStoreConstants.DELIM;
import static org.apache.rya.api.RdfCloudTripleStoreConstants.EMPTY_TEXT;
import static org.apache.rya.api.RdfCloudTripleStoreConstants.PRED_CF_TXT;
import static org.apache.rya.api.RdfCloudTripleStoreConstants.SUBJECT_CF_TXT;
import static org.apache.rya.api.RdfCloudTripleStoreConstants.SUBJECTPRED_CF_TXT;
import static org.apache.rya.api.RdfCloudTripleStoreConstants.PREDOBJECT_CF_TXT;
import static org.apache.rya.api.RdfCloudTripleStoreConstants.SUBJECTOBJECT_CF_TXT;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.rya.api.RdfCloudTripleStoreStatement;
import org.apache.rya.api.layout.TableLayoutStrategy;
import org.apache.rya.api.persist.RdfDAOException;
import org.apache.rya.api.persist.RdfEvalStatsDAO;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;
import org.openrdf.model.Resource;
import org.openrdf.model.Value;

/**
 * Class CloudbaseRdfEvalStatsDAO
 * Date: Feb 28, 2012
 * Time: 5:03:16 PM
 */
public class AccumuloRdfEvalStatsDAO implements RdfEvalStatsDAO<AccumuloRdfConfiguration> {

    private boolean initialized = false;
    private AccumuloRdfConfiguration conf = new AccumuloRdfConfiguration();

    private Collection<RdfCloudTripleStoreStatement> statements = new ArrayList<RdfCloudTripleStoreStatement>();
    private Connector connector;

    //    private String evalTable = TBL_EVAL;
    private TableLayoutStrategy tableLayoutStrategy;

    @Override
    public void init() throws RdfDAOException {
        try {
            if (isInitialized()) {
                throw new IllegalStateException("Already initialized");
            }
            checkNotNull(connector);
            tableLayoutStrategy = conf.getTableLayoutStrategy();
//            evalTable = conf.get(RdfCloudTripleStoreConfiguration.CONF_TBL_EVAL, evalTable);
//            conf.set(RdfCloudTripleStoreConfiguration.CONF_TBL_EVAL, evalTable);

            TableOperations tos = connector.tableOperations();
            AccumuloRdfUtils.createTableIfNotExist(tos, tableLayoutStrategy.getEval());
//            boolean tableExists = tos.exists(evalTable);
//            if (!tableExists)
//                tos.create(evalTable);
            initialized = true;
        } catch (Exception e) {
            throw new RdfDAOException(e);
        }
    }

 
    @Override
    public void destroy() throws RdfDAOException {
        if (!isInitialized()) {
            throw new IllegalStateException("Not initialized");
        }
        initialized = false;
    }

    @Override
    public boolean isInitialized() throws RdfDAOException {
        return initialized;
    }

    public Connector getConnector() {
        return connector;
    }

    public void setConnector(Connector connector) {
        this.connector = connector;
    }

    public AccumuloRdfConfiguration getConf() {
        return conf;
    }

    public void setConf(AccumuloRdfConfiguration conf) {
        this.conf = conf;
    }

	@Override
	public double getCardinality(AccumuloRdfConfiguration conf,
			org.apache.rya.api.persist.RdfEvalStatsDAO.CARDINALITY_OF card,
			List<Value> val, Resource context) throws RdfDAOException {
        try {
            Authorizations authorizations = conf.getAuthorizations();
            Scanner scanner = connector.createScanner(tableLayoutStrategy.getEval(), authorizations);
            Text cfTxt = null;
            if (CARDINALITY_OF.SUBJECT.equals(card)) {
                cfTxt = SUBJECT_CF_TXT;
            } else if (CARDINALITY_OF.PREDICATE.equals(card)) {
                cfTxt = PRED_CF_TXT;
            } else if (CARDINALITY_OF.OBJECT.equals(card)) {
//                cfTxt = OBJ_CF_TXT;     //TODO: How do we do object cardinality
                return Double.MAX_VALUE;
            } else if (CARDINALITY_OF.SUBJECTOBJECT.equals(card)) {
                cfTxt = SUBJECTOBJECT_CF_TXT;
            } else if (CARDINALITY_OF.SUBJECTPREDICATE.equals(card)) {
                cfTxt = SUBJECTPRED_CF_TXT;
            } else if (CARDINALITY_OF.PREDICATEOBJECT.equals(card)) {
                cfTxt = PREDOBJECT_CF_TXT;
            } else throw new IllegalArgumentException("Not right Cardinality[" + card + "]");
            Text cq = EMPTY_TEXT;
            if (context != null) {
                cq = new Text(context.stringValue().getBytes());
            }
            scanner.fetchColumn(cfTxt, cq);
            Iterator<Value> vals = val.iterator();
            String compositeIndex = vals.next().stringValue();
            while (vals.hasNext()){
            	compositeIndex += DELIM + vals.next().stringValue();
            }
            scanner.setRange(new Range(new Text(compositeIndex.getBytes())));
            Iterator<Map.Entry<Key, org.apache.accumulo.core.data.Value>> iter = scanner.iterator();
            if (iter.hasNext()) {
                return Double.parseDouble(new String(iter.next().getValue().get()));
            }
        } catch (Exception e) {
            throw new RdfDAOException(e);
        }

        //default
        return -1;
	}

	@Override
	public double getCardinality(AccumuloRdfConfiguration conf,
			org.apache.rya.api.persist.RdfEvalStatsDAO.CARDINALITY_OF card,
			List<Value> val) throws RdfDAOException {
		return getCardinality(conf, card, val, null);
	}
}
