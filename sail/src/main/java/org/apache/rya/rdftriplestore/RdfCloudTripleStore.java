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
package org.apache.rya.rdftriplestore;

import static com.google.common.base.Preconditions.checkNotNull;

import org.apache.rya.api.RdfCloudTripleStoreConfiguration;
import org.apache.rya.api.persist.RdfEvalStatsDAO;
import org.apache.rya.api.persist.RyaDAO;
import org.apache.rya.api.persist.RyaDAOException;
import org.apache.rya.api.persist.joinselect.SelectivityEvalDAO;
import org.apache.rya.rdftriplestore.inference.InferenceEngine;
import org.apache.rya.rdftriplestore.namespace.NamespaceManager;
import org.apache.rya.rdftriplestore.provenance.ProvenanceCollector;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.sail.SailConnection;
import org.eclipse.rdf4j.sail.SailException;
import org.eclipse.rdf4j.sail.helpers.AbstractSail;

public class RdfCloudTripleStore<C extends RdfCloudTripleStoreConfiguration> extends AbstractSail {

    private C conf;

    protected RyaDAO<C> ryaDAO;
    protected InferenceEngine inferenceEngine;
    protected RdfEvalStatsDAO<C> rdfEvalStatsDAO;
    protected SelectivityEvalDAO<C> selectEvalDAO;
    private NamespaceManager namespaceManager;
    protected ProvenanceCollector provenanceCollector;

    private static final ValueFactory VF = SimpleValueFactory.getInstance();

    @Override
    protected SailConnection getConnectionInternal() throws SailException {
        return new RdfCloudTripleStoreConnection(this, conf, VF);
    }

    @Override
    protected void initializeInternal() throws SailException {
        checkNotNull(ryaDAO);

        if (this.conf == null) {
            this.conf = ryaDAO.getConf();
        }

        checkNotNull(this.conf);

        try {
            if (!ryaDAO.isInitialized()) {
                ryaDAO.setConf(this.conf);
                ryaDAO.init();
            }
        } catch (final RyaDAOException e) {
            throw new SailException(e);
        }

        if (rdfEvalStatsDAO != null && !rdfEvalStatsDAO.isInitialized()) {
            rdfEvalStatsDAO.setConf(this.conf);
            rdfEvalStatsDAO.init();
        }

        if (namespaceManager == null) {
            this.namespaceManager = new NamespaceManager(ryaDAO, this.conf);
        }
    }

    @Override
    protected void shutDownInternal() throws SailException {

        try {
            if (namespaceManager != null) {
                namespaceManager.shutdown();
            }
            if (inferenceEngine != null) {
                inferenceEngine.destroy();
            }
            if (rdfEvalStatsDAO != null) {
                rdfEvalStatsDAO.destroy();
            }
            ryaDAO.destroy();
        } catch (final Exception e) {
            throw new SailException(e);
        }
    }

    @Override
    public ValueFactory getValueFactory() {
        return VF;
    }

    @Override
    public boolean isWritable() throws SailException {
        return true;
    }

    public synchronized C getConf() {
        return conf;
    }

    public synchronized void setConf(final C conf) {
        this.conf = conf;
    }

    public RdfEvalStatsDAO<C> getRdfEvalStatsDAO() {
        return rdfEvalStatsDAO;
    }

    public void setRdfEvalStatsDAO(final RdfEvalStatsDAO<C> rdfEvalStatsDAO) {
        this.rdfEvalStatsDAO = rdfEvalStatsDAO;
    }

    public SelectivityEvalDAO<C> getSelectEvalDAO() {
        return selectEvalDAO;
    }

    public void setSelectEvalDAO(final SelectivityEvalDAO<C> selectEvalDAO) {
        this.selectEvalDAO = selectEvalDAO;
    }

    public synchronized RyaDAO<C> getRyaDAO() {
        return ryaDAO;
    }

    public synchronized void setRyaDAO(final RyaDAO<C> ryaDAO) {
        this.ryaDAO = ryaDAO;
    }

    public synchronized InferenceEngine getInferenceEngine() {
        return inferenceEngine;
    }

    public synchronized void setInferenceEngine(final InferenceEngine inferenceEngine) {
        this.inferenceEngine = inferenceEngine;
    }

    public NamespaceManager getNamespaceManager() {
        return namespaceManager;
    }

    public void setNamespaceManager(final NamespaceManager namespaceManager) {
        this.namespaceManager = namespaceManager;
    }

    public ProvenanceCollector getProvenanceCollector() {
        return provenanceCollector;
    }

    public void setProvenanceCollector(final ProvenanceCollector provenanceCollector) {
        this.provenanceCollector = provenanceCollector;
    }

}
