package mvm.rya.rdftriplestore;

/*
 * #%L
 * mvm.rya.rya.sail.impl
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

import mvm.rya.api.RdfCloudTripleStoreConfiguration;
import mvm.rya.api.persist.RdfEvalStatsDAO;
import mvm.rya.api.persist.RyaDAO;
import mvm.rya.api.persist.RyaDAOException;
import mvm.rya.api.persist.joinselect.SelectivityEvalDAO;
import mvm.rya.rdftriplestore.inference.InferenceEngine;
import mvm.rya.rdftriplestore.namespace.NamespaceManager;
import mvm.rya.rdftriplestore.provenance.ProvenanceCollector;

import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.sail.SailConnection;
import org.openrdf.sail.SailException;
import org.openrdf.sail.helpers.SailBase;

import static com.google.common.base.Preconditions.checkNotNull;

public class RdfCloudTripleStore extends SailBase {

    private RdfCloudTripleStoreConfiguration conf;

    protected RyaDAO ryaDAO;
    protected InferenceEngine inferenceEngine;
    protected RdfEvalStatsDAO rdfEvalStatsDAO;
    protected SelectivityEvalDAO selectEvalDAO;
    private NamespaceManager namespaceManager;
    protected ProvenanceCollector provenanceCollector;

    private ValueFactory vf = new ValueFactoryImpl();

    @Override
    protected SailConnection getConnectionInternal() throws SailException {
        return new RdfCloudTripleStoreConnection(this, conf, vf);
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
        } catch (RyaDAOException e) {
            throw new SailException(e);
        }

        if (rdfEvalStatsDAO != null && !rdfEvalStatsDAO.isInitialized()) {
            rdfEvalStatsDAO.setConf(this.conf);
            rdfEvalStatsDAO.init();
        }

        //TODO: Support inferencing with ryadao
//        if (inferenceEngine != null && !inferenceEngine.isInitialized()) {
//            inferenceEngine.setConf(this.conf);
//            inferenceEngine.setRyaDAO(ryaDAO);
//            inferenceEngine.init();
//        }

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
        } catch (Exception e) {
            throw new SailException(e);
        }
    }

    @Override
    public ValueFactory getValueFactory() {
        return vf;
    }

    @Override
    public boolean isWritable() throws SailException {
        return true;
    }

    public RdfCloudTripleStoreConfiguration getConf() {
        return conf;
    }

    public void setConf(RdfCloudTripleStoreConfiguration conf) {
        this.conf = conf;
    }

    public RdfEvalStatsDAO getRdfEvalStatsDAO() {
        return rdfEvalStatsDAO;
    }

    public void setRdfEvalStatsDAO(RdfEvalStatsDAO rdfEvalStatsDAO) {
        this.rdfEvalStatsDAO = rdfEvalStatsDAO;
    }
    
    public SelectivityEvalDAO getSelectEvalDAO() {
        return selectEvalDAO;
    }
    
    public void setSelectEvalDAO(SelectivityEvalDAO selectEvalDAO) {
        this.selectEvalDAO = selectEvalDAO;
    }

    public RyaDAO getRyaDAO() {
        return ryaDAO;
    }

    public void setRyaDAO(RyaDAO ryaDAO) {
        this.ryaDAO = ryaDAO;
    }

    public InferenceEngine getInferenceEngine() {
        return inferenceEngine;
    }

    public void setInferenceEngine(InferenceEngine inferenceEngine) {
        this.inferenceEngine = inferenceEngine;
    }

    public NamespaceManager getNamespaceManager() {
        return namespaceManager;
    }

    public void setNamespaceManager(NamespaceManager namespaceManager) {
        this.namespaceManager = namespaceManager;
    }

    public ProvenanceCollector getProvenanceCollector() {
		return provenanceCollector;
	}

	public void setProvenanceCollector(ProvenanceCollector provenanceCollector) {
		this.provenanceCollector = provenanceCollector;
	}

}
