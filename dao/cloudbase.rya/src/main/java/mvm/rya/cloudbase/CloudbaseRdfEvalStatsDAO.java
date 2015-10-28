package mvm.rya.cloudbase;

import cloudbase.core.client.Connector;
import cloudbase.core.client.Scanner;
import cloudbase.core.client.admin.TableOperations;
import cloudbase.core.data.Key;
import cloudbase.core.data.Range;
import cloudbase.core.security.Authorizations;
import mvm.rya.api.RdfCloudTripleStoreConfiguration;
import mvm.rya.api.RdfCloudTripleStoreStatement;
import mvm.rya.api.layout.TableLayoutStrategy;
import mvm.rya.api.persist.RdfDAOException;
import mvm.rya.api.persist.RdfEvalStatsDAO;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.openrdf.model.Resource;
import org.openrdf.model.Value;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;
import static mvm.rya.api.RdfCloudTripleStoreConstants.*;

/**
 * Class CloudbaseRdfEvalStatsDAO
 * Date: Feb 28, 2012
 * Time: 5:03:16 PM
 */
public class CloudbaseRdfEvalStatsDAO implements RdfEvalStatsDAO<CloudbaseRdfConfiguration> {

    private boolean initialized = false;
    private CloudbaseRdfConfiguration conf = new CloudbaseRdfConfiguration();

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
            CloudbaseRdfUtils.createTableIfNotExist(tos, tableLayoutStrategy.getEval());
//            boolean tableExists = tos.exists(evalTable);
//            if (!tableExists)
//                tos.create(evalTable);
            initialized = true;
        } catch (Exception e) {
            throw new RdfDAOException(e);
        }
    }

    @Override
    public double getCardinality(CloudbaseRdfConfiguration conf, CARDINALITY_OF card, Value val) throws RdfDAOException {
        return this.getCardinality(conf, card, val, null);
    }

    @Override
    public double getCardinality(CloudbaseRdfConfiguration conf, CARDINALITY_OF card, Value val, Resource context) throws RdfDAOException {
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
            } else throw new IllegalArgumentException("Not right Cardinality[" + card + "]");
            Text cq = EMPTY_TEXT;
            if (context != null) {
                cq = new Text(context.stringValue().getBytes());
            }
            scanner.fetchColumn(cfTxt, cq);
            scanner.setRange(new Range(new Text(val.stringValue().getBytes())));
            Iterator<Map.Entry<Key, cloudbase.core.data.Value>> iter = scanner.iterator();
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

//    public String getEvalTable() {
//        return evalTable;
//    }
//
//    public void setEvalTable(String evalTable) {
//        this.evalTable = evalTable;
//    }

    public CloudbaseRdfConfiguration getConf() {
        return conf;
    }

    public void setConf(CloudbaseRdfConfiguration conf) {
        this.conf = conf;
    }
}
