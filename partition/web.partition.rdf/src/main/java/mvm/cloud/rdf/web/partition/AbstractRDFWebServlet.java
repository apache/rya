package mvm.cloud.rdf.web.partition;

import cloudbase.core.client.Connector;
import cloudbase.core.client.ZooKeeperInstance;
import mvm.mmrts.rdf.partition.PartitionSail;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.sail.SailRepository;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;

/**
 * Class AbstractRDFWebServlet
 * Date: Dec 13, 2010
 * Time: 9:44:08 AM
 */
public class AbstractRDFWebServlet extends HttpServlet implements RDFWebConstants {

    protected Repository repository;

    @Override
    public void init(ServletConfig config) throws ServletException {
        super.init(config);

        try {
            String instance = config.getInitParameter(INSTANCE_PARAM);
            String zk = config.getInitParameter(ZK_PARAM);
            String user = config.getInitParameter(USER_PARAM);
            String password = config.getInitParameter(PASSWORD_PARAM);
            String table = config.getInitParameter(TABLE_PARAM);
            String shardtable = config.getInitParameter(SHARDTABLE_PARAM);
            if (shardtable == null)
                shardtable = table;

            if (zk == null || instance == null || user == null || password == null || table == null)
                throw new ServletException("Configuration not correct");

            PartitionSail psail = new PartitionSail(instance, zk, user, password, table, shardtable);

            repository = new SailRepository(psail);
            repository.initialize();
        } catch (Exception e) {
            throw new ServletException(e);
        }
    }

    @Override
    public void destroy() {
        try {
            repository.shutDown();
        } catch (RepositoryException e) {
            e.printStackTrace();
        }
    }


    public Repository getRepository() {
        return repository;
    }

    public void setRepository(Repository repository) {
        this.repository = repository;
    }
}
