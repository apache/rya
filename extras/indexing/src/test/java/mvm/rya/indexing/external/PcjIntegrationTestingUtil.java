package mvm.rya.indexing.external;

import java.util.HashSet;
import java.util.Set;

import mvm.rya.accumulo.AccumuloRdfConfiguration;
import mvm.rya.api.persist.RyaDAOException;
import mvm.rya.indexing.RyaSailFactory;
import mvm.rya.indexing.accumulo.ConfigUtils;
import mvm.rya.indexing.external.tupleSet.ExternalTupleSet;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.openrdf.query.algebra.QueryModelNode;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.algebra.helpers.QueryModelVisitorBase;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.sail.SailRepository;
import org.openrdf.repository.sail.SailRepositoryConnection;
import org.openrdf.sail.Sail;

public class PcjIntegrationTestingUtil {

	public static Set<QueryModelNode> getTupleSets(TupleExpr te) {
		final ExternalTupleVisitor etv = new ExternalTupleVisitor();
		te.visit(etv);
		return etv.getExtTup();
	}

	public static void deleteCoreRyaTables(Connector accCon, String prefix)
			throws AccumuloException, AccumuloSecurityException,
			TableNotFoundException {
		final TableOperations ops = accCon.tableOperations();
		if (ops.exists(prefix + "spo")) {
			ops.delete(prefix + "spo");
		}
		if (ops.exists(prefix + "po")) {
			ops.delete(prefix + "po");
		}
		if (ops.exists(prefix + "osp")) {
			ops.delete(prefix + "osp");
		}
	}

	public static class ExternalTupleVisitor extends
			QueryModelVisitorBase<RuntimeException> {

		private final Set<QueryModelNode> eSet = new HashSet<>();

		@Override
		public void meetNode(QueryModelNode node) throws RuntimeException {
			if (node instanceof ExternalTupleSet) {
				eSet.add(node);
			}
			super.meetNode(node);
		}

		public Set<QueryModelNode> getExtTup() {
			return eSet;
		}

	}

	public static SailRepository getPcjRepo(String tablePrefix, String instance)
			throws AccumuloException, AccumuloSecurityException,
			RyaDAOException, RepositoryException {

		final AccumuloRdfConfiguration pcjConf = new AccumuloRdfConfiguration();
		pcjConf.set(ConfigUtils.USE_PCJ, "true");
		pcjConf.set(ConfigUtils.USE_MOCK_INSTANCE, "true");
		pcjConf.set(ConfigUtils.CLOUDBASE_INSTANCE, instance);
		pcjConf.setTablePrefix(tablePrefix);

		final Sail pcjSail = RyaSailFactory.getInstance(pcjConf);
		final SailRepository pcjRepo = new SailRepository(pcjSail);
		pcjRepo.initialize();
		return pcjRepo;
	}

	public static SailRepository getNonPcjRepo(String tablePrefix,
			String instance) throws AccumuloException,
			AccumuloSecurityException, RyaDAOException, RepositoryException {

		final AccumuloRdfConfiguration nonPcjConf = new AccumuloRdfConfiguration();
		nonPcjConf.set(ConfigUtils.USE_MOCK_INSTANCE, "true");
		nonPcjConf.set(ConfigUtils.CLOUDBASE_INSTANCE, instance);
		nonPcjConf.setTablePrefix(tablePrefix);

		final Sail nonPcjSail = RyaSailFactory.getInstance(nonPcjConf);
		final SailRepository nonPcjRepo = new SailRepository(nonPcjSail);
		nonPcjRepo.initialize();
		return nonPcjRepo;
	}

	public static void closeAndShutdown(SailRepositoryConnection connection,
			SailRepository repo) throws RepositoryException {
		connection.close();
		repo.shutDown();
	}

	public static void deleteIndexTables(Connector accCon, int tableNum, String prefix) throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
		final TableOperations ops = accCon.tableOperations();
		final String tablename = prefix + "INDEX_";
		for (int i = 1; i < tableNum + 1; i++) {
			if (ops.exists(tablename + i)) {
				ops.delete(tablename + i);
			}
		}
	}

}
