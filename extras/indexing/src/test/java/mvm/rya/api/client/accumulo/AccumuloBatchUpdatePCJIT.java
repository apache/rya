package mvm.rya.api.client.accumulo;

import static org.junit.Assert.assertEquals;

import java.util.HashSet;
import java.util.Set;

import org.apache.rya.indexing.pcj.storage.PrecomputedJoinStorage;
import org.apache.rya.indexing.pcj.storage.accumulo.AccumuloPcjStorage;
import org.junit.Test;
import org.openrdf.model.ValueFactory;
import org.openrdf.query.BindingSet;
import org.openrdf.query.impl.MapBindingSet;
import org.openrdf.sail.Sail;
import org.openrdf.sail.SailConnection;

import mvm.rya.accumulo.AccumuloITBase;
import mvm.rya.accumulo.AccumuloRdfConfiguration;
import mvm.rya.api.client.Install.InstallConfiguration;
import mvm.rya.api.client.RyaClient;
import mvm.rya.indexing.accumulo.ConfigUtils;
import mvm.rya.indexing.external.PrecomputedJoinIndexerConfig.PrecomputedJoinStorageType;
import mvm.rya.indexing.external.PrecomputedJoinIndexerConfig.PrecomputedJoinUpdaterType;
import mvm.rya.sail.config.RyaSailFactory;

/**
 * Integration tests the methods of {@link AccumuloBatchUpdatePCJ}.
 */
public class AccumuloBatchUpdatePCJIT extends AccumuloITBase {

    private static final String RYA_INSTANCE_NAME = "test_";

    @Test
    public void batchUpdate() throws Exception {
        // Setup a Rya Client.
        final AccumuloConnectionDetails connectionDetails = new AccumuloConnectionDetails(
                super.getUsername(),
                super.getPassword().toCharArray(),
                super.getInstanceName(),
                super.getZookeepers());
        final RyaClient ryaClient = AccumuloRyaClientFactory.build(connectionDetails, super.getConnector());

        // Install an instance of Rya on the mini accumulo cluster.
        ryaClient.getInstall().install(RYA_INSTANCE_NAME, InstallConfiguration.builder()
                .setEnablePcjIndex(true)
                .build());

        Sail sail = null;
        try {
            // Get a Sail connection backed by the installed Rya instance.
            final AccumuloRdfConfiguration ryaConf = new AccumuloRdfConfiguration();
            ryaConf.setTablePrefix(RYA_INSTANCE_NAME);
            ryaConf.set(ConfigUtils.CLOUDBASE_USER, super.getUsername());
            ryaConf.set(ConfigUtils.CLOUDBASE_PASSWORD, super.getPassword());
            ryaConf.set(ConfigUtils.CLOUDBASE_ZOOKEEPERS, super.getZookeepers());
            ryaConf.set(ConfigUtils.CLOUDBASE_INSTANCE, super.getInstanceName());
            ryaConf.set(ConfigUtils.USE_PCJ, "true");
            ryaConf.set(ConfigUtils.PCJ_STORAGE_TYPE, PrecomputedJoinStorageType.ACCUMULO.toString());
            ryaConf.set(ConfigUtils.PCJ_UPDATER_TYPE, PrecomputedJoinUpdaterType.NO_UPDATE.toString());
            sail = RyaSailFactory.getInstance( ryaConf );

            // Load some statements into the Rya instance.
            final ValueFactory vf = sail.getValueFactory();

            final SailConnection sailConn = sail.getConnection();
            sailConn.begin();
            sailConn.addStatement(vf.createURI("urn:Alice"), vf.createURI("urn:likes"), vf.createURI("urn:icecream"));
            sailConn.addStatement(vf.createURI("urn:Bob"), vf.createURI("urn:likes"), vf.createURI("urn:icecream"));
            sailConn.addStatement(vf.createURI("urn:Charlie"), vf.createURI("urn:likes"), vf.createURI("urn:icecream"));
            sailConn.addStatement(vf.createURI("urn:David"), vf.createURI("urn:likes"), vf.createURI("urn:icecream"));
            sailConn.addStatement(vf.createURI("urn:Eve"), vf.createURI("urn:likes"), vf.createURI("urn:icecream"));
            sailConn.addStatement(vf.createURI("urn:Frank"), vf.createURI("urn:likes"), vf.createURI("urn:icecream"));
            sailConn.addStatement(vf.createURI("urn:George"), vf.createURI("urn:likes"), vf.createURI("urn:icecream"));
            sailConn.addStatement(vf.createURI("urn:Hillary"), vf.createURI("urn:likes"), vf.createURI("urn:icecream"));

            sailConn.addStatement(vf.createURI("urn:Alice"), vf.createURI("urn:hasEyeColor"), vf.createURI("urn:blue"));
            sailConn.addStatement(vf.createURI("urn:Bob"), vf.createURI("urn:hasEyeColor"), vf.createURI("urn:blue"));
            sailConn.addStatement(vf.createURI("urn:Charlie"), vf.createURI("urn:hasEyeColor"), vf.createURI("urn:blue"));
            sailConn.addStatement(vf.createURI("urn:David"), vf.createURI("urn:hasEyeColor"), vf.createURI("urn:blue"));
            sailConn.addStatement(vf.createURI("urn:Eve"), vf.createURI("urn:hasEyeColor"), vf.createURI("urn:blue"));
            sailConn.addStatement(vf.createURI("urn:Frank"), vf.createURI("urn:hasEyeColor"), vf.createURI("urn:blue"));
            sailConn.addStatement(vf.createURI("urn:George"), vf.createURI("urn:hasEyeColor"), vf.createURI("urn:green"));
            sailConn.addStatement(vf.createURI("urn:Hillary"), vf.createURI("urn:hasEyeColor"), vf.createURI("urn:brown"));
            sailConn.commit();
            sailConn.close();

            // Create a PCJ for a SPARQL query.
            final PrecomputedJoinStorage pcjStorage = new AccumuloPcjStorage(super.getConnector(), RYA_INSTANCE_NAME);
            final String sparql = "SELECT ?name WHERE { ?name <urn:likes> <urn:icecream> . ?name <urn:hasEyeColor> <urn:blue> . }";
            final String pcjId = pcjStorage.createPcj(sparql);

            // Run the test.
            ryaClient.getBatchUpdatePCJ().batchUpdate(RYA_INSTANCE_NAME, pcjId);

            // Verify the correct results were loaded into the PCJ table.
            final Set<BindingSet> expectedResults = new HashSet<>();

            MapBindingSet bs = new MapBindingSet();
            bs.addBinding("name", vf.createURI("urn:Alice"));
            expectedResults.add(bs);

            bs = new MapBindingSet();
            bs.addBinding("name", vf.createURI("urn:Bob"));
            expectedResults.add(bs);

            bs = new MapBindingSet();
            bs.addBinding("name", vf.createURI("urn:Charlie"));
            expectedResults.add(bs);

            bs = new MapBindingSet();
            bs.addBinding("name", vf.createURI("urn:David"));
            expectedResults.add(bs);

            bs = new MapBindingSet();
            bs.addBinding("name", vf.createURI("urn:Eve"));
            expectedResults.add(bs);

            bs = new MapBindingSet();
            bs.addBinding("name", vf.createURI("urn:Frank"));
            expectedResults.add(bs);

            final Set<BindingSet> results = new HashSet<>();
            for(final BindingSet result : pcjStorage.listResults(pcjId)) {
                results.add( result );
            }

            assertEquals(expectedResults, results);

        } finally {
            if(sail != null) {
                sail.shutDown();
            }
        }
    }
}