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
package org.apache.rya.indexing.pcj.fluo;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.log4j.Logger;
import org.apache.rya.indexing.pcj.fluo.app.observers.FilterObserver;
import org.apache.rya.indexing.pcj.fluo.app.observers.JoinObserver;
import org.apache.rya.indexing.pcj.fluo.app.observers.QueryResultObserver;
import org.apache.rya.indexing.pcj.fluo.app.observers.StatementPatternObserver;
import org.apache.rya.indexing.pcj.fluo.app.observers.TripleObserver;
import org.apache.rya.indexing.pcj.fluo.app.query.FluoQueryColumns;
import org.apache.rya.indexing.pcj.fluo.app.query.FluoQueryMetadataDAO;
import org.apache.rya.indexing.pcj.fluo.app.query.QueryMetadata;
import org.apache.rya.indexing.pcj.storage.accumulo.BindingSetStringConverter;
import org.apache.rya.indexing.pcj.storage.accumulo.VariableOrder;
import org.junit.After;
import org.junit.Before;
import org.openrdf.model.Statement;
import org.openrdf.model.vocabulary.XMLSchema;
import org.openrdf.query.Binding;
import org.openrdf.query.BindingSet;
import org.openrdf.query.impl.MapBindingSet;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;

import com.google.common.io.Files;

import io.fluo.api.client.FluoClient;
import io.fluo.api.client.FluoFactory;
import io.fluo.api.client.Snapshot;
import io.fluo.api.config.FluoConfiguration;
import io.fluo.api.config.ObserverConfiguration;
import io.fluo.api.config.ScannerConfiguration;
import io.fluo.api.data.Bytes;
import io.fluo.api.iterator.ColumnIterator;
import io.fluo.api.iterator.RowIterator;
import io.fluo.api.mini.MiniFluo;
import mvm.rya.accumulo.AccumuloRdfConfiguration;
import mvm.rya.accumulo.AccumuloRyaDAO;
import mvm.rya.api.RdfCloudTripleStoreConfiguration;
import mvm.rya.api.domain.RyaStatement;
import mvm.rya.api.domain.RyaStatement.RyaStatementBuilder;
import mvm.rya.api.domain.RyaType;
import mvm.rya.api.domain.RyaURI;
import mvm.rya.api.resolver.RyaToRdfConversions;
import mvm.rya.rdftriplestore.RdfCloudTripleStore;
import mvm.rya.rdftriplestore.RyaSailRepository;

/**
 * Integration tests that ensure the Fluo application processes PCJs results correctly.
 * <p>
 * This class is being ignored because it doesn't contain any unit tests.
 */
public abstract class ITBase {
    private static final Logger log = Logger.getLogger(ITBase.class);

    public static final String USE_MOCK_INSTANCE = ".useMockInstance";
    public static final String CLOUDBASE_INSTANCE = "sc.cloudbase.instancename";
    public static final String CLOUDBASE_USER = "sc.cloudbase.username";
    public static final String CLOUDBASE_PASSWORD = "sc.cloudbase.password";

    protected static final String RYA_TABLE_PREFIX = "demo_";

    // Rya data store and connections.
    protected MiniAccumuloCluster accumulo = null;
    protected static Connector accumuloConn = null;
    protected RyaSailRepository ryaRepo = null;
    protected RepositoryConnection ryaConn = null;

    // Fluo data store and connections.
    protected MiniFluo fluo = null;
    protected FluoClient fluoClient = null;

    @Before
    public void setupMiniResources() throws IOException, InterruptedException, AccumuloException, AccumuloSecurityException, RepositoryException {
        // Initialize the Mini Accumulo that will be used to store Triples and get a connection to it.
        accumulo = startMiniAccumulo();

        // Setup the Rya library to use the Mini Accumulo.
        ryaRepo = setupRya(accumulo);
        ryaConn = ryaRepo.getConnection();

        // Initialize the Mini Fluo that will be used to store created queries.
        fluo = startMiniFluo();
        fluoClient = FluoFactory.newClient( fluo.getClientConfiguration() );
    }

    @After
    public void shutdownMiniResources() {
        if(ryaConn != null) {
            try {
                log.info("Shutting down Rya Connection.");
                ryaConn.close();
                log.info("Rya Connection shut down.");
            } catch(final Exception e) {
                log.error("Could not shut down the Rya Connection.", e);
            }
        }

        if(ryaRepo != null) {
            try {
                log.info("Shutting down Rya Repo.");
                ryaRepo.shutDown();
                log.info("Rya Repo shut down.");
            } catch(final Exception e) {
                log.error("Could not shut down the Rya Repo.", e);
            }
        }

        if(accumulo != null) {
            try {
                log.info("Shutting down the Mini Accumulo being used as a Rya store.");
                accumulo.stop();
                log.info("Mini Accumulo being used as a Rya store shut down.");
            } catch(final Exception e) {
                log.error("Could not shut down the Mini Accumulo.", e);
            }
        }

        if(fluoClient != null) {
            try {
                log.info("Shutting down Fluo Client.");
                fluoClient.close();
                log.info("Fluo Client shut down.");
            } catch(final Exception e) {
                log.error("Could not shut down the Fluo Client.", e);
            }
        }

        if(fluo != null) {
            try {
                log.info("Shutting down Mini Fluo.");
                fluo.close();
                log.info("Mini Fluo shut down.");
            } catch (final Exception e) {
                log.error("Could not shut down the Mini Fluo.", e);
            }
        }
    }

    /**
     * A helper fuction for creating a {@link BindingSet} from an array of {@link Binding}s.
     *
     * @param bindings - The bindings to include in the set. (not null)
     * @return A {@link BindingSet} holding the bindings.
     */
    protected static BindingSet makeBindingSet(final Binding... bindings) {
        final MapBindingSet bindingSet = new MapBindingSet();
        for(final Binding binding : bindings) {
            bindingSet.addBinding(binding);
        }
        return bindingSet;
    }

    /**
     * A helper function for creating a {@link RyaStatement} that represents a Triple.
     *
     * @param subject - The Subject of the Triple. (not null)
     * @param predicate - The Predicate of the Triple. (not null)
     * @param object - The Object of the Triple. (not null)
     * @return A Triple as a {@link RyaStatement}.
     */
    protected static RyaStatement makeRyaStatement(final String subject, final String predicate, final String object) {
        checkNotNull(subject);
        checkNotNull(predicate);
        checkNotNull(object);

        final RyaStatementBuilder builder = RyaStatement.builder()
            .setSubject( new RyaURI(subject) )
            .setPredicate( new RyaURI(predicate) );

        if(object.startsWith("http://")) {
            builder.setObject(new RyaURI(object) );
        } else {
            builder.setObject( new RyaType(object) );
        }

        return builder.build();
    }

    /**
     * A helper function for creating a {@link RyaStatement} that represents a Triple.
     *
     * @param subject - The Subject of the Triple. (not null)
     * @param predicate - The Predicate of the Triple. (not null)
     * @param object - The Object of the Triple. (not null)
     * @return A Triple as a {@link RyaStatement}.
     */
    protected static RyaStatement makeRyaStatement(final String subject, final String predicate, final int object) {
        checkNotNull(subject);
        checkNotNull(predicate);

        return RyaStatement.builder()
                .setSubject(new RyaURI(subject))
                .setPredicate(new RyaURI(predicate))
                .setObject( new RyaType(XMLSchema.INT, "" + object) )
                .build();
    }

    /**
     * A helper function for creating a Sesame {@link Statement} that represents a Triple..
     *
     * @param subject - The Subject of the Triple. (not null)
     * @param predicate - The Predicate of the Triple. (not null)
     * @param object - The Object of the Triple. (not null)
     * @return A Triple as a {@link Statement}.
     */
    protected static Statement makeStatement(final String subject, final String predicate, final String object) {
        checkNotNull(subject);
        checkNotNull(predicate);
        checkNotNull(object);

        final RyaStatement ryaStmt = makeRyaStatement(subject, predicate, object);
        return RyaToRdfConversions.convertStatement(ryaStmt);
    }

    /**
     * Fetches the binding sets that are the results of a specific SPARQL query
     * from the Fluo table.
     *
     * @param fluoClient- A connection to the Fluo table where the results reside. (not null)
     * @param sparql - This query's results will be fetched. (not null)
     * @return The binding sets for the query's results.
     */
    protected static Set<BindingSet> getQueryBindingSetValues(final FluoClient fluoClient, final String sparql) {
        final Set<BindingSet> bindingSets = new HashSet<>();

        try(Snapshot snapshot = fluoClient.newSnapshot()) {
            final String queryId = snapshot.get(Bytes.of(sparql), FluoQueryColumns.QUERY_ID).toString();

            // Fetch the query's variable order.
            final QueryMetadata queryMetadata = new FluoQueryMetadataDAO().readQueryMetadata(snapshot, queryId);
            final VariableOrder varOrder = queryMetadata.getVariableOrder();

            // Fetch the Binding Sets for the query.
            final ScannerConfiguration scanConfig = new ScannerConfiguration();
            scanConfig.fetchColumn(FluoQueryColumns.QUERY_BINDING_SET.getFamily(), FluoQueryColumns.QUERY_BINDING_SET.getQualifier());

            BindingSetStringConverter converter = new BindingSetStringConverter();

            final RowIterator rowIter = snapshot.get(scanConfig);
            while(rowIter.hasNext()) {
                final Entry<Bytes, ColumnIterator> row = rowIter.next();
                final String bindingSetString = row.getValue().next().getValue().toString();
                final BindingSet bindingSet = converter.convert(bindingSetString, varOrder);
                bindingSets.add(bindingSet);
            }
        }

        return bindingSets;
    }

    /**
     * Setup a Mini Accumulo cluster that uses a temporary directory to store its data.
     *
     * @return A Mini Accumulo cluster.
     */
    private static MiniAccumuloCluster startMiniAccumulo() throws IOException, InterruptedException, AccumuloException, AccumuloSecurityException {
        final File miniDataDir = Files.createTempDir();

        // Setup and start the Mini Accumulo.
        final MiniAccumuloCluster accumulo = new MiniAccumuloCluster(miniDataDir, "password");
        accumulo.start();

        // Store a connector to the Mini Accumulo.
        final Instance instance = new ZooKeeperInstance(accumulo.getInstanceName(), accumulo.getZooKeepers());
        accumuloConn = instance.getConnector("root", new PasswordToken("password"));

        return accumulo;
    }

    /**
     * Format a Mini Accumulo to be a Rya repository.
     *
     * @param accumulo - The Mini Accumulo cluster Rya will sit on top of. (not null)
     * @return The Rya repository sitting on top of the Mini Accumulo.
     */
    private static RyaSailRepository setupRya(final MiniAccumuloCluster accumulo) throws AccumuloException, AccumuloSecurityException, RepositoryException {
        checkNotNull(accumulo);

        // Setup the Rya Repository that will be used to create Repository Connections.
        final RdfCloudTripleStore ryaStore = new RdfCloudTripleStore();
        final AccumuloRyaDAO crdfdao = new AccumuloRyaDAO();
        crdfdao.setConnector(accumuloConn);

        // Setup Rya configuration values.
        final AccumuloRdfConfiguration conf = new AccumuloRdfConfiguration();
        conf.setTablePrefix("demo_");
        conf.setDisplayQueryPlan(true);

        conf.setBoolean(USE_MOCK_INSTANCE, true);
        conf.set(RdfCloudTripleStoreConfiguration.CONF_TBL_PREFIX, RYA_TABLE_PREFIX);
        conf.set(CLOUDBASE_USER, "root");
        conf.set(CLOUDBASE_PASSWORD, "password");
        conf.set(CLOUDBASE_INSTANCE, accumulo.getInstanceName());

        crdfdao.setConf(conf);
        ryaStore.setRyaDAO(crdfdao);

        final RyaSailRepository ryaRepo = new RyaSailRepository(ryaStore);
        ryaRepo.initialize();

        return ryaRepo;
    }

    /**
     * Override this method to provide an output configuration to the Fluo application.
     * <p>
     * Returns an empty map by default.
     *
     * @return The parameters that will be passed to {@link QueryResultObserver} at startup.
     */
    protected Map<String, String> makeExportParams() {
        return new HashMap<>();
    }

    /**
     * Setup a Mini Fluo cluster that uses a temporary directory to store its data.ll
     *
     * @return A Mini Fluo cluster.
     */
    protected MiniFluo startMiniFluo() {
        final File miniDataDir = Files.createTempDir();

        // Setup the observers that will be used by the Fluo PCJ Application.
        final List<ObserverConfiguration> observers = new ArrayList<>();
        observers.add(new ObserverConfiguration(TripleObserver.class.getName()));
        observers.add(new ObserverConfiguration(StatementPatternObserver.class.getName()));
        observers.add(new ObserverConfiguration(JoinObserver.class.getName()));
        observers.add(new ObserverConfiguration(FilterObserver.class.getName()));

        // Provide export parameters child test classes may provide to the export observer.
        final ObserverConfiguration exportObserverConfig = new ObserverConfiguration(QueryResultObserver.class.getName());
        exportObserverConfig.setParameters( makeExportParams() );
        observers.add(exportObserverConfig);

        // Configure how the mini fluo will run.
        final FluoConfiguration config = new FluoConfiguration();
        config.setApplicationName("IntegrationTests");
        config.setMiniDataDir(miniDataDir.getAbsolutePath());
        config.addObservers(observers);

        final MiniFluo miniFluo = FluoFactory.newMiniFluo(config);
        return miniFluo;
    }
}