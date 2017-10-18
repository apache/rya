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
package org.apache.rya.indexing.export;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.log4j.Logger;
import org.apache.rya.api.RdfCloudTripleStoreConfiguration;
import org.apache.rya.api.domain.RyaStatement;
import org.apache.rya.api.domain.RyaStatement.RyaStatementBuilder;
import org.apache.rya.api.domain.RyaType;
import org.apache.rya.api.domain.RyaURI;
import org.apache.rya.api.persist.RyaDAOException;
import org.apache.rya.api.resolver.RyaToRdfConversions;
import org.apache.rya.indexing.accumulo.ConfigUtils;
import org.apache.rya.indexing.pcj.fluo.app.observers.QueryResultObserver;
import org.apache.rya.mongodb.MongoDBRdfConfiguration;
import org.apache.rya.rdftriplestore.RyaSailRepository;
import org.apache.rya.rdftriplestore.inference.InferenceEngineException;
import org.apache.rya.sail.config.RyaSailFactory;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.vocabulary.XMLSchema;
import org.eclipse.rdf4j.query.Binding;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.impl.MapBindingSet;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.eclipse.rdf4j.sail.Sail;
import org.eclipse.rdf4j.sail.SailException;
import org.junit.AfterClass;

import com.mongodb.MongoClient;
import com.mongodb.MongoException;

import de.flapdoodle.embed.mongo.tests.MongodForTestsFactory;

/**
 * Integration tests that ensure the import/export process runs correctly.
 * <p>
 * This class is being ignored because it doesn't contain any unit tests.
 */
public abstract class ITBase {
    private static final Logger log = Logger.getLogger(ITBase.class);

    protected static final String RYA_TABLE_PREFIX = "demo_";

    protected static final String USER = "root";
    protected static final String PASSWORD = "password";

    protected static final String MONGO_USER = "testUser";
    protected static final String MONGO_PASSWORD = "testPSWD";

    // Rya data store and connections.
    protected static List<RyaSailRepository> ryaRepos = new ArrayList<>();
    protected static List<RepositoryConnection> ryaConns = new ArrayList<>();

    // Rya mongo configs
    protected static Map<MongoClient, MongoDBRdfConfiguration> configs = new HashMap<>();

    // Test Mongos
    protected static List<MongoClient> clients = new ArrayList<>();

    /**
     * @return A new {@link MongoClient}.  Note: This does not have RYA installed.
     * @throws MongoException
     * @throws InferenceEngineException
     * @throws RyaDAOException
     * @throws AccumuloSecurityException
     * @throws AccumuloException
     * @throws RepositoryException
     * @throws NumberFormatException
     * @throws IOException
     * @throws SailException
     */
    public static MongoClient getNewMongoResources(final String ryaInstanceName) throws MongoException, NumberFormatException, RepositoryException, AccumuloException, AccumuloSecurityException, RyaDAOException, InferenceEngineException, IOException, SailException {
        // Initialize the test mongo that will be used to host rya.
        final MongodForTestsFactory mongodTestFactory = new MongodForTestsFactory();
        final MongoClient newClient = mongodTestFactory.newMongo();
        clients.add(newClient);
        final String host = newClient.getAddress().getHost();
        final int port = newClient.getAddress().getPort();
        final RyaSailRepository newRepo = setupRya(ryaInstanceName, host, port, newClient);
        ryaRepos.add(newRepo);
        return newClient;
    }

    @AfterClass
    public static void shutdownMiniResources() throws RepositoryException {
        for(final RyaSailRepository repo : ryaRepos) {
            repo.shutDown();
        }
        for(final RepositoryConnection conn : ryaConns) {
            conn.close();
        }
        for(final MongoClient client : clients) {
            client.close();
        }
        ryaRepos.clear();
        ryaConns.clear();
        clients.clear();
    }

    /**
     * A helper fuction for creating a {@link BindingSet} from an array of
     * {@link Binding}s.
     *
     * @param bindings
     *            - The bindings to include in the set. (not null)
     * @return A {@link BindingSet} holding the bindings.
     */
    protected static BindingSet makeBindingSet(final Binding... bindings) {
        final MapBindingSet bindingSet = new MapBindingSet();
        for (final Binding binding : bindings) {
            bindingSet.addBinding(binding);
        }
        return bindingSet;
    }

    /**
     * A helper function for creating a {@link RyaStatement} that represents a
     * Triple.
     *
     * @param subject
     *            - The Subject of the Triple. (not null)
     * @param predicate
     *            - The Predicate of the Triple. (not null)
     * @param object
     *            - The Object of the Triple. (not null)
     * @return A Triple as a {@link RyaStatement}.
     */
    protected static RyaStatement makeRyaStatement(final String subject, final String predicate, final String object) {
        checkNotNull(subject);
        checkNotNull(predicate);
        checkNotNull(object);

        final RyaStatementBuilder builder = RyaStatement.builder().setSubject(new RyaURI(subject))
                .setPredicate(new RyaURI(predicate));

        if (object.startsWith("http://")) {
            builder.setObject(new RyaURI(object));
        } else {
            builder.setObject(new RyaType(object));
        }
        builder.setTimestamp(new Date().getTime());

        return builder.build();
    }

    /**
     * A helper function for creating a {@link RyaStatement} that represents a
     * Triple.
     *
     * @param subject
     *            - The Subject of the Triple. (not null)
     * @param predicate
     *            - The Predicate of the Triple. (not null)
     * @param object
     *            - The Object of the Triple. (not null)
     * @return A Triple as a {@link RyaStatement}.
     */
    protected static RyaStatement makeRyaStatement(final String subject, final String predicate, final int object) {
        checkNotNull(subject);
        checkNotNull(predicate);

        return RyaStatement.builder().setSubject(new RyaURI(subject)).setPredicate(new RyaURI(predicate))
                .setObject(new RyaType(XMLSchema.INT, "" + object)).build();
    }

    /**
     * A helper function for creating a RDF4J {@link Statement} that represents
     * a Triple..
     *
     * @param subject
     *            - The Subject of the Triple. (not null)
     * @param predicate
     *            - The Predicate of the Triple. (not null)
     * @param object
     *            - The Object of the Triple. (not null)
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
      * Sets up a Rya instance
      *
      * @param user
      * @param password
      * @param instanceName
      * @param zookeepers
      * @param appName
      * @return
      * @throws AccumuloException
      * @throws AccumuloSecurityException
      * @throws RepositoryException
      * @throws RyaDAOException
      * @throws NumberFormatException
      * @throws UnknownHostException
      * @throws InferenceEngineException
     * @throws SailException
      */
    protected static RyaSailRepository setupRya(final String ryaInstanceName,
            final String hostname, final int port, final MongoClient client)
            throws AccumuloException, AccumuloSecurityException, RepositoryException, RyaDAOException,
            NumberFormatException, UnknownHostException, InferenceEngineException, SailException {
        checkNotNull(ryaInstanceName);

        // Setup Rya configuration values.
        final MongoDBRdfConfiguration conf = getConf(ryaInstanceName, hostname, port);
        configs.put(client, conf);

        final Sail sail = RyaSailFactory.getInstance(conf);
        final RyaSailRepository ryaRepo = new RyaSailRepository(sail);
        return ryaRepo;
    }

    protected static MongoDBRdfConfiguration getConf(final String ryaInstanceName,
            final String hostname, final int port) {
        final MongoDBRdfConfiguration conf = new MongoDBRdfConfiguration();
        conf.setBoolean(ConfigUtils.USE_MONGO, true);
        conf.setBoolean(ConfigUtils.USE_MOCK_INSTANCE, false);

        conf.setTablePrefix(RYA_TABLE_PREFIX);

        conf.setDisplayQueryPlan(true);

        conf.set(ConfigUtils.CLOUDBASE_USER, USER);
        conf.set(ConfigUtils.CLOUDBASE_PASSWORD, PASSWORD);

        conf.set(MongoDBRdfConfiguration.MONGO_DB_NAME, "test");
        conf.set(MongoDBRdfConfiguration.MONGO_COLLECTION_PREFIX, "rya_");

        conf.set(RdfCloudTripleStoreConfiguration.CONF_TBL_PREFIX, "rya_");

        conf.setMongoPort(""+port);
        conf.setMongoHostname(hostname);
        conf.setMongoDBName(ryaInstanceName);
        return conf;
    }

    protected static MongoDBRdfConfiguration getConf(final MongoClient client) {
        return configs.get(client);
    }

    /**
     * Override this method to provide an output configuration to the Fluo
     * application.
     * <p>
     * Returns an empty map by default.
     *
     * @return The parameters that will be passed to {@link QueryResultObserver}
     *         at startup.
     */
    protected Map<String, String> makeExportParams() {
        return new HashMap<>();
    }
}