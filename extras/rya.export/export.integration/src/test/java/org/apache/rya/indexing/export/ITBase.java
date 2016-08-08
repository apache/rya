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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.log4j.Logger;
import org.apache.rya.indexing.pcj.fluo.app.observers.QueryResultObserver;
import org.junit.After;
import org.junit.Before;
import org.openrdf.model.Statement;
import org.openrdf.model.vocabulary.XMLSchema;
import org.openrdf.query.Binding;
import org.openrdf.query.BindingSet;
import org.openrdf.query.impl.MapBindingSet;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.openrdf.sail.Sail;

import com.mongodb.MongoClient;
import com.mongodb.MongoException;

import de.flapdoodle.embed.mongo.tests.MongodForTestsFactory;
import mvm.rya.api.RdfCloudTripleStoreConfiguration;
import mvm.rya.api.domain.RyaStatement;
import mvm.rya.api.domain.RyaStatement.RyaStatementBuilder;
import mvm.rya.api.domain.RyaType;
import mvm.rya.api.domain.RyaURI;
import mvm.rya.api.persist.RyaDAOException;
import mvm.rya.api.resolver.RyaToRdfConversions;
import mvm.rya.indexing.accumulo.ConfigUtils;
import mvm.rya.mongodb.MongoDBRdfConfiguration;
import mvm.rya.rdftriplestore.RyaSailRepository;
import mvm.rya.rdftriplestore.inference.InferenceEngineException;
import mvm.rya.sail.config.RyaSailFactory;

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

    // Rya data store and connections.
    protected RyaSailRepository ryaRepo = null;
    protected RepositoryConnection ryaConn = null;

    // Test Mongos
    private MongodForTestsFactory mongodTestFactory;
    protected List<MongoClient> clients = new ArrayList<>();

    @Before
    public void setup() throws IOException {
        mongodTestFactory = new MongodForTestsFactory();
    }

    /**
     * @return A new {@link MongoClient}.  Note: This does not have RYA installed.
     * @throws MongoException
     * @throws UnknownHostException
     */
    public MongoClient getnewMongoResources() throws UnknownHostException, MongoException {
        // Initialize the test mongo that will be used to host rya.
        final MongoClient newClient = mongodTestFactory.newMongo();
        clients.add(newClient);
        return newClient;
    }

    @After
    public void shutdownMiniResources() {
        for(final MongoClient client : clients) {
            client.close();
        }
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
     * A helper function for creating a Sesame {@link Statement} that represents
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
      */
    protected static RyaSailRepository setupRya(final String user, final String password, final String ryaInstanceName)
            throws AccumuloException, AccumuloSecurityException, RepositoryException, RyaDAOException,
            NumberFormatException, UnknownHostException, InferenceEngineException {
        checkNotNull(user);
        checkNotNull(password);
        checkNotNull(ryaInstanceName);

        // Setup Rya configuration values.
        final MongoDBRdfConfiguration conf = new MongoDBRdfConfiguration();
        conf.setTablePrefix(RYA_TABLE_PREFIX);
        conf.setDisplayQueryPlan(true);
        conf.setBoolean(ConfigUtils.USE_MOCK_INSTANCE, false);
        conf.set(ConfigUtils.CLOUDBASE_USER, user);
        conf.set(ConfigUtils.CLOUDBASE_PASSWORD, password);
        conf.set(MongoDBRdfConfiguration.USE_TEST_MONGO, "false");
        conf.set(MongoDBRdfConfiguration.MONGO_DB_NAME, "test");
        conf.set(MongoDBRdfConfiguration.MONGO_COLLECTION_PREFIX, "rya_");
        conf.set(RdfCloudTripleStoreConfiguration.CONF_TBL_PREFIX, "rya_");

        final Sail sail = RyaSailFactory.getInstance(conf);
        final RyaSailRepository ryaRepo = new RyaSailRepository(sail);
        ryaRepo.initialize();

        return ryaRepo;
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