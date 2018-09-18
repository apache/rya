/**
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
package rya.export.integration;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.Scanner;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.rya.api.domain.RyaStatement;
import org.apache.rya.api.persist.RyaDAO;
import org.apache.rya.export.api.store.FetchStatementException;
import org.apache.rya.export.api.store.RyaStatementStore;
import org.apache.rya.export.client.merge.MemoryMerger;
import org.apache.rya.export.mongo.MongoRyaStatementStore;
import org.apache.rya.export.mongo.policy.TimestampPolicyMongoRyaStatementStore;
import org.apache.rya.indexing.accumulo.ConfigUtils;
import org.apache.rya.mongodb.MongoDBRdfConfiguration;
import org.apache.rya.mongodb.MongoDBRyaDAO;
import org.apache.rya.rdftriplestore.RdfCloudTripleStore;
import org.apache.rya.sail.config.RyaSailFactory;
import org.bson.Document;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.repository.sail.SailRepositoryConnection;
import org.eclipse.rdf4j.sail.Sail;

import com.mongodb.MongoClient;

public class ExportExample {
    private static final File DEMO_DATA_FILE = new File("src/main/resources/military_obs.trig");
    private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("mm-dd-yyyy hh:mm:ss");
    private static final long TIMESTAMP = 1529519599999L;
    private static Configuration getHostConf() {
        final MongoDBRdfConfiguration conf = new MongoDBRdfConfiguration();
        conf.setTablePrefix("rya_");
        conf.setMongoPort("27017");
        conf.setMongoHostname("localhost");
        conf.setRyaInstanceName("rya_host");
        conf.setBoolean(ConfigUtils.USE_MONGO, true);
        conf.setAuths("A");
        return conf;
    }

    private static Configuration getChildConf() {
        final MongoDBRdfConfiguration conf = new MongoDBRdfConfiguration();
        conf.setTablePrefix("rya_");
        conf.setMongoPort("27017");
        conf.setMongoHostname("localhost");
        conf.setRyaInstanceName("rya_child");
        conf.setBoolean(ConfigUtils.USE_MONGO, true);
        conf.setAuths("B");
        return conf;
    }

    private static SailRepository getRepo(final Configuration conf) throws Exception {
        final Sail sail = RyaSailFactory.getInstance(conf);
        return new SailRepository(sail);
    }

    public static void main(final String[] args) throws Exception {
        final MongoDBRdfConfiguration hostConf = (MongoDBRdfConfiguration) getHostConf();
        final MongoDBRdfConfiguration childConf = (MongoDBRdfConfiguration) getChildConf();

        final SailRepository hostRepository = getRepo(hostConf);
        final SailRepositoryConnection hostConn = hostRepository.getConnection();

        final SailRepository childRepository = getRepo(childConf);
        final SailRepositoryConnection childConn = childRepository.getConnection();

        try {
            final Scanner s = new Scanner(System.in);
            final RyaDAO hostDAO = ((RdfCloudTripleStore) hostRepository.getSail()).getRyaDAO();
            final RyaDAO childDAO = ((RdfCloudTripleStore) childRepository.getSail()).getRyaDAO();
            final MongoClient client = new MongoClient(hostConf.getMongoHostname(), Integer.parseInt(hostConf.getMongoPort()));
            s.nextLine();

            final MongoRyaStatementStore baseHostStore = new MongoRyaStatementStore(client, hostConf.getRyaInstanceName(), (MongoDBRyaDAO) hostDAO);
            final TimestampPolicyMongoRyaStatementStore hostStore = new TimestampPolicyMongoRyaStatementStore(baseHostStore, TIMESTAMP);
            System.out.println("Host Store");
            printDB(hostStore);

            final MongoRyaStatementStore baseChildStore = new MongoRyaStatementStore(client, childConf.getRyaInstanceName(), (MongoDBRyaDAO) childDAO);
            System.out.println("\n\nChild Store");
            printDB(baseChildStore);

            System.out.println("Creating Exporter");
            // export only half-ish
            final MemoryMerger exporter = new MemoryMerger(hostStore, baseChildStore, hostConf.getRyaInstanceName(), 0L);
            s.nextLine();

            System.out.println("Exporting all data after " + DATE_FORMAT.format(new Date(TIMESTAMP)) + ".");
            exporter.runJob();
            s.nextLine();
            System.out.println("\n\nHost Store");
            printDB(hostStore);

            System.out.println("\n\nChild Store");
            printDB(baseChildStore);

            System.out.println("Removing an observation from the child database.");
            final String subjToRemove = "https://urldefense.proofpoint.com/v2/observed/obs-b07d5af7-dccd-4d73-ad52-eb2c7308a044";
            client.getDatabase("rya_child").getCollection("rya_triples").deleteMany(new Document("subject", subjToRemove));
            s.nextLine();

            final String subjToUpdate = "https://urldefense.proofpoint.com/v2/observed/obs-ab8123dc-0a9b-45d0-902b-e6a3df9f64bc";
            final String predToUpdate = "https://urldefense.proofpoint.com/v2/vocab/mft#destination";
            System.out.println("Updating data in host database.");
            final Document query = new Document();
            query.append("subject", subjToUpdate);
            query.append("predicate", predToUpdate);
            client.getDatabase("rya_host").getCollection("rya_triples").updateOne(
                    query,
                    new Document("$set", new Document("object", "MEDITERRANEAN SEA")));
            s.nextLine();

            System.out.println("\n\nHost Store");
            printDB(hostStore);

            System.out.println("\n\nChild Store");
            printDB(baseChildStore);

            s.nextLine();
            System.out.println("Merging data from child database.");
            final MemoryMerger merger = new MemoryMerger(baseChildStore, hostStore, hostConf.getRyaInstanceName(), 0L);
            merger.runJob();
            s.nextLine();

            System.out.println("Show updated data.");
            System.out.println("\n\nHost Store");
            printDB(hostStore);

            System.out.println("\n\nChild Store");
            printDB(baseChildStore);
            s.nextLine();
        } finally {
            System.out.println("Shutting down");
            closeQuietly(hostConn);
            closeQuietly(hostRepository);

            closeQuietly(childConn);
            closeQuietly(childRepository);
        }
    }

    private static final String STRIP_TYPE = "http://www.w3.org/1999/02/22-rdf-syntax-ns#";
    private static final String STRIP_OBS = "https://urldefense.proofpoint.com/v2/vocab/";
    private static void printDB(final RyaStatementStore store) throws FetchStatementException {
        int count = 0;
        System.out.printf("|-%s-|\n", StringUtils.center("", 90, "-"));
        System.out.printf("|-%20s | %45s | %18s -|\n", "Predicate", "Object", "Timestamp");
        System.out.printf("|-%s-|\n", StringUtils.center("", 90, "-"));
        final Iterator<RyaStatement> stmnts = store.fetchStatements();
        while (stmnts.hasNext()) {
            count++;
            final RyaStatement stmnt = stmnts.next();
            System.out.printf("| %20s | %45s | %s |\n",
                StringUtils.center(stmnt.getPredicate().getData().replaceAll(STRIP_OBS, "").replaceAll(STRIP_TYPE, ""), 20),
                StringUtils.center(stmnt.getObject().getData().replaceAll(STRIP_OBS, ""), 45),
                DATE_FORMAT.format(new Date(stmnt.getTimestamp())));
        }
        System.out.println("Total Statements: " + count);
        System.out.printf("|-%s-|\n", StringUtils.center("", 90, "-"));
    }

    private static void closeQuietly(final SailRepository repository) {
        if (repository != null) {
            try {
                repository.shutDown();
            } catch (final RepositoryException e) {
                // quietly absorb this exception
            }
        }
    }

    private static void closeQuietly(final SailRepositoryConnection conn) {
        if (conn != null) {
            try {
                conn.close();
            } catch (final RepositoryException e) {
                // quietly absorb this exception
            }
        }
    }
}
