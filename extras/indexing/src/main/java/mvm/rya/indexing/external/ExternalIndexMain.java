package mvm.rya.indexing.external;

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



import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import mvm.rya.accumulo.AccumuloRdfConfiguration;
import mvm.rya.accumulo.AccumuloRyaDAO;
import mvm.rya.indexing.accumulo.ConfigUtils;
import mvm.rya.indexing.external.tupleSet.AccumuloIndexSet;
import mvm.rya.indexing.external.tupleSet.ExternalTupleSet;
import mvm.rya.rdftriplestore.RdfCloudTripleStore;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.QueryResultHandlerException;
import org.openrdf.query.TupleQueryResultHandler;
import org.openrdf.query.TupleQueryResultHandlerException;
import org.openrdf.repository.sail.SailRepository;
import org.openrdf.repository.sail.SailRepositoryConnection;
import org.openrdf.sail.Sail;

import com.beust.jcommander.internal.Maps;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

public class ExternalIndexMain {

    private static String userStr = "";
    private static String passStr = "";

    private static String instStr = "";
    private static String zooStr = "";

    private static String tablePrefix = "";

    private static String AUTHS = "";

    public static void main(String[] args) throws Exception {
        Preconditions.checkArgument(args.length == 6, "java " + ExternalIndexMain.class.getCanonicalName()
                + " sparqlFile cbinstance cbzk cbuser cbpassword rdfTablePrefix.");

        final String sparqlFile = args[0];

        instStr = args[1];
        zooStr = args[2];
        userStr = args[3];
        passStr = args[4];
        tablePrefix = args[5];

        String queryString = FileUtils.readFileToString(new File(sparqlFile));


        // Look for Extra Indexes
        Instance inst = new ZooKeeperInstance(instStr, zooStr);
        Connector c = inst.getConnector(userStr, passStr.getBytes());

        System.out.println("Searching for Indexes");
        Map<String, String> indexTables = Maps.newLinkedHashMap();
        for (String table : c.tableOperations().list()) {
            if (table.startsWith(tablePrefix + "INDEX_")) {
                Scanner s = c.createScanner(table, new Authorizations());
                s.setRange(Range.exact(new Text("~SPARQL")));
                for (Entry<Key, Value> e : s) {
                    indexTables.put(table, e.getValue().toString());
                }
            }
        }

        List<ExternalTupleSet> index = Lists.newArrayList();

        if (indexTables.isEmpty()) {
            System.out.println("No Index found");
        } else {
            for (String table : indexTables.keySet()) {
                String indexSparqlString = indexTables.get(table);
                System.out.println("====================== INDEX FOUND ======================");
                System.out.println(" table : " + table);
                System.out.println(" sparql : ");
                System.out.println(indexSparqlString);

                index.add(new AccumuloIndexSet(indexSparqlString, c, table));
            }
        }

        // Connect to Rya
        Sail s = getRyaSail();
        SailRepository repo = new SailRepository(s);
        repo.initialize();

        // Perform Query

        CountingTupleQueryResultHandler count = new CountingTupleQueryResultHandler();

        SailRepositoryConnection conn;
        if (index.isEmpty()) {
            conn = repo.getConnection();

        } else {
            ExternalProcessor processor = new ExternalProcessor(index);

            Sail processingSail = new ExternalSail(s, processor);
            SailRepository smartSailRepo = new SailRepository(processingSail);
            smartSailRepo.initialize();

            conn = smartSailRepo.getConnection();
        }

        startTime = System.currentTimeMillis();
        lastTime = startTime;
        System.out.println("Query Started");
        conn.prepareTupleQuery(QueryLanguage.SPARQL, queryString).evaluate(count);

        System.out.println("Count of Results found : " + count.i);
        System.out.println("Total query time (s) : " + (System.currentTimeMillis() - startTime) / 1000.);
    }

    static long lastTime = 0;
    static long startTime = 0;

    private static class CountingTupleQueryResultHandler implements TupleQueryResultHandler {
        public int i = 0;

        @Override
        public void handleBoolean(boolean value) throws QueryResultHandlerException {
        }

        @Override
        public void handleLinks(List<String> linkUrls) throws QueryResultHandlerException {
        }

        @Override
        public void startQueryResult(List<String> bindingNames) throws TupleQueryResultHandlerException {
            System.out.println("First Result Recieved (s) : " + (System.currentTimeMillis() - startTime) / 1000.);
        }

        @Override
        public void endQueryResult() throws TupleQueryResultHandlerException {
        }

        @Override
        public void handleSolution(BindingSet bindingSet) throws TupleQueryResultHandlerException {
            i++;
            if (i % 10 == 0) {
                long mark = System.currentTimeMillis();
                System.out.println("Count : " + i + ". Time (s) : " + (mark - lastTime) / 1000.);
                lastTime = mark;
            }

        }

    }

    private static Configuration getConf() {

        Configuration conf = new Configuration();

        conf.set(ConfigUtils.CLOUDBASE_USER, userStr);
        conf.set(ConfigUtils.CLOUDBASE_PASSWORD, passStr);

        conf.set(ConfigUtils.CLOUDBASE_INSTANCE, instStr);
        conf.set(ConfigUtils.CLOUDBASE_ZOOKEEPERS, zooStr);

        conf.set(ConfigUtils.CLOUDBASE_AUTHS, AUTHS);
        conf.setBoolean(ConfigUtils.DISPLAY_QUERY_PLAN, true);
        return conf;
    }

    private static Sail getRyaSail() throws AccumuloException, AccumuloSecurityException {

        Connector connector = ConfigUtils.getConnector(getConf());

        final RdfCloudTripleStore store = new RdfCloudTripleStore();
        AccumuloRyaDAO crdfdao = new AccumuloRyaDAO();
        crdfdao.setConnector(connector);

        AccumuloRdfConfiguration conf = new AccumuloRdfConfiguration(getConf());
        conf.setTablePrefix(tablePrefix);
        crdfdao.setConf(conf);
        store.setRyaDAO(crdfdao);

        return store;
    }
}
