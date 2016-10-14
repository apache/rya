package org.apache.rya.reasoning.mr;

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
import java.io.IOException;

import org.apache.rya.accumulo.AccumuloRdfConfiguration;
import org.apache.rya.accumulo.AccumuloRdfConstants;
import org.apache.rya.accumulo.AccumuloRyaDAO;
import org.apache.rya.accumulo.mr.MRUtils;
import org.apache.rya.api.RdfCloudTripleStoreConstants;
import org.apache.rya.api.RdfCloudTripleStoreUtils;
import org.apache.rya.api.domain.RyaStatement;
import org.apache.rya.api.resolver.RyaTripleContext;
import org.apache.rya.api.resolver.triple.TripleRow;
import org.apache.rya.api.resolver.triple.TripleRowResolverException;
import org.apache.rya.rdftriplestore.RdfCloudTripleStore;
import org.apache.rya.rdftriplestore.RyaSailRepository;
import org.apache.rya.reasoning.Schema;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.ClientConfiguration;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.Job;

/**
 * Convenience methods for MapReduce reasoning tasks and options.
 */
public class MRReasoningUtils {
    // Configuration variables
    public static final String WORKING_DIR = "reasoning.workingDir";
    public static final String LOCAL_INPUT = "reasoning.inputLocal";
    public static final String DEBUG_FLAG = "reasoning.debug";
    public static final String OUTPUT_FLAG = "reasoning.output";
    public static final String STATS_FLAG = "reasoning.stats";

    // Variables used to pass information from drivers to jobs
    public static final String STEP_PROP = "reasoning.step";
    public static final String SCHEMA_UPDATE_PROP = "reasoning.schemaUpdate";

    // Used to construct input/output directories
    static final String OUTPUT_BASE = "step-";
    static final String SCHEMA_BASE = "schema-";
    static final String TEMP_SUFFIX = "a";
    // Named outputs for different kinds of facts
    static final String SCHEMA_OUT = "schema";
    static final String INCONSISTENT_OUT = "inconsistencies";
    static final String TERMINAL_OUT = "instance";
    static final String INTERMEDIATE_OUT = "intermediate";
    static final String DEBUG_OUT = "debug";

    /**
     * Load serialized schema information from a file.
     */
    public static Schema loadSchema(Configuration conf) {
        SchemaWritable schema = new SchemaWritable();
        try {
            FileSystem fs = FileSystem.get(conf);
            Path schemaPath = getSchemaPath(conf);
            if (fs.isDirectory(schemaPath)) {
                for (FileStatus status : fs.listStatus(schemaPath)) {
                    schemaPath = status.getPath();
                    if (status.isFile() && status.getLen() > 0
                        && !schemaPath.getName().startsWith(DEBUG_OUT)) {
                        break;
                    }
                }
            }
            SequenceFile.Reader in = new SequenceFile.Reader(conf,
                SequenceFile.Reader.file(schemaPath));
            NullWritable key = NullWritable.get();
            in.next(key, schema);
            in.close();
        }
        catch (IOException e) {
            e.printStackTrace();
        }
        return schema;
    }

    /**
     * Record that the schema was updated at this iteration.
     */
    static void schemaUpdated(Configuration conf) {
        conf.setInt(SCHEMA_UPDATE_PROP, getCurrentIteration(conf));
    }

    /**
     * Mark the beginning of the next iteration.
     */
    static void nextIteration(Configuration conf) {
        conf.setInt(STEP_PROP, getCurrentIteration(conf)+1);
    }

    /**
     * Convert an Accumulo row to a RyaStatement.
     */
    static RyaStatement getStatement(Key row, Value data, Configuration conf) {
        try {
            RyaTripleContext ryaContext = RyaTripleContext.getInstance(
                new AccumuloRdfConfiguration(conf));
            RyaStatement ryaStatement = ryaContext.deserializeTriple(
                RdfCloudTripleStoreConstants.TABLE_LAYOUT.SPO,
                new TripleRow(row.getRow().getBytes(), row.getColumnFamily().getBytes(),
                    row.getColumnQualifier().getBytes(), row.getTimestamp(),
                    row.getColumnVisibility().getBytes(), data.get()));
            return ryaStatement;
        }
        catch (TripleRowResolverException e) {
            e.printStackTrace();
            System.err.println("row: " + row);
            return null;
        }
        catch (IllegalArgumentException e) {
            e.printStackTrace();
            System.err.println("row: " + row);
            throw e;
        }
    }

    /**
     * Clean up intermediate data, unless debug=true
     */
    static void clean(Configuration conf) throws IOException {
        if (!debug(conf)) {
            int iteration = getCurrentIteration(conf);
            for (int i = 0; i <= iteration; i++) {
                deleteIfExists(conf, OUTPUT_BASE + i);
                deleteIfExists(conf, OUTPUT_BASE + i + TEMP_SUFFIX);
                deleteIfExists(conf, SCHEMA_BASE + i);
            }
            deleteIfExists(conf, "input");
        }
    }

    /**
     * If a local input path was given, upload it to HDFS and configure file
     * input. Useful for automating tests against small inputs.
     */
    static boolean uploadIfNecessary(Configuration conf)
            throws IOException {
        String local = conf.get(LOCAL_INPUT);
        if (local == null) {
            return false;
        }
        FileSystem fs = FileSystem.get(conf);
        String current = new File("").getAbsolutePath();
        Path sourcePath = new Path(current, local);
        Path destPath = getOutputPath(conf, "input");
        fs.copyFromLocalFile(false, true, sourcePath, destPath);
        conf.set(MRUtils.INPUT_PATH, destPath.toString());
        return true;
    }

    /**
     * Delete an HDFS directory if it exists
     */
    static void deleteIfExists(Configuration conf, String rel)
            throws IOException {
        FileSystem fs = FileSystem.get(conf);
        Path path = getOutputPath(conf, rel);
        if (fs.isDirectory(path) || fs.isFile(path)) {
            fs.delete(path, true);
        }
    }

    /**
     * Get a Repository from the configuration variables
     */
    static RyaSailRepository getRepository(Configuration conf)
            throws AccumuloException, AccumuloSecurityException {
        boolean mock = conf.getBoolean(MRUtils.AC_MOCK_PROP, false);
        String instance = conf.get(MRUtils.AC_INSTANCE_PROP, "instance");
        String username = conf.get(MRUtils.AC_USERNAME_PROP, "root");
        String password = conf.get(MRUtils.AC_PWD_PROP, "root");
        Instance accumulo;
        if (mock) {
            accumulo = new MockInstance(instance);
        }
        else {
            String zookeepers = conf.get(MRUtils.AC_ZK_PROP, "zoo");
            accumulo = new ZooKeeperInstance(instance, zookeepers);
        }
        Connector connector = accumulo.getConnector(username, new PasswordToken(password));
        AccumuloRdfConfiguration aconf = new AccumuloRdfConfiguration(conf);
        aconf.setTablePrefix(conf.get(MRUtils.TABLE_PREFIX_PROPERTY,
            RdfCloudTripleStoreConstants.TBL_PRFX_DEF));
        AccumuloRyaDAO dao = new AccumuloRyaDAO();
        dao.setConnector(connector);
        dao.setConf(aconf);
        RdfCloudTripleStore store = new RdfCloudTripleStore();
        store.setRyaDAO(dao);
        return new RyaSailRepository(store);
    }

    /**
     * Set up a MapReduce Job to use Accumulo as input.
     */
    static void configureAccumuloInput(Job job)
            throws AccumuloSecurityException {
        Configuration conf = job.getConfiguration();
        String username = conf.get(MRUtils.AC_USERNAME_PROP, "root");
        String password = conf.get(MRUtils.AC_PWD_PROP, "");
        String instance = conf.get(MRUtils.AC_INSTANCE_PROP, "instance");
        String zookeepers = conf.get(MRUtils.AC_ZK_PROP, "zoo");
        Authorizations auths;
        String auth = conf.get(MRUtils.AC_AUTH_PROP);
        if (auth != null) {
            auths = new Authorizations(auth.split(","));
        }
        else {
            auths = AccumuloRdfConstants.ALL_AUTHORIZATIONS;
        }
        AccumuloInputFormat.setZooKeeperInstance(job,
            ClientConfiguration.loadDefault()
            .withInstance(instance).withZkHosts(zookeepers));
        AccumuloInputFormat.setConnectorInfo(job, username, new PasswordToken(password));
        AccumuloInputFormat.setInputTableName(job, getTableName(conf));
        AccumuloInputFormat.setScanAuthorizations(job, auths);
    }

    /**
     * Get the table name that will be used for Accumulo input.
     */
    static String getTableName(Configuration conf) {
        String layout = conf.get(MRUtils.TABLE_LAYOUT_PROP,
            RdfCloudTripleStoreConstants.TABLE_LAYOUT.SPO.toString());
        String prefix = conf.get(MRUtils.TABLE_PREFIX_PROPERTY,
            RdfCloudTripleStoreConstants.TBL_PRFX_DEF);
        return RdfCloudTripleStoreUtils.layoutPrefixToTable(
            RdfCloudTripleStoreConstants.TABLE_LAYOUT.valueOf(layout), prefix);
    }

    /**
     * Whether we should output the final inferences.
     */
    static boolean shouldOutput(Configuration conf) {
        return conf.getBoolean(OUTPUT_FLAG, true);
    }

    /**
     * Return whether debug flag is on.
     */
    static boolean debug(Configuration conf) {
        return conf.getBoolean(DEBUG_FLAG, false);
    }

    /**
     * Return whether detailed statistics should be printed.
     */
    static boolean stats(Configuration conf) {
        return conf.getBoolean(STATS_FLAG, false);
    }

    /**
     * Get the Path for RDF file input, or null if not given.
     */
    static Path getInputPath(Configuration conf) {
        String in = conf.get(MRUtils.INPUT_PATH);
        if (in == null) {
            return null;
        }
        return new Path(in);
    }

    /**
     * Get the full output path for a configuration and relative pathname.
     */
    static Path getOutputPath(Configuration conf, String name) {
        String root = conf.get(WORKING_DIR, "tmp/reasoning");
        return new Path(root + "/" + name);
    }

    /**
     * Get the path to the Schema.
     */
    static Path getSchemaPath(Configuration conf) {
        int iteration = lastSchemaUpdate(conf);
        return getOutputPath(conf, SCHEMA_BASE + iteration);
    }

    /**
     * Get the current iteration, useful for keeping track of when facts were
     * generated.
     */
    public static int getCurrentIteration(Configuration conf) {
        return conf.getInt(STEP_PROP, 0);
    }

    /**
     * Get the time of the last change to the schema.
     */
    static int lastSchemaUpdate(Configuration conf) {
        return conf.getInt(SCHEMA_UPDATE_PROP, 0);
    }

    /**
     * True if the schema was just updated on the last pass.
     */
    public static boolean isSchemaNew(Configuration conf) {
        return lastSchemaUpdate(conf) == getCurrentIteration(conf) - 1;
    }
}
