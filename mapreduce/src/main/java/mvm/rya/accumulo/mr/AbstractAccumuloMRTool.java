package mvm.rya.accumulo.mr;

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

import mvm.rya.accumulo.AccumuloRdfConstants;
import mvm.rya.api.RdfCloudTripleStoreConfiguration;
import mvm.rya.api.RdfCloudTripleStoreConstants;
import mvm.rya.api.RdfCloudTripleStoreConstants.TABLE_LAYOUT;
import mvm.rya.api.RdfCloudTripleStoreUtils;
import mvm.rya.indexing.accumulo.ConfigUtils;

import java.io.IOException;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.ClientConfiguration;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat;
import org.apache.accumulo.core.client.mapreduce.AccumuloOutputFormat;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.iterators.user.AgeOffFilter;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.openrdf.rio.RDFFormat;

import com.google.common.base.Preconditions;

/**
 * Base class for MapReduce tools that interact with Accumulo-backed Rya. Holds
 * a {@link Configuration} to keep track of connection parameters.
 * <p>
 * Can be configured to read input from Rya, either as
 * {@link RyaStatementWritable}s or as Accumulo rows, or to read statements from
 * RDF files.
 * <p>
 * Can be configured to send output either by inserting RyaStatementWritables to
 * a Rya instance, or by writing arbitrary
 * {@link org.apache.accumulo.core.data.Mutation}s directly to Accumulo tables.
 */
public abstract class AbstractAccumuloMRTool implements Tool {
    static int DEFAULT_IO_SORT_MB = 256;

    protected Configuration conf;

    // Connection parameters
    protected String zk;
    protected String instance;
    protected String userName;
    protected String pwd;
    protected Authorizations authorizations;
    protected boolean mock = false;
    protected boolean hdfsInput = false;
    protected String ttl;
    protected String tablePrefix;
    protected TABLE_LAYOUT rdfTableLayout;

    /**
     * Gets the Configuration containing any relevant options.
     * @return This Tool's Configuration object.
     */
    @Override
    public Configuration getConf() {
        return conf;
    }

    /**
     * Set this Tool's Configuration.
     */
    @Override
    public void setConf(Configuration configuration) {
        this.conf = configuration;
    }

    /**
     * Initializes configuration parameters, checking that required parameters
     * are found and ensuring that options corresponding to multiple property
     * names are set consistently. Requires at least that the username,
     * password, and instance name are all configured. Zookeeper hosts must be
     * configured if not using a mock instance. Table prefix, if not provided,
     * will be set to {@link RdfCloudTripleStoreConstants#TBL_PRFX_DEF}. Should
     * be called before configuring input/output. See {@link MRUtils} for
     * configuration properties.
     */
    protected void init() {
        // Load configuration parameters
        zk = MRUtils.getACZK(conf);
        instance = MRUtils.getACInstance(conf);
        userName = MRUtils.getACUserName(conf);
        pwd = MRUtils.getACPwd(conf);
        mock = MRUtils.getACMock(conf, false);
        ttl = MRUtils.getACTtl(conf);
        tablePrefix = MRUtils.getTablePrefix(conf);
        rdfTableLayout = MRUtils.getTableLayout(conf, TABLE_LAYOUT.OSP);
        hdfsInput = conf.getBoolean(MRUtils.AC_HDFS_INPUT_PROP, false);
        // Set authorizations if specified
        String authString = conf.get(MRUtils.AC_AUTH_PROP);
        if (authString != null && !authString.isEmpty()) {
            authorizations = new Authorizations(authString.split(","));
            conf.set(ConfigUtils.CLOUDBASE_AUTHS, authString); // for consistency
        }
        else {
            authorizations = AccumuloRdfConstants.ALL_AUTHORIZATIONS;
        }
        // Set table prefix to the default if not set
        if (tablePrefix == null) {
            tablePrefix = RdfCloudTripleStoreConstants.TBL_PRFX_DEF;
            MRUtils.setTablePrefix(conf, tablePrefix);
        }
        // Check for required configuration parameters
        Preconditions.checkNotNull(instance, "Accumulo instance name [" + MRUtils.AC_INSTANCE_PROP + "] not set.");
        Preconditions.checkNotNull(userName, "Accumulo username [" + MRUtils.AC_USERNAME_PROP + "] not set.");
        Preconditions.checkNotNull(pwd, "Accumulo password [" + MRUtils.AC_PWD_PROP + "] not set.");
        Preconditions.checkNotNull(tablePrefix, "Table prefix [" + MRUtils.TABLE_PREFIX_PROPERTY + "] not set.");
        RdfCloudTripleStoreConstants.prefixTables(tablePrefix);
        // If connecting to real accumulo, set additional parameters and require zookeepers
        if (!mock) {
            Preconditions.checkNotNull(zk, "Zookeeper hosts not set (" + MRUtils.AC_ZK_PROP + ")");
            conf.setBoolean("mapred.map.tasks.speculative.execution", false);
            conf.setBoolean("mapred.reduce.tasks.speculative.execution", false);
            if (conf.get(MRUtils.HADOOP_IO_SORT_MB) == null) {
                conf.setInt(MRUtils.HADOOP_IO_SORT_MB, DEFAULT_IO_SORT_MB);
            }
            conf.set(ConfigUtils.CLOUDBASE_ZOOKEEPERS, zk); // for consistency
        }
        // Ensure consistency between alternative configuration properties
        conf.set(ConfigUtils.CLOUDBASE_INSTANCE, instance);
        conf.set(ConfigUtils.CLOUDBASE_USER, userName);
        conf.set(ConfigUtils.CLOUDBASE_PASSWORD, pwd);
        conf.setBoolean(ConfigUtils.USE_MOCK_INSTANCE, mock);
        conf.set(RdfCloudTripleStoreConfiguration.CONF_TBL_PREFIX, tablePrefix);
    }

    /**
     * Sets up Accumulo input for a job: the job receives
     * ({@link org.apache.accumulo.core.data.Key},
     * {@link org.apache.accumulo.core.data.Value}) pairs from the table
     * specified by the configuration (using
     * {@link MRUtils#TABLE_PREFIX_PROPERTY} and
     * {@link MRUtils#TABLE_LAYOUT_PROP}).
     * @param   job     MapReduce Job to configure
     * @throws  AccumuloSecurityException if connecting to Accumulo with the
     *          given username and password fails.
     */
    protected void setupAccumuloInput(Job job) throws AccumuloSecurityException {
        // set up accumulo input
        if (!hdfsInput) {
            job.setInputFormatClass(AccumuloInputFormat.class);
        } else {
            job.setInputFormatClass(AccumuloHDFSFileInputFormat.class);
        }
        AccumuloInputFormat.setConnectorInfo(job, userName, new PasswordToken(pwd));
        String tableName = RdfCloudTripleStoreUtils.layoutPrefixToTable(rdfTableLayout, tablePrefix);
        AccumuloInputFormat.setInputTableName(job, tableName);
        AccumuloInputFormat.setScanAuthorizations(job, authorizations);
        if (mock) {
            AccumuloInputFormat.setMockInstance(job, instance);
        } else {
            ClientConfiguration clientConfig = ClientConfiguration.loadDefault()
                    .withInstance(instance).withZkHosts(zk);
            AccumuloInputFormat.setZooKeeperInstance(job, clientConfig);
        }
        if (ttl != null) {
            IteratorSetting setting = new IteratorSetting(1, "fi", AgeOffFilter.class.getName());
            AgeOffFilter.setTTL(setting, Long.valueOf(ttl));
            AccumuloInputFormat.addIterator(job, setting);
        }
    }

    /**
     * Sets up Rya input for a job: the job receives
     * ({@link org.apache.hadoop.io.LongWritable}, {@link RyaStatementWritable})
     * pairs from a Rya instance. Uses the same configuration properties to
     * connect as direct Accumulo input, but returns statement data instead of
     * row data.
     * @param   job     Job to configure
     * @throws  AccumuloSecurityException if connecting to Accumulo with the
     *          given username and password fails.
     */
    protected void setupRyaInput(Job job) throws AccumuloSecurityException {
        setupAccumuloInput(job);
        job.setInputFormatClass(RyaInputFormat.class);
    }

    /**
     * Sets up RDF file input for a job: the job receives
     * ({@link org.apache.hadoop.io.LongWritable}, {@link RyaStatementWritable})
     * pairs from RDF file(s) found at the specified path.
     * @param   job   Job to configure
     * @param   inputPath     File or directory name
     * @param   defaultFormat  Default RDF serialization format, can be
     *                         overridden by {@link MRUtils#FORMAT_PROP}
     * @throws  IOException if there's an error interacting with the
     *          {@link org.apache.hadoop.fs.FileSystem}.
     */
    protected void setupFileInput(Job job, String inputPath, RDFFormat defaultFormat) throws IOException {
        RDFFormat format = MRUtils.getRDFFormat(conf);
        if (format == null) {
            format = defaultFormat;
        }
        RdfFileInputFormat.addInputPath(job, new Path(inputPath));
        RdfFileInputFormat.setRDFFormat(job, format);
        job.setInputFormatClass(RdfFileInputFormat.class);
    }

    /**
     * Sets up Accumulo output for a job: allows the job to write (String,
     * Mutation) pairs, where the Mutation will be written to the table named by
     * the String.
     * @param   job Job to configure
     * @param   outputTable Default table to send output to
     * @throws  AccumuloSecurityException if connecting to Accumulo with the
     *          given username and password fails
     */
    protected void setupAccumuloOutput(Job job, String outputTable) throws AccumuloSecurityException {
        AccumuloOutputFormat.setConnectorInfo(job, userName, new PasswordToken(pwd));
        AccumuloOutputFormat.setCreateTables(job, true);
        AccumuloOutputFormat.setDefaultTableName(job, outputTable);
        if (mock) {
            AccumuloOutputFormat.setMockInstance(job, instance);
        } else {
            ClientConfiguration clientConfig = ClientConfiguration.loadDefault()
                    .withInstance(instance).withZkHosts(zk);
            AccumuloOutputFormat.setZooKeeperInstance(job, clientConfig);
        }
        job.setOutputFormatClass(AccumuloOutputFormat.class);
    }

    /**
     * Sets up Rya output for a job: allows the job to write
     * {@link RyaStatementWritable} data, which will in turn be input into the
     * configured Rya instance. To perform secondary indexing, use the
     * configuration variables in {@link ConfigUtils}.
     * @param   job Job to configure
     * @throws  AccumuloSecurityException if connecting to Accumulo with the
     *          given username and password fails
     */
    protected void setupRyaOutput(Job job) throws AccumuloSecurityException {
        job.setOutputFormatClass(RyaOutputFormat.class);
        job.setOutputValueClass(RyaStatementWritable.class);
        // Specify default visibility of output rows, if given
        RyaOutputFormat.setDefaultVisibility(job, conf.get(MRUtils.AC_CV_PROP));
        // Specify named graph, if given
        RyaOutputFormat.setDefaultContext(job, conf.get(MRUtils.NAMED_GRAPH_PROP));
        // Set the output prefix
        RyaOutputFormat.setTablePrefix(job, tablePrefix);
        // Determine which indexers to use based on the config
        RyaOutputFormat.setFreeTextEnabled(job,  ConfigUtils.getUseFreeText(conf));
        RyaOutputFormat.setGeoEnabled(job,  ConfigUtils.getUseGeo(conf));
        RyaOutputFormat.setTemporalEnabled(job,  ConfigUtils.getUseTemporal(conf));
        RyaOutputFormat.setEntityEnabled(job,  ConfigUtils.getUseEntity(conf));
        // Configure the Accumulo connection
        AccumuloOutputFormat.setConnectorInfo(job, userName, new PasswordToken(pwd));
        AccumuloOutputFormat.setCreateTables(job, true);
        AccumuloOutputFormat.setDefaultTableName(job, tablePrefix + RdfCloudTripleStoreConstants.TBL_SPO_SUFFIX);
        if (mock) {
            RyaOutputFormat.setMockInstance(job, instance);
        } else {
            ClientConfiguration clientConfig = ClientConfiguration.loadDefault()
                    .withInstance(instance).withZkHosts(zk);
            AccumuloOutputFormat.setZooKeeperInstance(job, clientConfig);
        }
    }

    /**
     * Connects to Accumulo, using the stored connection parameters.
     * @return  A Connector to an Accumulo instance, which could be a mock
     *          instance.
     * @throws AccumuloException if connecting to Accumulo fails.
     * @throws AccumuloSecurityException if authenticating with Accumulo fails.
     */
    protected Connector getConnector() throws AccumuloSecurityException, AccumuloException {
        Instance zooKeeperInstance;
        if (mock) {
            zooKeeperInstance = new MockInstance(instance);
        }
        else {
            zooKeeperInstance = new ZooKeeperInstance(instance, zk);
        }
        return zooKeeperInstance.getConnector(userName, new PasswordToken(pwd));
    }
}
