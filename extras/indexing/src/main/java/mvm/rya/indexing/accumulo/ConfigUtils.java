package mvm.rya.indexing.accumulo;

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



import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.MultiTableBatchWriter;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.lang.Validate;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.log4j.Logger;
import org.openrdf.model.URI;
import org.openrdf.model.impl.URIImpl;

import com.google.common.collect.Lists;

import mvm.rya.accumulo.AccumuloRdfConfiguration;
import mvm.rya.api.RdfCloudTripleStoreConfiguration;
import mvm.rya.indexing.FilterFunctionOptimizer;
import mvm.rya.indexing.accumulo.entity.EntityCentricIndex;
import mvm.rya.indexing.accumulo.entity.EntityOptimizer;
import mvm.rya.indexing.accumulo.freetext.AccumuloFreeTextIndexer;
import mvm.rya.indexing.accumulo.freetext.LuceneTokenizer;
import mvm.rya.indexing.accumulo.freetext.Tokenizer;
import mvm.rya.indexing.accumulo.geo.GeoMesaGeoIndexer;
import mvm.rya.indexing.accumulo.temporal.AccumuloTemporalIndexer;
import mvm.rya.indexing.external.PrecompJoinOptimizer;
import mvm.rya.indexing.mongodb.MongoFreeTextIndexer;
import mvm.rya.indexing.mongodb.MongoGeoIndexer;

/**
 * A set of configuration utils to read a Hadoop {@link Configuration} object and create Cloudbase/Accumulo objects.
 */
public class ConfigUtils {
    private static final Logger logger = Logger.getLogger(ConfigUtils.class);

    public static final String CLOUDBASE_TBL_PREFIX = "sc.cloudbase.tableprefix";
    public static final String CLOUDBASE_AUTHS = "sc.cloudbase.authorizations";
    public static final String CLOUDBASE_INSTANCE = "sc.cloudbase.instancename";
    public static final String CLOUDBASE_ZOOKEEPERS = "sc.cloudbase.zookeepers";
    public static final String CLOUDBASE_USER = "sc.cloudbase.username";
    public static final String CLOUDBASE_PASSWORD = "sc.cloudbase.password";

    public static final String CLOUDBASE_WRITER_MAX_WRITE_THREADS = "sc.cloudbase.writer.maxwritethreads";
    public static final String CLOUDBASE_WRITER_MAX_LATENCY = "sc.cloudbase.writer.maxlatency";
    public static final String CLOUDBASE_WRITER_MAX_MEMORY = "sc.cloudbase.writer.maxmemory";

    public static final String FREE_TEXT_QUERY_TERM_LIMIT = "sc.freetext.querytermlimit";

    public static final String FREE_TEXT_DOC_TABLENAME = "sc.freetext.doctable";
    public static final String FREE_TEXT_TERM_TABLENAME = "sc.freetext.termtable";
    public static final String GEO_TABLENAME = "sc.geo.table";
    public static final String GEO_NUM_PARTITIONS = "sc.geo.numPartitions";
    public static final String TEMPORAL_TABLENAME = "sc.temporal.index";
    public static final String ENTITY_TABLENAME = "sc.entity.index";

    public static final String USE_GEO = "sc.use_geo";
    public static final String USE_FREETEXT = "sc.use_freetext";
    public static final String USE_TEMPORAL = "sc.use_temporal";
    public static final String USE_ENTITY = "sc.use_entity";
    public static final String USE_PCJ = "sc.use_pcj";
    public static final String USE_OPTIMAL_PCJ = "sc.use.optimal.pcj";

    public static final String USE_INDEXING_SAIL = "sc.use.indexing.sail";
    public static final String USE_EXTERNAL_SAIL = "sc.use.external.sail";

    public static final String USE_MOCK_INSTANCE = ".useMockInstance";

    public static final String NUM_PARTITIONS = "sc.cloudbase.numPartitions";

    private static final int WRITER_MAX_WRITE_THREADS = 1;
    private static final long WRITER_MAX_LATNECY = Long.MAX_VALUE;
    private static final long WRITER_MAX_MEMORY = 10000L;

    public static final String DISPLAY_QUERY_PLAN = "query.printqueryplan";

    public static final String FREETEXT_PREDICATES_LIST = "sc.freetext.predicates";
    public static final String FREETEXT_DOC_NUM_PARTITIONS = "sc.freetext.numPartitions.text";
    public static final String FREETEXT_TERM_NUM_PARTITIONS = "sc.freetext.numPartitions.term";

    public static final String TOKENIZER_CLASS = "sc.freetext.tokenizer.class";

    public static final String GEO_PREDICATES_LIST = "sc.geo.predicates";

    public static final String TEMPORAL_PREDICATES_LIST = "sc.temporal.predicates";

    public static final String USE_MONGO = "sc.useMongo";

    public static boolean isDisplayQueryPlan(final Configuration conf){
        return conf.getBoolean(DISPLAY_QUERY_PLAN, false);
    }

    /**
     * get a value from the configuration file and throw an exception if the value does not exist.
     *
     * @param conf
     * @param key
     * @return
     */
    private static String getStringCheckSet(final Configuration conf, final String key) {
        final String value = conf.get(key);
        Validate.notNull(value, key + " not set");
        return value;
    }

    /**
     * @param conf
     * @param tablename
     * @return if the table was created
     * @throws AccumuloException
     * @throws AccumuloSecurityException
     * @throws TableExistsException
     */
    public static boolean createTableIfNotExists(final Configuration conf, final String tablename) throws AccumuloException, AccumuloSecurityException,
            TableExistsException {
        final TableOperations tops = getConnector(conf).tableOperations();
        if (!tops.exists(tablename)) {
            logger.info("Creating table: " + tablename);
            tops.create(tablename);
            return true;
        }
        return false;
    }

    private static String getIndexTableName(final Configuration conf, final String indexTableNameConf, final String altSuffix){
        String value = conf.get(indexTableNameConf);
        if (value == null){
            final String defaultTableName = conf.get(RdfCloudTripleStoreConfiguration.CONF_TBL_PREFIX);
            Validate.notNull(defaultTableName, indexTableNameConf + " not set and " + RdfCloudTripleStoreConfiguration.CONF_TBL_PREFIX + " not set.  Cannot generate table name.");
            value = conf.get(RdfCloudTripleStoreConfiguration.CONF_TBL_PREFIX) + altSuffix;
        }
        return value;
    }

    public static String getFreeTextDocTablename(final Configuration conf) {
        return getIndexTableName(conf, FREE_TEXT_DOC_TABLENAME, "freetext");
    }

    public static String getFreeTextTermTablename(final Configuration conf) {
        return getIndexTableName(conf, FREE_TEXT_TERM_TABLENAME, "freetext_term");
    }

    public static int getFreeTextTermLimit(final Configuration conf) {
        return conf.getInt(FREE_TEXT_QUERY_TERM_LIMIT, 100);
    }

    public static String getGeoTablename(final Configuration conf) {
        return getIndexTableName(conf, GEO_TABLENAME, "geo");
    }

    public static String getTemporalTableName(final Configuration conf) {
        return getIndexTableName(conf, TEMPORAL_TABLENAME, "temporal");
    }


    public static String getEntityTableName(final Configuration conf) {
        return getIndexTableName(conf, ENTITY_TABLENAME, "entity");
    }


    public static Set<URI> getFreeTextPredicates(final Configuration conf) {
        return getPredicates(conf, FREETEXT_PREDICATES_LIST);
    }

    public static Set<URI> getGeoPredicates(final Configuration conf) {
        return getPredicates(conf, GEO_PREDICATES_LIST);
    }
    /**
     * Used for indexing statements about date & time instances and intervals.
     * @param conf
     * @return Set of predicate URI's whose objects should be date time literals.
     */
    public static Set<URI> getTemporalPredicates(final Configuration conf) {
        return getPredicates(conf, TEMPORAL_PREDICATES_LIST);
    }

    private static Set<URI> getPredicates(final Configuration conf, final String confName) {
        final String[] validPredicateStrings = conf.getStrings(confName, new String[] {});
        final Set<URI> predicates = new HashSet<URI>();
        for (final String prediateString : validPredicateStrings) {
            predicates.add(new URIImpl(prediateString));
        }
        return predicates;
    }

    public static Tokenizer getFreeTextTokenizer(final Configuration conf) {
        final Class<? extends Tokenizer> c = conf.getClass(TOKENIZER_CLASS, LuceneTokenizer.class, Tokenizer.class);
        return ReflectionUtils.newInstance(c, conf);
    }

    public static BatchWriter createDefaultBatchWriter(final String tablename, final Configuration conf) throws TableNotFoundException,
            AccumuloException, AccumuloSecurityException {
        final Long DEFAULT_MAX_MEMORY = getWriterMaxMemory(conf);
        final Long DEFAULT_MAX_LATENCY = getWriterMaxLatency(conf);
        final Integer DEFAULT_MAX_WRITE_THREADS = getWriterMaxWriteThreads(conf);
        final Connector connector = ConfigUtils.getConnector(conf);
        return connector.createBatchWriter(tablename, DEFAULT_MAX_MEMORY, DEFAULT_MAX_LATENCY, DEFAULT_MAX_WRITE_THREADS);
    }

    public static MultiTableBatchWriter createMultitableBatchWriter(final Configuration conf) throws AccumuloException, AccumuloSecurityException {
        final Long DEFAULT_MAX_MEMORY = getWriterMaxMemory(conf);
        final Long DEFAULT_MAX_LATENCY = getWriterMaxLatency(conf);
        final Integer DEFAULT_MAX_WRITE_THREADS = getWriterMaxWriteThreads(conf);
        final Connector connector = ConfigUtils.getConnector(conf);
        return connector.createMultiTableBatchWriter(DEFAULT_MAX_MEMORY, DEFAULT_MAX_LATENCY, DEFAULT_MAX_WRITE_THREADS);
    }

    public static Scanner createScanner(final String tablename, final Configuration conf) throws AccumuloException, AccumuloSecurityException,
            TableNotFoundException {
        final Connector connector = ConfigUtils.getConnector(conf);
        final Authorizations auths = ConfigUtils.getAuthorizations(conf);
        return connector.createScanner(tablename, auths);

    }

	public static BatchScanner createBatchScanner(final String tablename, final Configuration conf) throws AccumuloException, AccumuloSecurityException,
			TableNotFoundException {
		final Connector connector = ConfigUtils.getConnector(conf);
		final Authorizations auths = ConfigUtils.getAuthorizations(conf);
		Integer numThreads = null;
		if (conf instanceof RdfCloudTripleStoreConfiguration)
			numThreads = ((RdfCloudTripleStoreConfiguration) conf).getNumThreads();
		else
			numThreads = conf.getInt(RdfCloudTripleStoreConfiguration.CONF_NUM_THREADS, 2);
		return connector.createBatchScanner(tablename, auths, numThreads);
	}

    public static int getWriterMaxWriteThreads(final Configuration conf) {
        return conf.getInt(CLOUDBASE_WRITER_MAX_WRITE_THREADS, WRITER_MAX_WRITE_THREADS);
    }

    public static long getWriterMaxLatency(final Configuration conf) {
        return conf.getLong(CLOUDBASE_WRITER_MAX_LATENCY, WRITER_MAX_LATNECY);
    }

    public static long getWriterMaxMemory(final Configuration conf) {
        return conf.getLong(CLOUDBASE_WRITER_MAX_MEMORY, WRITER_MAX_MEMORY);
    }

    public static String getUsername(final JobContext job) {
        return getUsername(job.getConfiguration());
    }

    public static String getUsername(final Configuration conf) {
        return conf.get(CLOUDBASE_USER);
    }

    public static Authorizations getAuthorizations(final JobContext job) {
        return getAuthorizations(job.getConfiguration());
    }

    public static Authorizations getAuthorizations(final Configuration conf) {
        final String authString = conf.get(CLOUDBASE_AUTHS, "");
        if (authString.isEmpty()) {
            return new Authorizations();
        }
        return new Authorizations(authString.split(","));
    }

    public static Instance getInstance(final JobContext job) {
        return getInstance(job.getConfiguration());
    }

    public static Instance getInstance(final Configuration conf) {
        if (useMockInstance(conf)) {
            return new MockInstance(conf.get(CLOUDBASE_INSTANCE));
        }
        return new ZooKeeperInstance(conf.get(CLOUDBASE_INSTANCE), conf.get(CLOUDBASE_ZOOKEEPERS));
    }

    public static String getPassword(final JobContext job) {
        return getPassword(job.getConfiguration());
    }

    public static String getPassword(final Configuration conf) {
        return conf.get(CLOUDBASE_PASSWORD, "");
    }

    public static Connector getConnector(final JobContext job) throws AccumuloException, AccumuloSecurityException {
        return getConnector(job.getConfiguration());
    }

    public static Connector getConnector(final Configuration conf) throws AccumuloException, AccumuloSecurityException {
        final Instance instance = ConfigUtils.getInstance(conf);

        return instance.getConnector(getUsername(conf), getPassword(conf));
    }

    public static boolean useMockInstance(final Configuration conf) {
        return conf.getBoolean(USE_MOCK_INSTANCE, false);
    }

    private static int getNumPartitions(final Configuration conf) {
        return conf.getInt(NUM_PARTITIONS, 25);
    }

    public static int getFreeTextDocNumPartitions(final Configuration conf) {
        return conf.getInt(FREETEXT_DOC_NUM_PARTITIONS, getNumPartitions(conf));
    }

    public static int getFreeTextTermNumPartitions(final Configuration conf) {
        return conf.getInt(FREETEXT_TERM_NUM_PARTITIONS, getNumPartitions(conf));
    }

    public static int getGeoNumPartitions(final Configuration conf) {
        return conf.getInt(GEO_NUM_PARTITIONS, getNumPartitions(conf));
    }

    public static boolean getUseGeo(final Configuration conf) {
        return conf.getBoolean(USE_GEO, false);
    }

    public static boolean getUseFreeText(final Configuration conf) {
        return conf.getBoolean(USE_FREETEXT, false);
    }

    public static boolean getUseTemporal(final Configuration conf) {
        return conf.getBoolean(USE_TEMPORAL, false);
    }

    public static boolean getUseEntity(final Configuration conf) {
        return conf.getBoolean(USE_ENTITY, false);
    }

    public static boolean getUsePCJ(final Configuration conf) {
        return conf.getBoolean(USE_PCJ, false);
    }

    public static boolean getUseOptimalPCJ(final Configuration conf) {
        return conf.getBoolean(USE_OPTIMAL_PCJ, false);
    }

    public static boolean getUseMongo(final Configuration conf) {
        return conf.getBoolean(USE_MONGO, false);
    }


    public static void setIndexers(final RdfCloudTripleStoreConfiguration conf) {

        final List<String> indexList = Lists.newArrayList();
        final List<String> optimizers = Lists.newArrayList();

        boolean useFilterIndex = false;

        if (ConfigUtils.getUseMongo(conf)) {
            if (getUseGeo(conf)) {
                indexList.add(MongoGeoIndexer.class.getName());
                useFilterIndex = true;
            }
            if (getUseFreeText(conf)) {
                indexList.add(MongoFreeTextIndexer.class.getName());
                useFilterIndex = true;
            }
        } else {

            if (getUsePCJ(conf) || getUseOptimalPCJ(conf)) {
                conf.setPcjOptimizer(PrecompJoinOptimizer.class);
            }

            if (getUseGeo(conf)) {
                indexList.add(GeoMesaGeoIndexer.class.getName());
                useFilterIndex = true;
            }

            if (getUseFreeText(conf)) {
                indexList.add(AccumuloFreeTextIndexer.class.getName());
                useFilterIndex = true;
            }

            if (getUseTemporal(conf)) {
                indexList.add(AccumuloTemporalIndexer.class.getName());
                useFilterIndex = true;
            }

        }

        if (useFilterIndex) {
            optimizers.add(FilterFunctionOptimizer.class.getName());
        }

        if (getUseEntity(conf)) {
            indexList.add(EntityCentricIndex.class.getName());
            optimizers.add(EntityOptimizer.class.getName());

        }

        conf.setStrings(AccumuloRdfConfiguration.CONF_ADDITIONAL_INDEXERS, indexList.toArray(new String[]{}));
        conf.setStrings(AccumuloRdfConfiguration.CONF_OPTIMIZERS, optimizers.toArray(new String[]{}));
    }
}