package org.apache.rya.indexing.accumulo;

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

import static java.util.Objects.requireNonNull;

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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.log4j.Logger;
import org.openrdf.model.URI;
import org.openrdf.model.impl.URIImpl;

import com.google.common.base.Optional;
import com.google.common.collect.Lists;

import org.apache.rya.accumulo.AccumuloRdfConfiguration;
import org.apache.rya.api.RdfCloudTripleStoreConfiguration;
import org.apache.rya.api.instance.RyaDetails;
import org.apache.rya.indexing.FilterFunctionOptimizer;
import org.apache.rya.indexing.accumulo.entity.EntityCentricIndex;
import org.apache.rya.indexing.accumulo.entity.EntityOptimizer;
import org.apache.rya.indexing.accumulo.freetext.AccumuloFreeTextIndexer;
import org.apache.rya.indexing.accumulo.freetext.LuceneTokenizer;
import org.apache.rya.indexing.accumulo.freetext.Tokenizer;
import org.apache.rya.indexing.accumulo.temporal.AccumuloTemporalIndexer;
import org.apache.rya.indexing.external.PrecomputedJoinIndexer;
import org.apache.rya.indexing.mongodb.freetext.MongoFreeTextIndexer;
import org.apache.rya.indexing.pcj.matching.PCJOptimizer;

/**
 * A set of configuration utils to read a Hadoop {@link Configuration} object and create Cloudbase/Accumulo objects.
 * Soon will deprecate this class.  Use installer for the set methods, use {@link RyaDetails} for the get methods. 
 * New code must separate parameters that are set at Rya install time from that which is specific to the client.
 * Also Accumulo index tables are pushed down to the implementation and not configured in conf.   
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

    public static final String USE_FREETEXT = "sc.use_freetext";
    public static final String USE_TEMPORAL = "sc.use_temporal";
    public static final String USE_ENTITY = "sc.use_entity";
    public static final String USE_PCJ = "sc.use_pcj";
    public static final String USE_OPTIMAL_PCJ = "sc.use.optimal.pcj";
    public static final String USE_PCJ_UPDATER_INDEX = "sc.use.updater";

    public static final String FLUO_APP_NAME = "rya.indexing.pcj.fluo.fluoAppName";
    public static final String USE_PCJ_FLUO_UPDATER = "rya.indexing.pcj.updater.fluo";
    public static final String PCJ_STORAGE_TYPE = "rya.indexing.pcj.storageType";
    public static final String PCJ_UPDATER_TYPE = "rya.indexing.pcj.updaterType";


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
        requireNonNull(value, key + " not set");
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

    /**
     * Lookup the table name prefix in the conf and throw an error if it is null.
     * Future,  get table prefix from RyaDetails -- the Rya instance name
     *  -- also getting info from the RyaDetails should happen within RyaSailFactory and not ConfigUtils.
     * @param conf  Rya configuration map where it extracts the prefix (instance name)
     * @return  index table prefix corresponding to this Rya instance
     */
    public static String getTablePrefix(final Configuration conf) {
        final String tablePrefix;
        tablePrefix = conf.get(RdfCloudTripleStoreConfiguration.CONF_TBL_PREFIX);
        requireNonNull(tablePrefix, "Configuration key: " + RdfCloudTripleStoreConfiguration.CONF_TBL_PREFIX
                + " not set.  Cannot generate table name.");
        return tablePrefix;
    }

    public static int getFreeTextTermLimit(final Configuration conf) {
        return conf.getInt(FREE_TEXT_QUERY_TERM_LIMIT, 100);
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

    protected static Set<URI> getPredicates(final Configuration conf, final String confName) {
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
		if (conf instanceof RdfCloudTripleStoreConfiguration) {
			numThreads = ((RdfCloudTripleStoreConfiguration) conf).getNumThreads();
		} else {
			numThreads = conf.getInt(RdfCloudTripleStoreConfiguration.CONF_NUM_THREADS, 2);
		}
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

    protected static int getNumPartitions(final Configuration conf) {
        return conf.getInt(NUM_PARTITIONS, 25);
    }

    public static int getFreeTextDocNumPartitions(final Configuration conf) {
        return conf.getInt(FREETEXT_DOC_NUM_PARTITIONS, getNumPartitions(conf));
    }

    public static int getFreeTextTermNumPartitions(final Configuration conf) {
        return conf.getInt(FREETEXT_TERM_NUM_PARTITIONS, getNumPartitions(conf));
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
    
    public static boolean getUsePcjUpdaterIndex(final Configuration conf) {
        return conf.getBoolean(USE_PCJ_UPDATER_INDEX, false);
    }


    /**
     * @return The name of the Fluo Application this instance of RYA is
     *   using to incrementally update PCJs.
     */
    //TODO delete this eventually and use Details table
    public Optional<String> getFluoAppName(Configuration conf) {
        return Optional.fromNullable(conf.get(FLUO_APP_NAME));
    }


    public static boolean getUseMongo(final Configuration conf) {
        return conf.getBoolean(USE_MONGO, false);
    }


    public static void setIndexers(final RdfCloudTripleStoreConfiguration conf) {

        final List<String> indexList = Lists.newArrayList();
        final List<String> optimizers = Lists.newArrayList();

        boolean useFilterIndex = false;

        if (ConfigUtils.getUseMongo(conf)) {
             if (getUseFreeText(conf)) {
                indexList.add(MongoFreeTextIndexer.class.getName());
                useFilterIndex = true;
            }
        } else {

        	 if (getUsePCJ(conf) || getUseOptimalPCJ(conf)) {
                 conf.setPcjOptimizer(PCJOptimizer.class);
             }
             
             if(getUsePcjUpdaterIndex(conf)) {
             	indexList.add(PrecomputedJoinIndexer.class.getName());
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
        conf.setStrings(RdfCloudTripleStoreConfiguration.CONF_OPTIMIZERS, optimizers.toArray(new String[]{}));

    }



}
