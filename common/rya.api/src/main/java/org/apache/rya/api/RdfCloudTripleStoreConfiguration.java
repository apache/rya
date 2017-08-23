package org.apache.rya.api;

import java.util.HashSet;

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



import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.rya.api.domain.RyaURI;
import org.apache.rya.api.layout.TableLayoutStrategy;
import org.apache.rya.api.layout.TablePrefixLayoutStrategy;
import org.apache.rya.api.persist.RdfEvalStatsDAO;
import org.openrdf.query.algebra.evaluation.QueryOptimizer;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * Rdf triple store specific configuration
 */
public abstract class RdfCloudTripleStoreConfiguration extends Configuration {

    //    public static final String CONF_ISQUERYTIMEBASED = "query.timebased";
    public static final String CONF_TTL = "query.ttl";
    public static final String CONF_STARTTIME = "query.startTime";
    //    public static final String CONF_TIMEINDEXURIS = "query.timeindexuris";
    public static final String CONF_NUM_THREADS = "query.numthreads";
    public static final String CONF_PERFORMANT = "query.performant";
    public static final String CONF_INFER = "query.infer";
    public static final String CONF_USE_STATS = "query.usestats";
    public static final String CONF_USE_COMPOSITE = "query.usecompositecard";
    public static final String CONF_USE_SELECTIVITY = "query.useselectivity";
    public static final String CONF_TBL_PREFIX = "query.tblprefix";
    public static final String CONF_BATCH_SIZE = "query.batchsize";
    public static final String CONF_OFFSET = "query.offset";
    public static final String CONF_LIMIT = "query.limit";
    public static final String CONF_QUERYPLAN_FLAG = "query.printqueryplan";
    public static final String CONF_QUERY_AUTH = "query.auth";
    public static final String CONF_RESULT_FORMAT = "query.resultformat";
    public static final String CONF_CV = "conf.cv";
    public static final String CONF_TBL_SPO = "tbl.spo";
    public static final String CONF_TBL_PO = "tbl.po";
    public static final String CONF_TBL_OSP = "tbl.osp";
    public static final String CONF_TBL_NS = "tbl.ns";
    public static final String CONF_TBL_EVAL = "tbl.eval";
    public static final String CONF_PREFIX_ROW_WITH_HASH = "tbl.hashprefix";
    public static final String CONF_OPTIMIZERS = "query.optimizers";
    public static final String CONF_PCJ_OPTIMIZER = "pcj.query.optimizer";
    public static final String CONF_PCJ_TABLES = "pcj.index.tables";
    public static final String CONF_STATEMENT_METADATA_PROPERTIES = "statement.metadata.properites";
    public static final String CONF_USE_STATEMENT_METADATA = "use.statement.metadata";
    public static final String STATS_PUSH_EMPTY_RDFTYPE_DOWN = "conf.stats.rdftype.down";
    public static final String INFER_INCLUDE_INTERSECTION_OF = "infer.include.intersectionof";
    public static final String INFER_INCLUDE_INVERSEOF = "infer.include.inverseof";
    public static final String INFER_INCLUDE_ONE_OF = "infer.include.oneof";
    public static final String INFER_INCLUDE_SUBCLASSOF = "infer.include.subclassof";
    public static final String INFER_INCLUDE_SUBPROPOF = "infer.include.subpropof";
    public static final String INFER_INCLUDE_SYMMPROP = "infer.include.symmprop";
    public static final String INFER_INCLUDE_TRANSITIVEPROP = "infer.include.transprop";

    public static final String RDF_DAO_CLASS = "class.rdf.dao";
    public static final String RDF_EVAL_STATS_DAO_CLASS = "class.rdf.evalstats";

    public static final String REGEX_SUBJECT = "query.regex.subject";
    public static final String REGEX_PREDICATE = "query.regex.predicate";
    public static final String REGEX_OBJECT = "query.regex.object";
    private static final String[] EMPTY_STR_ARR = new String[0];

    private TableLayoutStrategy tableLayoutStrategy = new TablePrefixLayoutStrategy();

    public RdfCloudTripleStoreConfiguration() {
    }

    public RdfCloudTripleStoreConfiguration(final Configuration other) {
        super(other);
        if (other instanceof RdfCloudTripleStoreConfiguration) {
            setTableLayoutStrategy(((RdfCloudTripleStoreConfiguration) other).getTableLayoutStrategy());
        }
    }

    @Override
    public abstract RdfCloudTripleStoreConfiguration clone();

    public TableLayoutStrategy getTableLayoutStrategy() {
        return tableLayoutStrategy;
    }

    public void setTableLayoutStrategy(final TableLayoutStrategy tableLayoutStrategy) {
        if (tableLayoutStrategy != null) {
            this.tableLayoutStrategy = tableLayoutStrategy;
        } else {
            this.tableLayoutStrategy = new TablePrefixLayoutStrategy(); //default
        }
        set(CONF_TBL_SPO, this.tableLayoutStrategy.getSpo());
        set(CONF_TBL_PO, this.tableLayoutStrategy.getPo());
        set(CONF_TBL_OSP, this.tableLayoutStrategy.getOsp());
        set(CONF_TBL_NS, this.tableLayoutStrategy.getNs());
        set(CONF_TBL_EVAL, this.tableLayoutStrategy.getEval());
    }

    public Long getTtl() {
        final String val = get(CONF_TTL);
        if (val != null) {
            return Long.valueOf(val);
        }
        return null;
    }

    public void setTtl(final Long ttl) {
        Preconditions.checkNotNull(ttl);
        Preconditions.checkArgument(ttl >= 0, "ttl must be non negative");
        set(CONF_TTL, ttl.toString());
    }

    public Long getStartTime() {
        final String val = get(CONF_STARTTIME);
        if (val != null) {
            return Long.valueOf(val);
        }
        return null;
    }

    public void setStartTime(final Long startTime) {
        Preconditions.checkNotNull(startTime);
        Preconditions.checkArgument(startTime >= 0, "startTime must be non negative");
        set(CONF_STARTTIME, startTime.toString());
    }

    public Integer getNumThreads() {
        return getInt(CONF_NUM_THREADS, 2);
    }

    public void setNumThreads(final Integer numThreads) {
        Preconditions.checkNotNull(numThreads);
        Preconditions.checkArgument(numThreads > 0, "numThreads must be greater than 0");
        setInt(CONF_NUM_THREADS, numThreads);
    }

    public Boolean isPerformant() {
        return getBoolean(CONF_PERFORMANT, true);
    }

    public void setPerformant(final Boolean val) {
        Preconditions.checkNotNull(val);
        setBoolean(CONF_PERFORMANT, val);
    }

    public Boolean isInfer() {
        return getBoolean(CONF_INFER, false);
    }

    public void setInfer(final Boolean val) {
        Preconditions.checkNotNull(val);
        setBoolean(CONF_INFER, val);
    }

    public Boolean isUseStats() {
        return getBoolean(CONF_USE_STATS, false);
    }

    public void setUseStats(final Boolean val) {
        Preconditions.checkNotNull(val);
        setBoolean(CONF_USE_STATS, val);
    }

    public Boolean isUseSelectivity() {
        return getBoolean(CONF_USE_SELECTIVITY, false);
    }

    public void setUseSelectivity(final Boolean val) {
        Preconditions.checkNotNull(val);
        setBoolean(CONF_USE_SELECTIVITY, val);
    }

    public Boolean isPrefixRowsWithHash() {
        return getBoolean(CONF_PREFIX_ROW_WITH_HASH, false);
    }

    public void setPrefixRowsWithHash(final Boolean val) {
        Preconditions.checkNotNull(val);
        setBoolean(CONF_PREFIX_ROW_WITH_HASH, val);
    }

    public String getTablePrefix() {
        return get(CONF_TBL_PREFIX, RdfCloudTripleStoreConstants.TBL_PRFX_DEF);
    }

    public void setTablePrefix(final String tablePrefix) {
        Preconditions.checkNotNull(tablePrefix);
        set(CONF_TBL_PREFIX, tablePrefix);
        setTableLayoutStrategy(new TablePrefixLayoutStrategy(tablePrefix)); //TODO: Should we change the layout strategy
    }

    public Integer getBatchSize() {
        final String val = get(CONF_BATCH_SIZE);
        if (val != null) {
            return Integer.valueOf(val);
        }
        return null;
    }

    public void setBatchSize(final Long batchSize) {
        Preconditions.checkNotNull(batchSize);
        Preconditions.checkArgument(batchSize > 0, "Batch Size must be greater than 0");
        setLong(CONF_BATCH_SIZE, batchSize);
    }

    public Long getOffset() {
        final String val = get(CONF_OFFSET);
        if (val != null) {
            return Long.valueOf(val);
        }
        return null;
    }

    public void setOffset(final Long offset) {
        Preconditions.checkNotNull(offset);
        Preconditions.checkArgument(offset >= 0, "offset must be positive");
        setLong(CONF_OFFSET, offset);
    }

    public Long getLimit() {
        final String val = get(CONF_LIMIT);
        if (val != null) {
            return Long.valueOf(val);
        }
        return null;
    }

    public void setLimit(final Long limit) {
        Preconditions.checkNotNull(limit);
        Preconditions.checkArgument(limit >= 0, "limit must be positive");
        setLong(CONF_LIMIT, limit);
    }


    public Boolean isDisplayQueryPlan() {
        return getBoolean(CONF_QUERYPLAN_FLAG, false);
    }

    public void setDisplayQueryPlan(final Boolean val) {
        Preconditions.checkNotNull(val);
        setBoolean(CONF_QUERYPLAN_FLAG, val);
    }

    /**
     * @return
     * @deprecated
     */
    @Deprecated
    public String getAuth() {
        return Joiner.on(",").join(getAuths());
    }

    /**
     * @param auth
     * @deprecated
     */
    @Deprecated
    public void setAuth(final String auth) {
        Preconditions.checkNotNull(auth);
        setStrings(CONF_QUERY_AUTH, auth);
    }

    public String[] getAuths() {
        return getStrings(CONF_QUERY_AUTH, EMPTY_STR_ARR);
    }

    public void setAuths(final String... auths) {
        Preconditions.checkNotNull(auths);
        setStrings(CONF_QUERY_AUTH, auths);
    }

    public String getEmit() {
        return get(CONF_RESULT_FORMAT);
    }

    public void setEmit(final String emit) {
        Preconditions.checkNotNull(emit);
        set(CONF_RESULT_FORMAT, emit);
    }

    public String getCv() {
        return get(CONF_CV);
    }

    public void setCv(final String cv) {
        Preconditions.checkNotNull(cv);
        set(CONF_CV, cv);
    }


    public Boolean isUseCompositeCardinality() {
        return getBoolean(CONF_USE_COMPOSITE, true);
    }

    public void setCompositeCardinality(final Boolean val) {
        Preconditions.checkNotNull(val);
        setBoolean(CONF_USE_COMPOSITE, val);
    }


    public Boolean isStatsPushEmptyRdftypeDown() {
        return getBoolean(STATS_PUSH_EMPTY_RDFTYPE_DOWN, true);
    }

    public void setStatsPushEmptyRdftypeDown(final Boolean val) {
        Preconditions.checkNotNull(val);
        setBoolean(STATS_PUSH_EMPTY_RDFTYPE_DOWN, val);
    }

    /**
     * @return {@code true} if owl:intersectionOf inferencing is enabled.
     * {@code false} otherwise. Defaults to {@code true} if nothing is
     * specified.
     */
    public Boolean isInferIntersectionOf() {
        return getBoolean(INFER_INCLUDE_INTERSECTION_OF, true);
    }

    /**
     * Sets whether owl:intersectionOf inferencing is enabled or disabled.
     * @param value {@code true} if owl:intersectionOf inferencing is enabled.
     * {@code false} otherwise.
     */
    public void setInferIntersectionOf(final Boolean value) {
        Preconditions.checkNotNull(value);
        setBoolean(INFER_INCLUDE_INTERSECTION_OF, value);
    }

    public Boolean isInferInverseOf() {
        return getBoolean(INFER_INCLUDE_INVERSEOF, true);
    }

    public void setInferInverseOf(final Boolean val) {
        Preconditions.checkNotNull(val);
        setBoolean(INFER_INCLUDE_INVERSEOF, val);
    }

    /**
     * @return {@code true} if owl:oneOf inferencing is enabled.
     * {@code false} otherwise. Defaults to {@code true} if nothing is
     * specified.
     */
    public Boolean isInferOneOf() {
        return getBoolean(INFER_INCLUDE_ONE_OF, true);
    }

    /**
     * Sets whether owl:oneOf inferencing is enabled or disabled.
     * @param value {@code true} if owl:oneOf inferencing is enabled.
     * {@code false} otherwise.
     */
    public void setInferOneOf(final Boolean value) {
        Preconditions.checkNotNull(value);
        setBoolean(INFER_INCLUDE_ONE_OF, value);
    }

    public Boolean isInferSubClassOf() {
        return getBoolean(INFER_INCLUDE_SUBCLASSOF, true);
    }

    public void setInferSubClassOf(final Boolean val) {
        Preconditions.checkNotNull(val);
        setBoolean(INFER_INCLUDE_SUBCLASSOF, val);
    }

    public Boolean isInferSubPropertyOf() {
        return getBoolean(INFER_INCLUDE_SUBPROPOF, true);
    }

    public void setInferSubPropertyOf(final Boolean val) {
        Preconditions.checkNotNull(val);
        setBoolean(INFER_INCLUDE_SUBPROPOF, val);
    }

    public Boolean isInferSymmetricProperty() {
        return getBoolean(INFER_INCLUDE_SYMMPROP, true);
    }

    public void setInferSymmetricProperty(final Boolean val) {
        Preconditions.checkNotNull(val);
        setBoolean(INFER_INCLUDE_SYMMPROP, val);
    }

    public Boolean isInferTransitiveProperty() {
        return getBoolean(INFER_INCLUDE_TRANSITIVEPROP, true);
    }

    public void setInferTransitiveProperty(final Boolean val) {
        Preconditions.checkNotNull(val);
        setBoolean(INFER_INCLUDE_TRANSITIVEPROP, val);
    }

    public void setRdfEvalStatsDaoClass(final Class<? extends RdfEvalStatsDAO> rdfEvalStatsDaoClass) {
        Preconditions.checkNotNull(rdfEvalStatsDaoClass);
        setClass(RDF_EVAL_STATS_DAO_CLASS, rdfEvalStatsDaoClass, RdfEvalStatsDAO.class);
    }

    public Class<? extends RdfEvalStatsDAO> getRdfEvalStatsDaoClass() {
        return getClass(RDF_EVAL_STATS_DAO_CLASS, null, RdfEvalStatsDAO.class);
    }


    public void setPcjTables(final List<String> indexTables) {
        Preconditions.checkNotNull(indexTables);
        setStrings(CONF_PCJ_TABLES, indexTables.toArray(new String[]{}));
    }


    public List<String> getPcjTables() {
        final List<String> pcjTables = Lists.newArrayList();
        final String[] tables = getStrings(CONF_PCJ_TABLES);
        if(tables == null) {
            return pcjTables;
        }
        for(final String table: tables) {
            Preconditions.checkNotNull(table);
            pcjTables.add(table);
        }
        return pcjTables;
    }

    public void setUseStatementMetadata(final boolean useMetadata) {
        setBoolean(CONF_USE_STATEMENT_METADATA, useMetadata);
    }

    public boolean getUseStatementMetadata() {
        return getBoolean(CONF_USE_STATEMENT_METADATA, false);
    }

    public void setStatementMetadataProperties(final Set<RyaURI> metadataProperties) {

        final String[] propArray = new String[metadataProperties.size()];
        int i = 0;
        for(final RyaURI uri: metadataProperties) {
            propArray[i] = uri.getData();
            i++;
        }
        setStrings(CONF_STATEMENT_METADATA_PROPERTIES, propArray);
    }


    public Set<RyaURI> getStatementMetadataProperties() {
        final Set<RyaURI> uriSet = new HashSet<>();
        final String[] uriStrings = getStrings(CONF_STATEMENT_METADATA_PROPERTIES);
        if (uriStrings != null) {
            for (final String s : uriStrings) {
                uriSet.add(new RyaURI(s));
            }
        }
        return uriSet;
    }


    public void setPcjOptimizer(final Class<? extends QueryOptimizer> optimizer) {
        Preconditions.checkNotNull(optimizer);
        setClass(CONF_PCJ_OPTIMIZER, optimizer, QueryOptimizer.class);
    }

    public Class<QueryOptimizer> getPcjOptimizer() {
        final Class<? extends QueryOptimizer> opt = getClass(CONF_PCJ_OPTIMIZER, null, QueryOptimizer.class);
        if (opt != null) {
            Preconditions.checkArgument(QueryOptimizer.class.isAssignableFrom(opt));
            return (Class<QueryOptimizer>) opt;
        } else {
            return null;
        }
    }


    public void setOptimizers(final List<Class<? extends QueryOptimizer>> optimizers) {
        Preconditions.checkNotNull(optimizers);
        final List<String> strs = Lists.newArrayList();
        for (final Class ai : optimizers){
            Preconditions.checkNotNull(ai);
            strs.add(ai.getName());
        }

        setStrings(CONF_OPTIMIZERS, strs.toArray(new String[]{}));
    }

    public List<Class<QueryOptimizer>> getOptimizers() {
        final List<Class<QueryOptimizer>> opts = Lists.newArrayList();
        for (final Class<?> clazz : getClasses(CONF_OPTIMIZERS)){
            Preconditions.checkArgument(QueryOptimizer.class.isAssignableFrom(clazz));
            opts.add((Class<QueryOptimizer>) clazz);
        }

        return opts;
    }



    public String getRegexSubject() {
        return get(REGEX_SUBJECT);
    }

    public void setRegexSubject(final String regexSubject) {
        Preconditions.checkNotNull(regexSubject);
        set(REGEX_SUBJECT, regexSubject);
    }

    public String getRegexPredicate() {
        return get(REGEX_PREDICATE);
    }

    public void setRegexPredicate(final String regex) {
        Preconditions.checkNotNull(regex);
        set(REGEX_PREDICATE, regex);
    }

    public String getRegexObject() {
        return get(REGEX_OBJECT);
    }

    public void setRegexObject(final String regex) {
        Preconditions.checkNotNull(regex);
        set(REGEX_OBJECT, regex);
    }
}
