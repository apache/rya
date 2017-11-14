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
package org.apache.rya.indexing.accumulo.geo;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.util.Objects.requireNonNull;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;
import org.apache.rya.accumulo.experimental.AbstractAccumuloIndexer;
import org.apache.rya.api.RdfCloudTripleStoreConfiguration;
import org.apache.rya.api.domain.RyaStatement;
import org.apache.rya.api.resolver.RyaToRdfConversions;
import org.apache.rya.indexing.GeoIndexer;
import org.apache.rya.indexing.Md5Hash;
import org.apache.rya.indexing.StatementConstraints;
import org.apache.rya.indexing.StatementSerializer;
import org.apache.rya.indexing.accumulo.ConfigUtils;
import org.apache.rya.indexing.accumulo.geo.GeoTupleSet.GeoSearchFunctionFactory.NearQuery;
import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.geotools.data.DataStore;
import org.geotools.data.DataUtilities;
import org.geotools.data.FeatureSource;
import org.geotools.data.FeatureStore;
import org.geotools.factory.CommonFactoryFinder;
import org.geotools.factory.Hints;
import org.geotools.feature.DefaultFeatureCollection;
import org.geotools.feature.SchemaException;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.filter.text.cql2.CQLException;
import org.geotools.filter.text.ecql.ECQL;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.filter.Filter;
import org.opengis.filter.FilterFactory;
import org.opengis.filter.identity.Identifier;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.io.ParseException;

import mil.nga.giat.geowave.adapter.vector.FeatureDataAdapter;
import mil.nga.giat.geowave.adapter.vector.plugin.GeoWaveGTDataStore;
import mil.nga.giat.geowave.adapter.vector.plugin.GeoWaveGTDataStoreFactory;
import mil.nga.giat.geowave.adapter.vector.plugin.GeoWavePluginException;
import mil.nga.giat.geowave.adapter.vector.query.cql.CQLQuery;
import mil.nga.giat.geowave.core.geotime.ingest.SpatialDimensionalityTypeProvider;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.StoreFactoryFamilySpi;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.memory.MemoryStoreFactoryFamily;
import mil.nga.giat.geowave.core.store.query.EverythingQuery;
import mil.nga.giat.geowave.core.store.query.QueryOptions;
import mil.nga.giat.geowave.datastore.accumulo.AccumuloDataStore;
import mil.nga.giat.geowave.datastore.accumulo.AccumuloStoreFactoryFamily;

/**
 * A {@link GeoIndexer} wrapper around a GeoWave {@link AccumuloDataStore}. This class configures and connects to the Datastore, creates the
 * RDF Feature Type, and interacts with the Datastore.
 * <p>
 * Specifically, this class creates a RDF Feature type and stores each RDF Statement as a RDF Feature in the datastore. Each feature
 * contains the standard set of GeoWave attributes (Geometry, Start Date, and End Date). The GeoWaveGeoIndexer populates the Geometry
 * attribute by parsing the Well-Known Text contained in the RDF Statement’s object literal value.
 * <p>
 * The RDF Feature contains four additional attributes for each component of the RDF Statement. These attributes are:
 * <p>
 * <table border="1">
 * <tr>
 * <th>Name</th>
 * <th>Symbol</th>
 * <th>Type</th>
 * </tr>
 * <tr>
 * <td>Subject Attribute</td>
 * <td>S</td>
 * <td>String</td>
 * </tr>
 * </tr>
 * <tr>
 * <td>Predicate Attribute</td>
 * <td>P</td>
 * <td>String</td>
 * </tr>
 * </tr>
 * <tr>
 * <td>Object Attribute</td>
 * <td>O</td>
 * <td>String</td>
 * </tr>
 * </tr>
 * <tr>
 * <td>Context Attribute</td>
 * <td>C</td>
 * <td>String</td>
 * </tr>
 * </table>
 */
public class GeoWaveGeoIndexer extends AbstractAccumuloIndexer implements GeoIndexer  {

    private static final String TABLE_SUFFIX = "geo";

    private static final Logger logger = Logger.getLogger(GeoWaveGeoIndexer.class);

    private static final String FEATURE_NAME = "RDF";

    private static final String SUBJECT_ATTRIBUTE = "S";
    private static final String PREDICATE_ATTRIBUTE = "P";
    private static final String OBJECT_ATTRIBUTE = "O";
    private static final String CONTEXT_ATTRIBUTE = "C";
    private static final String GEO_ID_ATTRIBUTE = "geo_id";
    private static final String GEOMETRY_ATTRIBUTE = "geowave_index_geometry";

    private Set<IRI> validPredicates;
    private Configuration conf;
    private FeatureStore<SimpleFeatureType, SimpleFeature> featureStore;
    private FeatureSource<SimpleFeatureType, SimpleFeature> featureSource;
    private SimpleFeatureType featureType;
    private FeatureDataAdapter featureDataAdapter;
    private DataStore geoToolsDataStore;
    private mil.nga.giat.geowave.core.store.DataStore geoWaveDataStore;
    private final PrimaryIndex index = new SpatialDimensionalityTypeProvider().createPrimaryIndex();
    private boolean isInit = false;

    //initialization occurs in setConf because index is created using reflection
    @Override
    public void setConf(final Configuration conf) {
        this.conf = conf;
        if (!isInit) {
            try {
                initInternal();
                isInit = true;
            } catch (final IOException e) {
                logger.warn("Unable to initialize index.  Throwing Runtime Exception. ", e);
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public Configuration getConf() {
        return conf;
    }

    /**
     * @return the internal GeoTools{@link DataStore} used by the {@link GeoWaveGeoIndexer}.
     */
    public DataStore getGeoToolsDataStore() {
        return geoToolsDataStore;
    }

    /**
     * @return the internal GeoWave {@link DataStore} used by the {@link GeoWaveGeoIndexer}.
     */
    public mil.nga.giat.geowave.core.store.DataStore getGeoWaveDataStore() {
        return geoWaveDataStore;
    }

    private void initInternal() throws IOException {
        validPredicates = ConfigUtils.getGeoPredicates(conf);

        try {
            geoToolsDataStore = createDataStore(conf);
            geoWaveDataStore = ((GeoWaveGTDataStore) geoToolsDataStore).getDataStore();
        } catch (final GeoWavePluginException e) {
            logger.error("Failed to create GeoWave data store", e);
        }

        try {
            featureType = getStatementFeatureType(geoToolsDataStore);
        } catch (final IOException | SchemaException e) {
            throw new IOException(e);
        }

        featureDataAdapter = new FeatureDataAdapter(featureType);

        featureSource = geoToolsDataStore.getFeatureSource(featureType.getName());
        if (!(featureSource instanceof FeatureStore)) {
            throw new IllegalStateException("Could not retrieve feature store");
        }
        featureStore = (FeatureStore<SimpleFeatureType, SimpleFeature>) featureSource;
    }

    public Map<String, Serializable> getParams(final Configuration conf) {
        // get the configuration parameters
        final Instance instance = ConfigUtils.getInstance(conf);
        final String instanceId = instance.getInstanceName();
        final String zookeepers = instance.getZooKeepers();
        final String user = ConfigUtils.getUsername(conf);
        final String password = ConfigUtils.getPassword(conf);
        final String auths = ConfigUtils.getAuthorizations(conf).toString();
        final String tableName = getTableName(conf);
        final String tablePrefix = ConfigUtils.getTablePrefix(conf);

        final Map<String, Serializable> params = new HashMap<>();
        params.put("zookeeper", zookeepers);
        params.put("instance", instanceId);
        params.put("user", user);
        params.put("password", password);
        params.put("namespace", tableName);
        params.put("gwNamespace", tablePrefix + getClass().getSimpleName());

        params.put("Lock Management", LockManagementType.MEMORY.toString());
        params.put("Authorization Management Provider", AuthorizationManagementProviderType.EMPTY.toString());
        params.put("Authorization Data URL", null);
        params.put("Transaction Buffer Size", 10000);
        params.put("Query Index Strategy", QueryIndexStrategyType.HEURISTIC_MATCH.toString());
        return params;
    }

    /**
     * Creates the {@link DataStore} for the {@link GeoWaveGeoIndexer}.
     * @param conf the {@link Configuration}.
     * @return the {@link DataStore}.
     */
    public DataStore createDataStore(final Configuration conf) throws IOException, GeoWavePluginException {
        final Map<String, Serializable> params = getParams(conf);
        final Instance instance = ConfigUtils.getInstance(conf);
        final boolean useMock = instance instanceof MockInstance;

        final StoreFactoryFamilySpi storeFactoryFamily;
        if (useMock) {
            storeFactoryFamily = new MemoryStoreFactoryFamily();
        } else {
            storeFactoryFamily = new AccumuloStoreFactoryFamily();
        }

        final GeoWaveGTDataStoreFactory geoWaveGTDataStoreFactory = new GeoWaveGTDataStoreFactory(storeFactoryFamily);
        final DataStore dataStore = geoWaveGTDataStoreFactory.createNewDataStore(params);

        return dataStore;
    }

    private static SimpleFeatureType getStatementFeatureType(final DataStore dataStore) throws IOException, SchemaException {
        SimpleFeatureType featureType;

        final String[] datastoreFeatures = dataStore.getTypeNames();
        if (Arrays.asList(datastoreFeatures).contains(FEATURE_NAME)) {
            featureType = dataStore.getSchema(FEATURE_NAME);
        } else {
            featureType = DataUtilities.createType(FEATURE_NAME,
                SUBJECT_ATTRIBUTE + ":String," +
                PREDICATE_ATTRIBUTE + ":String," +
                OBJECT_ATTRIBUTE + ":String," +
                CONTEXT_ATTRIBUTE + ":String," +
                GEOMETRY_ATTRIBUTE + ":Geometry:srid=4326," +
                GEO_ID_ATTRIBUTE + ":String");

            dataStore.createSchema(featureType);
        }
        return featureType;
    }

    @Override
    public void storeStatements(final Collection<RyaStatement> ryaStatements) throws IOException {
        // create a feature collection
        final DefaultFeatureCollection featureCollection = new DefaultFeatureCollection();
        for (final RyaStatement ryaStatement : ryaStatements) {
            final Statement statement = RyaToRdfConversions.convertStatement(ryaStatement);
            // if the predicate list is empty, accept all predicates.
            // Otherwise, make sure the predicate is on the "valid" list
            final boolean isValidPredicate = validPredicates.isEmpty() || validPredicates.contains(statement.getPredicate());

            if (isValidPredicate && (statement.getObject() instanceof Literal)) {
                try {
                    final SimpleFeature feature = createFeature(featureType, statement);
                    featureCollection.add(feature);
                } catch (final ParseException e) {
                    logger.warn("Error getting geo from statement: " + statement.toString(), e);
                }
            }
        }

        // write this feature collection to the store
        if (!featureCollection.isEmpty()) {
            featureStore.addFeatures(featureCollection);
        }
    }

    @Override
    public void storeStatement(final RyaStatement statement) throws IOException {
        storeStatements(Collections.singleton(statement));
    }

    private static SimpleFeature createFeature(final SimpleFeatureType featureType, final Statement statement) throws ParseException {
        final String subject = StatementSerializer.writeSubject(statement);
        final String predicate = StatementSerializer.writePredicate(statement);
        final String object = StatementSerializer.writeObject(statement);
        final String context = StatementSerializer.writeContext(statement);

        // create the feature
        final Object[] noValues = {};

        // create the hash
        final String statementId = Md5Hash.md5Base64(StatementSerializer.writeStatement(statement));
        final SimpleFeature newFeature = SimpleFeatureBuilder.build(featureType, noValues, statementId);

        // write the statement data to the fields
        final Geometry geom = GeoParseUtils.getGeometry(statement, new GmlParser());
        if(geom == null || geom.isEmpty() || !geom.isValid()) {
            throw new ParseException("Could not create geometry for statement " + statement);
        }
        newFeature.setDefaultGeometry(geom);

        newFeature.setAttribute(SUBJECT_ATTRIBUTE, subject);
        newFeature.setAttribute(PREDICATE_ATTRIBUTE, predicate);
        newFeature.setAttribute(OBJECT_ATTRIBUTE, object);
        newFeature.setAttribute(CONTEXT_ATTRIBUTE, context);
        // GeoWave does not support querying based on a user generated feature ID
        // So, we create a separate ID attribute that it can query on.
        newFeature.setAttribute(GEO_ID_ATTRIBUTE, statementId);

        // preserve the ID that we created for this feature
        // (set the hint to FALSE to have GeoTools generate IDs)
        newFeature.getUserData().put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE);

        return newFeature;
    }

    private CloseableIteration<Statement, QueryEvaluationException> performQuery(final String type, final Geometry geometry,
            final StatementConstraints contraints) {
        final List<String> filterParms = new ArrayList<String>();

        filterParms.add(type + "(" + GEOMETRY_ATTRIBUTE + ", " + geometry + " )");

        if (contraints.hasSubject()) {
            filterParms.add("( " + SUBJECT_ATTRIBUTE + "= '" + contraints.getSubject() + "') ");
        }
        if (contraints.hasContext()) {
            filterParms.add("( " + CONTEXT_ATTRIBUTE + "= '" + contraints.getContext() + "') ");
        }
        if (contraints.hasPredicates()) {
            final List<String> predicates = new ArrayList<String>();
            for (final IRI u : contraints.getPredicates()) {
                predicates.add("( " + PREDICATE_ATTRIBUTE + "= '" + u.stringValue() + "') ");
            }
            filterParms.add("(" + StringUtils.join(predicates, " OR ") + ")");
        }

        final String filterString = StringUtils.join(filterParms, " AND ");
        logger.info("Performing geowave query : " + filterString);

        return getIteratorWrapper(filterString);
    }

    private CloseableIteration<Statement, QueryEvaluationException> getIteratorWrapper(final String filterString) {

        return new CloseableIteration<Statement, QueryEvaluationException>() {

            private CloseableIterator<SimpleFeature> featureIterator = null;

            CloseableIterator<SimpleFeature> getIterator() throws QueryEvaluationException {
                if (featureIterator == null) {
                    Filter cqlFilter;
                    try {
                        cqlFilter = ECQL.toFilter(filterString);
                    } catch (final CQLException e) {
                        logger.error("Error parsing query: " + filterString, e);
                        throw new QueryEvaluationException(e);
                    }

                    final CQLQuery cqlQuery = new CQLQuery(null, cqlFilter, featureDataAdapter);
                    final QueryOptions queryOptions = new QueryOptions(featureDataAdapter, index);

                    try {
                        featureIterator = geoWaveDataStore.query(queryOptions, cqlQuery);
                    } catch (final Exception e) {
                        logger.error("Error performing query: " + filterString, e);
                        throw new QueryEvaluationException(e);
                    }
                }
                return featureIterator;
            }

            @Override
            public boolean hasNext() throws QueryEvaluationException {
                return getIterator().hasNext();
            }

            @Override
            public Statement next() throws QueryEvaluationException {
                final SimpleFeature feature = getIterator().next();
                final String subjectString = feature.getAttribute(SUBJECT_ATTRIBUTE).toString();
                final String predicateString = feature.getAttribute(PREDICATE_ATTRIBUTE).toString();
                final String objectString = feature.getAttribute(OBJECT_ATTRIBUTE).toString();
                final Object context = feature.getAttribute(CONTEXT_ATTRIBUTE);
                final String contextString = context != null ? context.toString() : "";
                final Statement statement = StatementSerializer.readStatement(subjectString, predicateString, objectString, contextString);
                return statement;
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException("Remove not implemented");
            }

            @Override
            public void close() throws QueryEvaluationException {
                try {
                    getIterator().close();
                } catch (final IOException e) {
                    throw new QueryEvaluationException(e);
                }
            }
        };
    }

    @Override
    public CloseableIteration<Statement, QueryEvaluationException> queryEquals(final Geometry query, final StatementConstraints contraints) {
        return performQuery("EQUALS", query, contraints);
    }

    @Override
    public CloseableIteration<Statement, QueryEvaluationException> queryDisjoint(final Geometry query, final StatementConstraints contraints) {
        return performQuery("DISJOINT", query, contraints);
    }

    @Override
    public CloseableIteration<Statement, QueryEvaluationException> queryIntersects(final Geometry query, final StatementConstraints contraints) {
        return performQuery("INTERSECTS", query, contraints);
    }

    @Override
    public CloseableIteration<Statement, QueryEvaluationException> queryTouches(final Geometry query, final StatementConstraints contraints) {
        return performQuery("TOUCHES", query, contraints);
    }

    @Override
    public CloseableIteration<Statement, QueryEvaluationException> queryCrosses(final Geometry query, final StatementConstraints contraints) {
        return performQuery("CROSSES", query, contraints);
    }

    @Override
    public CloseableIteration<Statement, QueryEvaluationException> queryWithin(final Geometry query, final StatementConstraints contraints) {
        return performQuery("WITHIN", query, contraints);
    }

    @Override
    public CloseableIteration<Statement, QueryEvaluationException> queryContains(final Geometry query, final StatementConstraints contraints) {
        return performQuery("CONTAINS", query, contraints);
    }

    @Override
    public CloseableIteration<Statement, QueryEvaluationException> queryOverlaps(final Geometry query, final StatementConstraints contraints) {
        return performQuery("OVERLAPS", query, contraints);
    }
    
    @Override
    public CloseableIteration<Statement, QueryEvaluationException> queryNear(final NearQuery query,
            final StatementConstraints contraints) {
        throw new UnsupportedOperationException("Near queries are not supported in Accumulo.");
    }

    @Override
    public Set<IRI> getIndexablePredicates() {
        return validPredicates;
    }

    @Override
    public void flush() throws IOException {
        // TODO cache and flush features instead of writing them one at a time
    }

    @Override
    public void close() throws IOException {
        flush();
    }


    @Override
    public String getTableName() {
        return getTableName(conf);
    }

    /**
     * Get the Accumulo table that will be used by this index.
     * @param conf
     * @return table name guaranteed to be used by instances of this index
     */
    public static String getTableName(final Configuration conf) {
        return makeTableName( ConfigUtils.getTablePrefix(conf) );
    }

    /**
     * Make the Accumulo table name used by this indexer for a specific instance of Rya.
     *
     * @param ryaInstanceName - The name of the Rya instance the table name is for. (not null)
     * @return The Accumulo table name used by this indexer for a specific instance of Rya.
     */
    public static String makeTableName(final String ryaInstanceName) {
        requireNonNull(ryaInstanceName);
        return ryaInstanceName + TABLE_SUFFIX;
    }

    private void deleteStatements(final Collection<RyaStatement> ryaStatements) throws IOException {
        // create a feature collection
        final DefaultFeatureCollection featureCollection = new DefaultFeatureCollection();

        for (final RyaStatement ryaStatement : ryaStatements) {
            final Statement statement = RyaToRdfConversions.convertStatement(ryaStatement);
            // if the predicate list is empty, accept all predicates.
            // Otherwise, make sure the predicate is on the "valid" list
            final boolean isValidPredicate = validPredicates.isEmpty() || validPredicates.contains(statement.getPredicate());

            if (isValidPredicate && (statement.getObject() instanceof Literal)) {
                try {
                    final SimpleFeature feature = createFeature(featureType, statement);
                    featureCollection.add(feature);
                } catch (final ParseException e) {
                    logger.warn("Error getting geo from statement: " + statement.toString(), e);
                }
            }
        }

        // remove this feature collection from the store
        if (!featureCollection.isEmpty()) {
            final Set<Identifier> featureIds = new HashSet<Identifier>();
            final FilterFactory filterFactory = CommonFactoryFinder.getFilterFactory(null);
            final Set<String> stringIds = DataUtilities.fidSet(featureCollection);
            for (final String id : stringIds) {
                featureIds.add(filterFactory.featureId(id));
            }

            final String filterString = stringIds.stream().collect(Collectors.joining("','", "'", "'"));

            Filter filter = null;
            try {
                filter = ECQL.toFilter(GEO_ID_ATTRIBUTE + " IN (" + filterString + ")", filterFactory);
            } catch (final CQLException e) {
                logger.error("Unable to generate filter for deleting the statement.", e);
            }

            featureStore.removeFeatures(filter);
        }
    }


    @Override
    public void deleteStatement(final RyaStatement statement) throws IOException {
        deleteStatements(Collections.singleton(statement));
    }

    @Override
    public void init() {
    }

    @Override
    public void setConnector(final Connector connector) {
    }

    @Override
    public void destroy() {
    }

    @Override
    public void purge(final RdfCloudTripleStoreConfiguration configuration) {
        // delete existing data
        geoWaveDataStore.delete(new QueryOptions(), new EverythingQuery());
    }

    @Override
    public void dropAndDestroy() {
    }

    /**
     * The list of supported Geo Wave {@code LockingManagementFactory} types.
     */
    private static enum LockManagementType {
        MEMORY("memory");

        private final String name;

        /**
         * Creates a new {@link LockManagementType}.
         * @param name the name of the type. (not {@code null})
         */
        private LockManagementType(final String name) {
            this.name = checkNotNull(name);
        }

        @Override
        public String toString() {
            return name;
        }
    }

    /**
     * The list of supported Geo Wave {@code AuthorizationFactorySPI } types.
     */
    private static enum AuthorizationManagementProviderType {
        EMPTY("empty"),
        JSON_FILE("jsonFile");

        private final String name;

        /**
         * Creates a new {@link AuthorizationManagementProviderType}.
         * @param name the name of the type. (not {@code null})
         */
        private AuthorizationManagementProviderType(final String name) {
            this.name = checkNotNull(name);
        }

        @Override
        public String toString() {
            return name;
        }
    }

    /**
     * The list of supported Geo Wave {@code IndexQueryStrategySPI} types.
     */
    private static enum QueryIndexStrategyType {
        BEST_MATCH("Best Match"),
        HEURISTIC_MATCH("Heuristic Match"),
        PRESERVE_LOCALITY("Preserve Locality");

        private final String name;

        /**
         * Creates a new {@link QueryIndexStrategyType}.
         * @param name the name of the type. (not {@code null})
         */
        private QueryIndexStrategyType(final String name) {
            this.name = checkNotNull(name);
        }

        @Override
        public String toString() {
            return name;
        }
    }
}
