package mvm.rya.indexing.accumulo.geo;

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

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;
import org.geotools.data.DataStore;
import org.geotools.data.DataStoreFinder;
import org.geotools.data.DataUtilities;
import org.geotools.data.FeatureSource;
import org.geotools.data.FeatureStore;
import org.geotools.data.Query;
import org.geotools.factory.CommonFactoryFinder;
import org.geotools.factory.Hints;
import org.geotools.feature.DefaultFeatureCollection;
import org.geotools.feature.FeatureIterator;
import org.geotools.feature.SchemaException;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.filter.text.cql2.CQLException;
import org.geotools.filter.text.ecql.ECQL;
import org.locationtech.geomesa.accumulo.data.AccumuloDataStore;
import org.locationtech.geomesa.accumulo.index.Constants;
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.filter.Filter;
import org.opengis.filter.FilterFactory;
import org.opengis.filter.identity.Identifier;
import org.openrdf.model.Literal;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.query.QueryEvaluationException;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKTReader;

import info.aduna.iteration.CloseableIteration;
import mvm.rya.accumulo.experimental.AbstractAccumuloIndexer;
import mvm.rya.api.RdfCloudTripleStoreConfiguration;
import mvm.rya.api.domain.RyaStatement;
import mvm.rya.api.resolver.RyaToRdfConversions;
import mvm.rya.indexing.GeoIndexer;
import mvm.rya.indexing.Md5Hash;
import mvm.rya.indexing.StatementConstraints;
import mvm.rya.indexing.StatementSerializer;
import mvm.rya.indexing.accumulo.ConfigUtils;

/**
 * A {@link GeoIndexer} wrapper around a GeoMesa {@link AccumuloDataStore}. This class configures and connects to the Datastore, creates the
 * RDF Feature Type, and interacts with the Datastore.
 * <p>
 * Specifically, this class creates a RDF Feature type and stores each RDF Statement as a RDF Feature in the datastore. Each feature
 * contains the standard set of GeoMesa attributes (Geometry, Start Date, and End Date). The GeoMesaGeoIndexer populates the Geometry
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
public class GeoMesaGeoIndexer extends AbstractAccumuloIndexer implements GeoIndexer  {

    private static final String TABLE_SUFFIX = "geo";

    private static final Logger logger = Logger.getLogger(GeoMesaGeoIndexer.class);

    private static final String FEATURE_NAME = "RDF";

    private static final String SUBJECT_ATTRIBUTE = "S";
    private static final String PREDICATE_ATTRIBUTE = "P";
    private static final String OBJECT_ATTRIBUTE = "O";
    private static final String CONTEXT_ATTRIBUTE = "C";

    private Set<URI> validPredicates;
    private Configuration conf;
    private FeatureStore<SimpleFeatureType, SimpleFeature> featureStore;
    private FeatureSource<SimpleFeatureType, SimpleFeature> featureSource;
    private SimpleFeatureType featureType;
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


    private void initInternal() throws IOException {
        validPredicates = ConfigUtils.getGeoPredicates(conf);

        final DataStore dataStore = createDataStore(conf);

        try {
            featureType = getStatementFeatureType(dataStore);
        } catch (final IOException e) {
            throw new IOException(e);
        } catch (final SchemaException e) {
            throw new IOException(e);
        }

        featureSource = dataStore.getFeatureSource(featureType.getName());
        if (!(featureSource instanceof FeatureStore)) {
            throw new IllegalStateException("Could not retrieve feature store");
        }
        featureStore = (FeatureStore<SimpleFeatureType, SimpleFeature>) featureSource;
    }

    private static DataStore createDataStore(final Configuration conf) throws IOException {
        // get the configuration parameters
        final Instance instance = ConfigUtils.getInstance(conf);
        final boolean useMock = instance instanceof MockInstance;
        final String instanceId = instance.getInstanceName();
        final String zookeepers = instance.getZooKeepers();
        final String user = ConfigUtils.getUsername(conf);
        final String password = ConfigUtils.getPassword(conf);
        final String auths = ConfigUtils.getAuthorizations(conf).toString();
        final String tableName = getTableName(conf);
        final int numParitions = ConfigUtils.getGeoNumPartitions(conf);

        final String featureSchemaFormat = "%~#s%" + numParitions + "#r%" + FEATURE_NAME
                + "#cstr%0,3#gh%yyyyMMdd#d::%~#s%3,2#gh::%~#s%#id";
        // build the map of parameters
        final Map<String, Serializable> params = new HashMap<String, Serializable>();
        params.put("instanceId", instanceId);
        params.put("zookeepers", zookeepers);
        params.put("user", user);
        params.put("password", password);
        params.put("auths", auths);
        params.put("tableName", tableName);
        params.put("indexSchemaFormat", featureSchemaFormat);
        params.put("useMock", Boolean.toString(useMock));

        // fetch the data store from the finder
        return DataStoreFinder.getDataStore(params);
    }

    private static SimpleFeatureType getStatementFeatureType(final DataStore dataStore) throws IOException, SchemaException {
        SimpleFeatureType featureType;

        final String[] datastoreFeatures = dataStore.getTypeNames();
        if (Arrays.asList(datastoreFeatures).contains(FEATURE_NAME)) {
            featureType = dataStore.getSchema(FEATURE_NAME);
        } else {
            final String featureSchema = SUBJECT_ATTRIBUTE + ":String," //
                    + PREDICATE_ATTRIBUTE + ":String," //
                    + OBJECT_ATTRIBUTE + ":String," //
                    + CONTEXT_ATTRIBUTE + ":String," //
                    + Constants.SF_PROPERTY_GEOMETRY + ":Geometry:srid=4326";
            featureType = SimpleFeatureTypes.createType(FEATURE_NAME, featureSchema);
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
        final Geometry geom = (new WKTReader()).read(GeoParseUtils.getWellKnownText(statement));
        if(geom == null || geom.isEmpty() || !geom.isValid()) {
            throw new ParseException("Could not create geometry for statement " + statement);
        }
        newFeature.setDefaultGeometry(geom);

        newFeature.setAttribute(SUBJECT_ATTRIBUTE, subject);
        newFeature.setAttribute(PREDICATE_ATTRIBUTE, predicate);
        newFeature.setAttribute(OBJECT_ATTRIBUTE, object);
        newFeature.setAttribute(CONTEXT_ATTRIBUTE, context);

        // preserve the ID that we created for this feature
        // (set the hint to FALSE to have GeoTools generate IDs)
        newFeature.getUserData().put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE);

        return newFeature;
    }

    private CloseableIteration<Statement, QueryEvaluationException> performQuery(final String type, final Geometry geometry,
            final StatementConstraints contraints) {
        final List<String> filterParms = new ArrayList<String>();

        filterParms.add(type + "(" + Constants.SF_PROPERTY_GEOMETRY + ", " + geometry + " )");

        if (contraints.hasSubject()) {
            filterParms.add("( " + SUBJECT_ATTRIBUTE + "= '" + contraints.getSubject() + "') ");
        }
        if (contraints.hasContext()) {
            filterParms.add("( " + CONTEXT_ATTRIBUTE + "= '" + contraints.getContext() + "') ");
        }
        if (contraints.hasPredicates()) {
            final List<String> predicates = new ArrayList<String>();
            for (final URI u : contraints.getPredicates()) {
                predicates.add("( " + PREDICATE_ATTRIBUTE + "= '" + u.stringValue() + "') ");
            }
            filterParms.add("(" + StringUtils.join(predicates, " OR ") + ")");
        }

        final String filterString = StringUtils.join(filterParms, " AND ");
        logger.info("Performing geomesa query : " + filterString);

        return getIteratorWrapper(filterString);
    }

    private CloseableIteration<Statement, QueryEvaluationException> getIteratorWrapper(final String filterString) {

        return new CloseableIteration<Statement, QueryEvaluationException>() {

            private FeatureIterator<SimpleFeature> featureIterator = null;

            FeatureIterator<SimpleFeature> getIterator() throws QueryEvaluationException {
                if (featureIterator == null) {
                    Filter cqlFilter;
                    try {
                        cqlFilter = ECQL.toFilter(filterString);
                    } catch (final CQLException e) {
                        logger.error("Error parsing query: " + filterString, e);
                        throw new QueryEvaluationException(e);
                    }

                    final Query query = new Query(featureType.getTypeName(), cqlFilter);
                    try {
                        featureIterator = featureSource.getFeatures(query).features();
                    } catch (final IOException e) {
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
                final String contextString = feature.getAttribute(CONTEXT_ATTRIBUTE).toString();
                final Statement statement = StatementSerializer.readStatement(subjectString, predicateString, objectString, contextString);
                return statement;
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException("Remove not implemented");
            }

            @Override
            public void close() throws QueryEvaluationException {
                getIterator().close();
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
    public Set<URI> getIndexablePredicates() {
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
    public static String getTableName(Configuration conf) {
        return ConfigUtils.getTablePrefix(conf)  + TABLE_SUFFIX;
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
            final Filter filter = filterFactory.id(featureIds);
            featureStore.removeFeatures(filter);
        }
    }


    @Override
    public void deleteStatement(final RyaStatement statement) throws IOException {
        deleteStatements(Collections.singleton(statement));
    }

	@Override
	public void init() {
		// TODO Auto-generated method stub

	}

	@Override
	public void setConnector(final Connector connector) {
		// TODO Auto-generated method stub

	}

	@Override
	public void destroy() {
		// TODO Auto-generated method stub

	}

	@Override
	public void purge(final RdfCloudTripleStoreConfiguration configuration) {
		// TODO Auto-generated method stub

	}

	@Override
	public void dropAndDestroy() {
		// TODO Auto-generated method stub

	}
}
