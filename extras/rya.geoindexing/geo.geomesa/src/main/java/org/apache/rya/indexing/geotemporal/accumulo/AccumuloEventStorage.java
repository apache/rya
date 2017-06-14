/**
 *
 */
package org.apache.rya.indexing.geotemporal.accumulo;

import static java.util.Objects.requireNonNull;

import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Matcher;

import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.commons.lang.NotImplementedException;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;
import org.apache.rya.api.domain.RyaURI;
import org.apache.rya.indexing.GeoConstants;
import org.apache.rya.indexing.IndexingExpr;
import org.apache.rya.indexing.TemporalInstant;
import org.apache.rya.indexing.TemporalInstantRfc3339;
import org.apache.rya.indexing.TemporalInterval;
import org.apache.rya.indexing.accumulo.ConfigUtils;
import org.apache.rya.indexing.accumulo.geo.GeoParseUtils;
import org.apache.rya.indexing.accumulo.geo.GmlParser;
import org.apache.rya.indexing.geotemporal.model.Event;
import org.apache.rya.indexing.geotemporal.storage.EventStorage;
import org.geotools.data.DataStore;
import org.geotools.data.DataStoreFinder;
import org.geotools.data.DefaultTransaction;
import org.geotools.data.FeatureSource;
import org.geotools.data.FeatureStore;
import org.geotools.data.Query;
import org.geotools.data.Transaction;
import org.geotools.factory.CommonFactoryFinder;
import org.geotools.factory.GeoTools;
import org.geotools.factory.Hints;
import org.geotools.feature.DefaultFeatureCollection;
import org.geotools.feature.FeatureCollection;
import org.geotools.feature.FeatureIterator;
import org.geotools.feature.SchemaException;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.filter.Filters;
import org.geotools.filter.text.cql2.CQL;
import org.geotools.filter.text.cql2.CQLException;
import org.joda.time.DateTime;
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes;
import org.locationtech.geomesa.utils.interop.WKTUtils;
import org.opengis.feature.Feature;
import org.opengis.feature.FeatureVisitor;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.filter.Filter;
import org.opengis.filter.FilterFactory;
import org.opengis.filter.identity.FeatureId;
import org.openrdf.model.Literal;
import org.openrdf.model.URI;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.io.ParseException;

/**
 * Store and query Geo-temporal events using GeoMesa on Accumulo
 *
 */
public class AccumuloEventStorage implements EventStorage {

	public static final DateTime UNKNOWN_DATE = new DateTime(0);
	private static final String TEMPO_COUNT_ATTRIBUTE = "tempo_count";
	private static final String TRUE = "T";
	private static final String FALSE = "F";

	private static final Logger LOG = Logger.getLogger(AccumuloEventStorage.class);

	private static final String GEOMETRY_ATTRIBUTE = "geo";
	private static final String TEMPORAL_ATTRIBUTE = "instant";
	private static final String SUBJECT_ATTRIBUTE = "s";
	private static final String GEOUNKNOWN_ATTRIBUTE="geounknown";
	private static final String FEATURE_NAME = "RyaEvent";
	private static final String TABLE_SUFFIX = "event";

	private static final Geometry UNKNOWN_POINT=WKTUtils.read("POINT(0 0)");

	private FeatureStore<SimpleFeatureType, SimpleFeature> featureStore;
	private FeatureSource<SimpleFeatureType, SimpleFeature> featureSource;
	private SimpleFeatureType featureType;

	private boolean isInit = false;

	/**
	 * Construct with a working accumulo and rya name
	 */
	public AccumuloEventStorage() {
	}
     /** 
     * initialize.  This will check if its already initialized 
     */
    public void init(Configuration conf) {
        if (!isInit ) {
            try {
                initInternal(conf);
                isInit = true;
            } catch (final IOException e) {
                LOG.warn("Unable to initialize index.  Throwing Runtime Exception. ", e);
                throw new RuntimeException(e);
            }
        }
    }
	/**
	 * Setup configuration and find (SPI) the geomesa classes.
	 * 
	 * @throws IOException
	 */
	private void initInternal(Configuration conf) throws IOException {
		// validPredicates = ConfigUtils.getGeoPredicates(conf);
		Objects.requireNonNull(conf, "Configuration was not set before init().");
		
		// causes Geotools to use any datasource:
		//Hints.putSystemDefault(Hints.ENTITY_RESOLVER, NullEntityResolver.INSTANCE);
		
		final DataStore dataStore = createDataStore(conf);

		try {
			featureType = getStatementFeatureType(dataStore);
		} catch (final IOException | SchemaException e) {
			throw new IOException(e);
		}

		featureSource = dataStore.getFeatureSource(featureType.getName());
		if (!(featureSource instanceof FeatureStore)) {
			throw new IllegalStateException("Could not retrieve feature store");
		}
		featureStore = (FeatureStore<SimpleFeatureType, SimpleFeature>) featureSource;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.apache.rya.indexing.mongodb.update.RyaObjectStorage#create(java.lang.
	 * Object)
	 */
	@Override
	public void create(Event event)
			throws org.apache.rya.indexing.mongodb.update.RyaObjectStorage.ObjectAlreadyExistsException,
			org.apache.rya.indexing.mongodb.update.RyaObjectStorage.ObjectStorageException {
		requireNonNull(event, "the event is a required parameter");
		requireNonNull(featureType,"featureType is not initialized, Init() must be called first!");

		// create a feature collection
		final DefaultFeatureCollection featureCollection = new DefaultFeatureCollection();
		final SimpleFeature feature = createFeature(featureType, event);
		featureCollection.add(feature);
		if (get(event.getSubject()).isPresent())
			throw new EventAlreadyExistsException("Event already exists in index, use update.");

		// write this feature collection to the store
		if (!featureCollection.isEmpty()) {
			try {
				featureStore.addFeatures(featureCollection);
			} catch (final IOException e) {
				throw new ObjectStorageException("Error getting geo from event: " + event.toString(), e);
			}
		}
	}

	/**
	 * Convert from an Event to a event-Feature
	 */
	private static SimpleFeature createFeature(final SimpleFeatureType featureType, final Event event) {
		final String subject = event.getSubject().getData();
		// create the feature
		final Object[] noValues = {};

		// create the hash
		final String eventId = event.getUniqueHash();
		final SimpleFeature newFeature = SimpleFeatureBuilder.build(featureType, noValues, eventId);

		// write the statement data to the fields
		if (event.getGeometry().isPresent()) {
			newFeature.setDefaultGeometry(event.getGeometry().get());
			newFeature.setAttribute(GEOUNKNOWN_ATTRIBUTE, FALSE);
		} else {
			newFeature.setDefaultGeometry(UNKNOWN_POINT);
			newFeature.setAttribute(GEOUNKNOWN_ATTRIBUTE, TRUE);
		}
		newFeature.setAttribute(SUBJECT_ATTRIBUTE, subject);
		if (event.getInstant().isPresent()) {
			// TODO this conversion is suspect. Make sure TZ, milliseconds,
			// other stuff is preserved.
			newFeature.setAttribute(TEMPORAL_ATTRIBUTE, event.getInstant().get().getAsDateTime().toDate());
			newFeature.setAttribute(TEMPO_COUNT_ATTRIBUTE, new Integer(1));
			// TODO what to do with: event.getInterval()
		} else{ 
			newFeature.setAttribute(TEMPORAL_ATTRIBUTE, UNKNOWN_DATE );
			newFeature.setAttribute(TEMPO_COUNT_ATTRIBUTE, new Integer(0));
		}
			


		// preserve the ID that we created for this feature
		// (set the hint to FALSE to have GeoTools generate IDs)
		newFeature.getUserData().put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE);

		return newFeature;
	}

	/**
	 * Get an Object from the storage by its subject.
	 *
	 * @param subject
	 *            - Identifies which Object to get. (not null)
	 * @return The Object if one exists for the subject.
	 * @throws ObjectStorageException
	 *             A problem occurred while fetching the Object from the
	 *             storage.
	 */
	@Override
	public Optional<Event> get(RyaURI subject)
			throws org.apache.rya.indexing.mongodb.update.RyaObjectStorage.ObjectStorageException {
		requireNonNull(featureSource,"featureSource is not initialized, Init() must be called first!");
		requireNonNull(subject);
		// construct a (E)CQL filter from the search parameters,
		// and use that as the basis for the query
		Filter cqlFilter;
		cqlFilter = makeFilterOnSubject(subject);
		Query query = new Query(FEATURE_NAME, cqlFilter);

        // submit the query, and get back an iterator over matching features
        FeatureIterator<SimpleFeature> featureItr;
		try {
			featureItr = featureSource.getFeatures(query).features();
		} catch (IOException e) {
			throw new org.apache.rya.indexing.mongodb.update.RyaObjectStorage.ObjectStorageException("subject could not be converted to CQL, subject="+subject,e);
		}

        SimpleFeature feature = null;
        while (featureItr.hasNext()) {
        	feature = featureItr.next();
        	System.out.println("Feature found: "+feature);
        }
        
        final Event event = convertFeatureToEvent(feature);
		return event == null ? Optional.empty() : Optional.of(event);
	}
	/**
	 * @param feature
	 * @return
	 */
	private Event convertFeatureToEvent(SimpleFeature feature) {
		if (feature==null) 
			return null;
		Geometry geo = null;
		String unknownGeo = (String)feature.getAttribute(GEOUNKNOWN_ATTRIBUTE);
		if (unknownGeo==null || FALSE.equals(unknownGeo)) {
			geo = (Geometry)feature.getAttribute(GEOMETRY_ATTRIBUTE);
		}
		TemporalInstantRfc3339 instant;
		Integer tempoCount = (Integer)feature.getAttribute(TEMPO_COUNT_ATTRIBUTE);
		if (tempoCount==null || tempoCount == 1 ) {
			instant = new TemporalInstantRfc3339(new DateTime((java.util.Date)feature.getAttribute(TEMPORAL_ATTRIBUTE)));
		} else if (tempoCount == 2) {
			// TODO interval
			instant = null;
		} else {
			instant = null;
		}
		final Event event = (feature == null) ? null
				: Event.builder()
						.setSubject(new RyaURI(feature.getAttribute(SUBJECT_ATTRIBUTE).toString()))
						.setGeometry(geo)
        		.setTemporalInstant(instant)
        		.build();
		return event;
	}

	private String RyaURLToCQLString(RyaURI subject) throws CQLException {
		// TODO escape characters  to meet CQL syntax
//		Expression expr = CQL.toExpression("attName");
//		You can get the text again using:
//		CQL.toCQL( expr );

		return "'" + subject.getData() + "'";
	}
	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.apache.rya.indexing.mongodb.update.RyaObjectStorage#update(java.lang.
	 * Object, java.lang.Object)
	 */
	@Override
	public void update(Event old, Event updated)
			throws org.apache.rya.indexing.mongodb.update.RyaObjectStorage.StaleUpdateException,
			org.apache.rya.indexing.mongodb.update.RyaObjectStorage.ObjectStorageException {
		requireNonNull(old);
		requireNonNull(updated);

		// The updated entity must have the same Subject as the one it is replacing.
		if (!old.getSubject().equals(updated.getSubject())) {
			throw new EventStorageException(
					"The old Event and the updated Event must have the same Subject. " + "Old Subject: "
							+ old.getSubject().getData() + ", Updated Subject: " + updated.getSubject().getData());
		}
		
		// Lookup old in geomesa
		Optional<Event> event = get(old.getSubject()); 
		if (! event.isPresent()) {
			throw new StaleUpdateException(
					"Could not update missing old Event with Subject '" + old.getSubject().getData() + "'.");
		}
		// Delete the event with this subject, and create a new one.
		// TODO this should be a conditional and atomic update.  Implement locking and perhaps checking versions.
		delete(updated.getSubject());
		create(updated);
	}

	@Override
	public boolean delete(RyaURI subject)
			throws org.apache.rya.indexing.mongodb.update.RyaObjectStorage.ObjectStorageException {
		requireNonNull(subject, "the subject is a required parameter");
		requireNonNull(featureType,"featureType is not initialized, Init() must be called first!");
		int featuresRemoved = 0;
		// construct a filter from the search parameter: subject, and use that as the basis for the deletion
		Filter cqlFilter = makeFilterOnSubject(subject);
        Transaction transaction = new DefaultTransaction("removeBySubject");
		try {
			//featureStore.removeFeatures(cqlFilter);
	        final Set<FeatureId> removed = new HashSet<>();
	        FeatureCollection<SimpleFeatureType, SimpleFeature> collection = featureStore.getFeatures( new Query( FEATURE_NAME, cqlFilter, Query.NO_NAMES ));
	        collection.accepts( new FeatureVisitor() {
	            public void visit(Feature feature) {
	                removed.add( feature.getIdentifier() );
	            }
	        }, null );
	        featureStore.removeFeatures(cqlFilter);
	        transaction.commit();
	        transaction.close();
	        featuresRemoved = collection.size();
	        LOG.debug("Removed these Features: "+removed);
	    } catch (Exception eek) {
	        String msg = "";
			try {
				transaction.rollback();
		        transaction.close();
		    } catch (IOException e) {
				msg  = "IOException occured during rollback/close: "+e;
			}
			throw new org.apache.rya.indexing.mongodb.update.RyaObjectStorage.ObjectStorageException("Subject could not be deleted. "+msg+" -- subject="+subject,eek);
		}

		return featuresRemoved > 0; 
	}
	/**
	 * Given a subject, make a geotools geostore filter to select that subject.
	 * @param subject
	 * @return Filter that will match the subject.
	 * @throws ObjectStorageException
	 */
	private Filter makeFilterOnSubject(RyaURI subject) throws org.apache.rya.indexing.mongodb.update.RyaObjectStorage.ObjectStorageException {
		try {
			return CQL.toFilter(SUBJECT_ATTRIBUTE + " EQ " + RyaURLToCQLString(subject));
		} catch (CQLException e) {
			throw new org.apache.rya.indexing.mongodb.update.RyaObjectStorage.ObjectStorageException("subject could not be converted to CQL, subject="+subject,e);
		}
	}

	/**
	 * Search for {@link Event}s from the storage by its subject, geo filter functions and temporal functions. Will query
	 * based on present parameters.  IndexingExpr.contraints will be ignored!  The only constraint is subject, by using the Subject parameter.
	 *
	 * @param subject
	 *            - The subject key to find events.
	 * @param geoFilters
	 *            - The geo filters to find Events.
	 * @param temporalFilters
	 *            - The temporal filters to find Events.
	 * @return The {@link Event}, if one exists for the subject.
	 * @throws ObjectStorageException
	 *             A problem occurred while fetching the Entity from the
	 *             storage.
	 */
	@Override
	public Collection<Event> search(Optional<RyaURI> subject, Optional<Collection<IndexingExpr>> geoFilters,
			Optional<Collection<IndexingExpr>> temporalFilters)
			throws org.apache.rya.indexing.mongodb.update.RyaObjectStorage.ObjectStorageException {
		requireNonNull(subject, "the subject is a required parameter");
		requireNonNull(featureStore,"featureStore is not initialized, Init() must be called first!");
        final Collection<Event> found = new HashSet<Event>();
		// construct a filter from the search parameter: subject, and use that as the basis for the deletion
		Filter cqlFilter=null;;  //TODO remove null to let compiler verify assignment.
		//Collection<IndexingExpr> filtersAll = null; // new ArrayList<IndexingExpr>();
		FilterFactory ff = CommonFactoryFinder.getFilterFactory(GeoTools.getDefaultHints());
		if (subject.isPresent()) {
			cqlFilter = makeFilterOnSubject(subject.get());
		}
		try {
			if (geoFilters.isPresent()) {
				for (IndexingExpr geoIndexingExpr : geoFilters.get()) {
					String cql = makeCQLfromGeosparql(geoIndexingExpr);
					if (cqlFilter == null)
						cqlFilter = CQL.toFilter(cql);
					else
						cqlFilter = Filters.and(ff, cqlFilter, CQL.toFilter(cql));
				}
			}
			if (temporalFilters.isPresent()) {
				for (IndexingExpr tempoIndexingExpr : temporalFilters.get()) {
					String cql = makeCQLfromTemposparql(tempoIndexingExpr);
					if (cqlFilter == null)
						cqlFilter = CQL.toFilter(cql);
					else
						cqlFilter = Filters.and(ff, cqlFilter, CQL.toFilter(cql));
				} 
			}
		} catch (CQLException | ParseException e) {
			throw new ObjectStorageException(
					"Can't parse filter to CQL: geoFilters=" + geoFilters + ", temporalFilters=" + temporalFilters, e);
		}
		
        // submit the query, and get back an iterator over matching features
		try {
	        FeatureCollection<SimpleFeatureType, SimpleFeature> collection = featureStore.getFeatures( new Query( FEATURE_NAME, cqlFilter ) );
	        collection.accepts( new FeatureVisitor() {
	            public void visit(Feature feature) {
	            	boolean hasGeoFilter = geoFilters.isPresent() ;
	            	Event event = convertFeatureToEvent((SimpleFeature)feature);
	            	// For queries that have NO geo component, add it, otherwise add only features with geo component 
	            	if ( ! hasGeoFilter || event.getGeometry().isPresent() )
	            		//&& ( ! hasTempoFilter || event.getInstant().isPresent() ) //TODO remove unknown datetimes
	            	{
	            		found.add( event );
	            	}
	            	 

	            }
	        }, null );
	        LOG.debug("found these Features: "+found);
	    } catch (Exception eek) {
			throw new org.apache.rya.indexing.mongodb.update.RyaObjectStorage.ObjectStorageException("Subject could not be searched. -- subject="+subject,eek);
		}

		return found;
		 
	}
	
	private String makeCQLfromTemposparql(IndexingExpr sparqlIndexingExpression) {
		String EQUALS = "tag:rya-rdf.org,2015:temporal#equals";// TODO use strings from somewhere
    	if (sparqlIndexingExpression.getArguments().length > 1)
    		throw new Error("Multiple arguments.  Don't know what to do with them.") ; //TODO verify always 1 or handle more.
    	Object tempoExpr = sparqlIndexingExpression.getArguments()[0];
    	if (! (tempoExpr instanceof Literal)) {
    		throw new Error("Must be a literal.  Don't know what to do with this Value: tempoExpr="+tempoExpr) ; //TODO verify always Literal or handle more.
    	}
    	TemporalInstant instant;
    	TemporalInterval interval;
    	{
	    	String dateString = ((Literal)tempoExpr).stringValue();
			final Matcher matcher = TemporalInstantRfc3339.PATTERN.matcher(dateString);
	        if (matcher.find()) {
	            interval = TemporalInstantRfc3339.parseInterval(dateString);
	            instant=null;
	        } else {
	        	interval = null;
	        	DateTime dt = DateTime.parse(dateString);
	            instant = new TemporalInstantRfc3339(dt);
	        }
    	}
    	String cqlString;
    	
		String function = sparqlIndexingExpression.getFunction().toString();
		if (function .equals(EQUALS)) {  
			//String endDateString = (new TemporalInstantRfc3339(instant.getAsDateTime().plusMillis(1000)).getAsKeyString());
			//cqlString = "( " + TEMPORAL_ATTRIBUTE + "  DURING  " + instant.getAsKeyString() + "/" + endDateString + " )";
			//cqlString = "( " + TEMPORAL_ATTRIBUTE + "  DURING  " + instant.getAsKeyString() + "/T1S )"; // in a 1 second interval
			cqlString = "( " + TEMPORAL_ATTRIBUTE + "  =  '" + instant.getAsKeyString() + "' )";
			//cqlString = "TEQUALS( " + TEMPORAL_ATTRIBUTE + " , " + instant.getAsKeyString() + " )";
			System.out.println("Temporal expression: cqlString="+cqlString);
    	} else if (function.equals(EQUALS)) { //TODO
    		cqlString=null;
    	} else if (function.equals(EQUALS)) { //TODO
    		cqlString=null;
    	}else {
    		cqlString=null;
    		throw new NotImplementedException("Temporal filter expression not supported. sparqlIndexingExpression="+sparqlIndexingExpression + ", sparqlIndexingExpression.getFunction()=" + sparqlIndexingExpression.getFunction());  // TODO need more
    	}
        return cqlString;
	}
	/**
	 * Take a filter expression from RDF world and convert to CQL readable by GeoTools/geoMesa.
	 * @param type
	 * @param geometry
	 * @param contraints
	 * @return
	 * @throws ParseException 
	 */
    private String makeCQLfromGeosparql(IndexingExpr sparqlIndexingExpression) throws ParseException {
    	if (sparqlIndexingExpression.getArguments().length > 1)
    		throw new Error("Multiple arguments.  Don't know what to do with them.") ; //TODO verify always 1 or handle more.
    	Object geoExpr = sparqlIndexingExpression.getArguments()[0];
    	if (! (geoExpr instanceof Literal)) {
    		throw new Error("Must be a literal.  Don't know what to do with this Value: geoExpr="+geoExpr) ; //TODO verify always Literal or handle more.
    	}
    	Geometry geometry = GeoParseUtils.getGeometry((Literal) geoExpr, new GmlParser());
    	String cqlString;
    	URI function = sparqlIndexingExpression.getFunction();
	    if (function.equals(GeoConstants.GEO_SF_EQUALS)) {
    		cqlString = "EQUALS(" + GEOMETRY_ATTRIBUTE + ", " + geometry + " )";
		} else if (function.equals(GeoConstants.GEO_SF_DISJOINT)) {
    		cqlString = "DISJOINT" + "(" + GEOMETRY_ATTRIBUTE + ", " + geometry + " )";
		} else if (function.equals(GeoConstants.GEO_SF_INTERSECTS)) {
    		cqlString = "INTERSECTS" + "(" + GEOMETRY_ATTRIBUTE + ", " + geometry + " )";
		} else if (function.equals(GeoConstants.GEO_SF_TOUCHES)) {
    		cqlString = "TOUCHES" + "(" + GEOMETRY_ATTRIBUTE + ", " + geometry + " )";
		} else if (function.equals(GeoConstants.GEO_SF_CROSSES)) {
    		cqlString = "CROSSES" + "(" + GEOMETRY_ATTRIBUTE + ", " + geometry + " )";
		} else if (function.equals(GeoConstants.GEO_SF_WITHIN)) {
    		cqlString = "WITHIN" + "(" + GEOMETRY_ATTRIBUTE + ", " + geometry + " )";
		} else if (function.equals(GeoConstants.GEO_SF_CONTAINS)) {
    		cqlString = "CONTAINS" + "(" + GEOMETRY_ATTRIBUTE + ", " + geometry + " )";
		} else if (function.equals(GeoConstants.GEO_SF_OVERLAPS)) {
    		cqlString = "OVERLAPS" + "(" + GEOMETRY_ATTRIBUTE + ", " + geometry + " )";
		} else {
			cqlString=null;
    		throw new NotImplementedException("Unkown function. function="+function+" sparqlIndexingExpression="+sparqlIndexingExpression + ", sparqlIndexingExpression.getFunction()=" + sparqlIndexingExpression.getFunction());  // TODO need more
    	}
        return cqlString;
    }

	/**
	 * Establish the schema for our temporal type.
	 * See http://www.geomesa.org/documentation/user/accumulo/data_management.html
	 * 
	 * @param dataStore
	 *            the geotools wrapper for a Geomesa DB
	 * @return a geoTools featureType, comprising a name and schema
	 * @throws IOException
	 * @throws SchemaException
	 */
	private static SimpleFeatureType getStatementFeatureType(final DataStore dataStore)
			throws IOException, SchemaException {
		SimpleFeatureType featureType;

		final String[] datastoreFeatures = dataStore.getTypeNames();
		if (Arrays.asList(datastoreFeatures).contains(FEATURE_NAME)) {
			featureType = dataStore.getSchema(FEATURE_NAME);
		} else {
			final String featureSchema = //
					SUBJECT_ATTRIBUTE + ":String:index=full," //
							+ TEMPORAL_ATTRIBUTE + ":Date," //
							+ TEMPO_COUNT_ATTRIBUTE + ":Integer," // store 0 for unknown, 1 for instant, 2 for interval, 
							+ GEOUNKNOWN_ATTRIBUTE + ":String,"
							+ GEOMETRY_ATTRIBUTE
							+ ":Geometry:srid=4326;geomesa.mixed.geometries='true'";
			featureType = SimpleFeatureTypes.createType(FEATURE_NAME, featureSchema);
			dataStore.createSchema(featureType);
		}
		return featureType;
	}

	/**
	 * Instantiate a Geomesa client using SPI. This is taken care of by
	 * GeoTools.
	 * 
	 * @param conf
	 *            pick and choose the parameters from this configuration
	 * @return a geotools abstraction of a datastore to access the Geomesa
	 *         tables and index on Accumulo
	 * @throws IOException
	 */
	public static DataStore createDataStore(final Configuration conf) throws IOException {
		// get the configuration parameters
		final Instance instance = ConfigUtils.getInstance(conf);
		final boolean useMock = instance instanceof MockInstance;
		final String instanceId = instance.getInstanceName();
		final String zookeepers = instance.getZooKeepers();
		final String user = ConfigUtils.getUsername(conf);
		final String password = ConfigUtils.getPassword(conf);
		final String auths = ConfigUtils.getAuthorizations(conf).toString();
		final String tableName = getTableName(conf);

		// build the map of parameters
		final Map<String, Serializable> params = new HashMap<>();
		params.put("instanceId", instanceId);
		params.put("zookeepers", zookeepers);
		params.put("user", user);
		params.put("password", password);
		params.put("auths", auths);
		params.put("tableName", tableName);
		//params.put("indexSchemaFormat", featureSchemaFormat);
		params.put("useMock", Boolean.toString(useMock));

		// fetch the data store from the finder using SPI and reflection.
		return DataStoreFinder.getDataStore(params);
	}

	/**
	 * Get the Accumulo table that will be used by this index.
	 * 
	 * @param conf
	 * @return table name guaranteed to be used by instances of this index
	 */
	public static String getTableName(final Configuration conf) {
		return makeTableName(ConfigUtils.getTablePrefix(conf));
	}

	/**
	 * Make the Accumulo table name used by this indexer for a specific instance
	 * of Rya.
	 *
	 * @param ryaInstanceName
	 *            - The name of the Rya instance the table name is for. (not
	 *            null)
	 * @return The Accumulo table name used by this indexer for a specific
	 *         instance of Rya.
	 */
	public static String makeTableName(final String ryaInstanceName) {
		requireNonNull(ryaInstanceName);
		return ryaInstanceName + TABLE_SUFFIX;
	}
	public Object getFeatureType() {
		return featureType;
	}

}
