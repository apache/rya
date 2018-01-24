/**
 * 
 */
package org.apache.rya.indexing.geotemporal.accumulo;

import static java.util.Objects.requireNonNull;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Matcher;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;
import org.apache.rya.accumulo.experimental.AbstractAccumuloIndexer;
import org.apache.rya.accumulo.experimental.AccumuloIndexer;
import org.apache.rya.api.RdfCloudTripleStoreConfiguration;
import org.apache.rya.api.domain.RyaStatement;
import org.apache.rya.api.domain.RyaType;
import org.apache.rya.api.domain.RyaURI;
import org.apache.rya.api.resolver.RdfToRyaConversions;
import org.apache.rya.api.resolver.RyaToRdfConversions;
import org.apache.rya.indexing.GeoConstants;
import org.apache.rya.indexing.GeoIndexer;
import org.apache.rya.indexing.StatementConstraints;
import org.apache.rya.indexing.TemporalInstant;
import org.apache.rya.indexing.TemporalInstantRfc3339;
import org.apache.rya.indexing.TemporalInterval;
import org.apache.rya.indexing.accumulo.geo.GeoParseUtils;
import org.apache.rya.indexing.accumulo.geo.GmlParser;
import org.apache.rya.indexing.geotemporal.GeoTemporalIndexer;
import org.apache.rya.indexing.geotemporal.model.Event;
import org.apache.rya.indexing.geotemporal.storage.EventStorage;
import org.apache.rya.indexing.mongodb.update.RyaObjectStorage.ObjectStorageException;
import org.geotools.feature.DefaultFeatureCollection;
import org.joda.time.DateTime;
import org.opengis.feature.simple.SimpleFeature;
import org.openrdf.model.Literal;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.query.QueryEvaluationException;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.io.ParseException;

import info.aduna.iteration.CloseableIteration;

/**
 * Use GeoMesa to do date+time indexing along with geospatial
 *
 */
public class AccumuloGeoTemporalIndexer extends AbstractAccumuloIndexer implements AccumuloIndexer, GeoTemporalIndexer /*, GeoIndexer */{
	private static final Logger LOG = Logger.getLogger(AccumuloGeoTemporalIndexer.class);
    // Tables will be grouped by this name (table prefix)
    private String ryaInstanceName;
    private AccumuloEventStorage eventStorage;
    private Configuration conf;
	private Connector connector = null;
    private Set<URI> validPredicates;

    /**
     * 
     */
//    public AccumuloGeoTemporalIndexer(final Connector accumulo, final String ryaInstanceName) {
//        this.ryaInstanceName = requireNonNull(ryaInstanceName);
//    }

    /* (non-Javadoc)
     * @see org.apache.rya.api.persist.index.RyaSecondaryIndexer#getTableName()
     */
    @Override
    public String getTableName() {
        return ryaInstanceName + "_geoTemporal";
    }

    /* (non-Javadoc)
     * @see org.apache.rya.api.persist.index.RyaSecondaryIndexer#storeStatements(java.util.Collection)
     */
    @Override
    public void storeStatements(Collection<RyaStatement> statements) throws IOException {
        // create a feature collection
        for (final RyaStatement ryaStatement : statements) {
            // if the predicate list is empty, accept all predicates.
            // Otherwise, make sure the predicate is on the "valid" list
            final boolean isValidPredicate = validPredicates==null || validPredicates.isEmpty() || validPredicates.contains(ryaStatement.getPredicate());
            // skip it if it is a URI (otherwise it is probably a literal, see RyaToRdfConversions )
            if (isValidPredicate && ! (ryaStatement.getObject() instanceof RyaURI)) {
                final Event.Builder updated;
            	Optional<Event> old;
				try {
					old = eventStorage.get(ryaStatement.getSubject());
				} catch (ObjectStorageException e1) {
					throw new IOException("While looking for old subject event to update.",e1);
				}  
                if(!old.isPresent()) {
                    updated = Event.builder()
                    		.setSubject(ryaStatement.getSubject());
                } else {
                    updated = Event.builder(old.get());
                }

                final URI pred = ryaStatement.getObject().getDataType();
                if(pred.equals(GeoConstants.GEO_AS_WKT) || pred.equals(GeoConstants.GEO_AS_GML) ||
                   pred.equals(GeoConstants.XMLSCHEMA_OGC_WKT) || pred.equals(GeoConstants.XMLSCHEMA_OGC_GML)) {
                    //is geo
                    try {
//                        final Statement rdfStatement = RyaToRdfConversions.convertStatement(ryaStatement);
                        final Geometry geometry = GeoParseUtils.getGeometry(RyaToRdfConversions.convertStatement(ryaStatement), new GmlParser());
                        updated.setGeometry(geometry);
                    } catch (final ParseException e) {
                        LOG.error(e.getMessage(), e);
                        return;
                    }
                } else {
                    //is time
                    final String dateTime = ryaStatement.getObject().getData();
                    final Matcher matcher = TemporalInstantRfc3339.PATTERN.matcher(dateTime);
                    if (matcher.find()) {
                        final TemporalInterval interval = TemporalInstantRfc3339.parseInterval(dateTime);
                        updated.setTemporalInterval(interval);
                    } else {
                        final TemporalInstant instant = new TemporalInstantRfc3339(DateTime.parse(dateTime));
                        updated.setTemporalInstant(instant);
                    }
                }
                
  				final Event event = updated.build(); 
    			try {
    				if(old.isPresent()) {
    					eventStorage.update(old.get(), event);
    				} else {
    					eventStorage.create(event);
    				}
				} catch (ObjectStorageException e) {
					throw new IOException("Trouble while writing a new statement to the geotemporal index.", e);
				}
            }
        }
    }
	/* (non-Javadoc)
     * @see org.apache.rya.api.persist.index.RyaSecondaryIndexer#storeStatement(org.apache.rya.api.domain.RyaStatement)
     */
    @Override
    public void storeStatement(RyaStatement statement) throws IOException {
        storeStatements(Collections.singleton(statement));
    }

    /* (non-Javadoc)
     * @see org.apache.rya.api.persist.index.RyaSecondaryIndexer#deleteStatement(org.apache.rya.api.domain.RyaStatement)
     */
    @Override
    public void deleteStatement(RyaStatement stmt) throws IOException {
    	boolean success=false;
    	//TODO this should really delete the Geo portion and re-write the temporal alone, or visa-versa.
    	try {
			eventStorage.delete(stmt.getSubject());
		} catch (ObjectStorageException e) {
			throw new IOException("Attempting to delete statement in Accumulo GeoTemporal indexer.",e);
		}
    }

    /* (non-Javadoc)
     * @see org.apache.rya.api.persist.index.RyaSecondaryIndexer#dropGraph(org.apache.rya.api.domain.RyaURI[])
     */
    @Override
    public void dropGraph(RyaURI... graphs) {
        // TODO Auto-generated method stub
    	throw new Error("Not yet implemented.");
    }

    /* (non-Javadoc)
     * @see org.apache.rya.api.persist.index.RyaSecondaryIndexer#getIndexablePredicates()
     */
    @Override
    public Set<URI> getIndexablePredicates() {
        // TODO Auto-generated method stub
    	throw new Error("Not yet implemented.");
        //return null;
    }

    /* (non-Javadoc)
     * @see org.apache.rya.api.persist.index.RyaSecondaryIndexer#flush()
     */
    @Override
    public void flush() throws IOException {
        // TODO Auto-generated method stub
    	throw new Error("Not yet implemented.");
    }

    /* (non-Javadoc)
     * @see org.apache.rya.api.persist.index.RyaSecondaryIndexer#close()
     */
    @Override
    public void close() throws IOException {
    }

    /* (non-Javadoc)
     * @see org.apache.hadoop.conf.Configurable#setConf(org.apache.hadoop.conf.Configuration)
     */
    @Override
    public void setConf(Configuration conf) {
        Objects.requireNonNull("Required parameter: conf");
        this.conf = conf;
    }

    /* (non-Javadoc)
     * @see org.apache.hadoop.conf.Configurable#getConf()
     */
    @Override
    public Configuration getConf() {
        return this.conf;
    }

    /* (non-Javadoc)
     * @see org.apache.rya.indexing.GeoTemporalIndexer#getEventStorage(org.apache.hadoop.conf.Configuration)
     */
    @Override
    public EventStorage getEventStorage() {
        Objects.requireNonNull(eventStorage, "This is not initialized, call setconf() and init() first.");
        return eventStorage;
    }

    @Override
    public void init() {
        if (eventStorage==null){
            eventStorage = new AccumuloEventStorage();
            eventStorage.init(conf);
        }
    }

    @Override
    public void setConnector(Connector connector) {
    	this.connector  = connector;
    }

    @Override
    public void destroy() {
        // do nothing
    }

    @Override
    public void purge(RdfCloudTripleStoreConfiguration configuration) {
        // TODO Auto-generated method stub
        throw new Error("Not yet implemented.");
    }

    @Override
    public void dropAndDestroy() {
        // TODO Auto-generated method stub
        throw new Error("Not yet implemented.");
    }

}
