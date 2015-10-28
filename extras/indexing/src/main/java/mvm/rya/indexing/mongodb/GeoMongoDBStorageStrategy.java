package mvm.rya.indexing.mongodb;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import mvm.rya.indexing.StatementContraints;
import mvm.rya.indexing.accumulo.StatementSerializer;
import mvm.rya.indexing.accumulo.geo.GeoParseUtils;

import org.apache.commons.codec.binary.Hex;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.impl.ValueFactoryImpl;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKTReader;

public class GeoMongoDBStorageStrategy {

	private static final String ID = "_id";
	private static final String GEO = "location";
	private static final String CONTEXT = "context";
	private static final String PREDICATE = "predicate";
	private static final String OBJECT = "object";
	private static final String SUBJECT = "subject";
	public enum GeoQueryType {
		INTERSECTS {
			public String getKeyword() {
				return "$geoIntersects";
			}
		}, WITHIN {
			public String getKeyword() {
				return "$geoWithin";
			}
		},
		EQUALS {
			public String getKeyword() {
				return "$near";
			}
		};
		
		public abstract String getKeyword();
	}

	private double maxDistance;


	public GeoMongoDBStorageStrategy(double maxDistance) {
		this.maxDistance = maxDistance;
	}
	
	public void createIndices(DBCollection coll){
		coll.createIndex("{" + GEO + " : \"2dsphere\"" );
	}

	public DBObject getQuery(StatementContraints contraints, Geometry geo, GeoQueryType queryType) {
		BasicDBObject query;
		if (queryType.equals(GeoQueryType.EQUALS)){
			List<double[]> points = getCorrespondingPoints(geo);
			if (points.size() == 1){
				List circle = new ArrayList();
				circle.add(points.get(0));
				circle.add(maxDistance);
				BasicDBObject polygon = new BasicDBObject("$centerSphere", circle);
				query = new BasicDBObject(GEO,  new BasicDBObject(GeoQueryType.WITHIN.getKeyword(), polygon));
			}else {
				query = new BasicDBObject(GEO, points);
			}
			
		}
		else {
			query = new BasicDBObject(GEO, new BasicDBObject(queryType.getKeyword(), new BasicDBObject("$polygon", getCorrespondingPoints(geo))));
		}
		if (contraints.hasSubject()){
			query.append(SUBJECT, contraints.getSubject().toString());
		}
		if (contraints.hasPredicates()){
			Set<URI> predicates = contraints.getPredicates();
			if (predicates.size() > 1){
				BasicDBList or = new BasicDBList();
				for (URI pred : predicates){
					DBObject currentPred = new BasicDBObject(PREDICATE, pred.toString());
					or.add(currentPred);
				}
				query.append("$or", or);
			}
			else if (!predicates.isEmpty()){
				query.append(PREDICATE, predicates.iterator().next().toString());
			}
		}
		if (contraints.hasContext()){
			query.append(CONTEXT, contraints.getContext().toString());
		}
		
		return query;
	}


	public Statement deserializeDBObject(DBObject queryResult) {
		Map result = queryResult.toMap();
		String subject = (String) result.get(SUBJECT);
		String object = (String) result.get(OBJECT);
		String predicate = (String) result.get(PREDICATE);
		String context = (String) result.get(CONTEXT);
		if (!context.isEmpty()){
			return StatementSerializer.readStatement(subject, predicate, object, context);			
		}
		return StatementSerializer.readStatement(subject, predicate, object);
	}
	
	

	public DBObject serialize(Statement statement) throws ParseException{
		// if the object is wkt, then try to index it
        // write the statement data to the fields
        Geometry geo = (new WKTReader()).read(GeoParseUtils.getWellKnownText(statement));
        if(geo == null || geo.isEmpty() || !geo.isValid()) {
            throw new ParseException("Could not create geometry for statement " + statement);
        }
 		
		String context = "";
		if (statement.getContext() != null){
			context = StatementSerializer.writeContext(statement);
		}
		String id = StatementSerializer.writeSubject(statement) + " " + 
				StatementSerializer.writePredicate(statement) + " " +  StatementSerializer.writeObject(statement) + " " + context;
		byte[] bytes = id.getBytes();
		try {
			MessageDigest digest = MessageDigest.getInstance("SHA-1");
			bytes = digest.digest(bytes);
		} catch (NoSuchAlgorithmException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		BasicDBObject doc = new BasicDBObject(ID, new String(Hex.encodeHex(bytes)))
		.append(GEO, getCorrespondingPoints(geo))
		.append(SUBJECT, StatementSerializer.writeSubject(statement))
	    .append(PREDICATE, StatementSerializer.writePredicate(statement))
	    .append(OBJECT,  StatementSerializer.writeObject(statement))
	    .append(CONTEXT, context);
		return doc;
		
	}
	
	private List<double[]> getCorrespondingPoints(Geometry geo){
	       List<double[]> points = new ArrayList<double[]>();
	        for (Coordinate coord : geo.getCoordinates()){
	        	points.add(new double[] {
	        		coord.x, coord.y	
	        	});
	        }
	        return points;
		
	}

}
