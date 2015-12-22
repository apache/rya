package mvm.rya.indexing.mongodb;

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


import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import mvm.rya.api.domain.RyaStatement;
import mvm.rya.api.resolver.RdfToRyaConversions;
import mvm.rya.indexing.StatementContraints;
import mvm.rya.indexing.accumulo.StatementSerializer;
import mvm.rya.indexing.accumulo.geo.GeoParseUtils;
import mvm.rya.mongodb.dao.SimpleMongoDBStorageStrategy;

import org.apache.commons.codec.binary.Hex;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKTReader;

public class GeoMongoDBStorageStrategy extends SimpleMongoDBStorageStrategy{

	private static final String GEO = "location";
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

	public DBObject serialize(Statement statement) throws ParseException{
		// if the object is wkt, then try to index it
        // write the statement data to the fields
        Geometry geo = (new WKTReader()).read(GeoParseUtils.getWellKnownText(statement));
        if(geo == null || geo.isEmpty() || !geo.isValid()) {
            throw new ParseException("Could not create geometry for statement " + statement);
        }
 		RyaStatement ryaStatement = RdfToRyaConversions.convertStatement(statement);
 		BasicDBObject base = (BasicDBObject) super.serialize(ryaStatement);
 		base.append(GEO, getCorrespondingPoints(geo));	
		return base;
		
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
