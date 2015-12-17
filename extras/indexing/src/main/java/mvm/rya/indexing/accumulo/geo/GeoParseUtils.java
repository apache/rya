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


import org.apache.log4j.Logger;
import org.openrdf.model.Literal;
import org.openrdf.model.Statement;

import com.vividsolutions.jts.io.ParseException;

public class GeoParseUtils {
    static final Logger logger = Logger.getLogger(GeoParseUtils.class);

	public static String getWellKnownText(Statement statement) throws ParseException {
	    org.openrdf.model.Value v = statement.getObject();
	    if (!(v instanceof Literal)) {
	        throw new ParseException("Statement does not contain Literal: " + statement.toString());
	    }
	
	    Literal lit = (Literal) v;
	    if (!GeoConstants.XMLSCHEMA_OGC_WKT.equals(lit.getDatatype())) {
	        logger.warn("Literal is not of type " + GeoConstants.XMLSCHEMA_OGC_WKT + ": " + statement.toString());
	    }
	
	    return lit.getLabel().toString();
	}

}
