package org.apache.rya.indexing.accumulo.geo;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;

import javax.xml.parsers.ParserConfigurationException;

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
import org.geotools.gml3.GMLConfiguration;
import org.geotools.xml.Parser;
import org.openrdf.model.Literal;
import org.openrdf.model.Statement;
import org.xml.sax.SAXException;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKTReader;

import org.apache.rya.indexing.GeoConstants;

public class GeoParseUtils {
    static final Logger logger = Logger.getLogger(GeoParseUtils.class);
    /** 
     * @deprecated  Not needed since geo literals may be WKT or GML.
     *
     *    This method warns on a condition that must already be tested.  Replaced by
     *    {@link #getLiteral(Statement)} and {@link #getGeometry(Statement}
     *    and getLiteral(statement).toString()
     *    and getLiteral(statement).getDatatype()
     */
    @Deprecated
	public static String getWellKnownText(Statement statement) throws ParseException {
	    Literal lit = getLiteral(statement);
	    if (!GeoConstants.XMLSCHEMA_OGC_WKT.equals(lit.getDatatype())) {
	        logger.warn("Literal is not of type " + GeoConstants.XMLSCHEMA_OGC_WKT + ": " + statement.toString());
	    }
	    return lit.getLabel().toString();
	}

    public static Literal getLiteral(Statement statement) throws ParseException {
        org.openrdf.model.Value v = statement.getObject();
        if (!(v instanceof Literal)) {
            throw new ParseException("Statement does not contain Literal: " + statement.toString());
        }
        Literal lit = (Literal) v;
        return lit;
    }

    /**
     * Parse GML/wkt literal to Geometry
     * 
     * @param statement
     * @return
     * @throws ParseException
     * @throws ParserConfigurationException 
     * @throws SAXException 
     * @throws IOException 
     */
    public static Geometry getGeometry(Statement statement) throws ParseException {
        // handle GML or WKT
        Literal lit = getLiteral(statement);
        if (GeoConstants.XMLSCHEMA_OGC_WKT.equals(lit.getDatatype())) {
            final String wkt = lit.getLabel().toString();
            return (new WKTReader()).read(wkt);
        } else if (GeoConstants.XMLSCHEMA_OGC_GML.equals(lit.getDatatype())) {
            String gml = lit.getLabel().toString();
            try {
                return getGeometryGml(gml);
            } catch (IOException | SAXException | ParserConfigurationException e) {
                throw new ParseException(e);
            }
        } else {
            throw new ParseException("Literal is unknown geo type, expecting WKT or GML: " + statement.toString());
        }
    }
    /**
     * Convert GML/XML string into a geometry that can be indexed.
     * @param gmlString
     * @return
     * @throws IOException
     * @throws SAXException
     * @throws ParserConfigurationException
     */
    public static Geometry getGeometryGml(String gmlString) throws IOException, SAXException, ParserConfigurationException {
        Reader reader = new StringReader(gmlString); 
        GMLConfiguration gmlConfiguration = new GMLConfiguration();
        Parser gmlParser = new Parser(gmlConfiguration);
        //  gmlParser.setStrict(false);  // attempt at allowing deprecated elements, but no.
        //  gmlParser.setValidating(false);
        final Geometry geometry = (Geometry) gmlParser.parse(reader);
        // This sometimes gets populated with the SRS/CRS: geometry.getUserData()
        // Always returns 0 : geometry.getSRID() 
        //TODO geometry.setUserData(some default CRS); OR geometry.setSRID(some default CRS)
        
        return geometry;
    }

}
