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
import org.apache.rya.indexing.GeoConstants;
import org.geotools.gml3.GMLConfiguration;
import org.geotools.xml.Parser;
import org.openrdf.model.Literal;
import org.openrdf.model.Statement;
import org.openrdf.model.Value;
import org.openrdf.query.algebra.FunctionCall;
import org.openrdf.query.algebra.ValueConstant;
import org.openrdf.query.algebra.ValueExpr;
import org.openrdf.query.algebra.Var;
import org.xml.sax.SAXException;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKTReader;

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
	public static String getWellKnownText(final Statement statement) throws ParseException {
	    final Literal lit = getLiteral(statement);
	    if (!GeoConstants.XMLSCHEMA_OGC_WKT.equals(lit.getDatatype())) {
	        logger.warn("Literal is not of type " + GeoConstants.XMLSCHEMA_OGC_WKT + ": " + statement.toString());
	    }
	    return lit.getLabel().toString();
	}

    public static Literal getLiteral(final Statement statement) throws ParseException {
        final org.openrdf.model.Value v = statement.getObject();
        if (!(v instanceof Literal)) {
            throw new ParseException("Statement does not contain Literal: " + statement.toString());
        }
        final Literal lit = (Literal) v;
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
    public static Geometry getGeometry(final Statement statement) throws ParseException {
        // handle GML or WKT
        final Literal lit = getLiteral(statement);
        if (GeoConstants.XMLSCHEMA_OGC_WKT.equals(lit.getDatatype())) {
            final String wkt = lit.getLabel().toString();
            return (new WKTReader()).read(wkt);
        } else if (GeoConstants.XMLSCHEMA_OGC_GML.equals(lit.getDatatype())) {
            final String gml = lit.getLabel().toString();
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
    public static Geometry getGeometryGml(final String gmlString) throws IOException, SAXException, ParserConfigurationException {
        final Reader reader = new StringReader(gmlString);
        final GMLConfiguration gmlConfiguration = new GMLConfiguration();
        final Parser gmlParser = new Parser(gmlConfiguration);
        //  gmlParser.setStrict(false);  // attempt at allowing deprecated elements, but no.
        //  gmlParser.setValidating(false);
        final Geometry geometry = (Geometry) gmlParser.parse(reader);
        // This sometimes gets populated with the SRS/CRS: geometry.getUserData()
        // Always returns 0 : geometry.getSRID()
        //TODO geometry.setUserData(some default CRS); OR geometry.setSRID(some default CRS)

        return geometry;
    }

    /**
     * Extracts the arguments used in a {@link FunctionCall}.
     * @param matchName - The variable name to match to arguments used in the {@link FunctionCall}.
     * @param call - The {@link FunctionCall} to match against.
     * @return - The {@link Value}s matched.
     */
    public static Value[] extractArguments(final String matchName, final FunctionCall call) {
        final Value args[] = new Value[call.getArgs().size() - 1];
        int argI = 0;
        for (int i = 0; i != call.getArgs().size(); ++i) {
            final ValueExpr arg = call.getArgs().get(i);
            if (argI == i && arg instanceof Var && matchName.equals(((Var)arg).getName())) {
                continue;
            }
            if (arg instanceof ValueConstant) {
                args[argI] = ((ValueConstant)arg).getValue();
            } else if (arg instanceof Var && ((Var)arg).hasValue()) {
                args[argI] = ((Var)arg).getValue();
            } else {
                throw new IllegalArgumentException("Query error: Found " + arg + ", expected a Literal, BNode or URI");
            }
            ++argI;
        }
        return args;
    }
}
