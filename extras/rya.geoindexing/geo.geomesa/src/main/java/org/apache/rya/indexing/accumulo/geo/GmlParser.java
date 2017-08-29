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

import java.io.IOException;
import java.io.Reader;

import javax.xml.parsers.ParserConfigurationException;

import org.apache.rya.indexing.accumulo.geo.GeoParseUtils.GmlToGeometryParser;
import org.geotools.gml3.GMLConfiguration;
import org.xml.sax.SAXException;

import com.vividsolutions.jts.geom.Geometry;


/**
 * This wraps geotools parser for rya.geoCommon that cannot be dependent on geotools.
 *
 */
public class GmlParser implements GmlToGeometryParser {

	/* (non-Javadoc)
	 * @see org.apache.rya.indexing.accumulo.geo.GeoParseUtils.GmlToGeometryParser#parse(java.io.Reader)
	 */
	@Override
	public Geometry parse(Reader reader) throws IOException, SAXException, ParserConfigurationException {
		final org.geotools.xml.Parser gmlParser = new org.geotools.xml.Parser(new GMLConfiguration()); 
		return (Geometry) gmlParser.parse(reader);
	}

}
