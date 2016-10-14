package org.apache.rya.indexing;

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

import org.openrdf.model.URI;
import org.openrdf.model.impl.URIImpl;

/**
 * A set of URIs used in GeoSPARQL
 */
public class GeoConstants {
    public static final String NS_GEO = "http://www.opengis.net/ont/geosparql#";
    public static final String NS_GEOF = "http://www.opengis.net/def/function/geosparql/";

    public static final URI XMLSCHEMA_OGC_WKT = new URIImpl(NS_GEO + "wktLiteral");
    public static final URI GEO_AS_WKT = new URIImpl(NS_GEO + "asWKT");

    public static final URI XMLSCHEMA_OGC_GML = new URIImpl(NS_GEO + "gmlLiteral");
    public static final URI GEO_AS_GML = new URIImpl(NS_GEO + "asGML");

    public static final URI GEO_SF_EQUALS = new URIImpl(NS_GEOF + "sfEquals");
    public static final URI GEO_SF_DISJOINT = new URIImpl(NS_GEOF + "sfDisjoint");
    public static final URI GEO_SF_INTERSECTS = new URIImpl(NS_GEOF + "sfIntersects");
    public static final URI GEO_SF_TOUCHES = new URIImpl(NS_GEOF + "sfTouches");
    public static final URI GEO_SF_CROSSES = new URIImpl(NS_GEOF + "sfCrosses");
    public static final URI GEO_SF_WITHIN = new URIImpl(NS_GEOF + "sfWithin");
    public static final URI GEO_SF_CONTAINS = new URIImpl(NS_GEOF + "sfContains");
    public static final URI GEO_SF_OVERLAPS = new URIImpl(NS_GEOF + "sfOverlaps");
}
