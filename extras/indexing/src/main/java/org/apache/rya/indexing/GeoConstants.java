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

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;

/**
 * A set of URIs used in GeoSPARQL
 */
public class GeoConstants {
    private static final ValueFactory VF = SimpleValueFactory.getInstance();

    public static final String NS_GEO = "http://www.opengis.net/ont/geosparql#";
    public static final String NS_GEOF = "http://www.opengis.net/def/function/geosparql/";

    public static final IRI XMLSCHEMA_OGC_WKT = VF.createIRI(NS_GEO + "wktLiteral");
    public static final IRI GEO_AS_WKT = VF.createIRI(NS_GEO + "asWKT");

    public static final IRI XMLSCHEMA_OGC_GML = VF.createIRI(NS_GEO + "gmlLiteral");
    public static final IRI GEO_AS_GML = VF.createIRI(NS_GEO + "asGML");

    public static final IRI GEO_SF_EQUALS = VF.createIRI(NS_GEOF + "sfEquals");
    public static final IRI GEO_SF_DISJOINT = VF.createIRI(NS_GEOF + "sfDisjoint");
    public static final IRI GEO_SF_INTERSECTS = VF.createIRI(NS_GEOF + "sfIntersects");
    public static final IRI GEO_SF_TOUCHES = VF.createIRI(NS_GEOF + "sfTouches");
    public static final IRI GEO_SF_CROSSES = VF.createIRI(NS_GEOF + "sfCrosses");
    public static final IRI GEO_SF_WITHIN = VF.createIRI(NS_GEOF + "sfWithin");
    public static final IRI GEO_SF_CONTAINS = VF.createIRI(NS_GEOF + "sfContains");
    public static final IRI GEO_SF_OVERLAPS = VF.createIRI(NS_GEOF + "sfOverlaps");
    public static final IRI GEO_SF_NEAR = VF.createIRI(NS_GEOF + "sfNear");
}
