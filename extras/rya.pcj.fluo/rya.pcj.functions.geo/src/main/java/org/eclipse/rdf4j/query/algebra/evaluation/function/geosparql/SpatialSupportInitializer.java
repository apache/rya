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
package org.eclipse.rdf4j.query.algebra.evaluation.function.geosparql;

import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.WKTWriter;
import org.locationtech.spatial4j.context.SpatialContext;
import org.locationtech.spatial4j.context.jts.JtsSpatialContext;
import org.locationtech.spatial4j.shape.Shape;

import java.io.IOException;

/**
 * See https://bitbucket.org/pulquero/sesame-geosparql-jts
 *
 * GeoSPARQL standard defined at https://www.ogc.org/standards/geosparql
 */
public class SpatialSupportInitializer extends SpatialSupport {

    @Override
    protected SpatialContext createSpatialContext() {
        return JtsSpatialContext.GEO;
    }

    @Override
    protected SpatialAlgebra createSpatialAlgebra() {
        return new JtsSpatialAlgebra(JtsSpatialContext.GEO);
    }

    @Override
    protected WktWriter createWktWriter() {
        return new JtsWktWriter(JtsSpatialContext.GEO);
    }

    static class JtsSpatialAlgebra implements SpatialAlgebra {
        private final JtsSpatialContext context;

        public JtsSpatialAlgebra(JtsSpatialContext context) {
            this.context = context;
        }

        @Override
        public Shape buffer(Shape s, double distance) {
            return context.makeShape(context.getGeometryFrom(s).buffer(distance));
        }

        @Override
        public Shape convexHull(Shape s) {
            return context.makeShape(context.getGeometryFrom(s).convexHull());
        }

        @Override
        public Shape boundary(Shape s) {
            return context.makeShape(context.getGeometryFrom(s).getBoundary());
        }

        @Override
        public Shape envelope(Shape s) {
            return context.makeShape(context.getGeometryFrom(s).getEnvelope());
        }

        @Override
        public Shape union(Shape s1, Shape s2) {
            return context.makeShape(context.getGeometryFrom(s1).union(context.getGeometryFrom(s2)));
        }

        @Override
        public Shape intersection(Shape s1, Shape s2) {
            return context.makeShape(context.getGeometryFrom(s1).intersection(context.getGeometryFrom(s2)));
        }

        @Override
        public Shape symDifference(Shape s1, Shape s2) {
            return context.makeShape(context.getGeometryFrom(s1).symDifference(context.getGeometryFrom(s2)));
        }

        @Override
        public Shape difference(Shape s1, Shape s2) {
            return context.makeShape(context.getGeometryFrom(s1).difference(context.getGeometryFrom(s2)));
        }

        @Override
        public boolean relate(Shape s1, Shape s2, String intersectionPattern) {
            return context.getGeometryFrom(s1).relate(context.getGeometryFrom(s2), intersectionPattern);
        }

        @Override
        public boolean sfEquals(Shape s1, Shape s2) {
            return relate(s1, s2, "TFFFTFFFT");
        }

        @Override
        public boolean sfDisjoint(Shape s1, Shape s2) {
            return relate(s1, s2, "FF*FF****");
        }

        @Override
        public boolean sfIntersects(Shape s1, Shape s2) {
            return relate(s1, s2, "T********") || relate(s1, s2, "*T*******") || relate(s1, s2, "***T*****") || relate(s1, s2, "****T****");
        }

        @Override
        public boolean sfTouches(Shape s1, Shape s2) {
            return relate(s1, s2, "FT*******") || relate(s1, s2, "F**T*****") || relate(s1, s2, "F***T****");
        }

        @Override
        public boolean sfCrosses(Shape s1, Shape s2) {
            Geometry g1 = context.getGeometryFrom(s1);
            Geometry g2 = context.getGeometryFrom(s2);
            int d1 = g1.getDimension();
            int d2 = g2.getDimension();
            if ((d1 == 0 && d2 == 1) || (d1 == 0 && d2 == 2) || (d1 == 1 && d2 == 2)) {
                return g1.relate(g2, "T*T***T**");
            } else if (d1 == 1 && d2 == 1) {
                return g1.relate(g2, "0*T***T**");
            } else {
                return false;
            }
        }

        @Override
        public boolean sfWithin(Shape s1, Shape s2) {
            return relate(s1, s2, "T*F**F***");
        }

        @Override
        public boolean sfContains(Shape s1, Shape s2) {
            return relate(s1, s2, "T*****FF*");
        }

        @Override
        public boolean sfOverlaps(Shape s1, Shape s2) {
            Geometry g1 = context.getGeometryFrom(s1);
            Geometry g2 = context.getGeometryFrom(s2);
            int d1 = g1.getDimension();
            int d2 = g2.getDimension();
            if ((d1 == 2 && d2 == 2) || (d1 == 0 && d2 == 0)) {
                return g1.relate(g2, "T*T***T**");
            } else if (d1 == 1 && d2 == 1) {
                return g1.relate(g2, "1*T***T**");
            } else {
                return false;
            }
        }

        @Override
        public boolean ehEquals(Shape s1, Shape s2) {
            return relate(s1, s2, "TFFFTFFFT");
        }

        @Override
        public boolean ehDisjoint(Shape s1, Shape s2) {
            return relate(s1, s2, "FF*FF****");
        }

        @Override
        public boolean ehMeet(Shape s1, Shape s2) {
            return relate(s1, s2, "FT*******") || relate(s1, s2, "F**T*****") || relate(s1, s2, "F***T****");
        }

        @Override
        public boolean ehOverlap(Shape s1, Shape s2) {
            return relate(s1, s2, "T*T***T**");
        }

        @Override
        public boolean ehCovers(Shape s1, Shape s2) {
            return relate(s1, s2, "T*TFT*FF*");
        }

        @Override
        public boolean ehCoveredBy(Shape s1, Shape s2) {
            return relate(s1, s2, "TFF*TFT**");
        }

        @Override
        public boolean ehInside(Shape s1, Shape s2) {
            return relate(s1, s2, "TFF*FFT**");
        }

        @Override
        public boolean ehContains(Shape s1, Shape s2) {
            return relate(s1, s2, "T*TFF*FF*");
        }

        @Override
        public boolean rcc8dc(Shape s1, Shape s2) {
            return relate(s1, s2, "FFTFFTTTT");
        }

        @Override
        public boolean rcc8ec(Shape s1, Shape s2) {
            return relate(s1, s2, "FFTFTTTTT");
        }

        @Override
        public boolean rcc8po(Shape s1, Shape s2) {
            return relate(s1, s2, "TTTTTTTTT");
        }

        @Override
        public boolean rcc8tppi(Shape s1, Shape s2) {
            return relate(s1, s2, "TTTFTTFFT");
        }

        @Override
        public boolean rcc8tpp(Shape s1, Shape s2) {
            return relate(s1, s2, "TFFTTFTTT");
        }

        @Override
        public boolean rcc8ntpp(Shape s1, Shape s2) {
            return relate(s1, s2, "TFFTFFTTT");
        }

        @Override
        public boolean rcc8ntppi(Shape s1, Shape s2) {
            return relate(s1, s2, "TTTFFTFFT");
        }

        @Override
        public boolean rcc8eq(Shape s1, Shape s2) {
            return relate(s1, s2, "TFFFTFFFT");
        }

    }

    static class JtsWktWriter implements WktWriter {
        private final JtsSpatialContext context;

        public JtsWktWriter(JtsSpatialContext context) {
            this.context = context;
        }

        @Override
        public String toWkt(Shape s) throws IOException {
            return new WKTWriter().write(context.getGeometryFrom(s));
        }
    }
}
