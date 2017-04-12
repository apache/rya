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
package org.apache.rya.api.domain;

import java.util.Date;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.ISODateTimeFormat;
import org.openrdf.model.URI;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.model.vocabulary.XMLSchema;

import com.google.common.collect.ImmutableMap;

/**
 * Utility methods for using {@link RyaType}.
 */
public final class RyaTypeUtils {
    private static final ImmutableMap<Class<?>, RyaTypeMethod> METHOD_MAP =
        ImmutableMap.<Class<?>, RyaTypeMethod>builder()
            .put(Boolean.class, (v) -> booleanRyaType((Boolean) v))
            .put(Byte.class, (v) -> byteRyaType((Byte) v))
            .put(Date.class, (v) -> dateRyaType((Date) v))
            .put(DateTime.class, (v) -> dateRyaType((DateTime) v))
            .put(Double.class, (v) -> doubleRyaType((Double) v))
            .put(Float.class, (v) -> floatRyaType((Float) v))
            .put(Integer.class, (v) -> intRyaType((Integer) v))
            .put(Long.class, (v) -> longRyaType((Long) v))
            .put(Short.class, (v) -> shortRyaType((Short) v))
            .put(String.class, (v) -> stringRyaType((String) v))
            .put(URI.class, (v) -> uriRyaType((URI) v))
            .put(URIImpl.class, (v) -> uriRyaType((URIImpl) v))
            .build();

    /**
     * Represents a method inside the {@link RyaTypeUtils} class that can be
     * called.
     */
    private static interface RyaTypeMethod {
        /**
         * Calls the method within {@link RyaTypeUtils} with the supplied value.
         * @param value the object value.
         * @return the {@link RyaType}.
         */
        public RyaType callRyaTypeMethod(final Object value);
    }

    /**
     * Private constructor to prevent instantiation.
     */
    private RyaTypeUtils() {
    }

    /**
     * Creates a boolean {@link RyaType} object.
     * @param value the {@link Boolean} object.
     * @return the {@link RyaType} with the data type set to
     * {@link XMLSchema#BOOLEAN} and the data set to the specified
     * {@code value}.
     */
    public static RyaType booleanRyaType(final Boolean value) {
        return new RyaType(XMLSchema.BOOLEAN, Boolean.toString(value));
    }

    /**
     * Creates a byte {@link RyaType} object.
     * @param value the {@link Byte} object.
     * @return the {@link RyaType} with the data type set to
     * {@link XMLSchema#BYTE} and the data set to the specified {@code value}.
     */
    public static RyaType byteRyaType(final Byte value) {
        return new RyaType(XMLSchema.BYTE, Byte.toString(value));
    }

    /**
     * Creates a date {@link RyaType} object.
     * @param value the {@link Date} object.
     * @return the {@link RyaType} with the data type set to
     * {@link XMLSchema#DATETIME} and the data set to the specified
     * {@code value}.
     */
    public static RyaType dateRyaType(final Date value) {
        final DateTime dateTime = new DateTime(value.getTime());
        final StringBuffer sb = new StringBuffer();
        ISODateTimeFormat.dateTime().withZone(DateTimeZone.UTC).printTo(sb, dateTime.getMillis());
        final String formattedDate = sb.toString();
        return new RyaType(XMLSchema.DATE, formattedDate);
    }

    /**
     * Creates a date/time {@link RyaType} object.
     * @param value the {@link DateTime} object.
     * @return the {@link RyaType} with the data type set to
     * {@link XMLSchema#DATETIME} and the data set to the specified
     * {@code value}.
     */
    public static RyaType dateRyaType(final DateTime value) {
        final StringBuffer sb = new StringBuffer();
        ISODateTimeFormat.dateTime().withZone(DateTimeZone.UTC).printTo(sb, value.getMillis());
        final String formattedDate = sb.toString();
        return new RyaType(XMLSchema.DATETIME, formattedDate);
    }

    /**
     * Creates a double {@link RyaType} object.
     * @param value the {@link Double} object.
     * @return the {@link RyaType} with the data type set to
     * {@link XMLSchema#DOUBLE} and the data set to the specified {@code value}.
     */
    public static RyaType doubleRyaType(final Double value) {
        return new RyaType(XMLSchema.DOUBLE, Double.toString(value));
    }

    /**
     * Creates a float {@link RyaType} object.
     * @param value the {@link Float} object.
     * @return the {@link RyaType} with the data type set to
     * {@link XMLSchema#FLOAT} and the data set to the specified {@code value}.
     */
    public static RyaType floatRyaType(final Float value) {
        return new RyaType(XMLSchema.FLOAT, Float.toString(value));
    }

    /**
     * Creates an integer {@link RyaType} object.
     * @param value the {@link Integer} object.
     * @return the {@link RyaType} with the data type set to
     * {@link XMLSchema#INTEGER} and the data set to the specified
     * {@code value}.
     */
    public static RyaType intRyaType(final Integer value) {
        return new RyaType(XMLSchema.INTEGER, Integer.toString(value));
    }

    /**
     * Creates a long {@link RyaType} object.
     * @param value the {@link Long} object.
     * @return the {@link RyaType} with the data type set to
     * {@link XMLSchema#LONG} and the data set to the specified {@code value}.
     */
    public static RyaType longRyaType(final Long value) {
        return new RyaType(XMLSchema.LONG, Long.toString(value));
    }

    /**
     * Creates a short {@link RyaType} object.
     * @param value the {@link Short} object.
     * @return the {@link RyaType} with the data type set to
     * {@link XMLSchema#SHORT} and the data set to the specified {@code value}.
     */
    public static RyaType shortRyaType(final Short value) {
        return new RyaType(XMLSchema.SHORT, Short.toString(value));
    }

    /**
     * Creates a string {@link RyaType} object.
     * @param value the {@link String} object.
     * @return the {@link RyaType} with the data type set to
     * {@link XMLSchema#STRING} and the data set to the specified {@code value}.
     */
    public static RyaType stringRyaType(final String value) {
        return new RyaType(XMLSchema.STRING, value);
    }

    /**
     *
     * Creates a URI {@link RyaType} object.
     * @param value the {@link URI} object.
     * @return the {@link RyaType} with the data type set to
     * {@link XMLSchema#ANYURI} and the data set to the specified {@code value}.
     */
    public static RyaType uriRyaType(final URI value) {
        return new RyaType(XMLSchema.ANYURI, value.stringValue());
    }

    /**
     * Calls the appropriate {@link RyaTypeUtils} method based on the class
     * specified and initializes it with the supplied value.
     * @param classType the {@link Class} of {@link RyaType} to find.
     * @param value the value to initialize the {@link RyaType} with.
     * @return the {@link RyaType} or {@code null} if none could be found for
     * the specified {@code classType}.
     */
    public static RyaType getRyaTypeForClass(final Class<?> classType, final Object value) {
        final RyaTypeMethod method = METHOD_MAP.get(classType);
        RyaType ryaType = null;
        if (method != null) {
            ryaType = method.callRyaTypeMethod(value);
        }
        return ryaType;
    }
}