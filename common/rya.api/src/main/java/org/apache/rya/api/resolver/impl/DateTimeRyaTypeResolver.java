package org.apache.rya.api.resolver.impl;

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



import org.apache.rya.api.resolver.RyaTypeResolverException;
import org.calrissian.mango.types.LexiTypeEncoders;
import org.calrissian.mango.types.TypeEncoder;
import org.calrissian.mango.types.exception.TypeDecodingException;
import org.calrissian.mango.types.exception.TypeEncodingException;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;
import org.openrdf.model.vocabulary.XMLSchema;

import java.util.Date;

/**
 * Reverse index xml datetime strings
 * <p/>
 * Date: 7/13/12
 * Time: 7:33 AM
 */
public class DateTimeRyaTypeResolver extends RyaTypeResolverImpl {
    public static final int DATETIME_LITERAL_MARKER = 7;
    public static final TypeEncoder<Date, String> DATE_STRING_TYPE_ENCODER = LexiTypeEncoders.dateEncoder();
    public static final DateTimeFormatter XMLDATETIME_PARSER = org.joda.time.format.ISODateTimeFormat.dateTimeParser();
    public static final DateTimeFormatter UTC_XMLDATETIME_FORMATTER = ISODateTimeFormat.dateTime().withZone(DateTimeZone.UTC);


    public DateTimeRyaTypeResolver() {
        super((byte) DATETIME_LITERAL_MARKER, XMLSchema.DATETIME);
    }

    @Override
    protected String serializeData(String data) throws RyaTypeResolverException {
        try {
            DateTime dateTime = DateTime.parse(data, XMLDATETIME_PARSER);
            Date value = dateTime.toDate();
            return DATE_STRING_TYPE_ENCODER.encode(value);
        } catch (TypeEncodingException e) {
            throw new RyaTypeResolverException(
                    "Exception occurred serializing data[" + data + "]", e);
        }
    }

    @Override
    protected String deserializeData(String value) throws RyaTypeResolverException {
        try {
            Date date = DATE_STRING_TYPE_ENCODER.decode(value);
            return UTC_XMLDATETIME_FORMATTER.print(date.getTime());
        } catch (TypeDecodingException e) {
            throw new RyaTypeResolverException(
                    "Exception occurred deserializing data[" + value + "]", e);
        }
    }
}
