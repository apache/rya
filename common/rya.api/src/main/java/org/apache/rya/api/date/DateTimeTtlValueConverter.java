package org.apache.rya.api.date;

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



import org.openrdf.model.Value;

import javax.xml.datatype.DatatypeConfigurationException;
import javax.xml.datatype.DatatypeFactory;
import java.util.GregorianCalendar;
import java.util.TimeZone;

/**
 * Class DateTimeTtlValueConverter
 * @deprecated           2
 */
public class DateTimeTtlValueConverter implements TtlValueConverter {

    private Value start, stop;
    private TimeZone timeZone = TimeZone.getTimeZone("Zulu");

    @Override
    public void convert(String ttl, String startTime) {
        try {
            long start_l, stop_l;
            long ttl_l = Long.parseLong(ttl);
            stop_l = System.currentTimeMillis();
            if (startTime != null)
                stop_l = Long.parseLong(startTime);
            start_l = stop_l - ttl_l;

            GregorianCalendar cal = (GregorianCalendar) GregorianCalendar.getInstance();
            cal.setTimeZone(getTimeZone());
            cal.setTimeInMillis(start_l);
            DatatypeFactory factory = DatatypeFactory.newInstance();
            start = vf.createLiteral(factory.newXMLGregorianCalendar(cal));

            cal.setTimeInMillis(stop_l);
            stop = vf.createLiteral(factory.newXMLGregorianCalendar(cal));
        } catch (DatatypeConfigurationException e) {
            throw new RuntimeException("Exception occurred creating DataTypeFactory", e);
        }
    }

    @Override
    public Value getStart() {
        return start;
    }

    @Override
    public Value getStop() {
        return stop;
    }

    public TimeZone getTimeZone() {
        return timeZone;
    }

    public void setTimeZone(TimeZone timeZone) {
        this.timeZone = timeZone;
    }
}
