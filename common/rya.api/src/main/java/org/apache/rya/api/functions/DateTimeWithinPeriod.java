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
 */package org.apache.rya.api.functions;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import java.time.Duration;
import java.time.Instant;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.datatypes.XMLDatatypeUtil;
import org.eclipse.rdf4j.model.vocabulary.FN;
import org.eclipse.rdf4j.model.vocabulary.XMLSchema;
import org.eclipse.rdf4j.query.algebra.evaluation.ValueExprEvaluationException;
import org.eclipse.rdf4j.query.algebra.evaluation.function.Function;

/**
 * This {@link Function} determines whether two {@link XMLSchema#DATETIME}s occur within a specified period of time of
 * one another. The method {@link Function#evaluate(ValueFactory, Value...)} expects four values, where the first two
 * values are the datetimes, the third value is an integer indicating the period, and the fourth value is an IRI
 * indicating the time unit of the period. The IRI must be of Type DurationDescription in the OWL-Time ontology (see
 * <a href ="https://www.w3.org/TR/owl-time/">https://www.w3.org/TR/owl-time/</a>). Examples of valid time unit IRIs can
 * be found in the class {@link OWLTime} and below
 * <ul>
 * <li>http://www.w3.org/2006/time#days</li>
 * <li>http://www.w3.org/2006/time#hours</li>
 * <li>http://www.w3.org/2006/time#minutes</li>
 * <li>http://www.w3.org/2006/time#seconds</li>
 * </ul>
 *
 */
public class DateTimeWithinPeriod implements Function {

    private static final String FUNCTION_IRI = FN.NAMESPACE + "dateTimeWithin";

    @Override
    public String getURI() {
        return FUNCTION_IRI;
    }

    /**
     * Determines whether two datetimes occur within a specified period of time of one another. This method expects four
     * values, where the first two values are the datetimes, the third value is an integer indicating the period, and
     * the fourth value is an IRI indicating the time unit of the period. The IRI must be of Type DurationDescription in
     * the OWL-Time ontology (see <a href ="https://www.w3.org/TR/owl-time/">https://www.w3.org/TR/owl-time/</a>).
     * Examples of valid time unit IRIs can be found in the class {@link OWLTime} and below
     * <ul>
     * <li>http://www.w3.org/2006/time#days</li>
     * <li>http://www.w3.org/2006/time#hours</li>
     * <li>http://www.w3.org/2006/time#minutes</li>
     * <li>http://www.w3.org/2006/time#seconds</li>
     * </ul>
     *
     * @param valueFactory - factory for creating values (not null)
     * @param values - array of Value arguments for this Function (not null).
     */
    @Override
    public Value evaluate(ValueFactory valueFactory, Value... values) throws ValueExprEvaluationException {
        checkNotNull(valueFactory);
        checkNotNull(values);
        try {
            // general validation of input
            checkArgument(values.length == 4);
            checkArgument(values[0] instanceof Literal);
            checkArgument(values[1] instanceof Literal);
            checkArgument(values[2] instanceof Literal);
            checkArgument(values[3] instanceof IRI);

            Instant dateTime1 = convertToInstant((Literal) values[0]);
            Instant dateTime2 = convertToInstant((Literal) values[1]);
            long periodMillis = convertPeriodToMillis((Literal) values[2], (IRI) values[3]);
            long timeBetween = Math.abs(Duration.between(dateTime1, dateTime2).toMillis());

            return valueFactory.createLiteral(timeBetween < periodMillis);
        } catch (Exception e) {
            throw new ValueExprEvaluationException(e);
        }
    }

    private Instant convertToInstant(Literal literal) {
        String stringVal = literal.getLabel();
        IRI dataType = literal.getDatatype();
        checkArgument(dataType.equals(XMLSchema.DATETIME) || dataType.equals(XMLSchema.DATE),
                String.format("Invalid data type for date time. Data Type must be of type %s or %s .", XMLSchema.DATETIME, XMLSchema.DATE));
        checkArgument(XMLDatatypeUtil.isValidDateTime(stringVal) || XMLDatatypeUtil.isValidDate(stringVal), "Invalid date time value.");
        return literal.calendarValue().toGregorianCalendar().toInstant();
    }

    private long convertPeriodToMillis(Literal literal, IRI unit) {
        String stringVal = literal.getLabel();
        IRI dataType = literal.getDatatype();
        checkArgument(dataType.equals(XMLSchema.INTEGER) || dataType.equals(XMLSchema.INT), String
                .format("Invalid data type for period duration. Data Type must be of type %s or %s .", XMLSchema.INTEGER, XMLSchema.INT));
        checkArgument(XMLDatatypeUtil.isValidInteger(stringVal) || XMLDatatypeUtil.isValidInt(stringVal), "Invalid duration value.");
        return convertToMillis(Integer.parseInt(stringVal), unit);
    }

    /**
     * Converts the period duration to milliseconds.
     *
     * @param duration - duration of temporal period
     * @param unit - IRI indicating the time unit (IRI must be of type DurationDescription in the OWL-Time ontology
     *            indicated by the namespace <http://www.w3.org/2006/time#>)
     * @return - duration in milliseconds
     */
    private long convertToMillis(int duration, IRI unit) {
        checkArgument(duration > 0);
        return OWLTime.getMillis(duration, unit);
    }

}
