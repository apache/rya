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
package org.apache.rya.api.function.temporal;

import java.time.ZonedDateTime;
import java.time.format.DateTimeParseException;

import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.query.algebra.evaluation.ValueExprEvaluationException;
import org.openrdf.query.algebra.evaluation.function.Function;

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * Function for comparing a {@link ZonedDateTime} with an interval of {@link ZonedDateTime} objects in a SPARQL filter.
 */
@DefaultAnnotation(NonNull.class)
abstract class TemporalIntervalRelationFunction implements Function {
    @Override
    public Value evaluate(final ValueFactory valueFactory, final Value... args) throws ValueExprEvaluationException {
        if (args.length != 2) {
            throw new ValueExprEvaluationException(getURI() + " requires exactly 2 arguments, got " + args.length);
        }

        final String[] strInterval = args[1].stringValue().split("/");
        if (strInterval.length != 2) {
            throw new ValueExprEvaluationException(getURI() + " requires the second argument: " + args[1] + " to be 2 dates seperated by a \'/\'");
        }
        try {
            final ZonedDateTime date1 = ZonedDateTime.parse(args[0].stringValue());
            final ZonedDateTime[] interval = new ZonedDateTime[] {
                    ZonedDateTime.parse(strInterval[0]),
                    ZonedDateTime.parse(strInterval[1])
            };
            final boolean result = relation(date1, interval);

            return valueFactory.createLiteral(result);
        } catch (final DateTimeParseException e) {
            throw new ValueExprEvaluationException("Date/Times provided must be of the ISO-8601 format. Example: 2007-04-05T14:30Z");
        }
    }

    /**
     * The comparison function to perform between 2 {@link ZonedDateTime}
     * objects.
     *
     * @param d1 first {@link ZonedDateTime} to compare. (not null)
     * @param interval The interval represented by 2 {@link ZonedDateTime}s. (not null)
     * @return The result of the comparison between {@link ZonedDateTime}s.
     */
    protected abstract boolean relation(ZonedDateTime d1, ZonedDateTime[] interval);
}
