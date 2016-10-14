package org.apache.rya.indexing;

import org.openrdf.model.Statement;
import org.openrdf.query.QueryEvaluationException;

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


import info.aduna.iteration.CloseableIteration;
import org.apache.rya.api.persist.index.RyaSecondaryIndexer;

/**
 * A repository to store, index, and retrieve {@link Statement}s based on time.
 * Instants:
 *         Instant {before, equals, after} Instant
 *         Instant {before, after, inside} Interval
 *         Instant {hasBeginning, hasEnd}  Interval
 *
 * OWL-Time provides the interval relations:
 * <pre>
 * 		intervalEquals,
 * 		intervalBefore,
 * 		intervalMeets,
 * 		intervalOverlaps,
 * 		intervalStarts,
 * 		intervalDuring,
 * 		intervalFinishes,
 *
 * and their reverse interval relations:
 * 		intervalAfter,
 * 		intervalMetBy,
 * 		intervalOverlappedBy,
 * 		intervalStartedBy,
 * 		intervalContains,
 * 		intervalFinishedBy.
 *
 * from Allen paper in 1983
 *
 * Relation	Y Symbol Inverse Y
 * before    Y < > X
 * equal     Y = = X
 * meets     Y m mi X
 * overlaps  Y o oi X
 * during    Y d di X
 * starts    Y s si X
 * finishes  Y f fi X
 * </pre>
 *
 */

public interface TemporalIndexer extends RyaSecondaryIndexer {

    /* consider ParseException here */

    /*-
     *
     *    And Now, what you you've all been waiting for, the queries:
     *    the instant versions:
     *    format:   x {relation} y
     *    read:  Given literal y, find all statements where the date object x is ( x relation y )
     *         Instant {before, equals, after} Instant
     *         Instant {before, after, inside} Interval
     *         Instant {hasBeginning, hasEnd}  Interval
     *
     *    the Allen interval relations, as described above.
     *    	intervalEquals,
     * 		intervalBefore,
     * 		intervalMeets,
     * 		intervalOverlaps,
     * 		intervalStarts,
     * 		intervalDuring,
     * 		intervalFinishes
     *    and then the inverses, including after.
     */

    public abstract CloseableIteration<Statement, QueryEvaluationException> queryInstantEqualsInstant(
            TemporalInstant queryInstant, StatementConstraints contraints)
            throws QueryEvaluationException;;

    public abstract CloseableIteration<Statement, QueryEvaluationException> queryInstantBeforeInstant(
            TemporalInstant queryInstant, StatementConstraints contraints)
            throws QueryEvaluationException;;

    public abstract CloseableIteration<Statement, QueryEvaluationException> queryInstantAfterInstant(
            TemporalInstant queryInstant, StatementConstraints contraints)
            throws QueryEvaluationException;;

    public abstract CloseableIteration<Statement, QueryEvaluationException> queryInstantBeforeInterval(
            TemporalInterval givenInterval, StatementConstraints contraints)
            throws QueryEvaluationException;;

    public abstract CloseableIteration<Statement, QueryEvaluationException> queryInstantAfterInterval(
            TemporalInterval givenInterval, StatementConstraints contraints)
            throws QueryEvaluationException;

    public abstract CloseableIteration<Statement, QueryEvaluationException> queryInstantInsideInterval(
            TemporalInterval givenInterval, StatementConstraints contraints)
            throws QueryEvaluationException;

    public abstract CloseableIteration<Statement, QueryEvaluationException> queryInstantHasBeginningInterval(
            TemporalInterval queryInterval, StatementConstraints contraints)
            throws QueryEvaluationException;

    public abstract CloseableIteration<Statement, QueryEvaluationException> queryInstantHasEndInterval(
            TemporalInterval queryInterval, StatementConstraints contraints)
            throws QueryEvaluationException;

    /**
     * Returns statements that contain a time instance that is equal to the
     * queried time and meet the {@link StatementConstraints}.
     *
     * @param query
     *            the queried time instance
     * @param contraints
     *            the {@link StatementConstraints}
     * @return
     * @throws QueryEvaluationException
     */
    public abstract CloseableIteration<Statement, QueryEvaluationException> queryIntervalEquals(
            TemporalInterval query, StatementConstraints contraints)
            throws QueryEvaluationException;

    /**
     * Returns statements that contain a time instances that are before the
     * queried {@link TemporalInterval} and meet the {@link StatementConstraints}
     *
     * @param query
     *            the queried time instance
     * @param contraints
     *            the {@link StatementConstraints}
     * @return
     */
    public abstract CloseableIteration<Statement, QueryEvaluationException> queryIntervalBefore(
            TemporalInterval query, StatementConstraints contraints)
            throws QueryEvaluationException;

    /**
     * Returns statements that contain a time instance that is after the queried {@link TemporalInterval} and meet the {@link StatementConstraints}.
     *
     * @param query
     *            the queried time instance
     * @param contraints
     *            the {@link StatementConstraints}
     * @return
     */
    public abstract CloseableIteration<Statement, QueryEvaluationException> queryIntervalAfter(
            TemporalInterval query, StatementConstraints contraints)
            throws QueryEvaluationException;
}
