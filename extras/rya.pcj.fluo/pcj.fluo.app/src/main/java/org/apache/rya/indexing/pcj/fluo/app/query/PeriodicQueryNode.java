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
package org.apache.rya.indexing.pcj.fluo.app.query;

import java.util.Objects;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.rya.indexing.pcj.fluo.app.util.PeriodicQueryUtil;
import org.eclipse.rdf4j.query.algebra.QueryModelVisitor;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.algebra.UnaryTupleOperator;
import org.eclipse.rdf4j.query.algebra.evaluation.function.Function;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * This is a {@link UnaryTupleOperator} that gets placed in the parsed query
 * {@link TupleExpr} when a {@link Filter} is encountered in the SPARQL String that
 * contains the Periodic {@link Function} {@link PeriodicQueryUtil#PeriodicQueryURI}.
 * The PeiodicQueryNode is created from the arguments passed to the Periodic Function,
 * which consist of a time unit, a temporal period, a temporal window of time, and the
 * temporal variable in the query, which assumes a value indicated by the
 * Time ontology: http://www.w3.org/2006/time. The purpose of the PeriodicQueryNode
 * is to filter out all events that did not occur within the specified window of time
 * of this instant and to generate notifications at a regular interval indicated by the period.
 *
 */
public class PeriodicQueryNode extends UnaryTupleOperator {

    private TimeUnit unit;
    private long windowDuration;
    private long periodDuration;
    private String temporalVar;
    
    /**
     * Creates a PeriodicQueryNode from the specified values.
     * @param window - specifies the window of time that event must occur within from this instant
     * @param period - regular interval at which notifications are generated (must be leq window).
     * @param unit - time unit of the period and window
     * @param temporalVar - temporal variable in query used for filtering
     * @param arg - child of PeriodicQueryNode in parsed query
     */
    public PeriodicQueryNode(long window, long period, TimeUnit unit, String temporalVar, TupleExpr arg) {
        super(checkNotNull(arg));
        checkArgument(0 < period && period <= window);
        this.temporalVar = checkNotNull(temporalVar);
        this.unit = checkNotNull(unit);
        this.windowDuration = window;
        this.periodDuration = period;
    }
    
    /**
     * @return - temporal variable used to filter events
     */
    public String getTemporalVariable() {
        return temporalVar;
    }

    /**
     * @return window duration in millis
     */
    public long getWindowSize() {
        return windowDuration;
    }

    /**
     * @return period duration in millis
     */
    public long getPeriod() {
        return periodDuration;
    }

    /**
     * @return {@link TimeUnit} for window duration and period duration
     */
    public TimeUnit getUnit() {
        return unit;
    }
    
    @Override
    public <X extends Exception> void visit(QueryModelVisitor<X> visitor) throws X {
        visitor.meetOther(this);
    }
    
    @Override
    public boolean equals(Object other) {
        if(this == other) {
            return true;
        }
        
        if (other instanceof PeriodicQueryNode) {
            if (super.equals(other)) {
                PeriodicQueryNode metadata = (PeriodicQueryNode) other;
                return new EqualsBuilder().append(windowDuration, metadata.windowDuration).append(periodDuration, metadata.periodDuration)
                        .append(unit, metadata.unit).append(temporalVar, metadata.temporalVar).isEquals();
            }
            return false;
        }
        
        return false;
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(arg, unit, windowDuration, periodDuration, temporalVar);
    }
    
    /**
     * @return String representation of this node that is printed in when query tree is printed.
     */
    @Override
    public String getSignature() {
        StringBuilder sb = new StringBuilder();

        sb.append("PeriodicQueryNode(");
        sb.append("Var = " + temporalVar + ", ");
        sb.append("Window = " + windowDuration + " ms, ");
        sb.append("Period = " + periodDuration + " ms, ");
        sb.append("Time Unit = " + unit  + ")");
       

        return sb.toString();
    }
    
    @Override
    public PeriodicQueryNode clone() {
        PeriodicQueryNode clone = (PeriodicQueryNode)super.clone();
        clone.setArg(getArg().clone());
        clone.periodDuration = periodDuration;
        clone.windowDuration = windowDuration;
        clone.unit = unit;
        clone.temporalVar = temporalVar;
        return clone;
    }

}
