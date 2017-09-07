/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.rya.benchmark.periodic;


import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.google.common.base.Objects;

@Parameters(commandNames = { "periodic" }, commandDescription = "Run benchmark with a PeriodicQuery that uses Filter(function:periodic(?temporalVariable, <windowSize>, <updatePeriod>, <timeUnits>)).  This requires the Rya Periodic Notification Twill YARN Application to be running in addition to the Rya PCJ Updater Incremental Join Application.")
public class PeriodicQueryCommand extends BenchmarkOptions {

    @Parameter(names = { "-pqw", "--periodic-query-window" }, description = "The window size, in --periodic-query-time-units, for returning query results.", required = true)
    private double periodicQueryWindow;

    @Parameter(names = { "-pqp", "--periodic-query-period" }, description = "The period, in --periodic-query-time-units, for results of the windowed query to be returned.", required = true)
    private double periodicQueryPeriod;

    @Parameter(names = { "-pqtu", "--periodic-query-time-units" }, description = "The unit in time (days,hours,minutes)", required = true)
    private PeriodicQueryTimeUnits periodicQueryTimeUnits;

    @Parameter(names = { "-pqrt", "--periodic-query-registration-topic" }, description = "The kafka topic which periodic notification registration requests are published to.  (typically 'notifications')", required = true)
    private String periodicQueryRegistrationTopic;

    public enum PeriodicQueryTimeUnits {
        days, hours, minutes;
    }

    public PeriodicQueryTimeUnits getPeriodicQueryTimeUnits() {
        return periodicQueryTimeUnits;
    }

    public double getPeriodicQueryWindow() {
        return periodicQueryWindow;
    }

    public double getPeriodicQueryPeriod() {
        return periodicQueryPeriod;
    }

    public String getPeriodicQueryRegistrationTopic() {
        return periodicQueryRegistrationTopic;
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add("periodicQueryWindow", periodicQueryWindow)
                .add("periodicQueryPeriod", periodicQueryPeriod)
                .add("periodicQueryTimeUnits", periodicQueryTimeUnits)
                .add("periodicQueryRegistrationTopic", periodicQueryRegistrationTopic)
                .toString() + super.toString();
    }
}
