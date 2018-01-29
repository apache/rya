/**
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
package org.apache.rya.api.client.accumulo;

import static java.util.Objects.requireNonNull;

import org.apache.accumulo.core.client.Connector;
import org.apache.rya.accumulo.instance.AccumuloRyaInstanceDetailsRepository;
import org.apache.rya.api.client.InstanceExists;
import org.apache.rya.api.client.SetRyaStreamsConfiguration;
import org.apache.rya.api.client.SetRyaStreamsConfigurationBase;
import org.apache.rya.api.instance.RyaDetailsRepository;

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * An Accumulo implementation of {@link SetRyaStreamsConfiguration}.
 */
@DefaultAnnotation(NonNull.class)
public class AccumuloSetRyaStreamsConfiguration extends SetRyaStreamsConfigurationBase {

    private final Connector connector;

    /**
     * Constructs an instance of {@link AccumuloSetRyaStreamsConfiguration}.
     *
     * @param instanceExists - The interactor used to verify Rya instances exist. (not null)
     * @param connector - Provides programmatic access to the instance of Accumulo that hosts Rya instance. (not null)
     */
    public AccumuloSetRyaStreamsConfiguration(
            final InstanceExists instanceExists,
            final Connector connector) {
        super(instanceExists);
        this.connector = requireNonNull(connector);
    }

    @Override
    protected RyaDetailsRepository getRyaDetailsRepo(final String ryaInstance) {
        requireNonNull(ryaInstance);
        return new AccumuloRyaInstanceDetailsRepository(connector, ryaInstance);
    }
}