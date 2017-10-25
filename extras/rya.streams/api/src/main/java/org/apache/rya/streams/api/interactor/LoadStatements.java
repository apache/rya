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
package org.apache.rya.streams.api.interactor;

import java.nio.file.Path;

import org.apache.rya.api.model.VisibilityStatement;
import org.apache.rya.streams.api.exception.RyaStreamsException;

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * An interactor that is used to load {@link VisibilityStatement}s into a Rya Streaming program.
 */
@DefaultAnnotation(NonNull.class)
public interface LoadStatements {

    /**
     * Loads a series of statements from a RDF File into the Rya Streams system.
     *
     * @param statementsPath - The {@link Path} that will be loaded. (not null)
     * @param visibilities - The visibilities of the statements to load into Rya
     *        Streams. (not null)
     *        <p>
     *        <b>NOTE:</b> The file extension is used to determine the format of
     *        the RDF file.
     *
     * @throws RyaStreamsException Thrown when the format of the file provided is unknown,
     *         or not a valid RDF format.
     */
    public void load(final Path statementsPath, final String visibilities) throws RyaStreamsException;
}