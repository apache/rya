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
package org.apache.rya.indexing.pcj.storage;

import org.apache.rya.indexing.pcj.storage.accumulo.PcjTables;

/**
 * Indicates one of the {@link PcjTables} functions has failed to perform its task.
 */
public class PcjException extends Exception {
    private static final long serialVersionUID = 1L;

    /**
     * Constructs an instance of {@link PcjException}.
     *
     * @param message - Describes why the exception is being thrown.
     */
    public PcjException(final String message) {
        super(message);
    }

    /**
     * Constructs an instance of {@link PcjException}.
     *
     * @param message - Describes why the exception is being thrown.
     * @param cause - The exception that caused this one to be thrown.
     */
    public PcjException(final String message, final Throwable cause) {
        super(message, cause);
    }
}