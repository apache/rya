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
package org.apache.rya.periodic.notification.application;

/**
 * Exception thrown when attempting to create a {@link PeriodicNotificationApplication}.
 * Indicates that a factory was unable to create some component of the application 
 * because something was configured incorrectly.
 *
 */
public class PeriodicApplicationException extends Exception {

    private static final long serialVersionUID = 1L;

    /**
     * Creates a PeriodicApplicationException.
     * @param message - message contained in Exception
     */
    public PeriodicApplicationException(String message) {
        super(message);
    }
    
    /**
     * Creates a PeriodicApplicationException.
     * @param message - message contained in Exception
     * @param t - Exception that spawned this PeriodicApplicationException
     */
    public PeriodicApplicationException(String message, Throwable t) {
        super(message, t);
    }
}
