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
package org.apache.rya.periodic.notification.api;

/**
 * Interface providing basic life cycle functionality,
 * including stopping and starting any class implementing this
 * interface and checking whether is it running.
 *
 */
public interface LifeCycle {

    /**
     * Starts a running application.
     */
    public void start();

    /**
     * Stops a running application.
     */
    public void stop();
    
    /**
     * Determine if application is currently running.
     * @return true if application is running and false otherwise.
     */
    public boolean currentlyRunning();

}
