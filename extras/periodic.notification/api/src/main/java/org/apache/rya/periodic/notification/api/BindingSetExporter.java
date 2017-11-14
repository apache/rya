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

import org.eclipse.rdf4j.query.BindingSet;

/**
 * An Object that is used to export {@link BindingSet}s to an external repository or queuing system.
 *
 */
public interface BindingSetExporter {

    /**
     * This method exports the BindingSet to the external repository or queuing system
     * that this BindingSetExporter is configured to export to.
     * @param bindingSet - {@link BindingSet} to be exported
     * @throws ResultExportException
     */
    public void exportNotification(BindingSetRecord bindingSet) throws BindingSetRecordExportException;

}
