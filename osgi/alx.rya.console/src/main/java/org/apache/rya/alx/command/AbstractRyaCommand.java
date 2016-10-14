package org.apache.rya.alx.command;

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



import org.apache.rya.api.persist.RyaDAO;
import org.apache.karaf.shell.console.OsgiCommandSupport;
import org.openrdf.repository.Repository;
import org.osgi.util.tracker.ServiceTracker;

public abstract class AbstractRyaCommand extends OsgiCommandSupport {

    protected Repository repository;
    protected RyaDAO rdfDAO;

    @Override
    protected Object doExecute() throws Exception {
        ServiceTracker serviceTracker = new ServiceTracker(getBundleContext(), Repository.class.getName(), null);
        serviceTracker.open();
        repository = (Repository) serviceTracker.getService();
        serviceTracker.close();
        if (repository == null) {
            System.out.println("Sail Repository not available");
            return null;
        }

        serviceTracker = new ServiceTracker(getBundleContext(), RyaDAO.class.getName(), null);
        serviceTracker.open();
        rdfDAO = (RyaDAO) serviceTracker.getService();
        serviceTracker.close();
        if (rdfDAO == null) {
            System.out.println("Rdf DAO not available");
            return null;
        }

        return doRyaExecute();
    }

    protected abstract Object doRyaExecute() throws Exception;
}
