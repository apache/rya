package mvm.rya.api.persist;

/*
 * #%L
 * mvm.rya.rya.api
 * %%
 * Copyright (C) 2014 Rya
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

import info.aduna.iteration.CloseableIteration;
import mvm.rya.api.RdfCloudTripleStoreConfiguration;
import org.openrdf.model.Namespace;

/**
 * Date: 7/17/12
 * Time: 8:23 AM
 */
public interface RyaNamespaceManager<C extends RdfCloudTripleStoreConfiguration> extends RyaConfigured<C> {

    public void addNamespace(String pfx, String namespace) throws RyaDAOException;

    public String getNamespace(String pfx) throws RyaDAOException;

    public void removeNamespace(String pfx) throws RyaDAOException;

    public CloseableIteration<? extends Namespace, RyaDAOException> iterateNamespace() throws RyaDAOException;
}
