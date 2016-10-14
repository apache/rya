package org.apache.rya.rdftriplestore.namespace;

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



import info.aduna.iteration.CloseableIteration;
import org.apache.rya.api.RdfCloudTripleStoreConfiguration;
import org.apache.rya.api.persist.RdfDAOException;
import org.apache.rya.api.persist.RyaDAO;
import org.apache.rya.api.persist.RyaNamespaceManager;
import net.sf.ehcache.Cache;
import net.sf.ehcache.CacheManager;
import net.sf.ehcache.Element;
import net.sf.ehcache.Statistics;
import org.openrdf.model.Namespace;
import org.openrdf.sail.SailException;

import java.io.InputStream;

/**
 * Class NamespaceManager
 * Date: Oct 17, 2011
 * Time: 8:25:33 AM
 */
public class NamespaceManager {
    CacheManager cacheManager;
    Cache namespaceCache;
    public static final String NAMESPACE_CACHE_NAME = "namespace";
    private RdfCloudTripleStoreConfiguration conf;
    private RyaNamespaceManager namespaceManager;

    public NamespaceManager(RyaDAO ryaDAO, RdfCloudTripleStoreConfiguration conf) {
        this.conf = conf;
        initialize(ryaDAO);
    }

    protected void initialize(RyaDAO ryaDAO) {
        try {
            this.namespaceManager = ryaDAO.getNamespaceManager();

            InputStream cacheConfigStream = Thread.currentThread().getContextClassLoader().getResourceAsStream("ehcache.xml");
            if (cacheConfigStream == null) {
                this.cacheManager = CacheManager.create();
//                throw new RuntimeException("Cache Configuration does not exist");
            } else {
                this.cacheManager = CacheManager.create(cacheConfigStream);
            }
            this.namespaceCache = cacheManager.getCache(NAMESPACE_CACHE_NAME);
            if (namespaceCache == null) {
                cacheManager.addCache(NAMESPACE_CACHE_NAME);
            }


        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void shutdown() {
        if (cacheManager != null) {
            cacheManager.shutdown();
            cacheManager = null;
        }
    }

    public void addNamespace(String pfx, String namespace) {
        try {
            String savedNamespace = getNamespace(pfx);
            //if the saved ns is the same one being saved, don't do anything
            if (savedNamespace != null && savedNamespace.equals(namespace)) {
                return;
            }

            namespaceCache.put(new Element(pfx, namespace));
            namespaceManager.addNamespace(pfx, namespace);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public String getNamespace(String pfx) {
        //try in the cache first
        Element element = namespaceCache.get(pfx);
        if (element != null) {
            return (String) element.getValue();
        }

        try {
            String namespace = namespaceManager.getNamespace(pfx);
            if (namespace != null) {
                namespaceCache.put(new Element(pfx, namespace));
                return namespace;
            }
        } catch (Exception e) {
            //TODO: print or log?
        }
        return null;

    }

    public void removeNamespace(String pfx) {
        try {
            namespaceCache.remove(pfx);
            namespaceManager.removeNamespace(pfx);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public CloseableIteration<? extends Namespace, SailException> iterateNamespace() {
        try {
            //for this one we will go directly to the store
            final CloseableIteration<? extends Namespace, RdfDAOException> iteration = namespaceManager.iterateNamespace();
            return new CloseableIteration<Namespace, SailException>() {
                @Override
                public void close() throws SailException {
                    iteration.close();
                }

                @Override
                public boolean hasNext() throws SailException {
                    return iteration.hasNext();
                }

                @Override
                public Namespace next() throws SailException {
                    return iteration.next();
                }

                @Override
                public void remove() throws SailException {
                    iteration.remove();
                }
            };
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void printStatistics() {
        Statistics statistics = namespaceCache.getStatistics();
        if (statistics != null) { //TODO: use a logger please
            System.out.println("Namespace Cache Statisitics: ");
            System.out.println("--Hits: \t" + statistics.getCacheHits());
            System.out.println("--Misses: \t" + statistics.getCacheMisses());
            System.out.println("--Total Count: \t" + statistics.getObjectCount());
        }
    }
}
