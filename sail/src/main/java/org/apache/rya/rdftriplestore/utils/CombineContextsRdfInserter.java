package org.apache.rya.rdftriplestore.utils;

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



import org.openrdf.OpenRDFUtil;
import org.openrdf.model.*;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.helpers.RDFHandlerBase;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by IntelliJ IDEA.
 * User: RoshanP
 * Date: 3/23/12
 * Time: 9:50 AM
 * To change this template use File | Settings | File Templates.
 */
public class CombineContextsRdfInserter extends RDFHandlerBase {

    private final RepositoryConnection con;
    private Resource[] contexts = new Resource[0];
    private boolean preserveBNodeIDs;
    private final Map<String, String> namespaceMap;
    private final Map<String, BNode> bNodesMap;

    public CombineContextsRdfInserter(RepositoryConnection con) {
        this.con = con;
        preserveBNodeIDs = true;
        namespaceMap = new HashMap<String, String>();
        bNodesMap = new HashMap<String, BNode>();
    }

    public void setPreserveBNodeIDs(boolean preserveBNodeIDs) {
        this.preserveBNodeIDs = preserveBNodeIDs;
    }

    public boolean preservesBNodeIDs() {
        return preserveBNodeIDs;
    }

    public void enforceContext(Resource... contexts) {
        OpenRDFUtil.verifyContextNotNull(contexts);
        this.contexts = contexts;
    }

    public boolean enforcesContext() {
        return contexts.length != 0;
    }

    public Resource[] getContexts() {
        return contexts;
    }

    @Override
    public void endRDF()
            throws RDFHandlerException {
        for (Map.Entry<String, String> entry : namespaceMap.entrySet()) {
            String prefix = entry.getKey();
            String name = entry.getValue();

            try {
                if (con.getNamespace(prefix) == null) {
                    con.setNamespace(prefix, name);
                }
            } catch (RepositoryException e) {
                throw new RDFHandlerException(e);
            }
        }

        namespaceMap.clear();
        bNodesMap.clear();
    }

    @Override
    public void handleNamespace(String prefix, String name) {
        // FIXME: set namespaces directly when they are properly handled wrt
        // rollback
        // don't replace earlier declarations
        if (prefix != null && !namespaceMap.containsKey(prefix)) {
            namespaceMap.put(prefix, name);
        }
    }

    @Override
    public void handleStatement(Statement st)
            throws RDFHandlerException {
        Resource subj = st.getSubject();
        URI pred = st.getPredicate();
        Value obj = st.getObject();
        Resource ctxt = st.getContext();

        if (!preserveBNodeIDs) {
            if (subj instanceof BNode) {
                subj = mapBNode((BNode) subj);
            }

            if (obj instanceof BNode) {
                obj = mapBNode((BNode) obj);
            }

            if (!enforcesContext() && ctxt instanceof BNode) {
                ctxt = mapBNode((BNode) ctxt);
            }
        }

        try {
            if (enforcesContext()) {
                Resource[] ctxts = contexts;
                if (ctxt != null) {
                    ctxts = combineContexts(contexts, ctxt);
                }
                con.add(subj, pred, obj, ctxts);
            } else {
                con.add(subj, pred, obj, ctxt);
            }
        } catch (RepositoryException e) {
            throw new RDFHandlerException(e);
        }
    }

    private BNode mapBNode(BNode bNode) {
        BNode result = bNodesMap.get(bNode.getID());

        if (result == null) {
            result = con.getRepository().getValueFactory().createBNode();
            bNodesMap.put(bNode.getID(), result);
        }

        return result;
    }

    public static Resource[] combineContexts(Resource[] contexts, Resource ctxt) {
        if (contexts == null || ctxt == null) {
            throw new IllegalArgumentException("Contexts cannot be null");
        }
        int length = contexts.length;
        Resource[] ret = new Resource[length + 1];
        System.arraycopy(contexts, 0, ret, 0, length);
        ret[length] = ctxt;
        return ret;
    }
}
