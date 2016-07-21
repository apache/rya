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

package mvm.rya.blueprints.config

import com.tinkerpop.blueprints.Vertex
import com.tinkerpop.blueprints.Direction
import junit.framework.TestCase
import mvm.rya.api.RdfCloudTripleStoreConstants
import mvm.rya.api.resolver.RdfToRyaConversions
import mvm.rya.blueprints.sail.RyaSailEdge
import org.openrdf.model.ValueFactory
import org.openrdf.model.impl.StatementImpl
import org.openrdf.model.impl.ValueFactoryImpl

import static mvm.rya.api.RdfCloudTripleStoreConfiguration.CONF_TBL_PREFIX
import static mvm.rya.accumulo.mr.MRUtils.*
import org.apache.accumulo.core.security.Authorizations
import org.apache.accumulo.core.client.Connector
import mvm.rya.accumulo.AccumuloRyaDAO
import mvm.rya.accumulo.AccumuloRdfConfiguration
import org.apache.accumulo.core.client.mock.MockInstance
import org.apache.accumulo.core.client.admin.SecurityOperations
import org.apache.accumulo.core.Constants
import org.apache.accumulo.core.security.TablePermission

/**
 * Date: 5/9/12
 * Time: 3:11 PM
 */
class RyaGraphConfigurationTest extends TestCase {
    private String user = "user";

    private String pwd = "pwd";
    private String instance = "myinstance";
    private String tablePrefix = "t_";
    private Authorizations auths = Constants.NO_AUTHS;
    private Connector connector;
    private AccumuloRyaDAO ryaDAO;
    private ValueFactory vf = new ValueFactoryImpl();
    private String namespace = "urn:test#";
    private AccumuloRdfConfiguration conf;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        connector = new MockInstance(instance).getConnector(user, pwd.getBytes());
        connector.tableOperations().create(tablePrefix + RdfCloudTripleStoreConstants.TBL_SPO_SUFFIX);
        connector.tableOperations().create(tablePrefix + RdfCloudTripleStoreConstants.TBL_PO_SUFFIX);
        connector.tableOperations().create(tablePrefix + RdfCloudTripleStoreConstants.TBL_OSP_SUFFIX);
        connector.tableOperations().create(tablePrefix + RdfCloudTripleStoreConstants.TBL_NS_SUFFIX);
        SecurityOperations secOps = connector.securityOperations();
        secOps.createUser(user, pwd.getBytes(), auths);
        secOps.grantTablePermission(user, tablePrefix + RdfCloudTripleStoreConstants.TBL_SPO_SUFFIX, TablePermission.READ);
        secOps.grantTablePermission(user, tablePrefix + RdfCloudTripleStoreConstants.TBL_PO_SUFFIX, TablePermission.READ);
        secOps.grantTablePermission(user, tablePrefix + RdfCloudTripleStoreConstants.TBL_OSP_SUFFIX, TablePermission.READ);
        secOps.grantTablePermission(user, tablePrefix + RdfCloudTripleStoreConstants.TBL_NS_SUFFIX, TablePermission.READ);

        conf = new AccumuloRdfConfiguration();
        ryaDAO = new AccumuloRyaDAO();
        ryaDAO.setConnector(connector);
        conf.setTablePrefix(tablePrefix);
        ryaDAO.setConf(conf);
        ryaDAO.init();
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        connector.tableOperations().delete(tablePrefix + RdfCloudTripleStoreConstants.TBL_SPO_SUFFIX);
        connector.tableOperations().delete(tablePrefix + RdfCloudTripleStoreConstants.TBL_PO_SUFFIX);
        connector.tableOperations().delete(tablePrefix + RdfCloudTripleStoreConstants.TBL_OSP_SUFFIX);
        connector.tableOperations().delete(tablePrefix + RdfCloudTripleStoreConstants.TBL_NS_SUFFIX);
    }

    public void testGraphConfiguration() {
        def a = vf.createURI(namespace, "a")
        def statement = new StatementImpl(a, vf.createURI(namespace, "p"), vf.createLiteral("l"))
        def statement2 = new StatementImpl(a, vf.createURI(namespace, "p2"), vf.createLiteral("l"))
        ryaDAO.add(RdfToRyaConversions.convertStatement(statement));
        ryaDAO.add(RdfToRyaConversions.convertStatement(statement2));
        ryaDAO.add(RdfToRyaConversions.convertStatement(new StatementImpl(vf.createURI(namespace, "b"), vf.createURI(namespace, "p"), vf.createLiteral("l"))));
        ryaDAO.add(RdfToRyaConversions.convertStatement(new StatementImpl(vf.createURI(namespace, "c"), vf.createURI(namespace, "n"), vf.createLiteral("l"))));

        RyaGraphConfiguration.load()

        def graph = RyaGraphConfiguration.createGraph(
                [(AC_INSTANCE_PROP): instance,
                        (AC_MOCK_PROP): "true",
                        (AC_USERNAME_PROP): user,
                        (AC_PWD_PROP): pwd,
                        (CONF_TBL_PREFIX): tablePrefix,
//                 (CONF_QUERYPLAN_FLAG): "true",
                ]
        );

        def edge = graph.getEdge(RyaSailEdge.formatId(statement))
        assertNotNull(edge)
        Vertex vertex = graph.getVertex(a.stringValue())
        assertNotNull(vertex)
        def edges = vertex.getEdges(Direction.OUT).iterator().toList()
        assertEquals(2, edges.size())
        assertNotNull edges[0].subj
        assertNotNull edges[0].pred
        assertNotNull edges[0].obj
        assertNull edges[0].cntxt

        def queryEdges = graph.query(edges[0].subj, edges[0].pred, edges[0].obj, edges[0].cntxt)
        assertEquals edges[0], queryEdges[0]

        graph.shutdown()
    }
}
