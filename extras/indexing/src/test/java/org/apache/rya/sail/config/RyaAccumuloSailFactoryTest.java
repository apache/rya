package org.apache.rya.sail.config;

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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.InputStream;
import java.io.StringReader;
import java.util.HashMap;
import java.util.Map;

import org.apache.rya.sail.config.RyaAccumuloSailConfig;
import org.apache.rya.sail.config.RyaAccumuloSailFactory;

import org.apache.commons.io.IOUtils;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.openrdf.model.Graph;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.LinkedHashModel;
import org.openrdf.model.util.GraphUtil;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.config.ConfigTemplate;
import org.openrdf.repository.config.RepositoryConfig;
import org.openrdf.repository.config.RepositoryConfigSchema;
import org.openrdf.repository.config.RepositoryConfigUtil;
import org.openrdf.repository.config.RepositoryImplConfig;
import org.openrdf.repository.config.RepositoryRegistry;
import org.openrdf.repository.manager.LocalRepositoryManager;
import org.openrdf.repository.sail.config.SailRepositoryConfig;
import org.openrdf.repository.sail.config.SailRepositoryFactory;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFParser;
import org.openrdf.rio.Rio;
import org.openrdf.rio.helpers.StatementCollector;
import org.openrdf.sail.config.SailFactory;
import org.openrdf.sail.config.SailRegistry;

import com.google.common.collect.ImmutableMap;
import com.google.common.io.Files;

public class RyaAccumuloSailFactoryTest {

    private static RepositoryImplConfig getConfig() {
        RyaAccumuloSailConfig c = new RyaAccumuloSailConfig();
        c.setUser("root");
        c.setPassword("");
        c.setInstance("mock-instance");
        c.setMock(true);
        return new SailRepositoryConfig(c);
    }

    @Ignore
    @Test
    public void testCreateAccumuloSail() throws Exception {
        SailRepositoryFactory f = new SailRepositoryFactory();
        Repository r = f.getRepository(getConfig());
        r.initialize();
        RepositoryConnection rc = r.getConnection();
        rc.close();
    }

    @Ignore
    @Test
    public void testAddStatement() throws Exception {
        SailRepositoryFactory f = new SailRepositoryFactory();
        Repository r = f.getRepository(getConfig());
        r.initialize();
        RepositoryConnection rc = r.getConnection();

        ValueFactory vf = rc.getValueFactory();
        Statement s = vf.createStatement(vf.createURI("u:a"), vf.createURI("u:b"), vf.createURI("u:c"));

        assertFalse(rc.hasStatement(s, false));

        rc.add(s);

        Assert.assertTrue(rc.hasStatement(s, false));
        rc.close();
    }

    @Test
    public void testCreateFromTemplateName() throws Exception {
        LocalRepositoryManager repoman = new LocalRepositoryManager(Files.createTempDir());
        repoman.initialize();
        
        
        
        try(InputStream templateStream = RepositoryConfig.class.getResourceAsStream("RyaAccumuloSail.ttl")) {
            String template = IOUtils.toString(templateStream);
            
            final ConfigTemplate configTemplate = new ConfigTemplate(template);
            final Map<String, String> valueMap = ImmutableMap.<String, String> builder()
                    .put("Repository ID", "RyaAccumuloSail")
                    .put("Repository title", "RyaAccumuloSail Store")
                    .put("Rya Accumulo user", "root")
                    .put("Rya Accumulo password", "")
                    .put("Rya Accumulo instance", "dev")
                    .put("Rya Accumulo zookeepers", "zoo1,zoo2,zoo3")
                    .put("Rya Accumulo is mock", "true")
                    .build();
            
            final String configString = configTemplate.render(valueMap);
            
//            final Repository systemRepo = this.state.getManager().getSystemRepository();
            final Graph graph = new LinkedHashModel();
            final RDFParser rdfParser = Rio.createParser(RDFFormat.TURTLE);
            rdfParser.setRDFHandler(new StatementCollector(graph));
            rdfParser.parse(new StringReader(configString), RepositoryConfigSchema.NAMESPACE);
            final Resource repositoryNode = GraphUtil.getUniqueSubject(graph, RDF.TYPE,
                    RepositoryConfigSchema.REPOSITORY);
            final RepositoryConfig repConfig = RepositoryConfig.create(graph, repositoryNode);
            repConfig.validate();

            
            repoman.addRepositoryConfig(repConfig);
            
            Repository r = repoman.getRepository("RyaAccumuloSail");
            r.initialize();
            
        }

    }
    
    @Test
    public void testRyaAccumuloSailInManager() throws Exception {
//        Class<SailFactory> clazz = SailFactory.class;
//        ServiceLoader<SailFactory> loader = java.util.ServiceLoader.load(clazz, clazz.getClassLoader());
//
//        Iterator<SailFactory> services = loader.iterator();
//        
//        while (services.hasNext())
//        System.out.println(services.next());

        
        
        String ryaSailKey = RyaAccumuloSailFactory.SAIL_TYPE;

        assertTrue("Connot find RyaAccumuloSailFactory in Registry", SailRegistry.getInstance().has(ryaSailKey));

        SailFactory factory = SailRegistry.getInstance().get(ryaSailKey);
        Assert.assertNotNull("Cannot create RyaAccumuloSailFactory", factory);
        
        
//        for (String s : SailRegistry.getInstance().getKeys()) {
//            System.out.println("SailRegistry :: " + s);
//        }
//        System.out.println("Factory :: " + factory.getClass().getName());
        for (String s : RepositoryRegistry.getInstance().getKeys()) {
            System.out.println("RepositoryRegistry :: " + s);
        }
//        RepositoryManager m = new LocalRepositoryManager(Files.createTempDir());
//        m.initialize();
//        for (String s : RepositoryConfigUtil.getRepositoryIDs(m.getSystemRepository())) {
//            System.out.println("System :: " + s);
//        }
//        RepositoryConnection rc = m.getSystemRepository().getConnection();
//        RepositoryResult<Statement> results = rc.getStatements(null, null, null, false);
//        while(results.hasNext()) {
//            System.out.println("System :: " + results.next().toString());
//        }
//        RepositoryProvider.getRepository("SYSTEM");
//        RepositoryConfigSchema
        // RepositoryProvider.getRepository("RyaAccumuloSail");
    }
    
    @Test
    public void testParseTemplate() throws Exception{
        String template = IOUtils.toString(ClassLoader.getSystemResourceAsStream("org/openrdf/repository/config/RyaAccumuloSail.ttl"));
        ConfigTemplate ct = new ConfigTemplate(template);
        System.out.println(ct.getVariableMap());
    }
}
