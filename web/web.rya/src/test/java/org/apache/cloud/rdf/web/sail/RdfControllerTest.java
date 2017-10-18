package org.apache.cloud.rdf.web.sail;

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

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertTrue;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;
import static org.springframework.test.web.servlet.setup.MockMvcBuilders.standaloneSetup;

import org.apache.rya.api.RdfCloudTripleStoreConfiguration;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.web.WebAppConfiguration;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.ResultActions;
import org.springframework.web.context.WebApplicationContext;
import org.springframework.web.util.NestedServletException;

@RunWith(SpringJUnit4ClassRunner.class)
@WebAppConfiguration
@ContextConfiguration("/controllerTest-context.xml")
public class RdfControllerTest {

    @Autowired
    private WebApplicationContext wac;

    private MockMvc mockMvc;

    @Autowired
    private RdfController controller;

    @Autowired
    private Repository repository;

    @Rule public ExpectedException thrown = ExpectedException.none();

    @Before
    public void setup() {
        this.mockMvc = standaloneSetup(controller).build();
        try {
            RepositoryConnection con = repository.getConnection();
            con.add(getClass().getResourceAsStream("/test.nt"), "", RDFFormat.NTRIPLES);
            con.close();
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    @Test
    public void emptyQuery() throws Exception {
        this.mockMvc.perform(get("/queryrdf?query="))
                .andExpect(status().isOk());
    }

    @Test
    public void emptyQueryXMLFormat() throws Exception {
        this.mockMvc.perform(get("/queryrdf")
                .param("query", "SELECT * WHERE { ?s ?p ?o . }")
                .param("query.resultformat", "xml"))
                .andExpect(status().isOk())
                .andExpect(content().contentType(MediaType.TEXT_XML));
    }

    @Test
    public void emptyQueryJSONFormat() throws Exception {
        this.mockMvc.perform(get("/queryrdf")
                .param("query", "SELECT * WHERE { ?s ?p ?o . }")
                .param("query.resultformat", "json"))
                .andExpect(status().isOk())
                .andExpect(content().contentType(MediaType.APPLICATION_JSON));
    }

    @Test
    public void emptyQueryNoFormat() throws Exception {
        this.mockMvc.perform(get("/queryrdf")
                .param("query", "SELECT * WHERE { ?s ?p ?o . }"))
                .andExpect(status().isOk())
                .andExpect(content().contentType(MediaType.TEXT_XML));
    }
    
    /**
     * Make sure input variables with odd characters that could be json get encoded.
     * literals should be represented correctly, for example: "bad" in JSON becomes \"bad\" (java escaped \\\"bad\\\")
     * 
     * @throws Exception
     */
    @Test
    public void callbackEncodeCorrectlyJson() throws Exception {
        this.mockMvc.perform(get("/queryrdf") //
                        .param("query", "select ?x where { BIND( '''testbad\\\\or\"bad\"''' AS ?x) }") //
                        .param(RdfCloudTripleStoreConfiguration.CONF_QUERY_AUTH    , "test<bad>or{bad}or\"bad\"") //
                        .param(RdfCloudTripleStoreConfiguration.CONF_CV            , "test<bad>or{bad}or\"bad\"") //
                        .param(RdfCloudTripleStoreConfiguration.CONF_INFER         , "test<bad>or{bad}or\"bad\"") //
                        .param(RdfCloudTripleStoreConfiguration.CONF_RESULT_FORMAT, "json")) //
                        .andExpect(content().string(not(containsString("<bad>")))) // non-query param data should not be in the results
                        .andExpect(content().string(not(containsString("{bad}")))) // non-query param data should not be in the results
                        .andExpect(content().string(not(containsString("bad\\or")))) // should be bad\\\\or
                        .andExpect(content().string(not(containsString("\"bad\"")))); // should be \\\"bad\\\"
    }

    /**
     * Make sure input variables with odd characters that could be xml get encoded.
     * literals should be represented correctly, for example: <bad> in xml becomes &lt;bad&gt;
     * 
     * @throws Exception
     */
@Test
    public void encodeCorrectlyXml() throws Exception {
        this.mockMvc.perform(get("/queryrdf") //
                        .param("query", "select ?x where { BIND( '''test<bad>or&bador&apos;bador&quot;bad&quot;''' AS ?x ) }") //
                    .param(RdfCloudTripleStoreConfiguration.CONF_QUERY_AUTH    , "test<bad>or{bad}or\"bad\"") //
                    .param(RdfCloudTripleStoreConfiguration.CONF_CV            , "test<bad>or{bad}or\"bad\"") //
                    .param(RdfCloudTripleStoreConfiguration.CONF_INFER         , "test<bad>or{bad}or\"bad\"") //
                        .param(RdfCloudTripleStoreConfiguration.CONF_RESULT_FORMAT, "test<bad>or{bad}or\"bad\"")) //
                        .andExpect(content().string(not(containsString("<bad")))) // &lt;
                        .andExpect(content().string(not(containsString("bad>")))) // &gt;
                        .andExpect(content().string(not(containsString("{bad}"))))// non-query param data should not be in the results
                        .andExpect(content().string(not(containsString("&bad")))) // &amp;
                        .andExpect(content().string(not(containsString("'bad")))) // &amp;apos; converted from &apos;
                        .andExpect(content().string(not(containsString("\"bad")))); // &amp;&quot; converted from &quot;
}

    @Test
    public void malformedQuery() throws Exception {
        thrown.expect(NestedServletException.class);
        this.mockMvc.perform(get("/queryrdf")
                .param("query", "SELECT nothing WHERE { ?s ?p ?o }"));
    }

    @Test
    public void updateQuery() throws Exception {
        this.mockMvc.perform(get("/queryrdf")
                .param("query", "INSERT \n DATA\n {\n <http://mynamespace/ProductType1> <http://mynamespace#pred1> \"test\" }"))
                .andExpect(status().isOk());

        ValueFactory vf = repository.getValueFactory();
        RepositoryConnection con = repository.getConnection();

        IRI s = vf.createIRI("http://mynamespace/ProductType1");
        IRI p = vf.createIRI("http://mynamespace#pred1");
        Literal o = vf.createLiteral("test");

        assertTrue(con.getStatements(s, p, o, false).hasNext());
    }
    
    @Test
    public void constructQuery() throws Exception {
        this.mockMvc.perform(get("/queryrdf")
                .param("query", "INSERT DATA { <http://mynamespace/ProductType1> <http://mynamespace#pred1> \"test\" }"))
                .andExpect(status().isOk());
        
        ResultActions actions = this.mockMvc.perform(get("/queryrdf")
                .param("query", "CONSTRUCT {?subj  <http://mynamespace#pred1> \"test2\"} WHERE { ?subj  <http://mynamespace#pred1> \"test\" }"))
                .andExpect(status().isOk());
//        System.out.println(actions.andReturn().getResponse().getContentAsString());

    }

}
