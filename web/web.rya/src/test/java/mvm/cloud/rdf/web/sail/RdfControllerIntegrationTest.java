package mvm.cloud.rdf.web.sail;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.rio.RDFFormat;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.web.WebAppConfiguration;
import org.springframework.test.web.servlet.MockMvc;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;
import static org.springframework.test.web.servlet.setup.MockMvcBuilders.standaloneSetup;

@RunWith(SpringJUnit4ClassRunner.class)
@WebAppConfiguration
@ContextConfiguration({"/controllerIntegrationTest-accumulo.xml", "/controllerIntegrationTest-root.xml"})
public class RdfControllerIntegrationTest {

    private MockMvc mockMvc;

    @Autowired
    private RdfController controller;

    @Autowired
    private Repository repository;

    @Rule
    public ExpectedException thrown = ExpectedException.none();

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
        mockMvc.perform(get("/queryrdf?query="))
                .andExpect(status().isOk());
    }

    @Test
    public void emptyQueryXMLFormat() throws Exception {
        this.mockMvc.perform(get("/queryrdf")
                .param("query", "SELECT * WHERE { ?s a <http://mynamespace/ProductType> . }")
                .param("query.resultformat", "xml"))
                .andExpect(status().isOk())
                .andExpect(content().contentType(MediaType.TEXT_XML));
    }
}
