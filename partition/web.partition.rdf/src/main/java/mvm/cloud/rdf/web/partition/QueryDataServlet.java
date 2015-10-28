package mvm.cloud.rdf.web.partition;

import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.query.GraphQuery;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQuery;
import org.openrdf.query.resultio.sparqlxml.SPARQLResultsXMLWriter;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.openrdf.rio.rdfxml.RDFXMLWriter;

import javax.servlet.ServletException;
import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintStream;

import static mvm.mmrts.rdf.partition.PartitionConstants.*;

public class QueryDataServlet extends AbstractRDFWebServlet {

    private ValueFactory vf = new ValueFactoryImpl();

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp)
            throws ServletException, IOException {
        if (req == null || req.getInputStream() == null)
            return;

        String query = req.getParameter("query");
        String start = req.getParameter(START_BINDING);
        String end = req.getParameter(END_BINDING);
        String infer = req.getParameter("infer");
        String performant = req.getParameter("performant");
        String useStats = req.getParameter("useStats");
        String timeUris = req.getParameter("timeUris");

        System.out.println("Start[" + start + "] and End[" + end + "]");

        //validate infer, performant
        if (infer != null) {
            Boolean.parseBoolean(infer);
        } else if (performant != null) {
            Boolean.parseBoolean(performant);
        }

        if (query == null) {
            throw new ServletException("Please set a query");
        }
        if (query.toLowerCase().contains("select")) {
            try {
                performSelect(query, start, end, infer, performant, useStats, timeUris, resp);
            } catch (Exception e) {
                throw new ServletException(e);
            }
        } else if (query.toLowerCase().contains("construct")) {
            try {
                performConstruct(query, start, end, infer, performant, useStats, timeUris, resp);
            } catch (Exception e) {
                throw new ServletException(e);
            }
        } else {
            throw new ServletException("Invalid SPARQL query: " + query);
        }

    }

    private void performConstruct(String query, String start, String end, String infer, String performant, String useStats, String timeUris, HttpServletResponse resp)
            throws Exception {
        RepositoryConnection conn = null;
        try {
            ServletOutputStream os = resp.getOutputStream();
            conn = repository.getConnection();

            // query data
            GraphQuery graphQuery = conn.prepareGraphQuery(
                    QueryLanguage.SPARQL, query);
            if (start != null && start.length() > 0)
                graphQuery.setBinding(START_BINDING, vf.createLiteral(Long.parseLong(start)));
            if (end != null && end.length() > 0)
                graphQuery.setBinding(END_BINDING, vf.createLiteral(Long.parseLong(end)));
            if (performant != null && performant.length() > 0)
                graphQuery.setBinding("performant", vf.createLiteral(Boolean.parseBoolean(performant)));
            if (infer != null && infer.length() > 0)
                graphQuery.setBinding("infer", vf.createLiteral(Boolean.parseBoolean(infer)));
            if (useStats != null && useStats.length() > 0)
                graphQuery.setBinding("useStats", vf.createLiteral(Boolean.parseBoolean(useStats)));
            if (timeUris != null && timeUris.length() > 0)
                graphQuery.setBinding("timeUris", vf.createURI(timeUris));
            RDFXMLWriter rdfWriter = new RDFXMLWriter(os);
            graphQuery.evaluate(rdfWriter);

        } catch (Exception e) {
            resp.setStatus(500);
            e.printStackTrace(new PrintStream(resp.getOutputStream()));
            throw new ServletException(e);
        } finally {
            if (conn != null) {
                try {
                    conn.close();
                } catch (RepositoryException e) {

                }
            }
        }
    }

    private void performSelect(String query, String start, String end, String infer, String performant, String useStats, String timeUris, HttpServletResponse resp)
            throws Exception {
        RepositoryConnection conn = null;
        try {
            ServletOutputStream os = resp.getOutputStream();
            conn = repository.getConnection();

            // query data
            TupleQuery tupleQuery = conn.prepareTupleQuery(
                    QueryLanguage.SPARQL, query);
            if (start != null && start.length() > 0)
                tupleQuery.setBinding(START_BINDING, vf.createLiteral(Long.parseLong(start)));
            if (end != null && end.length() > 0)
                tupleQuery.setBinding(END_BINDING, vf.createLiteral(Long.parseLong(end)));
            if (performant != null && performant.length() > 0)
                tupleQuery.setBinding("performant", vf.createLiteral(Boolean.parseBoolean(performant)));
            if (infer != null && infer.length() > 0)
                tupleQuery.setBinding("infer", vf.createLiteral(Boolean.parseBoolean(infer)));
            if (useStats != null && useStats.length() > 0)
                tupleQuery.setBinding("useStats", vf.createLiteral(Boolean.parseBoolean(useStats)));
            if (timeUris != null && timeUris.length() > 0)
                tupleQuery.setBinding("timeUris", vf.createURI(timeUris));
            SPARQLResultsXMLWriter sparqlWriter = new SPARQLResultsXMLWriter(os);
            tupleQuery.evaluate(sparqlWriter);

        } catch (Exception e) {
            resp.setStatus(500);
            e.printStackTrace(new PrintStream(resp.getOutputStream()));
            throw new ServletException(e);
        } finally {
            if (conn != null) {
                try {
                    conn.close();
                } catch (RepositoryException e) {

                }
            }
        }
    }

    public Repository getRepository() {
        return repository;
    }

    public void setRepository(Repository repository) {
        this.repository = repository;
    }
}
