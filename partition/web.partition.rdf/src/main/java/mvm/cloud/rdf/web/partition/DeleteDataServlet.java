//package mvm.cloud.rdf.web.partition;
//
//import org.openrdf.query.QueryLanguage;
//import org.openrdf.query.TupleQuery;
//import org.openrdf.query.resultio.TupleQueryResultWriter;
//import org.openrdf.repository.RepositoryConnection;
//import org.openrdf.repository.RepositoryException;
//
//import javax.servlet.ServletException;
//import javax.servlet.http.HttpServletRequest;
//import javax.servlet.http.HttpServletResponse;
//import java.io.IOException;
//
//public class DeleteDataServlet extends AbstractRDFWebServlet {
//
//    @Override
//    protected void doGet(HttpServletRequest req, HttpServletResponse resp)
//            throws ServletException, IOException {
//        if (req == null || req.getInputStream() == null)
//            return;
//
//        String query_s = req.getParameter("query");
//
//        RepositoryConnection conn = null;
//        try {
//            conn = repository.getConnection();
//            // query data
//            TupleQuery tupleQuery = conn.prepareTupleQuery(
//                    QueryLanguage.SPARQL, query_s);
//            TupleQueryResultWriter deleter = new mvm.mmrts.rdftriplestore.cloudbase.QueryResultsDeleter(conn);
//            tupleQuery.evaluate(deleter);
//
//        } catch (Exception e) {
//            throw new ServletException(e);
//        } finally {
//            if (conn != null) {
//                try {
//                    conn.close();
//                } catch (RepositoryException e) {
//
//                }
//            }
//        }
//    }
//
//}
