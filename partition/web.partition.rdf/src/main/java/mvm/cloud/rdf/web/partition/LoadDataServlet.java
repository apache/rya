package mvm.cloud.rdf.web.partition;

import org.openrdf.model.Resource;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFParseException;

import javax.servlet.ServletException;
import javax.servlet.ServletInputStream;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

public class LoadDataServlet extends AbstractRDFWebServlet {

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp)
            throws ServletException, IOException {
        if (req == null || req.getInputStream() == null)
            return;

        String format_s = req.getParameter("format");
        RDFFormat format = RDFFormat.RDFXML;
        if (format_s != null) {
            format = RDFFormat.valueOf(format_s);
            if (format == null)
                throw new ServletException("RDFFormat[" + format_s + "] not found");
        }
        ServletInputStream stream = req.getInputStream();

        RepositoryConnection conn = null;
        try {
            conn = repository.getConnection();

            // generate data
            conn.add(stream, "", format, new Resource[]{});
            conn.commit();

            conn.close();
        } catch (RepositoryException e) {
            throw new ServletException(e);
        } catch (RDFParseException e) {
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

}
