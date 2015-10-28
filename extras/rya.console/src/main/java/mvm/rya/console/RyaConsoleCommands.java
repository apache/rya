package mvm.rya.console;


import info.aduna.iteration.CloseableIteration;

import java.io.FileInputStream;
import java.io.StringReader;
import java.util.Formatter;
import java.util.Locale;
import java.util.logging.Level;
import java.util.logging.Logger;

import mvm.rya.accumulo.AccumuloRdfConfiguration;
import mvm.rya.accumulo.AccumuloRyaDAO;
import mvm.rya.api.RdfCloudTripleStoreConfiguration;
import mvm.rya.api.domain.RyaStatement;
import mvm.rya.api.domain.RyaURI;
import mvm.rya.api.persist.RyaDAO;
import mvm.rya.api.persist.RyaDAOException;
import mvm.rya.api.persist.query.RyaQueryEngine;
import mvm.rya.api.resolver.RdfToRyaConversions;
import mvm.rya.api.resolver.RyaContext;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.openrdf.model.Statement;
import org.openrdf.rio.RDFHandler;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.RDFParser;
import org.openrdf.rio.ntriples.NTriplesParserFactory;
import org.springframework.shell.core.CommandMarker;
import org.springframework.shell.core.annotation.CliAvailabilityIndicator;
import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;
import org.springframework.stereotype.Component;

@Component
public class RyaConsoleCommands implements CommandMarker {

    private static final NTriplesParserFactory N_TRIPLES_PARSER_FACTORY = new NTriplesParserFactory();

    protected final Logger LOG = Logger.getLogger(getClass().getName());

    private RyaContext ryaContext = RyaContext.getInstance();
    private RyaDAO ryaDAO;
    private RDFParser ntrips_parser = null;

    public RyaConsoleCommands() {
        ntrips_parser = N_TRIPLES_PARSER_FACTORY.getParser();
        ntrips_parser.setRDFHandler(new RDFHandler() {

            public void startRDF() throws RDFHandlerException {

            }

            public void endRDF() throws RDFHandlerException {

            }

            public void handleNamespace(String s, String s1) throws RDFHandlerException {

            }

            public void handleStatement(Statement statement) throws RDFHandlerException {
                try {
                    RyaStatement ryaStatement = RdfToRyaConversions.convertStatement(statement);
                    ryaDAO.add(ryaStatement);
                } catch (Exception e) {
                    throw new RDFHandlerException(e);
                }
            }

            public void handleComment(String s) throws RDFHandlerException {

            }
        });
    }

    /**
     * commands:
     * 1. connect(instance, user, password, zk)
     * 1.a. disconnect
     * 2. query
     * 3. add
     */

    @CliAvailabilityIndicator({"connect"})
    public boolean isConnectAvailable() {
        return true;
    }

    @CliAvailabilityIndicator({"qt", "add", "load", "disconnect"})
    public boolean isCommandAvailable() {
        return ryaDAO != null;
    }

    @CliCommand(value = "qt", help = "Query with Triple Pattern")
    public String queryTriple(
            @CliOption(key = {"subject"}, mandatory = false, help = "Subject") final String subject,
            @CliOption(key = {"predicate"}, mandatory = false, help = "Predicate") final String predicate,
            @CliOption(key = {"object"}, mandatory = false, help = "Object") final String object,
            @CliOption(key = {"context"}, mandatory = false, help = "Context") final String context,
            @CliOption(key = {"maxResults"}, mandatory = false, help = "Maximum Number of Results", unspecifiedDefaultValue = "100") final String maxResults
    ) {
        try {
            RdfCloudTripleStoreConfiguration conf = ryaDAO.getConf().clone();
            if (maxResults != null) {
                conf.setLimit(Long.parseLong(maxResults));
            }
            RyaQueryEngine queryEngine = ryaDAO.getQueryEngine();
            CloseableIteration<RyaStatement, RyaDAOException> query =
                    queryEngine.query(new RyaStatement(
                            (subject != null) ? (new RyaURI(subject)) : null,
                            (predicate != null) ? (new RyaURI(predicate)) : null,
                            (object != null) ? (new RyaURI(object)) : null,
                            (context != null) ? (new RyaURI(context)) : null), conf);
            StringBuilder sb = new StringBuilder();
            Formatter formatter = new Formatter(sb, Locale.US);
            String format = "%-40s %-40s %-40s %-40s\n";
            formatter.format(format, "Subject", "Predicate",
                    "Object", "Context");
            while (query.hasNext()) {
                RyaStatement next = query.next();
                formatter.format(format, next.getSubject().getData(), next.getPredicate().getData(),
                        next.getObject().getData(), (next.getContext() != null) ? (next.getContext().getData()) : (null));
                sb.append("\n");
            }
            return sb.toString();
        } catch (Exception e) {
            LOG.log(Level.SEVERE, "", e);
        }
        return "";
    }

    @CliCommand(value = "load", help = "Load file")
    public void load(
            @CliOption(key = {"", "file"}, mandatory = true, help = "File of ntriples rdf to load") final String file
    ) {
        //diff formats?
        //diff types of urls
        try {
            ntrips_parser.parse(new FileInputStream(file), "");
        } catch (Exception e) {
            LOG.log(Level.SEVERE, "", e);
        }
    }

    @CliCommand(value = "add", help = "Add Statement")
    public void add(
            @CliOption(key = {"", "statement"}, mandatory = true, help = "Statement in NTriples format") final String statement) {
        try {
            ntrips_parser.parse(new StringReader(statement), "");
        } catch (Exception e) {
            LOG.log(Level.SEVERE, "", e);
        }
    }

    @CliCommand(value = "connect", help = "Connect to Rya Triple Store")
    public String connect(
            @CliOption(key = {"instance"}, mandatory = true, help = "Accumulo Instance") final String instance,
            @CliOption(key = {"user"}, mandatory = true, help = "Accumulo User") final String user,
            @CliOption(key = {"pwd"}, mandatory = true, help = "Accumulo Pwd") final String pwd,
            @CliOption(key = {"zk"}, mandatory = true, help = "Accumulo Zk (zk=mock for the mock instance)") final String zk,
            @CliOption(key = {"pre"}, mandatory = false, help = "Accumulo table prefix", unspecifiedDefaultValue = "rya_") final String pre) {
        try {
            //using Cloudbase
            Connector connector = null;
            AccumuloRyaDAO cryaDao = new AccumuloRyaDAO();
            if ("mock".equals(zk)) {
                //mock instance
                connector = new MockInstance(instance).getConnector(user, pwd);
            } else {
                connector = new ZooKeeperInstance(instance, zk).getConnector(user, pwd);
            }

            cryaDao.setConnector(connector);
            AccumuloRdfConfiguration configuration = new AccumuloRdfConfiguration();
            configuration.setTablePrefix(pre);
            cryaDao.setConf(configuration);
            cryaDao.init();
            this.ryaDAO = cryaDao;
            return "Connected to Accumulo";
        } catch (Exception e) {
            LOG.log(Level.SEVERE, "", e);
        }
        return "";
    }

    @CliCommand(value = "disconnect", help = "Disconnect from Rya Store")
    public String disconnect() {
        if (ryaDAO == null) {
            return "Command is not available because Rya is not connected. Please 'connect' first.";
        }
        try {
            this.ryaDAO.destroy();
            this.ryaDAO = null;
        } catch (RyaDAOException e) {
            LOG.log(Level.SEVERE, "", e);
        }
        return "";
    }

    public RyaDAO getRyaDAO() {
        return ryaDAO;
    }

    public void setRyaDAO(RyaDAO ryaDAO) {
        this.ryaDAO = ryaDAO;
    }
}