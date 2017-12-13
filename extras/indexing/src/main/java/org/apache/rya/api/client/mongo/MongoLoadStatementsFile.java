package org.apache.rya.api.client.mongo;

import static java.util.Objects.requireNonNull;

import java.io.IOException;
import java.nio.file.Path;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.log4j.Logger;
import org.apache.rya.api.client.InstanceDoesNotExistException;
import org.apache.rya.api.client.InstanceExists;
import org.apache.rya.api.client.LoadStatementsFile;
import org.apache.rya.api.client.RyaClientException;
import org.apache.rya.api.persist.RyaDAOException;
import org.apache.rya.mongodb.MongoDBRdfConfiguration;
import org.apache.rya.rdftriplestore.inference.InferenceEngineException;
import org.apache.rya.sail.config.RyaSailFactory;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.sail.SailRepository;
import org.openrdf.repository.sail.SailRepositoryConnection;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFParseException;
import org.openrdf.sail.Sail;
import org.openrdf.sail.SailException;

import com.mongodb.MongoClient;

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * An Mongo implementation of the {@link LoadStatementsFile} command.
 */
@DefaultAnnotation(NonNull.class)
public class MongoLoadStatementsFile extends MongoCommand implements LoadStatementsFile {
    private static final Logger log = Logger.getLogger(MongoLoadStatementsFile.class);

    private final InstanceExists instanceExists;

    /**
     * Constructs an instance of {@link MongoListInstances}.
     *
     * @param connectionDetails
     *            - Details to connect to the server. (not null)
     * @param client
     *            - Provides programmatic access to the instance of Mongo
     *            that hosts Rya instance. (not null)
     */
    public MongoLoadStatementsFile(MongoConnectionDetails connectionDetails, MongoClient client) {
        super(connectionDetails, client);
        instanceExists = new MongoInstanceExists(connectionDetails, client);
    }

    @Override
    public void loadStatements(String ryaInstanceName, Path statementsFile, RDFFormat format) throws InstanceDoesNotExistException, RyaClientException {
        requireNonNull(ryaInstanceName);
        requireNonNull(statementsFile);
        requireNonNull(format);

        // Ensure the Rya Instance exists.
        if (!instanceExists.exists(ryaInstanceName)) {
            throw new InstanceDoesNotExistException(String.format("There is no Rya instance named '%s'.", ryaInstanceName));
        }

        Sail sail = null;
        SailRepository sailRepo = null;
        SailRepositoryConnection sailRepoConn = null;
        // Get a Sail object that is connected to the Rya instance.
        final MongoDBRdfConfiguration ryaConf = getMongoConnectionDetails().build(ryaInstanceName);
        // ryaConf.setFlush(false); //Accumulo version said: RYA-327 should address this hardcoded value.
        try {
            sail = RyaSailFactory.getInstance(ryaConf);
        } catch (SailException | AccumuloException | AccumuloSecurityException | RyaDAOException | InferenceEngineException e) {
            throw new RyaClientException("While getting an sail instance.", e);
        }

        // Load the file.
        sailRepo = new SailRepository(sail);
        try {
            sailRepoConn = sailRepo.getConnection();
            sailRepoConn.add(statementsFile.toFile(), null, format);
        } catch (RDFParseException | RepositoryException | IOException e) {
            throw new RyaClientException("While getting a connection and adding statements from a file.", e);
        }
    }
}
