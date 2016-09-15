package mvm.rya.api.client;

import java.nio.file.Path;

import javax.annotation.ParametersAreNonnullByDefault;

/**
 * Loads a local file of RDF statements into an instance of Rya.
 */
@ParametersAreNonnullByDefault
public interface LoadStatementsFile {

    /**
     * Loads a local file of RDF statements into an instance of Rya.
     *
     * @param ryaInstanceName - The name of the Rya instance the statements will be loaded into. (not null)
     * @param statementsFile - A file that holds RDF statements that will be loaded. (not null)
     */
    public void loadStatements(String ryaInstanceName, Path statementsFile);
}