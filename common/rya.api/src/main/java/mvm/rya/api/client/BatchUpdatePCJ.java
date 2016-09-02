package mvm.rya.api.client;

import javax.annotation.ParametersAreNonnullByDefault;

/**
 * Batch update a PCJ index.
 */
@ParametersAreNonnullByDefault
public interface BatchUpdatePCJ {

    /**
     * Batch update a specific PCJ index using the {@link Statement}s that are
     * currently in the Rya instance.
     *
     * @param ryaInstanceName - The Rya instance whose PCJ will be updated. (not null)
     * @param pcjId - Identifies the PCJ index to update. (not null)
     * @throws InstanceDoesNotExistException No instance of Rya exists for the provided name.
     * @throws PCJDoesNotExistException No PCJ exists for the provided PCJ ID.
     * @throws RyaClientException Something caused the command to fail.
     */
    public void batchUpdate(String ryaInstanceName, String pcjId) throws InstanceDoesNotExistException, PCJDoesNotExistException, RyaClientException;
}