package org.apache.rya.indexing.pcj.fluo.app.provenance;

import java.nio.ByteBuffer;

import org.apache.rya.indexing.pcj.fluo.app.provenance.StatementProvenance.Operation;
import org.apache.rya.indexing.pcj.fluo.app.provenance.StatementProvenance.StatementEvent;

/**
 * TODO test, doc
 */
public class StatementEventSerde {

    /**
     * The length of a {@link StatementEvent} that has been serialized in bytes.
     */
    public static final int BYTE_LENGTH = 9;

    /**
     * TODO test, doc
     *
     * @param event
     * @return
     */
    public byte[] serialize(final StatementEvent event) {
        return ByteBuffer.allocate(BYTE_LENGTH)
            .putLong( event.getOperationNumber() )
            .put( (byte) event.getOperation().getId() )
            .array();
    }

    /**
     * TODO test, doc
     *
     * @param bytes
     * @return
     */
    public StatementEvent deserialize(final byte[] bytes) {
        final ByteBuffer buff = ByteBuffer.wrap(bytes);
        final long operationNumber = buff.getLong();
        final Operation operation = Operation.getById( buff.get() );
        return new StatementEvent(operationNumber, operation);
    }
}