package org.apache.rya.indexing.pcj.fluo.app.provenance;

import static java.util.Objects.requireNonNull;

/**
 * Serializes and deserializes {@link StatementProvenance} objects.
 */
public class StatementProvenanceSerde {

    /**
     * TODO impl, test, doc
     *
     * @param provenance
     * @return
     */
    public byte[] serialize(StatementProvenance provenance) {
        requireNonNull(provenance);

        return null;
    }

    /**
     * TODO impl, test, doc
     *
     * @param bytes
     * @return
     */
    public StatementProvenance deserialize(byte[] bytes) {
        requireNonNull(bytes);



        return null;
    }
}