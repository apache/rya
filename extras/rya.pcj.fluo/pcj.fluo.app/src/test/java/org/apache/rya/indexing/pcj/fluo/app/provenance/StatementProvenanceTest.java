/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.rya.indexing.pcj.fluo.app.provenance;

import static org.junit.Assert.assertEquals;

import java.util.Optional;

import org.apache.rya.indexing.pcj.fluo.app.provenance.StatementProvenance.ConflictingEventException;
import org.apache.rya.indexing.pcj.fluo.app.provenance.StatementProvenance.Operation;
import org.apache.rya.indexing.pcj.fluo.app.provenance.StatementProvenance.StatementEvent;
import org.junit.Test;

/**
 * Tests the methods of {@link StatementProvenance}.
 */
public class StatementProvenanceTest {

    @Test
    public void update_sameEvent() {
        final StatementProvenance provenance = new StatementProvenance();

        // Using the same event must return cleanly.
        final StatementEvent event = new StatementEvent(1L, Operation.INSERT);
        provenance.update(event);
        provenance.update(event);
    }

    @Test(expected = ConflictingEventException.class)
    public void update_conflictingEvent() {
        final StatementProvenance provenance = new StatementProvenance();

        // Both of these events have the same operation number. This isn't possible.
        final StatementEvent insert = new StatementEvent(1L, Operation.INSERT);
        final StatementEvent delete = new StatementEvent(1L, Operation.DELETE);

        provenance.update(insert);
        provenance.update(delete);
    }

    @Test
    public void getMostRecentEvent() {
        // The event with the latest operation number.
        final StatementEvent mostRecent = new StatementEvent(354L, Operation.INSERT);

        // Create a StatementProvenance that includes that event.
        final StatementProvenance provenance = new StatementProvenance();
        provenance.update( new StatementEvent(1L, Operation.INSERT) );
        provenance.update( new StatementEvent(15L, Operation.DELETE) );
        provenance.update( new StatementEvent(7L, Operation.INSERT) );
        provenance.update( mostRecent );
        provenance.update( new StatementEvent(2L, Operation.DELETE) );
        provenance.update( new StatementEvent(9L, Operation.INSERT) );

        // Ensure it is returned as the most recent.
        assertEquals(mostRecent, provenance.getMostRecentEvent().get());
    }

    @Test
    public void getMostRecentEvent_absent() {
        final StatementProvenance provenance = new StatementProvenance();

        // Nothing has been added yet, so make sure empty is returned.
        assertEquals(Optional.empty(), provenance.getMostRecentEvent());
    }
}