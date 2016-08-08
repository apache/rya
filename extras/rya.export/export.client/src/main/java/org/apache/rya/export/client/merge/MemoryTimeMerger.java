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
package org.apache.rya.export.client.merge;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Date;
import java.util.Iterator;

import org.apache.rya.export.api.Merger;
import org.apache.rya.export.api.StatementMerger;
import org.apache.rya.export.api.parent.MergeParentMetadata;
import org.apache.rya.export.api.parent.ParentMetadataDoesNotExistException;
import org.apache.rya.export.api.parent.ParentMetadataExistsException;
import org.apache.rya.export.api.parent.ParentMetadataRepository;
import org.apache.rya.export.api.store.AddStatementException;
import org.apache.rya.export.api.store.RyaStatementStore;

import mvm.rya.api.domain.RyaStatement;

/**
 * An in memory {@link Merger}.  Merges {@link RyaStatement}s from a parent
 * to a child.  The statements merged will be any that have a timestamp after
 * the provided time.  If there are any conflicting statements, the provided
 * {@link StatementMerger} will merge the statements and produce the desired
 * {@link RyaStatement}.
 */
public class MemoryTimeMerger implements Merger {
    final RyaStatementStore parentStore;
    final ParentMetadataRepository parentMetadata;
    final RyaStatementStore childStore;
    final ParentMetadataRepository childMetadata;
    final StatementMerger statementMerger;
    final Date timestamp;
    final String ryaInstanceName;

    /**
     * Creates a new {@link MemoryTimeMerger} to merge the statements from the parent to a child.
     * @param parentStore
     * @param childStore
     * @param childMetadata
     * @param parentMetadata
     * @param statementMerger
     * @param timestamp - The timestamp from which all parent statements will be merged into the child.
     */
    public MemoryTimeMerger(final RyaStatementStore parentStore, final RyaStatementStore childStore,
            final ParentMetadataRepository parentMetadata, final ParentMetadataRepository childMetadata,
            final StatementMerger statementMerger, final Date timestamp, final String ryaInstanceName) {
        this.parentStore = checkNotNull(parentStore);
        this.parentMetadata = checkNotNull(parentMetadata);
        this.childStore = checkNotNull(childStore);
        this.childMetadata = checkNotNull(childMetadata);
        this.statementMerger = checkNotNull(statementMerger);
        this.timestamp = checkNotNull(timestamp);
        this.ryaInstanceName = checkNotNull(ryaInstanceName);
    }

    @Override
    public void runJob() {
        //check the parent for a parent metadata repo
        if(parentMetadata != null) {
            //if has one, check to see if the child is the parent's parent
            try {
                if(parentMetadata.get().getRyaInstanceName().equals(ryaInstanceName)) {
                    //  if it is, we are importing

                } else {
                    //  else we are doing another layer of export
                    //    do export
                }
            } catch (final ParentMetadataDoesNotExistException e) {
            }
        } else {
            try {
                //else we are exporting
                //  do export
                export();
            } catch (final ParentMetadataExistsException e) {
                e.printStackTrace();
            }
        }
    }

    private void export() throws ParentMetadataExistsException {
        //setup parent metadata repo in the child
        final MergeParentMetadata metadata = new MergeParentMetadata.Builder()
            .setRyaInstanceName(ryaInstanceName)
            .setTimestamp(timestamp)
            .build();
        childMetadata.set(metadata);

        //fetch all statements after timestamp from the parent
        final Iterator<RyaStatement> statements = parentStore.fetchStatements();
        //add all the statements to the child
        while(statements.hasNext()) {
            try {
                childStore.addStatement(statements.next());
            } catch (final AddStatementException e) {
                e.printStackTrace();
            }
        }
    }

    private void importStatements() {
        //fetch all statements from the parent
        //fetch all statements from the child
        //while timestamp is less than copy time in parent repo
        //  if parent does not contain a statement that child does
        //    delete that statement from the child
        //  if parent contains a statement that child does not
        //    add statement
        //  else  merge

        //while timestamp is greater than copy time in parent repo
        //  if parent does not contain a statement that child does
        //    ignore
        //  if parent contains a statement that child does not
        //    add statement
        //  else merge
    }
}
