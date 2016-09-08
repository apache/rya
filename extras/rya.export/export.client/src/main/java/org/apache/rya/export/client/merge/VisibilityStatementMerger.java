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

import org.apache.rya.api.domain.RyaStatement;
import org.apache.rya.export.api.MergerException;
import org.apache.rya.export.api.StatementMerger;

import com.google.common.base.Joiner;
import com.google.common.base.Optional;

/**
 * Merges two statements together at the visibility.  Two statements can be
 * merged if the subject, predicate, and object are the same.  A merged statement
 * has an unchanged subject, predicate, and object but a joined visiblity.
 * The visibilities are joined with a logical AND, so a statement with
 * visiblity 'A' and another statement with visibility 'B' will be merged
 * to have visibility 'A&B'
 */
public class VisibilityStatementMerger implements StatementMerger {
    @Override
    public Optional<RyaStatement> merge(final Optional<RyaStatement> parent, final Optional<RyaStatement> child)
            throws MergerException {
        if(parent.isPresent()) {
            final RyaStatement parentStatement = parent.get();
            if(child.isPresent()) {
                final RyaStatement childStatement = child.get();
                final String pVis = new String(parentStatement.getColumnVisibility());
                final String cVis = new String(childStatement.getColumnVisibility());
                String visibility = "";
                final Joiner join = Joiner.on(")&(");
                if(pVis.isEmpty() || cVis.isEmpty()) {
                    visibility = (pVis + cVis).trim();
                } else {
                    visibility = "(" + join.join(pVis, cVis) + ")";
                }
                parentStatement.setColumnVisibility(visibility.getBytes());
                return Optional.of(parentStatement);
            }
            return parent;
        } else if(child.isPresent()) {
            return child;
        }
        return Optional.absent();
    }
}
