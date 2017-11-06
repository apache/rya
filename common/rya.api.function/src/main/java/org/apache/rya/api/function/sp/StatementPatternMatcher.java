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
package org.apache.rya.api.function.sp;

import static java.util.Objects.requireNonNull;

import java.util.Optional;

import org.openrdf.model.Statement;
import org.openrdf.model.Value;
import org.openrdf.query.BindingSet;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.algebra.Var;
import org.openrdf.query.algebra.evaluation.QueryBindingSet;

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * Matches {@link Statement}s against a {@link StatementPattern} and returns {@link BindingSet}s
 * when the statement matched the pattern.
 */
@DefaultAnnotation(NonNull.class)
public class StatementPatternMatcher {

    private final StatementPattern pattern;

    /**
     * Constructs an instance of {@link StatementPatternMatcher}.
     *
     * @param pattern - The pattern that will be matched against. (not null)
     */
    public StatementPatternMatcher(final StatementPattern pattern) {
        this.pattern = requireNonNull(pattern);
    }

    /**
     * Matches a {@link Statement} against the provided {@link StatementPattern} and returns a {@link BindingSet}
     * if the statement matched the pattern.
     *
     * @param statement - The statement that will be matched against the pattern. (not null)
     * @return A {@link BinidngSet} containing the statement's values filled in for the pattern's variables if
     *   the statement's values match the pattern's constants; otherwise empty.
     */
    public Optional<BindingSet> match(final Statement statement) {
        requireNonNull(statement);

        // Setup the resulting binding set that could be built from this Statement.
        final QueryBindingSet bs = new QueryBindingSet();

        if(matchesValue(pattern.getSubjectVar(), statement.getSubject(), bs) &&
                matchesValue(pattern.getPredicateVar(), statement.getPredicate(), bs) &&
                matchesValue(pattern.getObjectVar(), statement.getObject(), bs) &&
                matchesContext(pattern.getContextVar(), statement.getContext(), bs)) {
            return Optional.of(bs);
        } else {
            return Optional.empty();
        }
    }

    /**
     * The following table describes how a Subject, Predicate, and Object Var may be handled for a Statement and a
     * Statement Pattern:
     * <table border=1>
     *     <tr> <th>Pattern's var is constant</th> <th>Effect on resulting BS</th> </tr>
     *     <try> <td>yes</td> <td>Emit a BS if they match, no Context binding</td> </tr>
     *     <try> <td>no</td>  <td>Emit a BS with a binding for the variable</td> </tr>
     * </table>
     *
     * @param var - The statement pattern variable that is being matched. (not null)
     * @param stmtValue - The statement's value for the variable. (not null)
     * @param bs - The binding set that may be updated to include a binding for the variable. (not null)
     * @return {@code true} if he variable and the statement value match, otherwise {@code false},
     */
    private boolean matchesValue(final Var var, final Value stmtValue, final QueryBindingSet bs) {
        requireNonNull(var);
        requireNonNull(stmtValue);
        requireNonNull(bs);

        // If the var is a constant, statement's value must match the var's value.
        if(var.isConstant()) {
            if(!stmtValue.equals(var.getValue())) {
                return false;
            }
        } else {
            // Otherwise it is a variable to be filled in.
            bs.addBinding(var.getName(), stmtValue);
        }

        // Either the value matched the constant or the binding set was updated.
        return true;
    }

    /**
     * The following table describes how Context may be handled for a Statement and a Statement Pattern:
     * <table border=1>
     *   <tr> <th>Pattern's context state</th> <th>Statement has a context value</th> <th>Effect on resulting BS</th></tr>
     *   <tr> <td>not mentioned</td>  <td>yes</td> <td>Emit BS without a Context binding</td> </tr>
     *   <tr> <td>not mentioned</td>  <td>no</td>  <td>Emit BS without a Context binding</td> </tr>
     *   <tr> <td>has a constant</td> <td>yes</td> <td>Emit BS if they match, no Context binding</td> </tr>
     *   <tr> <td>has a constant</td> <td>no</td>  <td>Do not emit a BS</td> </tr>
     *   <tr> <td>has a variable</td> <td>yes</td> <td>Emit BS with Context binding</td> </tr>
     *   <tr> <td>has a variable</td> <td>no</td>  <td>Do not emit a BS</td> </tr>
     * </table>
     *
     * @param cntxVar - The statement pattern's context variable. This may be {@code null} when there is no context
     *   specified for the pattern.
     * @param stmtCntx - The statement's context value. This may be {@code null} when there was no context
     *   specified within the statement.
     * @param bs - The binding set that may be updated to include a context binding. (not null)
     * @return {@code true} if the the pattern's context variable and statement's context matched, otherwise {@code false}.
     */
    private boolean matchesContext(@Nullable final Var cntxVar, @Nullable final Value stmtCntx, final QueryBindingSet bs) {
        if(cntxVar == null) {
            // If there is no context, automatically matches.
            return true;
        } else if(stmtCntx == null) {
            // If no value was provided within the statement, then it does not match.
            return false;
        } else {
            // Otherwise handle it like a normal variable.
            return matchesValue(cntxVar, stmtCntx, bs);
        }
    }
}