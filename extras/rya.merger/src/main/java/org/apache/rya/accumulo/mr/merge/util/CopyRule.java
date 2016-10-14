/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.rya.accumulo.mr.merge.util;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.openrdf.model.Statement;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.algebra.And;
import org.openrdf.query.algebra.QueryModelNode;
import org.openrdf.query.algebra.QueryModelNodeBase;
import org.openrdf.query.algebra.QueryModelVisitor;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.algebra.ValueConstant;
import org.openrdf.query.algebra.ValueExpr;
import org.openrdf.query.algebra.Var;
import org.openrdf.query.algebra.evaluation.EvaluationStrategy;
import org.openrdf.query.algebra.evaluation.QueryBindingSet;
import org.openrdf.query.algebra.evaluation.ValueExprEvaluationException;
import org.openrdf.query.algebra.helpers.QueryModelVisitorBase;

import org.apache.rya.accumulo.mr.merge.util.QueryRuleset.QueryRulesetException;

/**
 * A rule that defines a subset of statements to copy at the RDF level. Consists of a
 * statement pattern and an optional filter expression.
 */
public class CopyRule extends QueryModelNodeBase {
    private static final ValueConstant TRUE = new ValueConstant(ValueFactoryImpl.getInstance().createLiteral(true));

    private static final String SUFFIX = UUID.randomUUID().toString();
    private static final Var SUBJ_VAR = new Var("subject_" + SUFFIX);
    private static final Var PRED_VAR = new Var("predicate_" + SUFFIX);
    private static final Var OBJ_VAR = new Var("object_" + SUFFIX);
    private static final Var CON_VAR = new Var("context_" + SUFFIX);
    private static final Var UNDEFINED_VAR = new Var("undefined_" + SUFFIX);

    /**
     * Return whether a concrete statement satisfies a condition.
     * @param stmt A statement from the input
     * @param condition The condition to test. Expects variables expressed using the standard variable
     * names used by this class, so it must be a rule's condition or derived from rules' conditions.
     * @param strategy The evaluation strategy to apply
     * @return True if the statement matches the condition
     * @throws ValueExprEvaluationException
     * @throws QueryEvaluationException
     */
    public static boolean accept(final Statement stmt, final ValueExpr condition, final EvaluationStrategy strategy)
            throws ValueExprEvaluationException, QueryEvaluationException {
        final QueryBindingSet bindings = new QueryBindingSet();
        bindings.addBinding(SUBJ_VAR.getName(), stmt.getSubject());
        bindings.addBinding(PRED_VAR.getName(), stmt.getPredicate());
        bindings.addBinding(OBJ_VAR.getName(), stmt.getObject());
        if (stmt.getContext() != null) {
            bindings.addBinding(CON_VAR.getName(), stmt.getContext());
        }
        return strategy.isTrue(condition, bindings);
    }

    /**
     * Detect that a condition is trivial and can be replaced. This may mean it is always
     * true, or that it contains undefined variables and therefore we must assume it is
     * true or risk missing relevant statements.
     * @param expr Condition to be evaluated
     * @return true if the condition is trivial
     */
    private static boolean trivialCondition(final ValueExpr expr) {
        // If the expression is null or the constant "true":
        if (expr == null || expr.equals(TRUE)) {
            return true;
        }
        // If the expression contains undefined variables:
        final VarSearchVisitor visitor = new VarSearchVisitor(UNDEFINED_VAR.getName());
        expr.visit(visitor);
        if (visitor.found) {
            return true;
        }
        // Otherwise, the condition is non-trivial.
        return false;
    }

    /**
     * Visitor that checks a tree for the existence of a given variable name.
     */
    private static class VarSearchVisitor extends QueryModelVisitorBase<RuntimeException> {
        boolean found = false;
        private final String queryVar;
        public VarSearchVisitor(final String queryVar) {
            this.queryVar = queryVar;
        }
        @Override
        public void meet(final Var var) {
            if (queryVar.equals(var.getName())) {
                found = true;
            }
        }
        @Override
        public void meetNode(final QueryModelNode node) {
            if (!found) {
                node.visitChildren(this);
            }
        }
    }

    /**
     * Visitor that standardizes variables to canonical names and restructures
     * operators to preserve meaningful expressions while eliminating undefined
     * conditions.
     */
    private static class RuleVisitor extends QueryModelVisitorBase<RuntimeException> {
        private final CopyRule rule;
        RuleVisitor(final CopyRule rule) {
            this.rule = rule;
        }
        @Override
        public void meet(final Var node) {
            final String oldName = node.getName();
            if (rule.varMap.containsKey(oldName)) {
                node.setName(rule.varMap.get(oldName).getName());
            }
            else {
                if (node.hasValue() || node.equals(SUBJ_VAR) || node.equals(PRED_VAR) || node.equals(OBJ_VAR) || node.equals(CON_VAR)) {
                    return;
                }
                node.setName(UNDEFINED_VAR.getName());
            }
        }
        /**
         * If we can't evaluate one half of an AND by looking at this statement alone, it might
         * turn out to be true in the full graph, so the statement is relevant if the half we
         * can evaluate is true. If we can't evaluate either half, then the AND is useless and
         * we must assume the statement is relevant. Otherwise, keep both sides.
         */
        @Override
        public void meet(final And expr) {
            final ValueExpr left = expr.getLeftArg();
            final ValueExpr right = expr.getRightArg();
            left.visit(this);
            right.visit(this);
            final QueryModelNode parent = expr.getParentNode();
            if (trivialCondition(left)) {
                if (trivialCondition(right)) {
                    // Both sides are trivial; replace whole node
                    parent.replaceChildNode(expr, null);
                }
                else {
                    // Left side trivial, right side good; replace node with right arg
                    parent.replaceChildNode(expr, right);
                }
            }
            else if (trivialCondition(right)) {
                // Left side good, right side trivial; replace node with left arg
                parent.replaceChildNode(expr, left);
            }
            // Otherwise, both sides are useful
        }
    }

    private final StatementPattern statement;
    private ValueExpr condition;
    private final Map<String, Var> varMap = new HashMap<>();
    private final RuleVisitor visitor = new RuleVisitor(this);

    /**
     * Instantiate a rule containing a StatementPattern, renaming any variables to canonical
     * subject/predicate/object forms and saving the mappings from the original variable names.
     * @param sp StatementPattern defining a set of triples to match
     */
    public CopyRule(final StatementPattern sp) throws QueryRulesetException {
        statement = sp;
        final Var subjVar = statement.getSubjectVar();
        final Var predVar = statement.getPredicateVar();
        final Var objVar = statement.getObjectVar();
        final Var conVar = statement.getContextVar();
        int variables = 0;
        if (subjVar == null || !subjVar.hasValue()) {
            sp.setSubjectVar(SUBJ_VAR);
            if (subjVar != null) {
                varMap.put(subjVar.getName(), SUBJ_VAR);
            }
            variables++;
        }
        if (predVar == null || !predVar.hasValue()) {
            sp.setPredicateVar(PRED_VAR);
            if (predVar != null) {
                varMap.put(predVar.getName(), PRED_VAR);
            }
            variables++;
        }
        if (objVar == null || !objVar.hasValue()) {
            sp.setObjectVar(OBJ_VAR);
            if (objVar != null) {
                varMap.put(objVar.getName(), OBJ_VAR);
            }
            variables++;
        }
        if (variables == 3) {
            throw new QueryRulesetException("Statement pattern with no constants would match every statement:\n" + sp);
        }
        if (conVar != null && !conVar.hasValue()) {
            sp.setContextVar(CON_VAR);
            varMap.put(conVar.getName(), CON_VAR);
        }
    }

    /**
     * Set the complete condition.
     */
    private void setCondition(final ValueExpr newCondition) {
        condition = newCondition;
        condition.setParentNode(this);
    }

    /**
     * Constrain a rule with a filter condition. If there are already conditions on the rule, they
     * will be ANDed together. If this rule doesn't define all the variables used in the condition
     * (because the rule only matches one statement pattern), it will assume those portions match,
     * so that we are guaranteed to include all relevant statements.
     * @param condition A boolean filter expression
     */
    public void addCondition(final ValueExpr condition) {
        final ValueExpr newCondition = condition.clone();
        if (this.condition == null) {
            setCondition(newCondition);
        }
        else {
            setCondition(new And(this.condition, newCondition));
        }
        this.condition.visit(visitor);
        // If, after rewriting, the condition still contains undefined variables, we can't
        // meaningfully apply it to reject statements.
        if (trivialCondition(this.condition)) {
            this.condition = null;
        }
    }

    /**
     * Get the rule's boolean filter condition, if any.
     * @return a ValueExpr that can be applied to any matching statements, or null.
     */
    public ValueExpr getCondition() {
        return condition;
    }

    /**
     * Get the rule's statement pattern.
     * @return A StatementPattern that defines which statements match the rule.
     */
    public StatementPattern getStatement() {
        return statement;
    }

    /**
     * Validate that this is a non-trivial rule. A trivial rule consists of
     * a statement pattern whose subject, predicate, and object are all
     * variables (therefore it would match every statement).
     * @return true if the rule is valid (non-trivial)
     */
    void validate() throws QueryRulesetException {
        if (!(statement.getSubjectVar().isConstant()
                || statement.getPredicateVar().isConstant()
                || statement.getObjectVar().isConstant())) {
            throw new QueryRulesetException("Statement pattern with no constants would match every statement:\n" + statement.toString());
        }
    }

    /**
     * Replace a node in the rule's condition with some replacement. If
     * @param current If this is found in the condition, apply the replacement
     * @param replacement Replace with this. If replacing the top-level node, the replacement
     *      must be a ValueExpr or null.
     */
    @Override
    public void replaceChildNode(final QueryModelNode current, final QueryModelNode replacement) {
        if (current.equals(condition) && replacement instanceof ValueExpr) {
            setCondition(((ValueExpr) replacement).clone());
        }
        else if (current.equals(condition) && replacement == null) {
            condition = null;
        }
        else if (condition != null) {
            condition.replaceChildNode(current, replacement.clone());
        }
    }

    /**
     * Apply a visitor to both the statement and any conditions.
     */
    @Override
    public <X extends Exception> void visit(final QueryModelVisitor<X> visitor) throws X {
        if (statement != null) {
            statement.visit(visitor);
        }
        if (condition != null) {
            condition.visit(visitor);
        }
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder(statement.toString().trim());
        if (condition != null) {
            sb.append("\n   Condition:\n   \t");
            sb.append(condition.toString().trim().replace("\n", "\n   \t"));
        }
        return sb.toString();
    }

    @Override
    public boolean equals(final Object obj) {
        if (!obj.getClass().equals(CopyRule.class)) {
            return false;
        }
        final CopyRule other = (CopyRule) obj;
        if ((statement != null && !statement.equals(other.statement))
                || (statement == null && other.statement != null)) {
            return false;
        }
        if ((condition != null && !condition.equals(other.condition))
                || (condition == null && other.condition != null)) {
            return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        int result = statement == null ? 0 : statement.hashCode();
        result += result * 41 + (condition == null ? 0 : condition.hashCode());
        return result;
    }

    /**
     * Detect whether this rule is at least as general as another.
     * @param other Rule to compare against
     * @return true if this rule will necessarily match everything the other rule would.
     */
    public boolean isGeneralizationOf(final CopyRule other) {
        if (statement == null || other.statement == null) {
            return false;
        }
        // If each component of the statement and the condition are at least as general
        // as the other rule's, then this rule is at least as general.
        return varIsGeneralization(statement.getSubjectVar(), other.statement.getSubjectVar())
                && varIsGeneralization(statement.getPredicateVar(), other.statement.getPredicateVar())
                && varIsGeneralization(statement.getObjectVar(), other.statement.getObjectVar())
                && varIsGeneralization(statement.getContextVar(), other.statement.getContextVar())
                && (condition == null || condition.equals(other.condition));
    }

    /**
     * Determine whether the first variable is at least as general as the second.
     */
    private static boolean varIsGeneralization(final Var first, final Var second) {
        if (first == null || !first.hasValue()) {
            // if first is a variable, it is at least as general
            return true;
        }
        // Otherwise, it is only at least as general if they are the same value
        return second != null && first.getValue().equals(second.getValue());
    }
}
