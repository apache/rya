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

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.accumulo.core.client.mapreduce.InputTableConfig;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.hadoop.io.Text;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.algebra.Var;

import org.apache.rya.api.RdfCloudTripleStoreConfiguration;
import org.apache.rya.api.RdfCloudTripleStoreConstants.TABLE_LAYOUT;
import org.apache.rya.api.RdfCloudTripleStoreUtils;
import org.apache.rya.api.domain.RyaStatement;
import org.apache.rya.api.query.strategy.ByteRange;
import org.apache.rya.api.query.strategy.TriplePatternStrategy;
import org.apache.rya.api.resolver.RdfToRyaConversions;
import org.apache.rya.api.resolver.RyaTripleContext;
import org.apache.rya.api.utils.NullableStatementImpl;

/**
 * A {@link QueryRuleset} that additionally maps rules to ranges in Accumulo tables. Also enables
 * copying one or more entire tables, independent of the query-derived rules.
 */
public class AccumuloQueryRuleset extends QueryRuleset {
    private final Map<TABLE_LAYOUT, List<Range>> tableRanges = new HashMap<>();
    private final List<String> entireTables = new LinkedList<String>();
    private final RyaTripleContext ryaContext;

    /**
     * Constructs the ruleset and the associated Ranges, given a Configuration that contains a SPARQL query.
     * @param conf Configuration including the query and connection information.
     * @throws IOException if the range can't be resolved
     * @throws QueryRulesetException if the query can't be translated to valid rules
     */
    public AccumuloQueryRuleset(final RdfCloudTripleStoreConfiguration conf) throws IOException, QueryRulesetException {
        // Extract StatementPatterns and conditions from the query
        super(conf);
        // Turn StatementPatterns into Ranges
        ryaContext = RyaTripleContext.getInstance(conf);
        for (final CopyRule rule : rules) {
            final StatementPattern sp = rule.getStatement();
            final Map.Entry<TABLE_LAYOUT, ByteRange> entry = getRange(sp);
            final TABLE_LAYOUT layout = entry.getKey();
            final ByteRange byteRange = entry.getValue();
            final Range range = new Range(new Text(byteRange.getStart()), new Text(byteRange.getEnd()));
            if (!tableRanges.containsKey(layout)) {
                tableRanges.put(layout, new LinkedList<Range>());
            }
            tableRanges.get(layout).add(range);
        }
    }

    /**
     * Turn a single StatementPattern into a Range.
     * @param conf
     * @throws IOException if the range can't be resolved
     */
    private Map.Entry<TABLE_LAYOUT, ByteRange> getRange(final StatementPattern sp) throws IOException {
        final Var context = sp.getContextVar();
        final Statement stmt = new NullableStatementImpl((Resource) sp.getSubjectVar().getValue(),
                (URI) sp.getPredicateVar().getValue(), sp.getObjectVar().getValue(),
                context == null ? null : (Resource) context.getValue());
        final RyaStatement rs = RdfToRyaConversions.convertStatement(stmt);
        final TriplePatternStrategy strategy = ryaContext.retrieveStrategy(rs);
        final Map.Entry<TABLE_LAYOUT, ByteRange> entry =
                strategy.defineRange(rs.getSubject(), rs.getPredicate(), rs.getObject(), rs.getContext(), conf);
        return entry;
    }

    /**
     * Add an instruction to select an entire table, with no restricting rule.
     */
    public void addTable(final String tableName) {
        entireTables.add(tableName);
    }

    /**
     * Get table names and input configurations for each range
     * @return A Map representing each table and {@link InputTableConfig} needed to get all the rows that match the rules.
     */
    public Map<String, InputTableConfig> getInputConfigs() {
        final Map<String, InputTableConfig> configs = new HashMap<>();
        for (final TABLE_LAYOUT layout : tableRanges.keySet()) {
            final String parentTable = RdfCloudTripleStoreUtils.layoutPrefixToTable(layout, conf.getTablePrefix());
            final InputTableConfig config = new InputTableConfig();
            config.setRanges(tableRanges.get(layout));
            configs.put(parentTable, config);
        }
        for (final String tableName : entireTables) {
            final InputTableConfig config = new InputTableConfig();
            final List<Range> ranges = new LinkedList<>();
            ranges.add(new Range());
            config.setRanges(ranges);
            configs.put(tableName, config);
        }
        return configs;
    }

    /**
     * Get the rules that apply to all statements within a Range. The range may not
     * contain every row relevant to the associated rule(s), but every row within the
     * range is relevant to the rule(s).
     * @param layout Defines which table the range is meant to scan
     * @param range The Range of rows in that table
     * @return Any rules in this ruleset that match the given table and contain the given range
     * @throws IOException if the Range can't be resolved
     */
    public List<CopyRule> getRules(final TABLE_LAYOUT layout, final Range range) throws IOException {
        final List<CopyRule> matchingRules = new LinkedList<>();
        for (final CopyRule rule : rules) {
            // Compare the rule to the given range
            final Map.Entry<TABLE_LAYOUT, ByteRange> entry = getRange(rule.getStatement());
            final TABLE_LAYOUT ruleLayout = entry.getKey();
            // If they apply to different tables, they are unrelated.
            if (!ruleLayout.equals(layout)) {
                continue;
            }
            // If the given range is contained in (or equal to) the rule's range, then the
            // rule matches and should be included.
            final ByteRange byteRange = entry.getValue();
            final Range ruleRange = new Range(new Text(byteRange.getStart()), new Text(byteRange.getEnd()));
            if (rangeContainsRange(ruleRange, range)) {
                matchingRules.add(rule);
            }
        }
        return matchingRules;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder(super.toString());
        for (final String fullTableName : entireTables) {
            sb.append("\n\tCopy entire table ").append(fullTableName).append("\n");
        }
        return sb.toString();
    }

    private static boolean rangeContainsRange(final Range r1, final Range r2) {
        // 1. If r1.start is infinite, r1 contains r2.start
        if (!r1.isInfiniteStartKey()) {
            // 2. Otherwise, if r2.start is infinite, r1 can't contain r2
            if (r2.isInfiniteStartKey()) {
                return false;
            }
            final Key start2 = r2.getStartKey();
            // 3. If r2 is inclusive, r1 needs to contain r2's start key.
            if (r2.isStartKeyInclusive()) {
                if (!r1.contains(start2)) {
                    return false;
                }
            }
            // 4. Otherwise, the only failure is if r2's start key comes first (they can be equal)
            else if (start2.compareTo(r1.getStartKey()) < 0) {
                return false;
            }
        }
        // Similar logic for end points
        if (!r1.isInfiniteStopKey()) {
            if (r2.isInfiniteStopKey()) {
                return false;
            }
            final Key end2 = r2.getEndKey();
            if (r2.isEndKeyInclusive()) {
                if (!r1.contains(end2)) {
                    return false;
                }
            }
            else if (end2.compareTo(r1.getEndKey()) > 0) {
                return false;
            }
        }
        return true;
    }
}