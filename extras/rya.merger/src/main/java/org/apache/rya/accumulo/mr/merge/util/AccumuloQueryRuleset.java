package org.apache.rya.accumulo.mr.merge.util;

/*
 * #%L
 * org.apache.rya.accumulo.mr.merge
 * %%
 * Copyright (C) 2014 Rya
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

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
    private Map<TABLE_LAYOUT, List<Range>> tableRanges = new HashMap<>();
    private List<String> entireTables = new LinkedList<String>();
    private RyaTripleContext ryaContext;

    /**
     * Constructs the ruleset and the associated Ranges, given a Configuration that contains a SPARQL query.
     * @param conf Configuration including the query and connection information.
     * @throws IOException if the range can't be resolved
     * @throws QueryRulesetException if the query can't be translated to valid rules
     */
    public AccumuloQueryRuleset(RdfCloudTripleStoreConfiguration conf) throws IOException, QueryRulesetException {
        // Extract StatementPatterns and conditions from the query
        super(conf);
        // Turn StatementPatterns into Ranges
        ryaContext = RyaTripleContext.getInstance(conf);
        for (CopyRule rule : rules) {
            StatementPattern sp = rule.getStatement();
            Map.Entry<TABLE_LAYOUT, ByteRange> entry = getRange(sp);
            TABLE_LAYOUT layout = entry.getKey();
            ByteRange byteRange = entry.getValue();
            Range range = new Range(new Text(byteRange.getStart()), new Text(byteRange.getEnd()));
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
    private Map.Entry<TABLE_LAYOUT, ByteRange> getRange(StatementPattern sp) throws IOException {
        Var context = sp.getContextVar();
        Statement stmt = new NullableStatementImpl((Resource) sp.getSubjectVar().getValue(),
                (URI) sp.getPredicateVar().getValue(), sp.getObjectVar().getValue(),
                context == null ? null : (Resource) context.getValue());
        RyaStatement rs = RdfToRyaConversions.convertStatement(stmt);
        TriplePatternStrategy strategy = ryaContext.retrieveStrategy(rs);
        Map.Entry<TABLE_LAYOUT, ByteRange> entry =
                strategy.defineRange(rs.getSubject(), rs.getPredicate(), rs.getObject(), rs.getContext(), conf);
        return entry;
    }

    /**
     * Add an instruction to select an entire table, with no restricting rule.
     */
    public void addTable(String tableName) {
        entireTables.add(tableName);
    }

    /**
     * Get table names and input configurations for each range
     * @return A Map representing each table and {@link InputTableConfig} needed to get all the rows that match the rules.
     */
    public Map<String, InputTableConfig> getInputConfigs() {
        Map<String, InputTableConfig> configs = new HashMap<>();
        for (TABLE_LAYOUT layout : tableRanges.keySet()) {
            String parentTable = RdfCloudTripleStoreUtils.layoutPrefixToTable(layout, conf.getTablePrefix());
            InputTableConfig config = new InputTableConfig();
            config.setRanges(tableRanges.get(layout));
            configs.put(parentTable, config);
        }
        for (String tableName : entireTables) {
            InputTableConfig config = new InputTableConfig();
            List<Range> ranges = new LinkedList<>();
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
    public List<CopyRule> getRules(TABLE_LAYOUT layout, Range range) throws IOException {
        List<CopyRule> matchingRules = new LinkedList<>();
        for (CopyRule rule : rules) {
            // Compare the rule to the given range
            Map.Entry<TABLE_LAYOUT, ByteRange> entry = getRange(rule.getStatement());
            TABLE_LAYOUT ruleLayout = entry.getKey();
            // If they apply to different tables, they are unrelated.
            if (!ruleLayout.equals(layout)) {
                continue;
            }
            // If the given range is contained in (or equal to) the rule's range, then the
            // rule matches and should be included.
            ByteRange byteRange = entry.getValue();
            Range ruleRange = new Range(new Text(byteRange.getStart()), new Text(byteRange.getEnd()));
            if (rangeContainsRange(ruleRange, range)) {
                matchingRules.add(rule);
            }
        }
        return matchingRules;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder(super.toString());
        for (String fullTableName : entireTables) {
            sb.append("\n\tCopy entire table ").append(fullTableName).append("\n");
        }
        return sb.toString();
    }

    private static boolean rangeContainsRange(Range r1, Range r2) {
        // 1. If r1.start is infinite, r1 contains r2.start
        if (!r1.isInfiniteStartKey()) {
            // 2. Otherwise, if r2.start is infinite, r1 can't contain r2
            if (r2.isInfiniteStartKey()) {
                return false;
            }
            Key start2 = r2.getStartKey();
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
            Key end2 = r2.getEndKey();
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