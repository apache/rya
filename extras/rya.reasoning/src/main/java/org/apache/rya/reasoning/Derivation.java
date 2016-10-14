package org.apache.rya.reasoning;

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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.TreeSet;

import org.apache.rya.reasoning.mr.ResourceWritable;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;
import org.openrdf.model.Resource;
import org.openrdf.model.Value;

/**
 * Represents the derivation of a fact.
 */
public class Derivation implements WritableComparable<Derivation>, Cloneable {
    // Derivation metadata
    // Which iteration we figured this out
    int iteration = 0;
    // The rule that was used to generate this fact
    OwlRule rule = OwlRule.NONE;
    // The node that directly derived this
    ResourceWritable node = new ResourceWritable();
    // The list of facts that were used to derive this
    TreeSet<Fact> sources = new TreeSet<>();
    // The nodes that directly derived each of the source facts
    Set<Resource> sourceNodes = new HashSet<>();

    // Used to indicate the empty derivation
    static final Derivation NONE = new Derivation();
    static final ResourceWritable EMPTY_NODE = new ResourceWritable();

    /**
     * Default constructor, contains no information
     */
    public Derivation() { }

    /**
     * Constructor that takes in the rule used in the derivation, the iteration
     * it was made, and the node that made it.
     */
    public Derivation(int iteration, OwlRule rule, Resource node) {
        this.iteration = iteration;
        this.rule = rule;
        this.setNode(node);
    }

    public int getIteration() { return iteration; }
    public OwlRule getRule() { return rule; }
    public Resource getNode() { return node.get(); }

    /**
     * Return the set of facts that implied this.
     */
    Set<Fact> getSources() {
        return sources;
    }

    /**
     * Return the set of nodes whose reasoners together produced this.
     */
    Set<Resource> getSourceNodes() {
        return sourceNodes;
    }

    /**
     * Set the direct source node, and add it to the set of sources.
     */
    void setNode(Resource node) {
        this.node.set(node);
        if (node != null) {
            sourceNodes.add(node);
        }
    }

    /**
     * Record that a particular fact was used to derive this.
     */
    public void addSource(Fact predecessor) {
        sources.add(predecessor);
        if (predecessor.isInference()) {
            sourceNodes.addAll(predecessor.getDerivation().sourceNodes);
        }
    }

    /**
     * Record that a set of facts was used to derive this one.
     */
    public void addSources(Set<Fact> predecessors) {
        for (Fact predecessor : predecessors) {
            addSource(predecessor);
        }
    }

    /**
     * Return whether a particular fact is identical to one used to derive this.
     */
    public boolean hasSource(Fact other) {
        for (Fact source : sources) {
            if (source.equals(other) || source.hasSource(other)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Return whether a particular node was responsible for making any inference
     * in any step of the derivation.
     */
    public boolean hasSourceNode(Value v) {
        return v instanceof Resource && sourceNodes.contains((Resource) v);
    }

    /**
     * Generate a String showing this derivation.
     * @param   multiline    Print a multi-line tree as opposed to a nested list
     * @param   schema      Use schema info to better explain BNodes
     */
    public String explain(boolean multiline, Schema schema) {
        return explain(multiline, "", schema);
    }

    /**
     * Generate a String showing this derivation. Doesn't incorporate schema
     * knowledge.
     * @param   multiline    Print a multi-line tree as opposed to a nested list
     */
    public String explain(boolean multiline) {
        return explain(multiline, "", null);
    }

    /**
     * Recursively generate a String to show the derivation.
     */
    String explain(boolean multiline, String prefix, Schema schema) {
        StringBuilder sb = new StringBuilder();
        sb.append("[").append(rule.desc).append("]");
        for (Iterator<Fact> iter = sources.iterator(); iter.hasNext();) {
            Fact source = iter.next();
            if (multiline) {
                sb.append("\n").append(prefix).append("   |");
                sb.append("\n").append(prefix).append("   +---");
                if (iter.hasNext()) {
                    sb.append(source.explain(multiline, prefix + "   |   ", schema));
                }
                else {
                    sb.append(source.explain(multiline, prefix + "       ", schema));
                }
            }
            else {
                sb.append(" (");
                sb.append(source.explain(multiline, "", schema));
                sb.append(")");
            }
        }
        return sb.toString();
    }

    /**
     * Return the full derivation.
     */
    @Override
    public String toString() {
        return explain(true);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        WritableUtils.writeEnum(out, rule);
        // If rule is NONE, no more information needs to be written
        if (rule != OwlRule.NONE) {
            out.writeInt(iteration);
            node.write(out);
            // Recurse on the sources
            out.writeInt(sources.size());
            for (Fact reason : sources) {
                reason.write(out);
            }
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        sources.clear();
        sourceNodes.clear();
        rule = WritableUtils.readEnum(in, OwlRule.class);
        if (rule == OwlRule.NONE) {
            iteration = 0;
            node = EMPTY_NODE;
        }
        else {
            iteration = in.readInt();
            node.readFields(in);
            if (node.get() != null) {
                sourceNodes.add(node.get());
            }
            int numPredecessors = in.readInt();
            for (int i = 0; i < numPredecessors; i++) {
                Fact fact = new Fact();
                fact.readFields(in);
                addSource(fact);
            }
        }
    }

    /**
     * Defines an ordering based on equals. Two Derivations belong together if
     * they have the same direct sources. (The sources' sources are irrelevant.)
     */
    @Override
    public int compareTo(Derivation other) {
        if (this.equals(other)) {
            return 0;
        }
        else if (other == null) {
            return 1;
        }
        // Compare two derivation trees
        int result = this.rule.compareTo(other.rule);
        if (result == 0) {
            // if they have the same rule, derivations must be different
            result = this.sources.size() - other.sources.size();
            if (result == 0) {
                // we know they're not equal (equals must have returned
                // false to get here), so do something arbitrary but
                // guaranteed to be symmetric
                Fact first1 = this.sources.first();
                Fact first2 = other.sources.first();
                result = first1.compareTo(first2);
                while (result == 0) {
                    first1 = this.sources.higher(first1);
                    if (first1 == null) {
                        result = -1;
                    }
                    else {
                        first2 = other.sources.higher(first2);
                        result = first1.compareTo(first2);
                    }
                }
            }
        }
        return result;
    }

    /**
     * Two Derivations are equivalent if they have equivalent sources. Indirect
     * sources may be different.
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || this.getClass() != o.getClass()) {
            return false;
        }
        Derivation other = (Derivation) o;
        if ((rule == null && other.rule != null)
            || (rule != null && !rule.equals(other.rule))) {
            return false;
        }
        if ((sources == null && other.sources != null)
            || (sources != null && !sources.equals(other.sources))) {
            return false;
        }
        return true;
    }

    /**
     * Rule + immediate sources indentify a derivation.
     */
    @Override
    public int hashCode() {
        int result = rule != null ? rule.ordinal() : 0;
        result = 37 * result + (sources != null ? sources.hashCode() : 0);
        return result;
    }

    @Override
    public Derivation clone() {
        Derivation other = new Derivation();
        other.setNode(this.node.get());
        other.addSources(this.sources);
        other.rule = this.rule;
        other.iteration = this.iteration;
        return other;
    }

    /**
     * Get the size of the derivation tree, computed by counting up the number
     * of distinct nodes were used in the derivation. An empty derivation has
     * span 0, a derivation from one reduce step has span 1, and a derivation
     * made by node A whose two sources were derived in the first reduce step
     * by nodes B and C has span 3.
     */
    public int span() {
        return sourceNodes.size();
    }
}
