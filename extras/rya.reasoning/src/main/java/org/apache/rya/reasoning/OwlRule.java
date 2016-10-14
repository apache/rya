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

/**
 * Represents the OWL RL/RDF rules used in reasoning.
 */
public enum OwlRule {
    // Schema rules, handled by Schema:
    SCM_CLS,
    SCM_SCO,
    SCM_EQC1,
    SCM_EQC2,
    SCM_OP,
    SCM_DP,
    SCM_SPO,
    SCM_EQP1,
    SCM_EQP2,
    SCM_DOM1,
    SCM_DOM2,
    SCM_RNG1,
    SCM_RNG2,
    SCM_HV,
    SCM_SVF1,
    SCM_SVF2,
    SCM_AVF1,
    SCM_AVF2,

    // Instance rules, handled by LocalReasoner:
    CLS_NOTHING2("No resource can have type owl:Nothing"),
    PRP_IRP("owl:IrreflexiveProperty -- Resource can't be related to itself via irreflexive property"),
    PRP_DOM("rdfs:domain -- Predicate's domain implies subject's type"),
    PRP_RNG("rdfs:range -- Predicate's range implies object's type"),
    CAX_SCO("owl:subClassOf -- Infer supertypes"),
    // Combine prp-inv1 and prp-inv2, since inverseOf is symmetric:
    PRP_INV("owl:inverseOf -- Relation via one property implies reverse relation via the inverse property"),
    PRP_SPO1("rdfs:subPropertyOf -- Relation via subproperty implies relation via superproperty"),
    PRP_SYMP("owl:SymmetricProperty -- Relation via this property is always bidirectional"),
    CLS_SVF2("owl:someValuesFrom(owl:Thing) -- Infer membership in the set of resources related via this property to anything"),
    CLS_HV2("owl:hasValue -- Infer membership in the set of all resources having a specific property+value"),
    CLS_HV1("owl:hasValue -- Infer a specific property+value from the subject's membership in the set of resources with that property+value"),

    // Combine multiple instance triples, handled by LocalReasoner:
    PRP_ASYP("owl:AsymmetricProperty -- Asymmetric property can't be bidirectional"),
    PRP_PDW("owl:propertyDisjointWith -- Two disjoint properties can't relate the same subject and object"),
    CAX_DW("owl:disjointWith -- Resource can't belong to two disjoint classes"),
    CLS_COM("owl:complementOf -- Resource can't belong to both a class and its complement"),
    CLS_MAXC1("owl:maxCardinality(0) -- Max cardinality 0 for this property implies subject can't have any relation via the property"),
    CLS_MAXQC2("owl:maxQualifiedCardinality(0/owl:Thing) -- Max cardinality 0 (with respect to owl:Thing) implies subject can't have any relation via the property"),
    PRP_TRP("owl:TransitiveProperty -- Infer transitive relation"),
    CLS_SVF1("owl:someValuesFrom -- Infer membership in the set of resources related via this property to an instance of the appropriate type"),
    CLS_AVF("owl:allValuesFrom -- Infer the object's type from the subject's membership in the set of resources whose values for this property all belong to one type"),

    NONE("No rule given");

    public String desc;
    OwlRule() { desc = this.toString(); }
    OwlRule(String desc) {
        this.desc = desc;
    }
}
