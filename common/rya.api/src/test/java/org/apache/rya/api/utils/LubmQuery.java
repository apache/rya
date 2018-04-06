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
package org.apache.rya.api.utils;

import static java.util.Objects.requireNonNull;

import java.util.List;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;

/**
 * Holds common LUBM sample test queries.
 */
public enum LubmQuery {
    /**
     * This query bears large input and high selectivity. It queries about just
     * one class and one property and does not assume any hierarchy information
     * or inference.
     */
    LUBM_QUERY_1(
        "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> \n" +
        "PREFIX ub: <http://swat.cse.lehigh.edu/onto/univ-bench.owl#> \n" +
        "SELECT ?X WHERE \n" +
        "{ \n" +
        "  ?X rdf:type ub:GraduateStudent . \n" +
        "  ?X ub:takesCourse <http://www.Department0.University0.edu/GraduateCourse0> \n" +
        "}",
        true
    ),

    /**
     * This query increases in complexity: 3 classes and 3 properties are
     * involved. Additionally, there is a triangular pattern of relationships
     * between the objects involved.
     */
    LUBM_QUERY_2(
        "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> \n" +
        "PREFIX ub: <http://swat.cse.lehigh.edu/onto/univ-bench.owl#> \n" +
        "SELECT ?X ?Y ?Z WHERE \n" +
        "{ \n" +
        "  ?X rdf:type ub:GraduateStudent . \n" +
        "  ?Y rdf:type ub:University . \n" +
        "  ?Z rdf:type ub:Department . \n" +
        "  ?X ub:memberOf ?Z .\n" +
        "  ?Z ub:subOrganizationOf ?Y . \n" +
        "  ?X ub:undergraduateDegreeFrom ?Y \n" +
        "}",
        true
    ),

    /**
     * This query is similar to Query 1 but class Publication has a wide
     * hierarchy.
     */
    LUBM_QUERY_3(
        "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> \n" +
        "PREFIX ub: <http://swat.cse.lehigh.edu/onto/univ-bench.owl#> \n" +
        "SELECT ?X WHERE \n" +
        "{ \n" +
        "  ?X rdf:type ub:Publication . \n" +
        "  ?X ub:publicationAuthor <http://www.Department0.University0.edu/AssistantProfessor0> \n" +
        "}",
        true
    ),

    /**
     * This query has small input and high selectivity. It assumes subClassOf
     * relationship between Professor and its subclasses. Class Professor has a
     * wide hierarchy. Another feature is that it queries about multiple
     * properties of a single class.
     */
    LUBM_QUERY_4(
        "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> \n" +
        "PREFIX ub: <http://swat.cse.lehigh.edu/onto/univ-bench.owl#> \n" +
        "SELECT ?X ?Y1 ?Y2 ?Y3 WHERE \n" +
        "{ \n" +
        "  ?X rdf:type ub:Professor . \n" +
        "  ?X ub:worksFor <http://www.Department0.University0.edu> . \n" +
        "  ?X ub:name ?Y1 . \n" +
        "  ?X ub:emailAddress ?Y2 . \n" +
        "  ?X ub:telephone ?Y3 \n" +
        "}",
        false
    ),

    /**
     * This query assumes subClassOf relationship between Person and its
     * subclasses and subPropertyOf relationship between memberOf and its
     * subproperties. Moreover, class Person features a deep and wide hierarchy.
     */
    LUBM_QUERY_5(
        "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> \n" +
        "PREFIX ub: <http://swat.cse.lehigh.edu/onto/univ-bench.owl#> \n" +
        "SELECT ?X WHERE \n" +
        "{ \n" +
        "  ?X rdf:type ub:Person . \n" +
        "  ?X ub:memberOf <http://www.Department0.University0.edu> \n" +
        "}",
        false
    ),

    /**
     * This query queries about only one class. But it assumes both the explicit
     * subClassOf relationship between UndergraduateStudent and Student and the
     * implicit one between GraduateStudent and Student. In addition, it has
     * large input and low selectivity.
     */
    LUBM_QUERY_6(
        "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> \n" +
        "PREFIX ub: <http://swat.cse.lehigh.edu/onto/univ-bench.owl#> \n" +
        "SELECT ?X WHERE \n" +
        "{ \n" +
        "  ?X rdf:type ub:Student \n" +
        "}",
        false
    ),

    /**
     * This query is similar to Query 6 in terms of class Student but it
     * increases in the number of classes and properties and its selectivity is
     * high.
     */
    LUBM_QUERY_7(
        "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> \n" +
        "PREFIX ub: <http://swat.cse.lehigh.edu/onto/univ-bench.owl#> \n" +
        "SELECT ?X ?Y WHERE \n" +
        "{ \n" +
        "  ?X rdf:type ub:Student . \n" +
        "  ?Y rdf:type ub:Course . \n" +
        "  ?X ub:takesCourse ?Y . \n" +
        "  <http://www.Department0.University0.edu/AssociateProfessor0> ub:teacherOf ?Y \n" +
        "}",
        false
    ),

    /**
     * This query is further more complex than Query 7 by including one more
     * property.
     */
    LUBM_QUERY_8(
        "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> \n" +
        "PREFIX ub: <http://swat.cse.lehigh.edu/onto/univ-bench.owl#> \n" +
        "SELECT ?X ?Y ?Z WHERE \n" +
        "{ \n" +
        "  ?X rdf:type ub:Student . \n" +
        "  ?Y rdf:type ub:Department .\n" +
        "  ?X ub:memberOf ?Y . \n" +
        "  ?Y ub:subOrganizationOf <http://www.University0.edu> . \n" +
        "  ?X ub:emailAddress ?Z \n" +
        "}",
        false
    ),

    /**
     * Besides the aforementioned features of class Student and the wide
     * hierarchy of class Faculty, like Query 2, this query is characterized by
     * the most classes and properties in the query set and there is a
     * triangular pattern of relationships.
     */
    LUBM_QUERY_9(
        "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> \n" +
        "PREFIX ub: <http://swat.cse.lehigh.edu/onto/univ-bench.owl#> \n" +
        "SELECT ?X ?Y ?Z WHERE \n" +
        "{ \n" +
        "  ?X rdf:type ub:Student . \n" +
        "  ?Y rdf:type ub:Faculty . \n" +
        "  ?Z rdf:type ub:Course . \n" +
        "  ?X ub:advisor ?Y . \n" +
        "  ?Y ub:teacherOf ?Z . \n" +
        "  ?X ub:takesCourse ?Z \n" +
        "}",
        false
    ),

    /**
     * This query differs from Query 6, 7, 8 and 9 in that it only requires the
     * (implicit) subClassOf relationship between GraduateStudent and Student,
     * i.e., subClassOf relationship between UndergraduateStudent and Student
     * does not add to the results.
     */
    LUBM_QUERY_10(
        "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> \n" +
        "PREFIX ub: <http://swat.cse.lehigh.edu/onto/univ-bench.owl#> \n" +
        "SELECT ?X WHERE \n" +
        "{ \n" +
        "  ?X rdf:type ub:Student . \n" +
        "  ?X ub:takesCourse <http://www.Department0.University0.edu/GraduateCourse0> \n" +
        "}",
        false
    ),

    /**
     * Query 11, 12 and 13 are intended to verify the presence of certain OWL
     * reasoning capabilities in the system. In this query, property
     * subOrganizationOf is defined as transitive. Since in the benchmark data,
     * instances of ResearchGroup are stated as a sub-organization of a
     * Department individual and the later suborganization of a University
     * individual, inference about the subOrgnizationOf relationship between
     * instances of ResearchGroup and University is required to answer this
     * query. Additionally, its input is small.
     */
    LUBM_QUERY_11(
        "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> \n" +
        "PREFIX ub: <http://swat.cse.lehigh.edu/onto/univ-bench.owl#> \n" +
        "SELECT ?X WHERE \n" +
        "{ \n" +
        "  ?X rdf:type ub:ResearchGroup . \n" +
        "  ?X ub:subOrganizationOf <http://www.University0.edu> \n" +
        "}",
        false
    ),

    /**
     * The benchmark data do not produce any instances of class Chair. Instead,
     * each Department individual is linked to the chair professor of that
     * department by property headOf. Hence this query requires realization,
     * i.e., inference that that professor is an instance of class Chair because
     * he or she is the head of a department. Input of this query is small as
     * well.
     */
    LUBM_QUERY_12(
        "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> \n" +
        "PREFIX ub: <http://swat.cse.lehigh.edu/onto/univ-bench.owl#> \n" +
        "SELECT ?X ?Y WHERE \n" +
        "{ \n" +
        "  ?X rdf:type ub:Chair . \n" +
        "  ?Y rdf:type ub:Department . \n" +
        "  ?X ub:worksFor ?Y . \n" +
        "  ?Y ub:subOrganizationOf <http://www.University0.edu> \n" +
        "}",
        false
    ),

    /**
     * Property hasAlumnus is defined in the benchmark ontology as the inverse
     * of property degreeFrom, which has three subproperties:
     * undergraduateDegreeFrom, mastersDegreeFrom, and doctoralDegreeFrom. The
     * benchmark data state a person as an alumnus of a university using one of
     * these three subproperties instead of hasAlumnus. Therefore, this query
     * assumes subPropertyOf relationships between degreeFrom and its
     * subproperties, and also requires inference about inverseOf.
     */
    LUBM_QUERY_13(
        "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> \n" +
        "PREFIX ub: <http://swat.cse.lehigh.edu/onto/univ-bench.owl#> \n" +
        "SELECT ?X WHERE \n" +
        "{ \n" +
        "  ?X rdf:type ub:Person . \n" +
        "  <http://www.University0.edu> ub:hasAlumnus ?X \n" +
        "}",
        false
    ),

    /**
     * This query is the simplest in the test set. This query represents those
     * with large input and low selectivity and does not assume any hierarchy
     * information or inference.
     */
    LUBM_QUERY_14(
        "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> \n" +
        "PREFIX ub: <http://swat.cse.lehigh.edu/onto/univ-bench.owl#> \n" +
        "SELECT ?X WHERE \n" +
        "{ \n" +
        "  ?X rdf:type ub:UndergraduateStudent \n" +
        "}",
        true
    );

    private String sparqlQuery;
    private boolean isSupported;

    /**
     * Creates a new {@link LubmQuery}.
     * @param sparqlQuery the SPARQL query. (not {@code null})
     * @param isSupported {@code true} if the query type is supported by Rya.
     * {@code false} otherwise.
     */
    private LubmQuery(final String sparqlQuery, final boolean isSupported) {
        this.sparqlQuery = requireNonNull(sparqlQuery);
        this.isSupported = isSupported;
    }

    /**
     * @return the SPARQL query.
     */
    public String getSparqlQuery() {
        return sparqlQuery;
    }

    /**
     * @return {@code true} if the query type is supported by Rya. {@code false}
     * otherwise.
     */
    public boolean isSupported() {
        return isSupported;
    }

    /**
     * @return a {@link List} of every sample {@link LubmQuery} that is
     * supported by Rya.
     */
    public static List<LubmQuery> getSupportedQueries() {
        final Builder<LubmQuery> builder = ImmutableList.builder();
        for (final LubmQuery lubmQuery : LubmQuery.values()) {
            if (lubmQuery.isSupported()) {
                builder.add(lubmQuery);
            }
        }
        return builder.build();
    }

    /**
     * @return a {@link List} of every sample {@link LubmQuery} that is NOT
     * supported by Rya.
     */
    public static List<LubmQuery> getUnsupportedQueries() {
        final Builder<LubmQuery> builder = ImmutableList.builder();
        for (final LubmQuery lubmQuery : LubmQuery.values()) {
            if (!lubmQuery.isSupported()) {
                builder.add(lubmQuery);
            }
        }
        return builder.build();
    }
}