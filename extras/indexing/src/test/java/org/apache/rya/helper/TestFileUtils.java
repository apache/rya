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
package org.apache.rya.helper;

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableSet;

/**
 * Holds constants relating to the RDF test files that will be used.
 */
public final class TestFileUtils {
    public static final String RDF_FILE_DIR = "/rdf_format_files/";

    /**
     * All the test files that are found in
     * "src/test/resources/rdf_format_files/" with their respective triple
     * counts.
     */
    public static final Set<TestFile> TEST_FILES = ImmutableSet.of(
            new TestFile(RDF_FILE_DIR + "ntriples_data.nt", 3),
            new TestFile(RDF_FILE_DIR + "n3_data.n3", 12),
            new TestFile(RDF_FILE_DIR + "rdfxml_data.owl", 2),
            new TestFile(RDF_FILE_DIR + "turtle_data.ttl", 7),
            new TestFile(RDF_FILE_DIR + "trig_data.trig", 5),
            new TestFile(RDF_FILE_DIR + "trix_data.trix", 3),
            new TestFile(RDF_FILE_DIR + "nquads_data.nq", 2),
            new TestFile(RDF_FILE_DIR + "jsonld_data.jsonld", 3),
            new TestFile(RDF_FILE_DIR + "rdfjson_data.rj", 1),
            new TestFile(RDF_FILE_DIR + "binary_data.brf", 4)
        );

    /**
     * Constant that holds the total number of triples from all the test files
     * held in {@link #TEST_FILES}.
     */
    public static final int TOTAL_TRIPLES =
        TEST_FILES.stream().map(TestFile::getExpectedCount).collect(Collectors.summingInt(Integer::intValue));

    /**
     * Convenience map to get the triple count from the test file path held in
     * {@link #TEST_FILES}.
     */
    public static final Map<String, Integer> FILE_TO_COUNT_MAP =
        TEST_FILES.stream().collect(Collectors.toMap(TestFile::getPath, TestFile::getExpectedCount));

    /**
     * Private constructor to prevent instantiation.
     */
    private TestFileUtils() {
    }
}