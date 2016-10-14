package org.apache.rya.api.persist.query;

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



import org.apache.rya.api.domain.RyaStatement;
import org.apache.rya.api.domain.RyaURI;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 */
public class RyaQueryTest {

    @Test
    public void testBuildQueryWithOptions() {
        RyaURI subj = new RyaURI("urn:test#1234");
        RyaURI pred = new RyaURI("urn:test#pred");
        RyaURI obj = new RyaURI("urn:test#obj");
        RyaStatement ryaStatement = new RyaStatement(subj, pred, obj);
        String[] auths = {"U,FOUO"};
        long currentTime = System.currentTimeMillis();
        RyaQuery ryaQuery = RyaQuery.builder(ryaStatement).setAuths(auths).setNumQueryThreads(4).setRegexObject("regexObj")
                .setRegexPredicate("regexPred").setRegexSubject("regexSubj").setTtl(100l).setBatchSize(10).
                        setCurrentTime(currentTime).setMaxResults(1000l)
                .build();

        assertNotNull(ryaQuery);
        assertEquals(ryaStatement, ryaQuery.getQuery());
        assertEquals(4, (int) ryaQuery.getNumQueryThreads());
        assertEquals("regexObj", ryaQuery.getRegexObject());
        assertEquals("regexPred", ryaQuery.getRegexPredicate());
        assertEquals("regexSubj", ryaQuery.getRegexSubject());
        assertEquals(100l, (long) ryaQuery.getTtl());
        assertEquals(10, (int) ryaQuery.getBatchSize());
        assertEquals(currentTime, (long) ryaQuery.getCurrentTime());
        assertEquals(1000l, (long) ryaQuery.getMaxResults());
        assertTrue(Arrays.equals(auths, ryaQuery.getAuths()));
    }
}
