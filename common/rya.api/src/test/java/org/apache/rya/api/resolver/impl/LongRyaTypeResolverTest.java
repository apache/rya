package org.apache.rya.api.resolver.impl;

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



import org.apache.rya.api.domain.RyaType;
import org.junit.Test;
import org.openrdf.model.vocabulary.XMLSchema;

import java.util.Random;

import static junit.framework.Assert.assertEquals;

/**
 * Date: 9/7/12
 * Time: 2:53 PM
 */
public class LongRyaTypeResolverTest {

    @Test
    public void testSerialization() throws Exception {
        Long i = randomLong();
        byte[] serialize = new LongRyaTypeResolver().serialize(new RyaType(XMLSchema.LONG, i.toString()));
        assertEquals(i, new Long(new LongRyaTypeResolver().deserialize(serialize).getData()));
    }

    private long randomLong() {
        return new Random(System.currentTimeMillis()).nextLong();
    }

}
