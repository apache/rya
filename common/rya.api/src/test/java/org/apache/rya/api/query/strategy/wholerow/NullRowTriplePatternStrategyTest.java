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
package org.apache.rya.api.query.strategy.wholerow;

import org.apache.rya.api.RdfCloudTripleStoreConfiguration;
import org.apache.rya.api.RdfCloudTripleStoreConstants;
import org.apache.rya.api.domain.RyaIRI;
import org.apache.rya.api.domain.RyaType;
import org.apache.rya.api.query.strategy.ByteRange;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Arrays;

import static org.apache.rya.api.RdfCloudTripleStoreConstants.LAST_BYTES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class NullRowTriplePatternStrategyTest {

  public class MockRdfConfiguration extends RdfCloudTripleStoreConfiguration {

    @Override
    public RdfCloudTripleStoreConfiguration clone() {
      return new MockRdfConfiguration();
    }

  }

  public NullRowTriplePatternStrategyTest() {
  }

  @BeforeClass
  public static void setUpClass() {
  }

  @AfterClass
  public static void tearDownClass() {
  }

  @Before
  public void setUp() {
  }

  @After
  public void tearDown() {
  }

  /**
   * Test of getLayout method, of class NullRowTriplePatternStrategy.
   */
  @Test
  public void testGetLayout() {
    NullRowTriplePatternStrategy instance = new NullRowTriplePatternStrategy();
    RdfCloudTripleStoreConstants.TABLE_LAYOUT expResult = RdfCloudTripleStoreConstants.TABLE_LAYOUT.SPO;
    RdfCloudTripleStoreConstants.TABLE_LAYOUT result = instance.getLayout();
    assertEquals(expResult, result);
  }

  /**
   * Test of defineRange method, of class NullRowTriplePatternStrategy.
   * @throws java.lang.Exception
   */
  @Test
  public void testDefineRange() throws Exception {
    RyaIRI subject = null;
    RyaIRI predicate = null;
    RyaType object = null;
    RyaIRI context = null;
    RdfCloudTripleStoreConfiguration conf = new MockRdfConfiguration();
    NullRowTriplePatternStrategy instance = new NullRowTriplePatternStrategy();
    ByteRange expResult = new ByteRange(new byte[]{}, LAST_BYTES);
    ByteRange result = instance.defineRange(subject, predicate, object, context, conf);
    assertEquals(RdfCloudTripleStoreConstants.TABLE_LAYOUT.SPO, instance.getLayout());
    assertTrue(Arrays.equals(expResult.getStart(), result.getStart()));
    assertTrue(Arrays.equals(expResult.getEnd(), result.getEnd()));
  }

  /**
   * Test of handles method, of class NullRowTriplePatternStrategy.
   */
  @Test
  public void testHandles() {
    RyaIRI subject = null;
    RyaIRI predicate = null;
    RyaType object = null;
    RyaIRI context = null;
    NullRowTriplePatternStrategy instance = new NullRowTriplePatternStrategy();
    assertTrue(instance.handles(subject, predicate, object, context));

    RyaIRI uri = new RyaIRI("urn:test#1234");
    assertFalse(instance.handles(uri, predicate, object, context));
    assertFalse(instance.handles(subject, uri, object, context));
    assertFalse(instance.handles(subject, predicate, uri, context));
  }
  
}
