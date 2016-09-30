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


import org.junit.Test;

/**
 * Solves the problem of examples getting bugs when tests are passing.
 * Fixers generally don't run the examples or don't know they are affecting the examples.
 * Examples generally create and destroy a mock instance of Accumulo, or a test instance of MongoDB.
 */
public class ExamplesTest {
	/**
	 * Calls main.  Fails if errors are thrown and not caught. parameters are not used.
	 * @throws Exception
	 */
	@Test
	public void RyaDirectExampleTest() throws Exception {
		RyaDirectExample.main(new String[] {});
	}
	/**
	 * Calls main.  Fails if errors are thrown and not caught. parameters are not used.
	 * @throws Exception
	 */
	@Test
	public void RyaClientExampleTest() throws Exception {
		 RyaClientExample.main(new String[] {});
	}
	/**
	 * Calls main.  Fails if errors are thrown and not caught. parameters are not used.
	 * @throws Exception
	 */
	@Test
	public void MongoRyaDirectExampleTest() throws Exception {
		 MongoRyaDirectExample.main(new String[] {});
	}
	/**
	 * Calls main.  Fails if errors are thrown and not caught. parameters are not used.
	 * @throws Exception
	 */
	@Test
	public void EntityDirectExampleTest() throws Exception {
		 EntityDirectExample.main(new String[] {});
	}
}
