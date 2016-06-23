package mvm.rya.dynamodb.iter;
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

import java.io.IOException;
import java.util.Iterator;

import org.calrissian.mango.collect.CloseableIterable;

import mvm.rya.api.domain.RyaStatement;

public class RyaItemCloseableIterable implements CloseableIterable<RyaStatement> {


	private NonCloseableRyaItemIterator iterator;

	public RyaItemCloseableIterable(NonCloseableRyaItemIterator iterator) {
		this.iterator = iterator;
	}

	@Override
	public Iterator<RyaStatement> iterator() {
		// TODO Auto-generated method stub
		return iterator;
	}

	@Override
	public void closeQuietly() {
		//TODO  don't know what to do here
	}

	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
	}

}
