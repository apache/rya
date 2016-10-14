package org.apache.rya.mongodb.iter;

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


import java.util.Iterator;

import org.apache.rya.api.domain.RyaStatement;
import org.apache.rya.api.persist.RyaDAOException;

public class NonCloseableRyaStatementCursorIterator implements Iterator<RyaStatement> {

	RyaStatementCursorIterator iterator;
	
	@Override
	public boolean hasNext() {
		return iterator.hasNext();
	}

	@Override
	public RyaStatement next() {
		return iterator.next();
	}

	public NonCloseableRyaStatementCursorIterator(
			RyaStatementCursorIterator iterator) {
		this.iterator = iterator;
	}

	@Override
	public void remove() {
		try {
			iterator.remove();
		} catch (RyaDAOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
