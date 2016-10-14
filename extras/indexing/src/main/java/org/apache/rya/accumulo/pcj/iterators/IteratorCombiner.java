package org.apache.rya.accumulo.pcj.iterators;

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

import info.aduna.iteration.CloseableIteration;

import java.util.Collection;
import java.util.Iterator;
import java.util.NoSuchElementException;

import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryEvaluationException;

import com.google.common.base.Preconditions;

/**
 *	This {@link CloseableIteration} takes in a list of CloseableIterations
 *	and merges them together into a single CloseableIteration.
 *
 */
public class IteratorCombiner implements
		CloseableIteration<BindingSet, QueryEvaluationException> {


	private Collection<CloseableIteration<BindingSet, QueryEvaluationException>> iterators;
	private Iterator<CloseableIteration<BindingSet, QueryEvaluationException>> iteratorIterator;
	private CloseableIteration<BindingSet, QueryEvaluationException> currIter;
	private boolean isEmpty = false;
	private boolean hasNextCalled = false;
	private BindingSet next;

	public IteratorCombiner(Collection<CloseableIteration<BindingSet, QueryEvaluationException>> iterators) {
		Preconditions.checkArgument(iterators.size() > 0);
		this.iterators = iterators;
		iteratorIterator = iterators.iterator();
		currIter = iteratorIterator.next();
	}

	@Override
	public boolean hasNext() throws QueryEvaluationException {
		if (!hasNextCalled && !isEmpty) {
			while (currIter.hasNext() || iteratorIterator.hasNext()) {
				if(!currIter.hasNext()) {
					currIter = iteratorIterator.next();
				}
				if(!currIter.hasNext()) {
					continue;
				}
				next = currIter.next();
				hasNextCalled = true;
				return true;
			}
			isEmpty = true;
			return false;
		} else if (isEmpty) {
			return false;
		} else {
			return true;
		}
	}

	@Override
	public BindingSet next() throws QueryEvaluationException {
		if (hasNextCalled) {
			hasNextCalled = false;
		} else if (isEmpty) {
			throw new NoSuchElementException();
		} else {
			if (this.hasNext()) {
				hasNextCalled = false;
			} else {
				throw new NoSuchElementException();
			}
		}
		return next;
	}

	@Override
	public void remove() throws QueryEvaluationException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void close() throws QueryEvaluationException {
		for(CloseableIteration<BindingSet, QueryEvaluationException> iterator: iterators) {
			iterator.close();
		}
	}

}
