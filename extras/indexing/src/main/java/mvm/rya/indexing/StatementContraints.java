package mvm.rya.indexing;

/*
 * #%L
 * mvm.rya.indexing.accumulo
 * %%
 * Copyright (C) 2014 Rya
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

import java.util.Set;

import org.openrdf.model.Resource;
import org.openrdf.model.URI;

public class StatementContraints {
	private Resource context = null;
	private Resource subject = null;
	private Set<URI> predicates = null;

	public StatementContraints setContext(Resource context) {
		this.context = context;
		return this;
	}

	public StatementContraints setPredicates(Set<URI> predicates) {
		this.predicates = predicates;
		return this;
	}

	public StatementContraints setSubject(Resource subject) {
		this.subject = subject;
		return this;
	}

	public Resource getContext() {
		return context;
	}

	public Set<URI> getPredicates() {
		return predicates;
	}

	public Resource getSubject() {
		return subject;
	}

	public boolean hasSubject() {
		return subject != null;
	}

	public boolean hasPredicates() {
		return predicates != null && !predicates.isEmpty();
	}

	public boolean hasContext() {
		return context != null;
	}

}
