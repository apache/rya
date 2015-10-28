package mvm.rya.indexing.accumulo.freetext;

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

import java.util.SortedSet;
import java.util.TreeSet;

/**
 * A {@link Tokenizer} that splits on whitespace.
 */
public class SimpleTokenizer implements Tokenizer {

	@Override
	public SortedSet<String> tokenize(String sting) {
		SortedSet<String> set = new TreeSet<String>();
		for (String token : sting.split("\\s+")) {
			String t = token.trim().toLowerCase();
			if (!t.isEmpty()) {
				set.add(t);
			}
		}
		return set;
	}
}
