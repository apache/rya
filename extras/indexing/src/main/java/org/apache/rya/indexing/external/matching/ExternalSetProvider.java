/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.rya.indexing.external.matching;
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
import java.util.List;

import org.openrdf.query.algebra.evaluation.impl.ExternalSet;

/**
 * Interface for extracting {@link ExternalSet}s from specified {@link QuerySegment}s.
 *
 * @param <T> - Extension of ExternalSet class
 */
public interface ExternalSetProvider<T extends ExternalSet> {

    /**
     * Extract all {@link ExternalSet}s from specified QuerySegment.
     *
     * @param segment
     * @return - List of ExternalSets
     */
    public List<T> getExternalSets(QuerySegment<T> segment);

    /**
     * Extract an Iterator over Lists of ExternalSets. This allows an ExtenalSetProvider to pass back
     * different combinations of ExternalSets for the purposes of query optimization.
     *
     * @param segment
     * @return - Iterator over different combinations of ExternalSets
     */
    public Iterator<List<T>> getExternalSetCombos(QuerySegment<T> segment);

}
