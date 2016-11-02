/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rya.mongodb.document.visibility;

import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Text;

/**
 * Validate the document visibility is a valid expression and set the visibility for a Mutation. See {@link DocumentVisibility#DocumentVisibility(byte[])} for the
 * definition of an expression.
 *
 * <p>
 * The expression is a sequence of characters from the set [A-Za-z0-9_-.] along with the binary operators "&amp;" and "|" indicating that both operands are
 * necessary, or the either is necessary. The following are valid expressions for visibility:
 *
 * <pre>
 * A
 * A|B
 * (A|B)&amp;(C|D)
 * orange|(red&amp;yellow)
 * </pre>
 *
 * <p>
 * The following are not valid expressions for visibility:
 *
 * <pre>
 * A|B&amp;C
 * A=B
 * A|B|
 * A&amp;|B
 * ()
 * )
 * dog|!cat
 * </pre>
 *
 * <p>
 * In addition to the base set of visibilities, any character can be used in the expression if it is quoted. If the quoted term contains '&quot;' or '\', then
 * escape the character with '\'. The {@link #quote(String)} method can be used to properly quote and escape terms automatically. The following is an example of
 * a quoted term:
 *
 * <pre>
 * &quot;A#C&quot; &amp; B
 * </pre>
 */
public class DocumentVisibility extends ColumnVisibility {
    /**
     * Creates an empty visibility. Normally, elements with empty visibility can be seen by everyone. Though, one could change this behavior with filters.
     *
     * @see #DocumentVisibility(String)
     */
    public DocumentVisibility() {
        super();
    }

    /**
     * Creates a document visibility for a Mutation.
     *
     * @param expression
     *          An expression of the rights needed to see this mutation. The expression syntax is defined at the class-level documentation
     */
    public DocumentVisibility(final String expression) {
        super(expression);
    }

    /**
     * Creates a document visibility for a Mutation.
     *
     * @param expression
     *          visibility expression
     * @see #DocumentVisibility(String)
     */
    public DocumentVisibility(final Text expression) {
        super(expression);
    }

    /**
     * Creates a document visibility for a Mutation from a string already encoded in UTF-8 bytes.
     *
     * @param expression
     *          visibility expression, encoded as UTF-8 bytes
     * @see #DocumentVisibility(String)
     */
    public DocumentVisibility(final byte[] expression) {
        super(expression);
    }
}