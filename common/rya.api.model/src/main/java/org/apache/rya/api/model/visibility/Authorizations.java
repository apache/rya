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
package org.apache.rya.api.model.visibility;

import java.io.Serializable;

/**
 * A collection of authorization strings.
 * 
 * XXX
 * This class has been copied over because Rya has decided to use the Accumulo
 * implementation of visibilities to control who is able to access what data
 * within a Rya instance. Until we implement an Accumulo agnostic method for
 * handling those visibility expressions, we have chosen to pull the Accumulo
 * code into our API.
 *
 * Copied from accumulo's  org.apache.accumulo.core.security.ByteSequence
 *   <dependancy>
 *     <groupId>org.apache.accumulo</groupId>
 *     <artifactId>accumulo-core</artifactId>
 *     <version>1.6.4</version>
 *   </dependancy>
 */
public class Authorizations implements Serializable {
    private static final long serialVersionUID = 1L;

    private static final boolean[] validAuthChars = new boolean[256];

    /**
     * A special header string used when serializing instances of this class.
     *
     * @see #serialize()
     */
    public static final String HEADER = "!AUTH1:";

    static {
        for (int i = 0; i < 256; i++) {
            validAuthChars[i] = false;
        }

        for (int i = 'a'; i <= 'z'; i++) {
            validAuthChars[i] = true;
        }

        for (int i = 'A'; i <= 'Z'; i++) {
            validAuthChars[i] = true;
        }

        for (int i = '0'; i <= '9'; i++) {
            validAuthChars[i] = true;
        }

        validAuthChars['_'] = true;
        validAuthChars['-'] = true;
        validAuthChars[':'] = true;
        validAuthChars['.'] = true;
        validAuthChars['/'] = true;
    }

    static final boolean isValidAuthChar(final byte b) {
        return validAuthChars[0xff & b];
    }
}
