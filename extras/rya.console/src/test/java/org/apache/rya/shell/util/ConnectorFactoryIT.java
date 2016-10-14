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
package org.apache.rya.shell.util;

import java.nio.CharBuffer;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.junit.Test;

import org.apache.rya.accumulo.AccumuloITBase;

/**
 * Tests the methods of {@link ConnectorFactory}.
 */
public class ConnectorFactoryIT extends AccumuloITBase {

    @Test
    public void connect_successful() throws AccumuloException, AccumuloSecurityException {
        // Setup the values that will be tested with.
        final CharSequence password = CharBuffer.wrap( getPassword() );

        final ConnectorFactory ac = new ConnectorFactory();
        ac.connect(getUsername(),
                password,
                getInstanceName(),
                getZookeepers());
    }

    @Test(expected = AccumuloSecurityException.class)
    public void connect_wrongCredentials() throws AccumuloException, AccumuloSecurityException {
        // Setup the values that will be tested with.
        final CharSequence password = CharBuffer.wrap( new char[] {'w','r','o','n','g','p','a','s','s'} );

        final ConnectorFactory ac = new ConnectorFactory();
        ac.connect(getUsername(),
                password,
                getInstanceName(),
                getZookeepers());
    }
}