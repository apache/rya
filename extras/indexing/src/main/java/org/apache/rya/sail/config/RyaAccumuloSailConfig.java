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
package org.apache.rya.sail.config;

import org.apache.rya.accumulo.AccumuloRdfConfiguration;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.sail.config.AbstractSailImplConfig;
import org.eclipse.rdf4j.sail.config.SailConfigException;

import java.util.Set;

/**
 * @deprecated Use {@link AccumuloRdfConfiguration} instead.
 */
@Deprecated
public class RyaAccumuloSailConfig extends AbstractSailImplConfig {

    public static final String NAMESPACE = "http://rya.apache.org/RyaAccumuloSail/Config#";

    public static final IRI INSTANCE;
    public static final IRI USER;
    public static final IRI PASSWORD;
    public static final IRI ZOOKEEPERS;
    public static final IRI IS_MOCK;

    static {
        final ValueFactory factory = SimpleValueFactory.getInstance();
        USER = factory.createIRI(NAMESPACE, "user");
        PASSWORD = factory.createIRI(NAMESPACE, "password");
        INSTANCE = factory.createIRI(NAMESPACE, "instance");
        ZOOKEEPERS = factory.createIRI(NAMESPACE, "zookeepers");
        IS_MOCK = factory.createIRI(NAMESPACE, "isMock");
    }

    private String user = "root";
    private String password = "root";
    private String instance = "dev";
    private String zookeepers = "zoo1,zoo2,zoo3";
    private boolean isMock = false;

    public RyaAccumuloSailConfig() {
        super(RyaAccumuloSailFactory.SAIL_TYPE);
    }

    public String getUser() {
        return user;
    }

    public void setUser(final String user) {
        this.user = user;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(final String password) {
        this.password = password;
    }

    public String getInstance() {
        return instance;
    }

    public void setInstance(final String instance) {
        this.instance = instance;
    }

    public String getZookeepers() {
        return zookeepers;
    }

    public void setZookeepers(final String zookeepers) {
        this.zookeepers = zookeepers;
    }

    public boolean isMock() {
        return isMock;
    }

    public void setMock(final boolean isMock) {
        this.isMock = isMock;
    }

    public AccumuloRdfConfiguration toRdfConfiguation() {
        final AccumuloRdfConfiguration conf = new AccumuloRdfConfiguration();
        return conf;
    }

    @Override
    public void validate() throws SailConfigException {
        super.validate();
    }

    @Override
    public Resource export(final Model model) {
        final Resource implNode = super.export(model);

        final ValueFactory v = SimpleValueFactory.getInstance();

        model.add(implNode, USER, v.createLiteral(user));
        model.add(implNode, PASSWORD, v.createLiteral(password));
        model.add(implNode, INSTANCE, v.createLiteral(instance));
        model.add(implNode, ZOOKEEPERS, v.createLiteral(zookeepers));
        model.add(implNode, IS_MOCK, v.createLiteral(isMock));

        return implNode;
    }

    @Override
    public void parse(final Model model, final Resource implNode) throws SailConfigException {
        super.parse(model, implNode);
        System.out.println("parsing");

        try {
            final Set<Value> userLit = model.filter(implNode, USER, null).objects();
            if (userLit.size() == 1) {
                setUser(userLit.iterator().next().stringValue());
            }
            final Set<Value> pwdLit = model.filter(implNode, PASSWORD, null).objects();
            if (pwdLit.size() == 1) {
                setPassword(pwdLit.iterator().next().stringValue());
            }
            final Set<Value> instLit = model.filter(implNode, INSTANCE, null).objects();
            if (instLit.size() == 1) {
                setInstance(instLit.iterator().next().stringValue());
            }
            final Set<Value> zooLit = model.filter(implNode, ZOOKEEPERS, null).objects();
            if (zooLit.size() == 1) {
                setZookeepers(zooLit.iterator().next().stringValue());
            }
            final Set<Value> mockLit = model.filter(implNode, IS_MOCK, null).objects();
            if (mockLit.size() == 1) {
                setMock(Boolean.parseBoolean(mockLit.iterator().next().stringValue()));
            }
        } catch (final Exception e) {
            throw new SailConfigException(e.getMessage(), e);
        }
    }
}
