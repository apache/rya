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
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.util.GraphUtil;
import org.eclipse.rdf4j.model.util.GraphUtilException;
import org.eclipse.rdf4j.sail.config.AbstractSailImplConfig;
import org.eclipse.rdf4j.sail.config.SailConfigException;

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

        @SuppressWarnings("deprecation")
        final
        ValueFactory v = model.getValueFactory();

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
            final Literal userLit = GraphUtil.getOptionalObjectLiteral(model, implNode, USER);
            if (userLit != null) {
                setUser(userLit.getLabel());
            }
            final Literal pwdLit = GraphUtil.getOptionalObjectLiteral(model, implNode, PASSWORD);
            if (pwdLit != null) {
                setPassword(pwdLit.getLabel());
            }
            final Literal instLit = GraphUtil.getOptionalObjectLiteral(model, implNode, INSTANCE);
            if (instLit != null) {
                setInstance(instLit.getLabel());
            }
            final Literal zooLit = GraphUtil.getOptionalObjectLiteral(model, implNode, ZOOKEEPERS);
            if (zooLit != null) {
                setZookeepers(zooLit.getLabel());
            }
            final Literal mockLit = GraphUtil.getOptionalObjectLiteral(model, implNode, IS_MOCK);
            if (mockLit != null) {
                setMock(Boolean.parseBoolean(mockLit.getLabel()));
            }
        } catch (final GraphUtilException e) {
            throw new SailConfigException(e.getMessage(), e);
        }
    }
}
