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
package org.apache.rya.api;

import org.apache.hadoop.conf.Configuration;

/**
 * This is a base class meant to be extended by Rya configuration builders. Any
 * class extending this class inherits setter methods that should be common to
 * all Rya implementations.
 * 
 * @param <B>
 *            the configuration builder returned by each of the setter methods
 * @param <C>
 *            the configuration object returned by the builder when build is
 *            called
 */
public abstract class RdfCloudTripleStoreConfigurationBuilder<B extends RdfCloudTripleStoreConfigurationBuilder<B, C>, C extends RdfCloudTripleStoreConfiguration> {

    private String prefix = "rya_";
    private String auths;
    private String visibilities;
    private boolean useInference = false;
    private boolean displayPlan = false;

    /**
     * Auxiliary method to return specified Builder in a type safe way
     * 
     * @return specified builder
     */
    protected abstract B confBuilder();

    /**
     * Auxiliary method to return type of configuration object constructed by
     * builder in a type safe way
     * 
     * @return specified configuration
     */
    protected abstract C createConf();

    /**
     * Set whether to use backwards chaining inferencing during query
     * evaluation. The default value is false.
     * 
     * @param useInference
     *            - turn inferencing on and off in Rya
     * @return B - concrete builder class for chaining method invocations
     */
    public B setUseInference(boolean useInference) {
        this.useInference = useInference;
        return confBuilder();
    }

    /**
     * 
     * Sets the authorization for querying the underlying data store.
     * 
     * @param auths
     *            - authorizations for querying underlying datastore
     * @return B - concrete builder class for chaining method invocations
     */
    public B setAuths(String auths) {
        this.auths = auths;
        return confBuilder();
    }

    /**
     * Set the column visibities for ingested triples. If no value is set,
     * triples won't have a visibility.
     * 
     * @param visibilities
     *            - visibilities assigned to any triples inserted into Rya
     * @return B - concrete builder class for chaining method invocations
     */
    public B setVisibilities(String visibilites) {
        this.visibilities = visibilites;
        return confBuilder();
    }

    /**
     *
     * Sets the prefix for the Rya instance to connect to. This parameter is
     * required and the default value is "rya_"
     * 
     * @param prefix
     *            - the prefix for the Rya instance
     * @return B - concrete builder class for chaining method invocations
     */
    public B setRyaPrefix(String prefix) {
        this.prefix = prefix;
        return confBuilder();
    }

    /**
     * Set whether to display query plan during optimization. The default value
     * is false.
     * 
     * @param displayPlan
     *            - display the parsed query plan during query evaluation
     *            (useful for debugging)
     * @return B - concrete builder class for chaining method invocations
     */
    public B setDisplayQueryPlan(boolean displayPlan) {
        this.displayPlan = displayPlan;
        return confBuilder();
    }

    /**
     * 
     * @return {@link RdfCloudTripleStoreConfiguration} object with specified parameters set
     */
    public C build() {
        return getConf(createConf());
    }

    /**
     * Assigns builder values to appropriate parameters within the {@link Configuration} object.
     * 
     * @param conf - Configuration object
     * @return - Configuration object with parameters set
     */
    private C getConf(C conf) {

        conf.setInfer(useInference);
        conf.setTablePrefix(prefix);
        conf.setInt("sc.cloudbase.numPartitions", 3);
        conf.setAuths(auths);
        if (visibilities != null) {
            conf.setCv(visibilities);
        }
        conf.setDisplayQueryPlan(displayPlan);

        return conf;
    }

    protected static boolean getBoolean(String boolString) {
        return Boolean.parseBoolean(boolString);
    }

}
