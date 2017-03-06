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
package org.apache.rya.accumulo;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.conf.Configuration;
import org.apache.rya.accumulo.experimental.AccumuloIndexer;
import org.apache.rya.api.RdfCloudTripleStoreConfiguration;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

public class AccumuloRdfConfiguration extends RdfCloudTripleStoreConfiguration {

    public static final String USE_MOCK_INSTANCE = ".useMockInstance";
    public static final String CLOUDBASE_INSTANCE = "sc.cloudbase.instancename";
    public static final String CLOUDBASE_ZOOKEEPERS = "sc.cloudbase.zookeepers";
    public static final String CLOUDBASE_USER = "sc.cloudbase.username";
    public static final String CLOUDBASE_PASSWORD = "sc.cloudbase.password";

    public static final String MAXRANGES_SCANNER = "ac.query.maxranges";

    public static final String CONF_ADDITIONAL_INDEXERS = "ac.additional.indexers";

    public static final String CONF_FLUSH_EACH_UPDATE = "ac.dao.flush";

    public static final String ITERATOR_SETTINGS_SIZE = "ac.iterators.size";
    public static final String ITERATOR_SETTINGS_BASE = "ac.iterators.%d.";
    public static final String ITERATOR_SETTINGS_NAME = ITERATOR_SETTINGS_BASE + "name";
    public static final String ITERATOR_SETTINGS_CLASS = ITERATOR_SETTINGS_BASE + "iteratorClass";
    public static final String ITERATOR_SETTINGS_PRIORITY = ITERATOR_SETTINGS_BASE + "priority";
    public static final String ITERATOR_SETTINGS_OPTIONS_SIZE = ITERATOR_SETTINGS_BASE + "optionsSize";
    public static final String ITERATOR_SETTINGS_OPTIONS_KEY = ITERATOR_SETTINGS_BASE + "option.%d.name";
    public static final String ITERATOR_SETTINGS_OPTIONS_VALUE = ITERATOR_SETTINGS_BASE + "option.%d.value";

    public AccumuloRdfConfiguration() {
        super();
    }

    public AccumuloRdfConfiguration(Configuration other) {
        super(other);
    }

    public AccumuloRdfConfigurationBuilder getBuilder() {
    	return new AccumuloRdfConfigurationBuilder();
    }
    
    /**
     * Creates an AccumuloRdfConfiguration object from a Properties file.  This method assumes
     * that all values in the Properties file are Strings and that the Properties file uses the keys below.
     * See accumulo/rya/src/test/resources/properties/rya.properties for an example.
     * <br>
     * <ul>
     * <li>"accumulo.auths" - String of Accumulo authorizations. Default is empty String.
     * <li>"accumulo.visibilities" - String of Accumulo visibilities assigned to ingested triples.
     * <li>"accumulo.instance" - Accumulo instance name (required)
     * <li>"accumulo.user" - Accumulo user (required)
     * <li>"accumulo.password" - Accumulo password (required)
     * <li>"accumulo.rya.prefix" - Prefix for Accumulo backed Rya instance.  Default is "rya_"
     * <li>"accumulo.zookeepers" - Zookeepers for underlying Accumulo instance (required if not using Mock)
     * <li>"use.mock" - Use a MockAccumulo instance as back-end for Rya instance.  Default is false.
     * <li>"use.prefix.hashing" - Use prefix hashing for triples.  Helps avoid hot-spotting.  Default is false.
     * <li>"use.count.stats" - Use triple pattern cardinalities for query optimization.   Default is false.
     * <li>"use.join.selectivity" - Use join selectivity for query optimization.  Default is false.
     * <li>"use.display.plan" - Display query plan during evaluation.  Useful for debugging.   Default is true.
     * <li>"use.inference" - Use backward chaining inference during query evaluation.   Default is false.
     * </ul>
     * <br>
     * @param props - Properties file containing Accumulo specific configuration parameters
     * @return AccumumuloRdfConfiguration with properties set
     */
    
    public static AccumuloRdfConfiguration fromProperties(Properties props) {
    	return AccumuloRdfConfigurationBuilder.fromProperties(props).build();
    }
    
    @Override
    public AccumuloRdfConfiguration clone() {
        return new AccumuloRdfConfiguration(this);
    }
    
    /**
     * Sets the Accumulo username from the configuration object that is meant to
     * be used when connecting a {@link Connector} to Accumulo.
     *
     */
    public void setAccumuloUser(String user) {
    	Preconditions.checkNotNull(user);
    	set(CLOUDBASE_USER, user);
    }
    
    /**
     * Get the Accumulo username from the configuration object that is meant to
     * be used when connecting a {@link Connector} to Accumulo.
     *
     * @return The username if one could be found; otherwise {@code null}.
     */
    public String getAccumuloUser(){
    	return get(CLOUDBASE_USER); 
    }
    
    /**
     * Sets the Accumulo password from the configuration object that is meant to
     * be used when connecting a {@link Connector} to Accumulo.
     *
     */
    public void setAccumuloPassword(String password) {
    	Preconditions.checkNotNull(password);
    	set(CLOUDBASE_PASSWORD, password);
    }
    
    /**
     * Get the Accumulo password from the configuration object that is meant to
     * be used when connecting a {@link Connector} to Accumulo.
     *
     * @return The password if one could be found; otherwise an empty string.
     */
    public String getAccumuloPassword() {
    	return get(CLOUDBASE_PASSWORD);
    }
    
    /**
     * Sets a comma delimited list of the names of the Zookeeper servers from
     * the configuration object that is meant to be used when connecting a
     * {@link Connector} to Accumulo.
     *
     */
    public void setAccumuloZookeepers(String zookeepers) {
    	Preconditions.checkNotNull(zookeepers);
    	set(CLOUDBASE_ZOOKEEPERS, zookeepers);
    }
    
    /**
     * Get a comma delimited list of the names of the Zookeeper servers from
     * the configuration object that is meant to be used when connecting a
     * {@link Connector} to Accumulo.
     *
     * @return The zookeepers list if one could be found; otherwise {@code null}.
     */
    public String getAccumuloZookeepers() {
    	return get(CLOUDBASE_ZOOKEEPERS);
    }
    
    /**
     * Sets the Accumulo instance name from the configuration object that is
     * meant to be used when connecting a {@link Connector} to Accumulo.
     *
     */
    public void setAccumuloInstance(String instance) {
    	Preconditions.checkNotNull(instance);
    	set(CLOUDBASE_INSTANCE, instance);
    }
    
    /**
     * Get the Accumulo instance name from the configuration object that is
     * meant to be used when connecting a {@link Connector} to Accumulo.
     *
     * @return The instance name if one could be found; otherwise {@code null}.
     */
    public String getAccumuloInstance() {
    	return get(CLOUDBASE_INSTANCE);
    }
    
    /**
     * Tells the Rya instance to use a Mock instance of Accumulo as its backing.
     *
     */
    public void setUseMockAccumulo(boolean useMock) {
    	setBoolean(USE_MOCK_INSTANCE, useMock);
    }
    
    /**
     * Indicates that a Mock instance of Accumulo is being used to back the Rya instance.
     *
     * @return {@code true} if the Rya instance is backed by a mock Accumulo; otherwise {@code false}.
     */
    public boolean getUseMockAccumulo() {
    	return getBoolean(USE_MOCK_INSTANCE, false);
    }
    

    /**
     * Indicates that a Mock instance of Accumulo is being used to back the Rya instance.
     *
     * @return {@code true} if the Rya instance is backed by a mock Accumulo; otherwise {@code false}.
     */
    public boolean useMockInstance() {
        return super.getBoolean(USE_MOCK_INSTANCE, false);
    }

    /**
     * Get the Accumulo username from the configuration object that is meant to
     * be used when connecting a {@link Connector} to Accumulo.
     *
     * @return The username if one could be found; otherwise {@code null}.
     */
    public String getUsername() {
        return super.get(CLOUDBASE_USER);
    }

    /**
     * Get the Accumulo password from the configuration object that is meant to
     * be used when connecting a {@link Connector} to Accumulo.
     *
     * @return The password if one could be found; otherwise an empty string.
     */
    public String getPassword() {
        return super.get(CLOUDBASE_PASSWORD, "");
    }

    /**
     * Get the Accumulo instance name from the configuration object that is
     * meant to be used when connecting a {@link Connector} to Accumulo.
     *
     * @return The instance name if one could be found; otherwise {@code null}.
     */
    public String getInstanceName() {
        return super.get(CLOUDBASE_INSTANCE);
    }

    /**
     * Get a comma delimited list of the names of the Zookeeper servers from
     * the configuration object that is meant to be used when connecting a
     * {@link Connector} to Accumulo.
     *
     * @return The zookeepers list if one could be found; otherwise {@code null}.
     */
    public String getZookeepers() {
        return super.get(CLOUDBASE_ZOOKEEPERS);
    }

    public Authorizations getAuthorizations() {
        String[] auths = getAuths();
        if (auths == null || auths.length == 0) {
            return AccumuloRdfConstants.ALL_AUTHORIZATIONS;
        }
        return new Authorizations(auths);
    }

    public void setMaxRangesForScanner(Integer max) {
        setInt(MAXRANGES_SCANNER, max);
    }

    public Integer getMaxRangesForScanner() {
        return getInt(MAXRANGES_SCANNER, 2);
    }

    public void setAdditionalIndexers(Class<? extends AccumuloIndexer>... indexers) {
        List<String> strs = Lists.newArrayList();
        for (Class<? extends AccumuloIndexer> ai : indexers){
            strs.add(ai.getName());
        }

        setStrings(CONF_ADDITIONAL_INDEXERS, strs.toArray(new String[]{}));
    }

    public List<AccumuloIndexer> getAdditionalIndexers() {
        return getInstances(CONF_ADDITIONAL_INDEXERS, AccumuloIndexer.class);
    }
    public boolean flushEachUpdate(){
        return getBoolean(CONF_FLUSH_EACH_UPDATE, true);
    }

    public void setFlush(boolean flush){
        setBoolean(CONF_FLUSH_EACH_UPDATE, flush);
    }

    public void setAdditionalIterators(IteratorSetting... additionalIterators){
        //TODO do we need to worry about cleaning up
        this.set(ITERATOR_SETTINGS_SIZE, Integer.toString(additionalIterators.length));
        int i = 0;
        for(IteratorSetting iterator : additionalIterators) {
            this.set(String.format(ITERATOR_SETTINGS_NAME, i), iterator.getName());
            this.set(String.format(ITERATOR_SETTINGS_CLASS, i), iterator.getIteratorClass());
            this.set(String.format(ITERATOR_SETTINGS_PRIORITY, i), Integer.toString(iterator.getPriority()));
            Map<String, String> options = iterator.getOptions();

            this.set(String.format(ITERATOR_SETTINGS_OPTIONS_SIZE, i), Integer.toString(options.size()));
            Iterator<Entry<String, String>> it = options.entrySet().iterator();
            int j = 0;
            while(it.hasNext()) {
                Entry<String, String> item = it.next();
                this.set(String.format(ITERATOR_SETTINGS_OPTIONS_KEY, i, j), item.getKey());
                this.set(String.format(ITERATOR_SETTINGS_OPTIONS_VALUE, i, j), item.getValue());
                j++;
            }
            i++;
        }
    }

    public IteratorSetting[] getAdditionalIterators(){
        int size = Integer.valueOf(this.get(ITERATOR_SETTINGS_SIZE, "0"));
        if(size == 0) {
            return new IteratorSetting[0];
        }

        IteratorSetting[] settings = new IteratorSetting[size];
        for(int i = 0; i < size; i++) {
            String name = this.get(String.format(ITERATOR_SETTINGS_NAME, i));
            String iteratorClass = this.get(String.format(ITERATOR_SETTINGS_CLASS, i));
            int priority = Integer.valueOf(this.get(String.format(ITERATOR_SETTINGS_PRIORITY, i)));

            int optionsSize = Integer.valueOf(this.get(String.format(ITERATOR_SETTINGS_OPTIONS_SIZE, i)));
            Map<String, String> options = new HashMap<>(optionsSize);
            for(int j = 0; j < optionsSize; j++) {
                String key = this.get(String.format(ITERATOR_SETTINGS_OPTIONS_KEY, i, j));
                String value = this.get(String.format(ITERATOR_SETTINGS_OPTIONS_VALUE, i, j));
                options.put(key, value);
            }
            settings[i] = new IteratorSetting(priority, name, iteratorClass, options);
        }

        return settings;
    }
}
