package org.apache.rya.accumulo;

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



import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.rya.accumulo.experimental.AccumuloIndexer;
import org.apache.rya.api.RdfCloudTripleStoreConfiguration;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.conf.Configuration;

import com.google.common.collect.Lists;

/**
 * Created by IntelliJ IDEA.
 * Date: 4/25/12
 * Time: 3:24 PM
 * To change this template use File | Settings | File Templates.
 */
public class AccumuloRdfConfiguration extends RdfCloudTripleStoreConfiguration {

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

    @Override
    public AccumuloRdfConfiguration clone() {
        return new AccumuloRdfConfiguration(this);
    }

    public Authorizations getAuthorizations() {
        String[] auths = getAuths();
        if (auths == null || auths.length == 0)
            return AccumuloRdfConstants.ALL_AUTHORIZATIONS;
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
            Map<String, String> options = new HashMap<String, String>(optionsSize);
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
