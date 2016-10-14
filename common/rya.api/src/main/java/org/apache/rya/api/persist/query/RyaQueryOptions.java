package org.apache.rya.api.persist.query;

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



import org.apache.rya.api.RdfCloudTripleStoreConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

/**
 */
public class RyaQueryOptions {
    private static final Logger logger = LoggerFactory.getLogger(RyaQueryOptions.class);
    //options
    protected String[] auths;
    protected Long ttl;
    protected Long currentTime;
    protected Long maxResults;
    protected Integer numQueryThreads = 4;
    protected Integer batchSize = 1000;
    protected String regexSubject;
    protected String regexPredicate;
    protected String regexObject;
    protected RdfCloudTripleStoreConfiguration conf;

    public static class RyaOptionsBuilder<T extends RyaOptionsBuilder> {
        private RyaQueryOptions options;

        public RyaOptionsBuilder(RyaQueryOptions query) {
            this.options = query;
        }

        public T load(RdfCloudTripleStoreConfiguration conf) {
        	options.setConf(conf);
            return (T) this.setAuths(conf.getAuths())
                    .setBatchSize(conf.getBatchSize())
                    .setCurrentTime(conf.getStartTime())
                    .setMaxResults(conf.getLimit())
                    .setNumQueryThreads(conf.getNumThreads())
                    .setRegexObject(conf.getRegexObject())
                    .setRegexPredicate(conf.getRegexPredicate())
                    .setRegexSubject(conf.getRegexSubject())
                    .setTtl(conf.getTtl());
        }

        public T setAuths(String[] auths) {
            options.setAuths(auths);
            return (T) this;
        }

        public T setRegexObject(String regexObject) {
            options.setRegexObject(regexObject);
            return (T) this;
        }

        public T setRegexPredicate(String regexPredicate) {
            options.setRegexPredicate(regexPredicate);
            return (T) this;
        }

        public T setRegexSubject(String regexSubject) {
            options.setRegexSubject(regexSubject);
            return (T) this;
        }

        public T setBatchSize(Integer batchSize) {
            options.setBatchSize(batchSize);
            return (T) this;
        }

        public T setNumQueryThreads(Integer numQueryThreads) {
            options.setNumQueryThreads(numQueryThreads);
            return (T) this;
        }

        public T setMaxResults(Long maxResults) {
            options.setMaxResults(maxResults);
            return (T) this;
        }

        public T setCurrentTime(Long currentTime) {
            options.setCurrentTime(currentTime);
            return (T) this;
        }

        public T setTtl(Long ttl) {
            options.setTtl(ttl);
            return (T) this;
        }
    }

    public RdfCloudTripleStoreConfiguration getConf() {
    	return conf;
    }

    public void setConf(RdfCloudTripleStoreConfiguration conf) {
    	this.conf = conf;
    }

    public Long getTtl() {
        return ttl;
    }

    public void setTtl(Long ttl) {
        this.ttl = ttl;
    }

    public Long getCurrentTime() {
        return currentTime;
    }

    public void setCurrentTime(Long currentTime) {
        this.currentTime = currentTime;
    }

    public Integer getNumQueryThreads() {
        return numQueryThreads;
    }

    public void setNumQueryThreads(Integer numQueryThreads) {
        this.numQueryThreads = numQueryThreads;
    }

    public Long getMaxResults() {
        return maxResults;
    }

    public void setMaxResults(Long maxResults) {
        this.maxResults = maxResults;
    }

    public Integer getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(Integer batchSize) {
        this.batchSize = batchSize;
    }

    public String getRegexSubject() {
        return regexSubject;
    }

    public void setRegexSubject(String regexSubject) {
        this.regexSubject = regexSubject;
    }

    public String getRegexPredicate() {
        return regexPredicate;
    }

    public void setRegexPredicate(String regexPredicate) {
        this.regexPredicate = regexPredicate;
    }

    public String getRegexObject() {
        return regexObject;
    }

    public void setRegexObject(String regexObject) {
        this.regexObject = regexObject;
    }

    public String[] getAuths() {
        return auths;
    }

    public void setAuths(String[] auths) {
        if (auths == null) {
            this.auths = new String[0];
        } else {
            this.auths = auths.clone();
        }
    }

    @Override
    public String toString() {
        return "RyaQueryOptions{" +
                "auths=" + (auths == null ? null : Arrays.asList(auths)) +
                ", ttl=" + ttl +
                ", currentTime=" + currentTime +
                ", maxResults=" + maxResults +
                ", numQueryThreads=" + numQueryThreads +
                ", batchSize=" + batchSize +
                ", regexSubject='" + regexSubject + '\'' +
                ", regexPredicate='" + regexPredicate + '\'' +
                ", regexObject='" + regexObject + '\'' +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        RyaQueryOptions that = (RyaQueryOptions) o;

        if (!Arrays.equals(auths, that.auths)) return false;
        if (batchSize != null ? !batchSize.equals(that.batchSize) : that.batchSize != null) return false;
        if (currentTime != null ? !currentTime.equals(that.currentTime) : that.currentTime != null) return false;
        if (maxResults != null ? !maxResults.equals(that.maxResults) : that.maxResults != null) return false;
        if (numQueryThreads != null ? !numQueryThreads.equals(that.numQueryThreads) : that.numQueryThreads != null)
            return false;
        if (regexObject != null ? !regexObject.equals(that.regexObject) : that.regexObject != null) return false;
        if (regexPredicate != null ? !regexPredicate.equals(that.regexPredicate) : that.regexPredicate != null)
            return false;
        if (regexSubject != null ? !regexSubject.equals(that.regexSubject) : that.regexSubject != null) return false;
        if (ttl != null ? !ttl.equals(that.ttl) : that.ttl != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = auths != null ? Arrays.hashCode(auths) : 0;
        result = 31 * result + (ttl != null ? ttl.hashCode() : 0);
        result = 31 * result + (currentTime != null ? currentTime.hashCode() : 0);
        result = 31 * result + (maxResults != null ? maxResults.hashCode() : 0);
        result = 31 * result + (numQueryThreads != null ? numQueryThreads.hashCode() : 0);
        result = 31 * result + (batchSize != null ? batchSize.hashCode() : 0);
        result = 31 * result + (regexSubject != null ? regexSubject.hashCode() : 0);
        result = 31 * result + (regexPredicate != null ? regexPredicate.hashCode() : 0);
        result = 31 * result + (regexObject != null ? regexObject.hashCode() : 0);
        return result;
    }
}
