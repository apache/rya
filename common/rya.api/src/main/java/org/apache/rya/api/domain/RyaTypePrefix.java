package org.apache.rya.api.domain;

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



import org.openrdf.model.URI;

import static org.apache.rya.api.RdfCloudTripleStoreConstants.DELIM;
import static org.apache.rya.api.RdfCloudTripleStoreConstants.LAST;

/**
 * Date: 7/24/12
 * Time: 3:26 PM
 */
public class RyaTypePrefix extends RyaTypeRange {

    public RyaTypePrefix(URI datatype, String prefix) {
        super();
        setPrefix(datatype, prefix);
    }

    public RyaTypePrefix(String prefix) {
        super();
        setPrefix(prefix);
    }

    public void setPrefix(String prefix) {
        setStart(new RyaType(prefix + DELIM));
        setStop(new RyaType(prefix + LAST));
    }

    public void setPrefix(URI datatype, String prefix) {
        setStart(new RyaType(datatype, prefix + DELIM));
        setStop(new RyaType(datatype, prefix + LAST));
    }

    public String getPrefix() {
        String data = getStart().getData();
        return data.substring(0, data.length() - 1);
    }
}
