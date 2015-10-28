package mvm.rya.api.domain;

/*
 * #%L
 * mvm.rya.rya.api
 * %%
 * Copyright (C) 2014 Rya
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */


import org.openrdf.model.URI;
import org.openrdf.model.vocabulary.XMLSchema;

/**
 * Base Rya Type
 * Date: 7/16/12
 * Time: 11:45 AM
 */
public class RyaType implements Comparable {

    private URI dataType;
    private String data;

    public RyaType() {
        setDataType(XMLSchema.STRING);
    }

    public RyaType(String data) {
        this(XMLSchema.STRING, data);
    }


    public RyaType(URI dataType, String data) {
        setDataType(dataType);
        setData(data);
    }

    /**
     * TODO: Can we get away without using the openrdf URI
     *
     * @return
     */
    public URI getDataType() {
        return dataType;
    }

    public String getData() {
        return data;
    }

    public void setDataType(URI dataType) {
        this.dataType = dataType;
    }

    public void setData(String data) {
        this.data = data;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("RyaType");
        sb.append("{dataType=").append(dataType);
        sb.append(", data='").append(data).append('\'');
        sb.append('}');
        return sb.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        RyaType ryaType = (RyaType) o;

        if (data != null ? !data.equals(ryaType.data) : ryaType.data != null) return false;
        if (dataType != null ? !dataType.equals(ryaType.dataType) : ryaType.dataType != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = dataType != null ? dataType.hashCode() : 0;
        result = 31 * result + (data != null ? data.hashCode() : 0);
        return result;
    }

    @Override
    public int compareTo(Object o) {
        if (o != null && this.getClass().isInstance(o)) {
            RyaType other = (RyaType) o;
            return this.getData().compareTo(other.getData());
        }
        return -1;
    }
}
