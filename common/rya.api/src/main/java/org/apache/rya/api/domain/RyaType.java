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
package org.apache.rya.api.domain;

import java.util.Objects;

import org.apache.commons.lang.builder.CompareToBuilder;
import org.apache.commons.lang.builder.EqualsBuilder;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.vocabulary.XMLSchema;

/**
 * Base Rya Type
 * Date: 7/16/12
 * Time: 11:45 AM
 */
public class RyaType implements Comparable<RyaType> {

    private IRI dataType;
    private String data;
    private String language;

    /**
     * Creates a new instance of {@link RyaType}.
     */
    public RyaType() {
        this(null);
    }

    /**
     * Creates a new instance of {@link RyaType} of type
     * {@link XMLSchema#STRING} and with no language.
     * @param data the data string.
     */
    public RyaType(final String data) {
        this(XMLSchema.STRING, data);
    }

    /**
     * Creates a new instance of {@link RyaType} with no language.
     * @param dataType the {@link IRI} data type.
     * @param data the data string.
     */
    public RyaType(final IRI dataType, final String data) {
        this(dataType, data, null);
    }

    /**
     * Creates a new instance of {@link RyaType}.
     * @param dataType the {@link IRI} data type.
     * @param data the data string.
     * @param language the language code.
     */
    public RyaType(final IRI dataType, final String data, final String language) {
        this.dataType = dataType;
        this.data = data;
        this.language = language;
    }

    /**
     * TODO: Can we get away without using the RDF4J IRI
     *
     * @return
     */
    public IRI getDataType() {
        return dataType;
    }

    public String getData() {
        return data;
    }

    public void setDataType(final IRI dataType) {
        this.dataType = dataType;
    }

    public void setData(final String data) {
        this.data = data;
    }

    /**
     * @return the language code.
     */
    public String getLanguage() {
        return language;
    }

    /**
     * Sets the language code.
     * @param language the language code.
     */
    public void setLanguage(final String language) {
        this.language = language;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("RyaType");
        sb.append("{dataType=").append(dataType);
        sb.append(", data='").append(data).append('\'');
        if (language != null) {
            sb.append(", language='").append(language).append('\'');
        }
        sb.append('}');
        return sb.toString();
    }

    /**
     * Determine equality based on string representations of data, datatype, and
     * language.
     * @param o The object to compare with
     * @return {@code true} if the other object is also a RyaType and the data,
     * datatype, and language all match.
     */
    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || !(o instanceof RyaType)) {
            return false;
        }
        final RyaType other = (RyaType) o;
        final EqualsBuilder builder = new EqualsBuilder()
                .append(getData(), other.getData())
                .append(getDataType(), other.getDataType())
                .append(getLanguage(), other.getLanguage());
        return builder.isEquals();
    }

    /**
     * Generate a hash based on the string representations of data, datatype,
     * and language.
     * @return A hash consistent with equals.
     */
    @Override
    public int hashCode() {
        return Objects.hash(dataType, data, language);
    }

    /**
     * Define a natural ordering based on data, datatype, and language.
     * @param o The object to compare with
     * @return 0 if the data string, the datatype string, and the language
     * string representation match between the objects, where matching is
     * defined by string comparison or all being null;
     * Otherwise, an integer whose sign yields a consistent ordering.
     */
    @Override
    public int compareTo(final RyaType o) {
        if (o == null) {
            return 1;
        }
        final String dataTypeStr = getDataType() != null ? getDataType().stringValue() : null;
        final String otherDataTypeStr = o.getDataType() != null ? o.getDataType().stringValue() : null;
        final CompareToBuilder builder = new CompareToBuilder()
                .append(getData(), o.getData())
                .append(dataTypeStr, otherDataTypeStr)
                .append(getLanguage(), o.getLanguage());
        return builder.toComparison();
    }
}
