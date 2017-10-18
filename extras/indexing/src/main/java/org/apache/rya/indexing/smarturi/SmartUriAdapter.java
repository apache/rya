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
package org.apache.rya.indexing.smarturi;

import java.io.UnsupportedEncodingException;
import java.net.URISyntaxException;
import java.net.URLDecoder;
import java.util.*;
import java.util.Map.Entry;

import com.google.common.base.Charsets;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.ImmutableMap;
import com.google.common.primitives.Doubles;
import com.google.common.primitives.Floats;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.NameValuePair;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.client.utils.URLEncodedUtils;
import org.apache.http.message.BasicNameValuePair;
import org.apache.rya.api.domain.RyaType;
import org.apache.rya.api.domain.RyaURI;
import org.apache.rya.api.resolver.RdfToRyaConversions;
import org.apache.rya.api.resolver.RyaToRdfConversions;
import org.apache.rya.indexing.entity.model.Entity;
import org.apache.rya.indexing.entity.model.Property;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.impl.URIImpl;
import org.eclipse.rdf4j.model.vocabulary.XMLSchema;
import org.joda.time.DateTime;
import org.joda.time.format.ISODateTimeFormat;

/**
 * Interface for serializing and deserializing Smart URIs.
 */
public class SmartUriAdapter {
    private static final String ENTITY_TYPE_MAP_URN = "urn://entityTypeMap";
    private static final IRI RYA_TYPES_URI = new URIImpl("urn://ryaTypes");

    /**
     * Private constructor to prevent instantiation.
     */
    private SmartUriAdapter() {
    }

    private static IRI createTypePropertiesUri(final ImmutableMap<RyaURI, ImmutableMap<RyaURI, Property>> typeProperties) throws SmartUriException {
        final List<NameValuePair> nameValuePairs = new ArrayList<>();
        for (final Entry<RyaURI, ImmutableMap<RyaURI, Property>> typeProperty : typeProperties.entrySet()) {
            final RyaURI type = typeProperty.getKey();
            final Map<RyaURI, Property> propertyMap = typeProperty.getValue();
            final IRI typeUri = createIndividualTypeWithPropertiesUri(type, propertyMap);
            final String keyString = type.getDataType().getLocalName();
            final String valueString = typeUri.getLocalName();
            nameValuePairs.add(new BasicNameValuePair(keyString, valueString));
        }

        final URIBuilder uriBuilder = new URIBuilder();
        uriBuilder.addParameters(nameValuePairs);

        String uriString;
        try {
            final java.net.URI uri = uriBuilder.build();
            final String queryString = uri.getRawSchemeSpecificPart();
            uriString = "urn:test" + queryString;
        } catch (final URISyntaxException e) {
            throw new SmartUriException("Unable to create type properties for the Smart URI", e);
        }

        return new URIImpl(uriString);
    }

    private static String getShortNameForType(final RyaURI type) throws SmartUriException {
        final String shortName = new URIImpl(type.getData()).getLocalName();
        return shortName;
    }


    private static String addTypePrefixToUri(final String uriString, final String typePrefix) {
        final String localName = new URIImpl(uriString).getLocalName();
        final String beginning = StringUtils.removeEnd(uriString, localName);
        final String formattedUriString = beginning + typePrefix + localName;
        return formattedUriString;
    }

    private static String removeTypePrefixFromUri(final String uriString, final String typePrefix) {
        final String localName = new URIImpl(uriString).getLocalName();
        final String beginning = StringUtils.removeEnd(uriString, localName);
        final String replacement = localName.replaceFirst(typePrefix + ".", "");
        final String formattedUriString = beginning + replacement;
        return formattedUriString;
    }

    private static Map<RyaURI, String> createTypeMap(final List<RyaURI> types) throws SmartUriException {
        final Map<RyaURI, String> map = new LinkedHashMap<>();
        for (final RyaURI type : types) {
            final String shortName = getShortNameForType(type);
            map.put(type, shortName);
        }
        return map;
    }

    private static IRI createTypeMapUri(final List<RyaURI> types) throws SmartUriException {
        final List<NameValuePair> nameValuePairs = new ArrayList<>();
        for (final RyaURI type : types) {
            final String shortName = getShortNameForType(type);
            nameValuePairs.add(new BasicNameValuePair(type.getData(), shortName));
        }

        final URIBuilder uriBuilder = new URIBuilder();
        uriBuilder.addParameters(nameValuePairs);

        String uriString;
        try {
            final java.net.URI uri = uriBuilder.build();
            final String queryString = uri.getRawSchemeSpecificPart();
            uriString = ENTITY_TYPE_MAP_URN + queryString;
        } catch (final URISyntaxException e) {
            throw new SmartUriException("Unable to create type properties for the Smart URI", e);
        }

        return new URIImpl(uriString);
    }

    private static Map<RyaURI, String> convertUriToTypeMap(final IRI typeMapUri) throws SmartUriException {
        final Map<RyaURI, String> map = new HashMap<>();
        java.net.URI uri;
        try {
            final URIBuilder uriBuilder = new URIBuilder(typeMapUri.stringValue());
            uri = uriBuilder.build();
        } catch (final URISyntaxException e) {
            throw new SmartUriException("Unable to parse Rya type map in Smart URI", e);
        }

        final List<NameValuePair> params = URLEncodedUtils.parse(uri, Charsets.UTF_8.name());

        for (final NameValuePair param : params) {
            final String name = param.getName();
            final String value = param.getValue();
            final RyaURI type = new RyaURI(name);
            map.put(type, value);
        }
        return map;
    }

    private static IRI createIndividualTypeWithPropertiesUri(final RyaURI type, final Map<RyaURI, Property> map) throws SmartUriException {
        final List<NameValuePair> nameValuePairs = new ArrayList<>();
        for (final Entry<RyaURI, Property> entry : map.entrySet()) {
            final RyaURI key = entry.getKey();
            final Property property = entry.getValue();

            final RyaType ryaType = property.getValue();
            final String keyString = (new URIImpl(key.getData())).getLocalName();
            final Value value = RyaToRdfConversions.convertValue(ryaType);
            final String valueString = value.stringValue();
            nameValuePairs.add(new BasicNameValuePair(keyString, valueString));
        }

        final URIBuilder uriBuilder = new URIBuilder();
        uriBuilder.addParameters(nameValuePairs);

        String uriString;
        try {
            final java.net.URI uri = uriBuilder.build();
            final String queryString = uri.getRawSchemeSpecificPart();
            uriString = type.getData()/*new URIImpl(type.getData()).getLocalName()*/ + queryString;
        } catch (final URISyntaxException e) {
            throw new SmartUriException("Unable to create type URI with all its properties for the Smart URI", e);
        }

        return new URIImpl(uriString);
    }

    private static Entity convertMapToEntity(final RyaURI subject, final Map<RyaURI, Map<IRI, Value>> map) {
        final Entity.Builder entityBuilder = Entity.builder();
        entityBuilder.setSubject(subject);

        for (final Entry<RyaURI, Map<IRI, Value>> typeEntry : map.entrySet()) {
            final RyaURI type = typeEntry.getKey();
            final Map<IRI, Value> subMap = typeEntry.getValue();
            entityBuilder.setExplicitType(type);
            for (final Entry<IRI, Value> entry : subMap.entrySet()) {
                final IRI uri = entry.getKey();
                final Value value = entry.getValue();
                final RyaURI ryaUri = new RyaURI(uri.stringValue());
                final RyaURI ryaName = new RyaURI(uri.stringValue());
                final RyaType ryaType = new RyaType(value.stringValue());
                final Property property = new Property(ryaName, ryaType);
                entityBuilder.setProperty(ryaUri, property);
            }
        }
        final Entity entity = entityBuilder.build();
        return entity;
    }

    public static RyaURI findSubject(final IRI uri) throws SmartUriException {
        final String uriString = uri.stringValue();
        return findSubject(uriString);
    }

    public static RyaURI findSubject(final String uriString) throws SmartUriException {
        java.net.URI uri;
        try {
            uri = new java.net.URI(uriString);
        } catch (final URISyntaxException e) {
            throw new SmartUriException("Could not find subject in Smart URI", e);
        }
        final RyaURI subject;
        final String fullFragment = uri.getFragment();
        if (fullFragment != null) {
            final int queryPosition = fullFragment.indexOf("?");
            String partialFragment = null;
            if (queryPosition != - 1) {
                partialFragment = fullFragment.substring(0, queryPosition);
            }
            final String subjectString = uri.getScheme() + ":" + uri.getSchemeSpecificPart() + "#" + partialFragment;
            subject = new RyaURI(subjectString);
        } else {
            final int queryPosition = uriString.indexOf("?");
            String subjectString = null;
            if (queryPosition != - 1) {
                subjectString = uriString.substring(0, queryPosition);
            } else {
                subjectString = uriString;
            }
            subject = new RyaURI(subjectString);
        }

        return subject;
    }


    /**
     * Serializes an {@link Entity} into a Smart {@link IRI}.
     * @param entity the {@link Entity} to serialize into a Smart URI.
     * @return the Smart {@link IRI}.
     * @throws SmartUriException
     */
    public static IRI serializeUriEntity(final Entity entity) throws SmartUriException {
        final Map<IRI, Value> objectMap = new LinkedHashMap<>();

        // Adds the entity's types to the Smart URI
        final List<RyaURI> typeIds = entity.getExplicitTypeIds();
        final Map<RyaURI, String> ryaTypeMap = createTypeMap(typeIds);
        final IRI ryaTypeMapUri = createTypeMapUri(typeIds);
        final RyaType valueRyaType = new RyaType(XMLSchema.ANYURI, ryaTypeMapUri.stringValue());
        final Value typeValue = RyaToRdfConversions.convertValue(valueRyaType);
        objectMap.put(RYA_TYPES_URI, typeValue);

        final RyaURI subject = entity.getSubject();
        final Map<RyaURI, ImmutableMap<RyaURI, Property>> typeMap = entity.getProperties();
        for (final Entry<RyaURI, ImmutableMap<RyaURI, Property>> typeEntry : typeMap.entrySet()) {
            final RyaURI type = typeEntry.getKey();
            String typeShortName = ryaTypeMap.get(type);
            typeShortName = typeShortName != null ? typeShortName + "." : "";
            final ImmutableMap<RyaURI, Property> typeProperties = typeEntry.getValue();
            for (final Entry<RyaURI, Property> properties : typeProperties.entrySet()) {
                final RyaURI key = properties.getKey();
                final Property property = properties.getValue();
                final String valueString = property.getValue().getData();
                final RyaType ryaType = property.getValue();

                //final RyaType ryaType = new RyaType(new URIImpl(key.getData()), valueString);

                final Value value = RyaToRdfConversions.convertValue(ryaType);

                String formattedKey = key.getData();
                if (StringUtils.isNotBlank(typeShortName)) {
                    formattedKey = addTypePrefixToUri(formattedKey, typeShortName);
                }
                final IRI uri = new URIImpl(formattedKey);
                objectMap.put(uri, value);
            }
        }

        return serializeUri(subject, objectMap);
    }

    /**
     * Serializes a map into a URI.
     * @param subject the {@link RyaURI} subject of the Entity. Identifies the
     * thing that is being represented as an Entity.
     * @param map the {@link Map} of {@link IRI}s to {@link Value}s.
     * @return the Smart {@link IRI}.
     * @throws SmartUriException
     */
    public static IRI serializeUri(final RyaURI subject, final Map<IRI, Value> map) throws SmartUriException {
        final String subjectData = subject.getData();
        final int fragmentPosition = subjectData.indexOf("#");
        String prefix = subjectData;
        String fragment = null;
        if (fragmentPosition > -1) {
            prefix = subjectData.substring(0, fragmentPosition);
            fragment = subjectData.substring(fragmentPosition + 1, subjectData.length());
        }

        URIBuilder uriBuilder = null;
        try {
            if (fragmentPosition > -1) {
                uriBuilder = new URIBuilder(new java.net.URI("urn://" + fragment));
            } else {
                uriBuilder = new URIBuilder(new java.net.URI(subjectData.replaceFirst(":", "://")));
            }
        } catch (final URISyntaxException e) {
            throw new SmartUriException("Unable to serialize a Smart URI from the provided properties", e);
        }
        final List<NameValuePair> nameValuePairs = new ArrayList<>();

        for (final Entry<IRI, Value> entry : map.entrySet()) {
            final IRI key = entry.getKey();
            final Value value = entry.getValue();
            nameValuePairs.add(new BasicNameValuePair(key.getLocalName(), value.stringValue()));
        }

        uriBuilder.setParameters(nameValuePairs);

        IRI uri = null;
        try {
            if (fragmentPosition > -1) {
                final java.net.URI partialUri = uriBuilder.build();
                final String uriString = StringUtils.removeStart(partialUri.getRawSchemeSpecificPart(), "//");
                final URIBuilder fragmentUriBuilder = new URIBuilder(new java.net.URI(prefix));
                fragmentUriBuilder.setFragment(uriString);
                final String fragmentUriString = fragmentUriBuilder.build().toString();
                uri = new URIImpl(fragmentUriString);
            } else {
                final String uriString = uriBuilder.build().toString();
                uri = new URIImpl(uriString);
            }
        } catch (final URISyntaxException e) {
            throw new SmartUriException("Smart URI could not serialize the property map.", e);
        }

        return uri;
    }

    /**
     * Deserializes a URI into a map of URI's to values.
     * @param uri the {@link IRI}.
     * @return the {@link Map} of {@link IRI}s to {@link Value}s.
     * @throws SmartUriException
     */
    public static Map<IRI, Value> deserializeUri(final IRI uri) throws SmartUriException {
        final String uriString = uri.stringValue();
        final int fragmentPosition = uriString.indexOf("#");
        String prefix = uriString.substring(0, fragmentPosition + 1);
        if (fragmentPosition == -1) {
            prefix = uriString.split("\\?", 2)[0];
        }
        final String fragment = uriString.substring(fragmentPosition + 1, uriString.length());
        java.net.URI queryUri;

        URIBuilder uriBuilder = null;
        try {
             if (fragmentPosition > -1) {
                 queryUri = new java.net.URI("urn://" + fragment);
             } else {
                 queryUri = new java.net.URI(uriString);
             }
            uriBuilder = new URIBuilder(queryUri);
        } catch (final URISyntaxException e) {
            throw new SmartUriException("Unable to deserialize Smart URI", e);
        }
        final Map<IRI, Value> map = new HashMap<>();
        final RyaURI subject = findSubject(uri.stringValue());

        final List<NameValuePair> parameters = uriBuilder.getQueryParams();
        Map<RyaURI, String> entityTypeMap = new LinkedHashMap<>();
        Map<String, RyaURI> invertedEntityTypeMap = new LinkedHashMap<>();
        final Map<RyaURI, Map<IRI, Value>> fullMap = new LinkedHashMap<>();
        for (final NameValuePair pair : parameters) {
            final String keyString = pair.getName();
            final String valueString = pair.getValue();

            final IRI keyUri = new URIImpl(prefix + keyString);
            final String decoded;
            try {
                decoded = URLDecoder.decode(valueString, Charsets.UTF_8.name());
            } catch (final UnsupportedEncodingException e) {
                throw new SmartUriException("", e);
            }
            final IRI type = TypeDeterminer.determineType(decoded);
            if (type == XMLSchema.ANYURI) {
                if (keyString.equals(RYA_TYPES_URI.getLocalName())) {
                    entityTypeMap = convertUriToTypeMap(new URIImpl(decoded));
                    invertedEntityTypeMap = HashBiMap.create(entityTypeMap).inverse();
                }
            } else {
                final int keyPrefixLocation = keyString.indexOf(".");
                final String keyPrefix = keyString.substring(0, keyPrefixLocation);
                final RyaURI keyCorrespondingType = invertedEntityTypeMap.get(keyPrefix);
                final String keyName = keyString.substring(keyPrefixLocation + 1, keyString.length());
                final RyaType ryaType = new RyaType(type, valueString);

                final Value value = RyaToRdfConversions.convertValue(ryaType);

                final String formattedKeyUriString = removeTypePrefixFromUri(keyUri.stringValue(), keyPrefix);
                final IRI formattedKeyUri = new URIImpl(formattedKeyUriString);

                map.put(formattedKeyUri, value);
            }
        }
        return map;
    }

    public static Entity deserializeUriEntity(final IRI uri) throws SmartUriException {
        final String uriString = uri.stringValue();
        final int fragmentPosition = uriString.indexOf("#");
        String prefix = uriString.substring(0, fragmentPosition + 1);
        if (fragmentPosition == -1) {
            prefix = uriString.split("\\?", 2)[0];
        }
        final String fragment = uriString.substring(fragmentPosition + 1, uriString.length());
        java.net.URI queryUri;

        URIBuilder uriBuilder = null;
        try {
             if (fragmentPosition > -1) {
                 queryUri = new java.net.URI("urn://" + fragment);
             } else {
                 queryUri = new java.net.URI(uriString);
             }
            uriBuilder = new URIBuilder(queryUri);
        } catch (final URISyntaxException e) {
            throw new SmartUriException("Unable to deserialize Smart URI", e);
        }

        final RyaURI subject = findSubject(uri.stringValue());

        final List<NameValuePair> parameters = uriBuilder.getQueryParams();
        Map<RyaURI, String> entityTypeMap = new LinkedHashMap<>();
        Map<String, RyaURI> invertedEntityTypeMap = new LinkedHashMap<>();
        final Map<RyaURI, Map<IRI, Value>> fullMap = new LinkedHashMap<>();
        for (final NameValuePair pair : parameters) {
            final String keyString = pair.getName();
            final String valueString = pair.getValue();

            final IRI keyUri = new URIImpl(prefix + keyString);
            final String decoded;
            try {
                decoded = URLDecoder.decode(valueString, Charsets.UTF_8.name());
            } catch (final UnsupportedEncodingException e) {
                throw new SmartUriException("", e);
            }
            final IRI type = TypeDeterminer.determineType(decoded);
            if (type == XMLSchema.ANYURI) {
                if (keyString.equals(RYA_TYPES_URI.getLocalName())) {
                    entityTypeMap = convertUriToTypeMap(new URIImpl(decoded));
                    invertedEntityTypeMap = HashBiMap.create(entityTypeMap).inverse();
                }
            } else {
                final int keyPrefixLocation = keyString.indexOf(".");
                final String keyPrefix = keyString.substring(0, keyPrefixLocation);
                final RyaURI keyCorrespondingType = invertedEntityTypeMap.get(keyPrefix);
                final String keyName = keyString.substring(keyPrefixLocation + 1, keyString.length());
                final RyaType ryaType = new RyaType(type, valueString);

                final Value value = RyaToRdfConversions.convertValue(ryaType);

                final String formattedKeyUriString = removeTypePrefixFromUri(keyUri.stringValue(), keyPrefix);
                final IRI formattedKeyUri = new URIImpl(formattedKeyUriString);
                final Map<IRI, Value> map = fullMap.get(keyCorrespondingType);

                if (map == null) {
                    final Map<IRI, Value> subMap = new HashMap<>();
                    subMap.put(formattedKeyUri, value);
                    fullMap.put(keyCorrespondingType, subMap);
                } else {
                    map.put(formattedKeyUri, value);
                    fullMap.put(keyCorrespondingType, map);
                }
            }
        }
        final Entity entity = convertMapToEntity(subject, fullMap);
        return entity;
    }

    private static final class TypeDeterminer {
        /**
         * Private constructor to prevent instantiation.
         */
        private TypeDeterminer() {
        }

        private static IRI determineType(final String data) {
            if (Ints.tryParse(data) != null) {
                return XMLSchema.INTEGER;
            } else if (Doubles.tryParse(data) != null) {
                return XMLSchema.DOUBLE;
            } else if (Floats.tryParse(data) != null) {
                return XMLSchema.FLOAT;
            } else if (isShort(data)) {
                return XMLSchema.SHORT;
            } else if (Longs.tryParse(data) != null) {
                return XMLSchema.LONG;
            } if (Boolean.parseBoolean(data)) {
                return XMLSchema.BOOLEAN;
            } else if (isByte(data)) {
                return XMLSchema.BYTE;
            } else if (isDate(data)) {
                return XMLSchema.DATETIME;
            } else if (isUri(data)) {
                return XMLSchema.ANYURI;
            }

            return XMLSchema.STRING;
        }

        private static boolean isDate(final String data) {
            try {
                DateTime.parse(data, ISODateTimeFormat.dateTimeParser());
                return true;
            } catch (final IllegalArgumentException e) {
                // not a date
                return false;
            }
        }

        private static boolean isShort(final String data) {
            try {
                Short.parseShort(data);
                return true;
            } catch (final NumberFormatException e) {
                // not a short
                return false;
            }
        }

        private static boolean isByte(final String data) {
            try {
                Byte.parseByte(data);
                return true;
            } catch (final NumberFormatException e) {
                // not a byte
                return false;
            }
        }

        private static boolean isUri(final String data) {
            try {
                final String decoded = URLDecoder.decode(data, Charsets.UTF_8.name());
                new URIImpl(decoded);
                return true;
            } catch (final IllegalArgumentException | UnsupportedEncodingException e) {
                // not a URI
                return false;
            }
        }
    }

    public static Map<IRI, Value> entityToValueMap(final Entity entity) {
        final Map<IRI, Value> map = new LinkedHashMap<>();
        for (final Entry<RyaURI, ImmutableMap<RyaURI, Property>> entry : entity.getProperties().entrySet()) {
            for (final Entry<RyaURI, Property> property : entry.getValue().entrySet()) {
                final RyaURI propertyKey = property.getKey();
                final IRI uri = new URIImpl(propertyKey.getData());
                final Property propertyValue = property.getValue();
                final Value value = RyaToRdfConversions.convertValue(propertyValue.getValue());
                map.put(uri, value);
            }
        }
        return map;
    }

    /**
     * Converts a {@link Map} of {@link IRI}/{@link Value}s to a {@link Set} of
     * {@link Property}s.
     * @param map the {@link Map} of {@link IRI}/{@link Value}.
     * @return the {@link Set} of {@link Property}s.
     */
    public static Set<Property> mapToProperties(final Map<IRI, Value> map) {
        final Set<Property> properties = new LinkedHashSet<>();
        for (final Entry<IRI, Value> entry : map.entrySet()) {
            final IRI uri = entry.getKey();
            final Value value = entry.getValue();

            final RyaURI ryaUri = new RyaURI(uri.stringValue());
            final RyaType ryaType = RdfToRyaConversions.convertValue(value);

            final Property property = new Property(ryaUri, ryaType);
            properties.add(property);
        }
        return properties;
    }

    public static Map<IRI, Value> propertiesToMap(final Set<Property> properties) {
        final Map<IRI, Value> map = new LinkedHashMap<>();
        for (final Property property : properties) {
            final IRI uri = new URIImpl(property.getName().getData());
            final Value value = RyaToRdfConversions.convertValue(property.getValue());
            map.put(uri, value);
        }
        return map;
    }
}