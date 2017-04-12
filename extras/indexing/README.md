<!-- Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License. -->

# Rya Indexing

___

This project contains implementations of Rya's Indexing components.

Rya Indexing supports the following datastores:

  * Accumulo

  * MongoDB

## Entity Indexing
An Entity is a named concept that has at least one defined structure
and a bunch of values that fit within each of those structures. A structure is
defined by a Type. A value that fits within that Type is a Property.
</p>
For example, suppose we want to represent a type of icecream as an Entity.
First we must define what properties an icecream entity may have:
```
    Type ID: <urn:icecream>
 Properties: <urn:brand>
             <urn:flavor>
             <urn:ingredients>
             <urn:nutritionalInformation>
```
Now we can represent our icecream whose brand is "Awesome Icecream" and whose
flavor is "Chocolate", but has no ingredients or nutritional information, as
an Entity by doing the following:
```
final Entity entity = Entity.builder()
             .setSubject(new RyaURI("urn:GTIN-14/00012345600012"))
             .setExplicitType(new RyaURI("urn:icecream"))
             .setProperty(new RyaURI("urn:icecream"), new Property(new RyaURI("urn:brand"), new RyaType(XMLSchema.STRING, "Awesome Icecream")))
             .setProperty(new RyaURI("urn:icecream"), new Property(new RyaURI("urn:flavor"), new RyaType(XMLSchema.STRING, "Chocolate")))
             .build();
```
The two types of Entities that may be created are implicit and explicit.
An implicit Entity is one who has at least one Property that matches
the Type, but nothing has explicitly indicated it is of  that Type.
Once something has done so, it is an explicitly typed Entity.

### Smart URI
A Smart URI is an indentifier of a specific Entity with all its data fields and associated values. Smart URI's are only currently supported by MongoDB instances of Rya.

The Smart URI format:

[subject URI]?ryaTypes=[encoded map of type URIs to type names]&[type1.propertyName1]=[value1]&[type1.propertyName2]=[value2]&[type2.propertyName1]=[value3]

The "subject URI" can be any URI.  The Entities' properties are expressed as query parameters in the Smart URI.  Since the Smart URI may represent an Entity of multiple Types, the "ryaTypes" query parameter is used to keep track of which property belongs to which Type.  The Type's mapped short name is appended to each property name and each name/value pair has their characters escaped properly to produce a valid URI.  This means all foreign character and special characters will will be encoded so that they conform to [RFC-3986](https://www.ietf.org/rfc/rfc3986.txt).

Using the "icecream" Type Entity example from the "Entity Indexing" section above, the Smart URI representation would be:

`urn://GTIN-14/00012345600012?ryaTypes=urn%3A%2F%2FentityTypeMap%3Furn%253Aicecream%3Dicecream&icecream.brand=Awesome+Icecream&icecream.flavor=Chocolate`

As an example of an Entity with multiple Types, lets consider the Entity as also being part of the following "dessert" type:
```xml
    Type ID: <urn:dessert>
 Properties: <urn:storageNeeded>
             <urn:utensilUsed>
```

The Smart URI representation would be:

`urn://GTIN-14/00012345600012?ryaTypes=urn%3A%2F%2FentityTypeMap%3Furn%253Aicecream%3Dicecream%26urn%253Adessert%3Ddessert&dessert.storageNeeded=Freezer&dessert.utensilUsed=Spoon&icecream.brand=Awesome+Icecream&icecream.flavor=Chocolate`

#### Smart URI Entity Duplication Detection

In some cases, data that is close enough together to be considered nearly identical should be treated as duplicate Entities.  Duplicate data detection can be enabled so that newly found Entities that appear close enough to existing Entities should not be created.

##### Configuring Duplication Detection

This sections discusses how to configure the various options of Smart URI Entity Duplication Detection.  To edit Duplication Detection, create or modify the `duplicate_data_detection_config.xml` in the `conf` directory.

It should look similar to this:
```xml
<duplicateDataDetectionConfiguration>
    <enableDetection>true</enableDetection>
    <tolerances>
        <booleanTolerance>
            <value>0</value>
            <type>DIFFERENCE</type>
        </booleanTolerance>
        <byteTolerance>
            <value>0</value>
            <type>DIFFERENCE</type>
        </byteTolerance>
        <dateTolerance>
            <value>500</value>
            <type>DIFFERENCE</type>
        </dateTolerance>
        <doubleTolerance>
            <value>0.01%</value>
            <type>PERCENTAGE</type>
        </doubleTolerance>
        <floatTolerance>
            <value>0.01%</value>
            <type>PERCENTAGE</type>
        </floatTolerance>
        <integerTolerance>
            <value>1</value>
            <type>DIFFERENCE</type>
        </integerTolerance>
        <longTolerance>
            <value>1</value>
            <type>DIFFERENCE</type>
        </longTolerance>
        <shortTolerance>
            <value>1</value>
            <type>DIFFERENCE</type>
        </shortTolerance>
        <stringTolerance>
            <value>1</value>
            <type>DIFFERENCE</type>
        </stringTolerance>
        <uriTolerance>
            <value>1</value>
            <type>DIFFERENCE</type>
        </uriTolerance>
    </tolerances>
    <termMappings>
        <termMapping>
            <term>example</term>
            <equivalents>
                <equivalent>sample</equivalent>
                <equivalent>case</equivalent>
            </equivalents>
        </termMapping>
        <termMapping>
            <term>test</term>
            <equivalents>
                <equivalent>exam</equivalent>
                <equivalent>quiz</equivalent>
            </equivalents>
        </termMapping>
    </termMappings>
</duplicateDataDetectionConfiguration>
```

###### Enabling/Disabling Duplication Detection

To enable detection, set `<enableDetection>` to "true".  Setting to "false" will disable it.

###### Tolerance Type Configuration

Each data type can have a tolerance type set for it (either "PERCENTAGE" or "DIFFERENCE").  If "DIFFERENCE" is selected then the data types are considered duplicates if they are within the `<value>` specified.  The `<value>` must be positive.  So, the two integer values 50000 and 50001 are considered duplicates when configured like this:
```xml
        <integerTolerance>
            <value>1</value>
            <type>DIFFERENCE</type>
        </integerTolerance>
```

If "PERCENTAGE" is selected then the values must be within a certain percent to one another to be considered nearly identical duplicates.  This is useful when dealing with really small numbers or really large numbers where an exact absolute difference is hard to determine to consider them nearly equal.  The `<value>` can be expressed in percent or decimal form.  So, one percent can be entered as "1%" or "0.01" in the `<value>` field.  The `<value>` must be positive and cannot be greater than 100%.

If a Type's tolerance is not specified it defers to a default value.
The default values are:

| **Class** | **Tolerance Type** | **Value** |
|-----------|--------------------|-----------|
| boolean   | DIFFERENCE         | 0         |
| byte      | DIFFERENCE         | 0         |
| date      | DIFFERENCE         | 500 (ms)  |
| double    | PERCENTAGE         | 0.01%     |
| float     | PERCENTAGE         | 0.01%     |
| integer   | DIFFERENCE         | 1         |
| long      | DIFFERENCE         | 1         |
| short     | DIFFERENCE         | 1         |
| string    | PERCENTAGE         | 5%        |
| uri       | DIFFERENCE         | 1         |

###### Equivalent Terms Configuration

Words that one wants to consider equivalent can be inserted into a map under the `<termMappings>` part of the configuration file.  An example `<termMapping>` entry is shown below:

```xml
        <termMapping>
            <term>example</term>
            <equivalents>
                <equivalent>sample</equivalent>
                <equivalent>case</equivalent>
            </equivalents>
        </termMapping>
```

This `<termMapping>` means that a new Entity with a field value of "sample" or "case" would be equivalent to an existing field value of "example" but this relationship is only one way.  If a field value of "case" already exists then a new field value "example" is not equivalent.  The relationship can be made bidirectional by adding another `<termMapping>`.

```xml
        <termMapping>
            <term>case</term>
            <equivalents>
                <equivalent>example</equivalent>
            </equivalents>
        </termMapping>
```
