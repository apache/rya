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

import java.nio.charset.StandardCharsets;

import org.apache.hadoop.io.Text;
import org.apache.rya.api.domain.RyaSchema;
import org.apache.rya.api.domain.RyaType;
import org.apache.rya.api.domain.RyaIRI;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;

public class RdfCloudTripleStoreConstants {

    public static final String NAMESPACE = RyaSchema.NAMESPACE;
    public static final String AUTH_NAMESPACE = RyaSchema.AUTH_NAMESPACE;
    public static ValueFactory VALUE_FACTORY = SimpleValueFactory.getInstance();
    public static IRI RANGE = VALUE_FACTORY.createIRI(NAMESPACE, "range");
    public static IRI PARTITION_TIMERANGE = VALUE_FACTORY.createIRI("urn:org.apache.mmrts.partition.rdf/08/2011#", "timeRange");
    public static Literal EMPTY_LITERAL = VALUE_FACTORY.createLiteral(0);
    public static final byte EMPTY_BYTES[] = new byte[0];
    public static final Text EMPTY_TEXT = new Text();

    public static final Long MAX_MEMORY = 10000000l;
    public static final Long MAX_TIME = 60000l;
    public static final Integer NUM_THREADS = 4;

//    public static final String TS = "ts";
//    public static final Text TS_TXT = new Text(TS);

//    public static final String INFO = "info";
//    public static final Text INFO_TXT = new Text(INFO);

    public static final String SUBJECT_CF = "s";
    public static final Text SUBJECT_CF_TXT = new Text(SUBJECT_CF);
    public static final String PRED_CF = "p";
    public static final Text PRED_CF_TXT = new Text(PRED_CF);
    public static final String OBJ_CF = "o";
    public static final Text OBJ_CF_TXT = new Text(OBJ_CF);
    public static final String SUBJECTOBJECT_CF = "so";
    public static final Text SUBJECTOBJECT_CF_TXT = new Text(SUBJECTOBJECT_CF);
    public static final String SUBJECTPRED_CF = "sp";
    public static final Text SUBJECTPRED_CF_TXT = new Text(SUBJECTPRED_CF);
    public static final String PREDOBJECT_CF = "po";
    public static final Text PREDOBJECT_CF_TXT = new Text(PREDOBJECT_CF);

    public static final String TBL_PRFX_DEF = "rya_";
    public static final String TBL_SPO_SUFFIX = "spo";
    public static final String TBL_PO_SUFFIX = "po";
    public static final String TBL_OSP_SUFFIX = "osp";
    public static final String TBL_EVAL_SUFFIX = "eval";
    public static final String TBL_STATS_SUFFIX = "prospects";
    public static final String TBL_SEL_SUFFIX = "selectivity";
    public static final String TBL_NS_SUFFIX = "ns";
    public static String TBL_SPO = TBL_PRFX_DEF + TBL_SPO_SUFFIX;
    public static String TBL_PO = TBL_PRFX_DEF + TBL_PO_SUFFIX;
    public static String TBL_OSP = TBL_PRFX_DEF + TBL_OSP_SUFFIX;
    public static String TBL_EVAL = TBL_PRFX_DEF + TBL_EVAL_SUFFIX;
    public static String TBL_STATS = TBL_PRFX_DEF + TBL_STATS_SUFFIX;
    public static String TBL_SEL = TBL_PRFX_DEF + TBL_SEL_SUFFIX;
    public static String TBL_NAMESPACE = TBL_PRFX_DEF + TBL_NS_SUFFIX;

    public static Text TBL_SPO_TXT = new Text(TBL_SPO);
    public static Text TBL_PO_TXT = new Text(TBL_PO);
    public static Text TBL_OSP_TXT = new Text(TBL_OSP);
    public static Text TBL_EVAL_TXT = new Text(TBL_EVAL);
    public static Text TBL_NAMESPACE_TXT = new Text(TBL_NAMESPACE);

    public static void prefixTables(String prefix) {
        if (prefix == null) {
            prefix = TBL_PRFX_DEF;
        }
        TBL_SPO = prefix + TBL_SPO_SUFFIX;
        TBL_PO = prefix + TBL_PO_SUFFIX;
        TBL_OSP = prefix + TBL_OSP_SUFFIX;
        TBL_EVAL = prefix + TBL_EVAL_SUFFIX;
        TBL_NAMESPACE = prefix + TBL_NS_SUFFIX;

        TBL_SPO_TXT = new Text(TBL_SPO);
        TBL_PO_TXT = new Text(TBL_PO);
        TBL_OSP_TXT = new Text(TBL_OSP);
        TBL_EVAL_TXT = new Text(TBL_EVAL);
        TBL_NAMESPACE_TXT = new Text(TBL_NAMESPACE);
    }

    public static final String INFO_NAMESPACE = "namespace";
    public static final Text INFO_NAMESPACE_TXT = new Text(INFO_NAMESPACE);

    public static final byte DELIM_BYTE = 0;
    public static final byte TYPE_DELIM_BYTE = 1;
    public static final byte LAST_BYTE = -1; //0xff
    public static final byte[] LAST_BYTES = new byte[]{LAST_BYTE};
    public static final byte[] TYPE_DELIM_BYTES = new byte[]{TYPE_DELIM_BYTE};
    public static final String DELIM = "\u0000";
    public static final String DELIM_STOP = "\u0001";
    public static final String LAST = "\uFFDD";
    public static final String TYPE_DELIM = new String(TYPE_DELIM_BYTES, StandardCharsets.UTF_8);
    public static final byte[] DELIM_BYTES = DELIM.getBytes(StandardCharsets.UTF_8);
    public static final byte[] DELIM_STOP_BYTES = DELIM_STOP.getBytes(StandardCharsets.UTF_8);


    /* RECORD TYPES */
    public static final int URI_MARKER = 7;

    public static final int BNODE_MARKER = 8;

    public static final int PLAIN_LITERAL_MARKER = 9;

    public static final int LANG_LITERAL_MARKER = 10;

    public static final int DATATYPE_LITERAL_MARKER = 11;

    public static final int EOF_MARKER = 127;

    //	public static final Authorizations ALL_AUTHORIZATIONS = new Authorizations(
    //	"_");

    public enum TABLE_LAYOUT {
        SPO, PO, OSP
    }

    //TODO: This should be in a version file somewhere
    public static IRI RTS_SUBJECT = VALUE_FACTORY.createIRI(NAMESPACE, "rts");
    public static RyaIRI RTS_SUBJECT_RYA = new RyaIRI(RTS_SUBJECT.stringValue());
    public static IRI RTS_VERSION_PREDICATE = VALUE_FACTORY.createIRI(NAMESPACE, "version");
    public static RyaIRI RTS_VERSION_PREDICATE_RYA = new RyaIRI(RTS_VERSION_PREDICATE.stringValue());
    public static final Value VERSION = VALUE_FACTORY.createLiteral("3.0.0");
    public static RyaType VERSION_RYA = new RyaType(VERSION.stringValue());

    public static String RYA_CONFIG_AUTH = "RYACONFIG";
}
