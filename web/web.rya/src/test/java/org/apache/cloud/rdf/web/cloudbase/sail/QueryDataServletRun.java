package org.apache.cloud.rdf.web.cloudbase.sail;

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



import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLEncoder;

public class QueryDataServletRun {

    public static void main(String[] args) {
        try {
//		String query = "SELECT ?artist WHERE { ?abt <http://www.recshop.fake/cd#year> \"1988\"." +
//				" }";
            String artistQuery = "SELECT ?artist WHERE { "
                    + " ?abt <http://www.recshop.fake/cd#artist> ?artist . "
                    + " ?abt <http://www.recshop.fake/cd#year> \"1993\" . "
                    + "}";
//		String query = "SELECT ?pred ?obj WHERE { <http://www.recshop.fake/cd/Empire_Burlesque> ?pred ?obj }";
//		String query = "SELECT ?pred ?label ?obj WHERE { <http://purl.org/swag/sbp/tab#A5> ?pred ?obj ." +
//				" ?obj <http://www.w3.org/2000/01/rdf-schema#label> ?label }";
            long dayBefore = System.currentTimeMillis() - 86400000;
            System.out.println(dayBefore);
//        String query = "SELECT DISTINCT ?obj WHERE { ?serv <http://org.apache.com/rdf/mm/relatesTo> <http://org.apache.com/rdf/mm/LTS::stratus30> . " +
//				" ?serv <http://org.apache.com/rdf/mm/relatesTo> ?obj ." +
//                " ?serv <http://org.apache.com/rdf/mm/timestamp> ?ts ." +
////                " FILTER (?ts >= '"+dayBefore+"') " +
//                " }" +
//                " ORDER BY ?obj ";

            String giveAllClusters = "SELECT DISTINCT ?uu WHERE { ?uu <http://org.apache.com/rdf/mm/relatesTo> ?obj . " +
                    " }" +
                    " ORDER BY ?uu ";

//        String query = "SELECT DISTINCT ?obj WHERE { <http://org.apache.com/rdf/mm/1a4eaa7c-842c-456a-94c0-6547de6be841> <http://org.apache.com/rdf/mm/relatesTo> ?obj . " +
//                " }" +
//                " ORDER BY ?obj ";

            //hasfunction query
            String hasFunctionQuery = "SELECT DISTINCT ?obj WHERE { ?uu <http://org.apache.com/rdf/mm/hasFunction> <http://org.apache.com/rdf/mm/america> . " +
                    " ?uu <http://org.apache.com/rdf/mm/relatesTo> ?obj" +
                    " }" +
                    " ORDER BY ?obj ";

            String allFunctions = "SELECT DISTINCT ?func ?obj WHERE { ?uu <http://org.apache.com/rdf/mm/hasFunction> ?func . " +
                    " ?uu <http://org.apache.com/rdf/mm/relatesTo> ?obj" +
                    " }" +
                    " ORDER BY ?func ";

            String allFunctionsThresh = "SELECT DISTINCT ?func ?obj ?thresh WHERE { ?uu <http://org.apache.com/rdf/mm/hasFunction> ?func . " +
                    " ?uu <http://org.apache.com/rdf/mm/relatesTo> ?obj ." +
                    " ?uu <http://org.apache.com/rdf/mm/threshold> ?thresh" +
                    " }" +
                    " ORDER BY ?func ";


            String cwdQuery = "SELECT DISTINCT ?obj ?packname WHERE { ?subj <urn:org.apache.cwd/2.0/man/uuid> ?obj . " +
                    " ?subj <urn:org.apache.cwd/2.0/man/installedPackages> ?instPacks ." +
                    " ?instPacks <urn:org.apache.cwd/2.0/man/package> ?packid ." +
                    " ?packid <urn:org.apache.cwd/2.0/man/name> ?packname } ";

            String cwdAllServersQuery = "SELECT DISTINCT ?obj WHERE { ?subj <urn:org.apache.cwd/2.0/man/uuid> ?obj } ";

            // rearrange for better filter
            // 0.124s
            String lubm1 = "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" +
                    " PREFIX ub: <urn:lubm:rdfts#>\n" +
                    " SELECT * WHERE\n" +
                    " {\n" +
                    "     ?x ub:takesCourse <http://www.Department0.University0.edu/GraduateCourse2> .\n" +
                    "     ?x rdf:type ub:GraduateStudent .\n" +
                    " }";

            // 142s
            // not sure why it is so long will have to do some more tests
            String lubm2 = "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" +
                    " PREFIX ub: <urn:edu.lubm#>\n" +
                    " SELECT * WHERE\n" +
                    " {\n" +
                    "      ?z ub:subOrganizationOf ?y .\n" +
                    "      ?y rdf:type ub:University .\n" +
                    "      ?z rdf:type ub:Department .\n" +
                    "      ?x ub:memberOf ?z .\n" +
                    "      ?x rdf:type ub:GraduateStudent .\n" +
                    "      ?x ub:undergraduateDegreeFrom ?y .\n" +
                    " }";

            String lubm2_a = "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" +
                    " PREFIX ub: <http://test.univ.onto.org#>\n" +
                    " SELECT * WHERE\n" +
                    " {\n" +
                    "      ?x rdf:type ub:GraduateStudent .\n" +
                    "      ?x ub:memberOf ?z .\n" +
                    "      ?z ub:subOrganizationOf ?y .\n" +
                    "      ?z rdf:type ub:Department .\n" +
                    "      ?y rdf:type ub:University .\n" +
//                "      ?x ub:undergraduateDegreeFrom ?y .\n" +
                    " }";

            // 0.127s
            // Rearranged to put the assistant professor first, better filtering
            String lubm3 = "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" +
                    " PREFIX ub: <urn:edu.lubm#>\n" +
                    " SELECT * WHERE\n" +
                    " {\n" +
                    "      ?x ub:publicationAuthor <http://www.Department0.University0.edu/AssistantProfessor0> .\n" +
                    "      ?x rdf:type ub:Publication .\n" +
                    " }";

//        had to infer relationships myself
//        0.671s
            String lubm4 = "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" +
                    " PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n" +
                    " PREFIX ub: <urn:edu.lubm#>\n" +
                    " SELECT * WHERE\n" +
                    " {\n" +
                    "      ?y ub:worksFor <http://www.Department0.University5.edu> .\n" +
                    "      ?x rdfs:subClassOf ub:Professor .\n" +
                    "      ?y rdf:type ?x .\n" +
                    "      ?y ub:name ?y1 .\n" +
                    "      ?y ub:emailAddress ?y2 .\n" +
                    "      ?y ub:telephone ?y3 .\n" +
                    " }";

            //lubm5, we cannot do inferring for more than one level now. Person is too difficult

            //lubm6, we cannot do the implicit inference between Student and GraduateStudent

            //lubm14
            //0.1s
            String lubm14 = "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" +
                    " PREFIX ub: <urn:edu.lubm#>\n" +
                    " SELECT * WHERE\n" +
                    " {\n" +
                    "     ?x rdf:type ub:UndergraduateStudent .\n" +
                    " }";

            String bongoAllCollections = "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" +
                    " PREFIX bg: <http://org.apache.com/rdf/bongo/bongo.owl#>\n" +
                    " SELECT * WHERE\n" +
                    " {\n" +
                    "     ?x rdf:type bg:Collection .\n" +
                    "     ?x bg:uniqueid ?uid .\n" +
                    "     ?x bg:title ?title .\n" +
                    "     ?x bg:hasAuthor ?author .\n" +
                    "     ?x bg:marking ?marking .\n" +
                    " }";

            String bongoEntriesForCategory = "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" +
                    " PREFIX bg: <http://org.apache.com/rdf/bongo/bongo.owl#>\n" +
                    " SELECT ?uniqueid WHERE\n" +
                    " {\n" +
                    "     ?entryid bg:inCollection bg:CollA .\n" +
                    "     ?entryid rdf:type bg:Entry .\n" +
                    "     ?entryid bg:uniqueid ?uniqueid .\n" +
                    "     ?entryid bg:hasCategory ?category .\n" +
                    "     FILTER (?category = \"cat1\") \n" +
                    " }";

            String bongoEntriesForAuthor = "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" +
                    " PREFIX bg: <http://org.apache.com/rdf/bongo/bongo.owl#>\n" +
                    " SELECT ?uniqueid WHERE\n" +
                    " {\n" +
                    "     ?entryid bg:inCollection bg:CollA .\n" +
                    "     ?entryid rdf:type bg:Entry .\n" +
                    "     ?entryid bg:uniqueid ?uniqueid .\n" +
                    "     ?entryid bg:hasAuthor ?author .\n" +
                    "     FILTER (?author = \"andrew2\") \n" +
                    " }";

            String bongoEntriesForModifiedTime = "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" +
                    " PREFIX bg: <http://org.apache.com/rdf/bongo/bongo.owl#>\n" +
                    " PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\n" +
                    " SELECT DISTINCT ?entryid WHERE\n" +
                    " {\n" +
                    "     ?entryid bg:inCollection bg:CollA .\n" +
                    "     ?entryid rdf:type bg:Entry .\n" +
                    "     ?entryid bg:uniqueid ?uniqueid .\n" +
                    "     ?entryid bg:modifiedTime ?modifiedTime .\n" +
                    "     FILTER (xsd:dateTime(?modifiedTime) >= \"2011-10-21T13:18:30\"^^xsd:dateTime) \n" +
                    " }";
            String bongoEntriesSortTitle = "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" +
                    " PREFIX bg: <http://org.apache.com/rdf/bongo/bongo.owl#>\n" +
                    " PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\n" +
                    " SELECT DISTINCT ?uniqueid WHERE\n" +
                    " {\n" +
                    "     ?entryid bg:inCollection bg:CollA .\n" +
                    "     ?entryid rdf:type bg:Entry .\n" +
                    "     ?entryid bg:uniqueid ?uniqueid .\n" +
                    "     ?entryid bg:title ?title .\n" +
                    " } ORDER BY ?title";

            String bongoEntriesForTitle = "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" +
                    " PREFIX bg: <http://org.apache.com/rdf/bongo/bongo.owl#>\n" +
                    " PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\n" +
                    " SELECT DISTINCT ?uniqueid WHERE\n" +
                    " {\n" +
                    "     ?entryid bg:inCollection bg:CollA .\n" +
                    "     ?entryid rdf:type bg:Entry .\n" +
                    "     ?entryid bg:uniqueid ?uniqueid .\n" +
                    "     ?entryid bg:title ?title .\n" +
                    "     FILTER (regex(?title,\"Entry1Title\")) }";

            String bongoQuery = "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" +
                    " PREFIX bg: <http://org.apache.com/rdf/bongo/bongo.owl#>\n" +
                    " SELECT * WHERE\n" +
                    " {\n" +
                    "     ?col rdf:type bg:Collection .\n" +
//                "     OPTIONAL{ bg:latency_mixture2_perSupplier_norm2\\/S\\/P\\/Stock\\/Google_simple\\/6 bg:uniqueid ?uniqueid} .\n" +
//                "     OPTIONAL{ bg:'latency_mixture2_perSupplier_norm2/S/P/Stock/Google_simple/6' bg:title ?title} .\n" +
//                "     OPTIONAL{ bg:latency_mixture2_perSupplier_norm2/S/P/Stock/Google_simple/6 bg:name ?name} .\n" +
//                "     OPTIONAL{ bg:latency_mixture2_perSupplier_norm2/S/P/Stock/Google_simple/6 bg:marking ?marking} .\n" +
//                "     OPTIONAL{ bg:latency_mixture2_perSupplier_norm2/S/P/Stock/Google_simple/6 bg:hasAuthor ?author} .\n" +
                    " }";

            String bongoAllEntriesInCollection = "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" +
                    " PREFIX bg: <http://org.apache.com/rdf/bongo/bongo.owl#>\n" +
                    " SELECT * WHERE\n" +
                    " {\n" +
                    "     ?y bg:inCollection bg:CollA .\n" +
                    "     ?y rdf:type bg:Entry .\n" +
                    "     ?y bg:uniqueid ?uid .\n" +
                    "     ?y bg:title ?title .\n" +
                    "     ?y bg:etag ?etag .\n" +
                    " }";

            String bongoAllForEntry1 = "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" +
                    " PREFIX bg: <http://org.apache.com/rdf/bongo/bongo.owl#>\n" +
                    " SELECT * WHERE\n" +
                    " {\n" +
                    "     bg:EntryM rdf:type bg:Entry .\n" +
//                "     bg:EntryN bg:inCollection bg:CollectionN .\n" +
                    "     bg:EntryM bg:mimeType ?mimeType .\n" +
                    "     bg:EntryM bg:etag ?etag .\n" +
                    "     OPTIONAL { bg:EntryM bg:slug ?slug}.\n" +
                    "     bg:EntryM bg:uniqueid ?uniqueid .\n" +
//                "     bg:EntryN bg:title ?title .\n" +
//                "     bg:EntryN bg:marking ?marking .\n" +
//                "     bg:EntryN bg:mediaMarking ?mediaMarking .\n" +
//                "     bg:EntryN bg:editedTime ?editedTime .\n" +
//                "     bg:EntryN bg:modifiedTime ?modifiedTime .\n" +
//                "     bg:EntryN bg:publishedTime ?publishedTime .\n" +
//                "     bg:EntryN bg:mediaStorageId ?mediaStorageId .\n" +
//                "     bg:EntryN bg:mediaModifiedTime ?mediaModifiedTime .\n" +
//                "     bg:EntryN bg:entryStorageId ?entryStorageId .\n" +
                    " }";

            String bongoEntryAllAuthors = "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" +
                    " PREFIX bg: <http://org.apache.com/rdf/bongo/bongo.owl#>\n" +
                    " SELECT * WHERE\n" +
                    " {\n" +
                    "     bg:Entry1 bg:hasAuthor ?y .\n" +
                    " }";

            String bongoEntriesModAfter = "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" +
                    " PREFIX bg: <http://org.apache.com/rdf/bongo/bongo.owl#>\n" +
                    " PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\n" +
                    " SELECT * WHERE\n" +
                    " {\n" +
                    "     ?x bg:editedTime ?edTime .\n" +
                    "     FILTER (xsd:dateTime(?edTime) >= \"2010-01-01T00:00:00\"^^xsd:dateTime)\n" +
                    " }";

            String cimData = "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" +
                    " PREFIX mm: <http://org.apache.com/owl/mm.owl#>\n" +
                    " PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\n" +
                    " SELECT * WHERE\n" +
                    " {\n" +
                    "     ?x rdf:type mm:ComputerSystem .\n" +
                    "     ?x mm:hasRunningOS ?y .\n" +
                    "     ?y mm:name ?z .\n" +
                    " }";

            String cimData2 = "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" +
                    "  PREFIX mm: <http://org.apache.com/owl/mm.owl#>\n" +
                    "  PREFIX mmcs: <http://org.apache.com/owl/mm.owl#urn:uuid:some:>\n" +
                    "  SELECT  ?pred ?obj WHERE {\n" +
                    "       mmcs:computersystem ?pred ?obj\n" +
                    "  }";

            String cimData3 = "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" +
                    "SELECT ?pred ?obj WHERE {\n" +
                    "<http://org.apache.com/owl/mm.owl#urn:mm:org.apache:lts:root/cimv2:PG_OperatingSystem.CreationClassName=CIM_OperatingSystem,CSCreationClassName=CIM_UnitaryComputerSystem,CSName=nimbus02.bullpen.net,Name=Red_Hat_Enterprise_Linux_Server> ?pred ?obj\n" +
                    "}";

            String cimHasInstalledSoftware = "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" +
                    "PREFIX mm: <http://org.apache.com/owl/2010/10/mm.owl#>\n" +
                    "SELECT DISTINCT ?obj ?name ?caption WHERE {\n" +
//                "     <http://org.apache.com/owl/2010/10/mm.owl#urn:mm:org.apache:LTS:root/cimv2:PG_OperatingSystem:CIM_ComputerSystem:stratus06.bullpen.net:Red_Hat_Enterprise_Linux_Server> mm:hasInstalledSoftware ?obj .\n" +
                    "     ?serv mm:hasInstalledSoftware ?obj .\n" +
                    "      ?obj mm:name ?name ;\n" +
                    "           mm:caption ?caption .\n" +
                    "}";

            String cimHasRunningSoftware = "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" +
                    "PREFIX mm: <http://org.apache.com/owl/2010/10/mm.owl#>\n" +
                    "SELECT * WHERE {\n" +
                    "     <http://org.apache.com/owl/2010/10/mm.owl#urn:mm:org.apache:LTS:root/cimv2:PG_OperatingSystem:CIM_ComputerSystem:stratus10:Red_Hat_Enterprise_Linux_Server> mm:hasRunningProcess ?obj .\n" +
                    "     ?obj mm:name ?name ; \n" +
                    "          mm:handle ?handle ; \n" +
                    "          mm:description ?description ; \n" +
                    "          mm:caption ?caption ; \n" +
                    "          mm:parameters ?params . \n" +
                    "}";

            String cimCpu = "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" +
                    "PREFIX mm: <http://org.apache.com/owl/2010/10/mm.owl#>\n" +
                    "SELECT * \n" +
                    "WHERE {\n" +
                    "     <http://org.apache.com/owl/2010/10/mm.owl#urn:mm:org.apache:LTS:root/cimv2:CIM_ComputerSystem:stratus10> mm:hasProcessor ?obj .\n" +
                    "     ?obj mm:maxClockSpeed ?speed .\n" +
                    "     ?obj mm:loadPercentage ?load .\n" +
                    "     ?obj mm:elementName ?type ." +
                    "}";

            String cimCpuLoad = "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" +
                    "PREFIX mm: <http://org.apache.com/owl/2010/10/mm.owl#>\n" +
                    "SELECT * \n" +
                    "WHERE {\n" +
                    "     <http://org.apache.com/owl/2010/10/mm.owl#urn:mm:org.apache:LTS:root/cimv2:CIM_ComputerSystem:stratus10> mm:hasProcessor ?obj .\n" +
                    "     ?obj mm:loadPercentage ?load ." +
                    "}";


            String cimHasFileSystem = "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" +
                    "PREFIX mm: <http://org.apache.com/owl/2010/10/mm.owl#>\n" +
                    "SELECT * WHERE {\n" +
//                "     <http://org.apache.com/owl/2010/10/mm.owl#urn:mm:org.apache:LTS:root/cimv2:PG_OperatingSystem:CIM_ComputerSystem:stratus10:Red_Hat_Enterprise_Linux_Server> mm:hasFileSystem ?obj ." +
                    "     ?serv mm:hasFileSystem ?obj ." +
                    "     ?obj mm:availableSpace ?available .\n" +
                    "     ?obj mm:fileSystemSize ?size .\n" +
                    "     ?obj mm:percentageSpaceUse ?use ." +
                    "}";

            String clusterKolm = "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" +
                    "PREFIX mm: <http://org.apache.com/owl/2010/10/mm.owl#>\n" +
                    "PREFIX kolm: <http://org.apache.com/lrn/2010/11/kolm#>\n" +
                    "SELECT ?name ?cluster ?srv ?ncd ?thresh ?ts WHERE {\n" +
                    "     ?cluster kolm:relatesTo ?pt ;\n" +
                    "              kolm:threshold ?thresh .\n" +
                    "     ?pt kolm:serverRef ?srv ;\n" +
                    "         kolm:ncd ?ncd ;\n" +
                    "         kolm:timestamp ?ts .\n" +
                    "     ?srv mm:CSName ?name .\n" +
                    "} \n" +
                    " ORDER BY ?cluster ?srv ?ncd";

            String clusterKolm2 = "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" +
                    "PREFIX mm: <http://org.apache.com/owl/2010/10/mm.owl#>\n" +
                    "PREFIX kolm: <http://org.apache.com/lrn/2010/11/kolm#>\n" +
                    "SELECT ?cserv ?srv ?ncd ?thresh ?ts WHERE {\n" +
                    "     ?cpt kolm:ncd \"0.0\" .\n" +
                    "     ?cpt kolm:serverRef ?cserv .\n" +
                    "     ?cluster kolm:relatesTo ?cpt ;\n" +
                    "              kolm:relatesTo ?pt ;\n" +
                    "              kolm:timestamp ?cts ;\n" +
                    "              kolm:threshold ?thresh .\n" +
                    "     ?pt kolm:serverRef ?srv ;\n" +
                    "         kolm:ncd ?ncd ;\n" +
                    "         kolm:timestamp ?ts .\n" +
//                "     ?srv mm:CSName ?name .\n" +
                    " FILTER (?cts >= \"1290616617624\")" +
                    "} \n" +
                    " ORDER BY ?cserv ?ncd ?srv";

            String clusterKolmOtherClusters = "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" +
                    "PREFIX mm: <http://org.apache.com/owl/2010/10/mm.owl#>\n" +
                    "PREFIX kolm: <http://org.apache.com/lrn/2010/11/kolm#>\n" +
                    "SELECT ?cserv ?srv ?ncd WHERE {\n" +
                    "     ?cpt kolm:ncd \"0.0\" .\n" +
                    "     ?cpt kolm:serverRef ?cserv .\n" +
                    "     ?cluster kolm:relatesTo ?cpt .\n" +
                    "     ?cluster kolm:distanceTo ?pt .\n" +
                    "     ?cluster kolm:timestamp ?cts .\n" +
//                "              kolm:threshold ?thresh .\n" +
                    "     ?pt kolm:serverRef ?srv ;\n" +
                    "         kolm:ncd ?ncd ;\n" +
                    "         kolm:timestamp ?ts .\n" +
//                "     ?srv mm:CSName ?name .\n" +
                    " FILTER (?cts >= \"1290616617624\")" +
                    "} \n" +
                    " ORDER BY ?cserv ?srv ?ncd";

            String clusterKolmStratus13 = "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" +
                    "PREFIX mm: <http://org.apache.com/owl/2010/10/mm.owl#>\n" +
                    "PREFIX kolm: <http://org.apache.com/lrn/2010/11/kolm#>\n" +
                    "SELECT DISTINCT ?srv ?ncd WHERE {\n" +
                    "     ?pt kolm:serverRef <http://org.apache.com/owl/2010/10/mm.owl#urn:mm:org.apache:LTS:root/cimv2:PG_OperatingSystem:CIM_ComputerSystem:stratus13:Red_Hat_Enterprise_Linux_Server> .\n" +
                    "     ?cluster kolm:relatesTo ?pt .\n" +
                    "     ?cluster kolm:relatesTo ?pt2 .\n" +
                    "     ?pt2 kolm:serverRef ?srv .\n" +
//                "     ?cluster kolm:relatesTo ?pt ;\n" +
//                "              kolm:threshold ?thresh .\n" +
//                "     ?pt kolm:serverRef <http://org.apache.com/owl/2010/10/mm.owl#urn:mm:org.apache:LTS:root/cimv2:PG_OperatingSystem:CIM_ComputerSystem:stratus10:Red_Hat_Enterprise_Linux_Server> ;\n" +
                    "       ?pt2  kolm:ncd ?ncd .\n" +
                    "       ?cluster kolm:timestamp ?ts .\n" +
//                "     <http://org.apache.com/owl/2010/10/mm.owl#urn:mm:org.apache:LTS:root/cimv2:PG_OperatingSystem:CIM_ComputerSystem:stratus10:Red_Hat_Enterprise_Linux_Server> mm:CSName ?name .\n" +
                    "} \n" +
                    " ORDER BY ?ncd";

            String cimLatestMeasure = "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" +
                    "PREFIX mm: <http://org.apache.com/owl/2010/10/mm.owl#>\n" +
                    "SELECT ?proc ?val ?time WHERE {\n" +
                    "     ?proc mm:loadPercentage ?val .\n" +
                    "     ?subj rdf:subject ?proc .\n" +
                    "     ?subj rdf:object ?val2 .\n" +
                    "     ?subj  rdf:type rdf:Statement ;\n" +
                    "     \t    mm:reportedAt ?time .\n" +
                    " FILTER (?val2 = ?val) }\n" +
                    "ORDER BY DESC(?time)\n" +
                    "LIMIT 250";

//        String query = "DELETE {?subj <http://org.apache.com/rdf/mm/relatesTo> <http://org.apache.com/rdf/mm/LTS::stratus30>} WHERE { ?subj <http://org.apache.com/rdf/mm/relatesTo> <http://org.apache.com/rdf/mm/LTS::stratus30>}";
//
            String query = "select * where {\n" +
                    "<http://mynamespace/ProductType1> ?p ?o.\n" +
                    "}";
            System.out.println(query);
            System.out.println(System.currentTimeMillis());

            /**
             * Create url object to POST to the running container
             */

            String queryenc = URLEncoder.encode(query, "UTF-8");

            URL url = new URL("http://localhost:8080/rdfTripleStore/queryrdf?query=" + queryenc);
            URLConnection urlConnection = url.openConnection();
            urlConnection.setDoOutput(true);

            /**
             * Get the corresponding response from server, if any
             */
            BufferedReader rd = new BufferedReader(new InputStreamReader(
                    urlConnection.getInputStream()));
            String line;
            while ((line = rd.readLine()) != null) {
                System.out.println(line);
            }
            rd.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
