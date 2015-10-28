package mvm.mmrts.rdf.partition;

import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.query.*;
import org.openrdf.query.resultio.sparqlxml.SPARQLResultsXMLWriter;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.sail.SailRepository;

import java.util.Calendar;
import java.util.List;

public class QueryPartitionData {

    public void run() throws Exception {
        try {
            // if (args.length == 0) {
            // throw new IllegalArgumentException("Specify query file");
            // }
            // String fileLoc = args[0];
            // File queryFile = new File(fileLoc);
            // final ByteArrayOutputStream baos = new ByteArrayOutputStream();
            // ByteStreams.copy(new FileInputStream(queryFile), baos);
            // String query = new String(baos.toByteArray());

            final PartitionSail store = new PartitionSail("stratus", "stratus13:2181", "root", "password",
                    "partTest", "shardIndexTest");
//            store.setTablePrefix("str_");
//            store.getTimeUris().put(new URIImpl("http://here/2010/tracked-data-provenance/ns#performedAt"), DateTimeTtlValueConverter.class);
//            store.getTimeUris().add(new URIImpl("http://mvm.com/rdf/2011/02/model#timestamp"));
//            store.setPerformant(true);
//            store.setUseStatistics(false);
//            store.setInferencing(false);
//            store.setDisplayQueryPlan(false);

//            store.setStartTime("1302811169088");
//            store.setTtl("86400000");
//            store.setPerformant(false);
//            store.setInstance("nimbus");
//            store.setServer("10.40.189.123");
            Repository myRepository = new SailRepository(store);
            myRepository.initialize();

//			BufferedOutputStream os = new BufferedOutputStream(
//					new FileOutputStream("query.out"));

            RepositoryConnection conn = myRepository.getConnection();

            // String query =
            // "SELECT ?pred ?obj WHERE { <http://mvm-model/mvm#mm_7afb494d-dddc-4f1c-8b5b-0413b3ebe783> ?pred ?obj }";
            // String query =
            // "SELECT ?pred ?obj WHERE { <http://mvm-model/mvm#mm_b10c2c18-80c3-41ef-8069-9df1a6e8879c> ?pred ?obj }";
            // String query =
            // "SELECT ?pred ?obj WHERE { <http://mvm-model/mvm#mm_001ca4e0-521f-440e-9e72-9b59bb14fd2c> ?pred ?obj }";

            String cimHasInstalledSoftware = "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" +
                    "PREFIX mm: <http://mvm.com/owl/2010/10/mm.owl#>\n" +
                    "SELECT * WHERE {\n" +
                    "     ?serv mm:hasRunningOS ?obj .\n" +
                    "      ?obj mm:name ?name ;\n" +
//                    "           mm:caption ?caption .\n" +
                    "}";

            String cimHasRunningSoftware = "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" +
                    "PREFIX mm: <http://mvm.com/owl/mm.owl#>\n" +
                    "SELECT * WHERE {\n" +
                    "     <http://mvm.com/owl/mm.owl#urn:mm:mvm:LTS:root/cimv2:PG_OperatingSystem:CIM_ComputerSystem:stratus06.bullpen.net:Red_Hat_Enterprise_Linux_Server> mm:hasRunningOS ?obj .\n" +
                    "     ?obj mm:name ?name ; \n" +
                    "          mm:handle ?handle ; \n" +
                    "          mm:description ?description ; \n" +
                    "          mm:caption ?caption . \n" +
                    "}";

            String artistQuery = "SELECT * WHERE { "
                    + " ?subj ?pred \"Bonnie Tyler\" . "
                    + "}";
            String lubm1 = "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" +
                    " PREFIX ub: <urn:lubm:test#>\n" +
                    " SELECT * WHERE\n" +
                    " {\n" +
                    "     ?x ub:takesCourse <http://www.Department0.University0.edu/GraduateCourse0> .\n" +
                    "     ?x rdf:type ub:GraduateStudent .\n" +
                    "     ?x ub:name ?name .\n" +
//                    "     FILTER regex(?name, \"GraduateStudent44\", \"i\") .\n" +
                    " }";

            String gradStudent44 = "SELECT * WHERE { <http://www.Department0.University0.edu/GraduateStudent44> ?p ?o.}";

            String lubm4 = "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" +
                    " PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n" +
                    " PREFIX ub: <urn:test:onto:univ#>\n" +
                    " SELECT * WHERE\n" +
                    " {\n" +
                    "      { ?pred rdfs:subPropertyOf ub:worksFor . ?y ?pred <http://www.Department0.University0.edu> }\n" +
                    "       UNION " +
                    "      { ?y ub:worksFor <http://www.Department0.University0.edu> }\n" +
                    "      { ?x rdfs:subClassOf ub:Professor . ?y rdf:type ?x }\n" +
                    "       UNION " +
                    "      { ?y rdf:type ub:Professor }\n" +
                    "      ?y ub:name ?y1 .\n" +
                    "      ?y ub:emailAddress ?y2 .\n" +
                    "      ?y ub:telephone ?y3 .\n" +
                    " }";

            String lubm4_clean = "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" +
                    " PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n" +
                    " PREFIX ub: <urn:test:onto:univ#>\n" +
                    " SELECT * WHERE\n" +
                    " {\n" +
                    "      ?y ub:worksFor <http://www.Department0.University0.edu>.\n" +
                    "      ?y rdf:type ub:FullProfessor.\n" +
                    "      ?y ub:name ?y1 .\n" +
                    " } ORDER BY ?y";

            String cimLatestMeasure = "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" +
                    "PREFIX mm: <http://mvm.com/owl/2010/10/mm.owl#>\n" +
                    "SELECT ?proc ?val ?time WHERE {\n" +
                    "     ?proc mm:loadPercentage ?val .\n" +
                    "     ?subj rdf:subject ?proc .\n" +
                    "     ?subj rdf:object ?val2 .\n" +
                    "     ?subj  rdf:type rdf:Statement ;\n" +
                    "     \t    mm:reportedAt ?time .\n" +
                    " FILTER (?val2 = ?val) }\n" +
                    "ORDER BY DESC(?time)\n" +
                    "LIMIT 25";

            String cimHasFileSystemSpecific = "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" +
                    "PREFIX mm: <http://mvm.com/owl/mm.owl#>\n" +
                    "SELECT * WHERE {\n" +
                    "     <http://mvm.com/owl/mm.owl#urn:mm:mvm:lts:root/cimv2:PG_OperatingSystem.CreationClassName=CIM_OperatingSystem,CSCreationClassName=CIM_UnitaryComputerSystem,CSName=roshan.bullpen.net,Name=Red_Hat_Enterprise_Linux_Server> mm:hasFileSystem ?obj ." +
//                "     ?serv mm:hasFileSystem ?obj ." +
                    "}";

            String allObjects = "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" +
                    "PREFIX mm: <http://mvm.com/owl/2010/10/mm.owl#>\n" +
                    "SELECT * WHERE {\n" +
                    "   {?s <http://www.w3.org/1999/02/22-rdf-syntax-ns#object> ?o.} }";

            String deletePkgTrkr = "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" +
                    " PREFIX mm: <http://mvm.com/owl/2010/10/mm.owl#>\n" +
                    " PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\n" +
                    "PREFIX prov: <http://this.doc/2010/tracked-data-provenance/ns#>\n" +
                    " SELECT DISTINCT * WHERE\n" +
                    " {\n" +
                    "     ?subj rdf:type prov:AggregatePkgInfo.\n" +
                    "     ?subj ?pred ?obj." +
                    " }";

            String lubm5 = "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" +
                    " PREFIX ub: <urn:test:onto:univ#>\n" +
                    "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n" +
                    " SELECT * WHERE\n" +
                    " {\n" +
                    "      { ?pred rdfs:subPropertyOf ub:memberOf . ?thing ?pred <http://www.Department0.University0.edu> }\n" +
                    "       UNION " +
                    "      { ?thing ub:memberOf <http://www.Department0.University0.edu> }\n" +
//                    "      { ?type rdfs:subClassOf ub:Person . ?thing rdf:type ?type }\n" +
//                    "       UNION " +
//                    "      { ?thing rdf:type ub:Person }\n" +
                    " }";

            String lubm5_clean = "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" +
                    " PREFIX ub: <urn:test:onto:univ#>\n" +
                    "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n" +
                    " SELECT * WHERE\n" +
                    " {\n" +
                    "      ?thing ub:memberOf <http://www.Department0.University0.edu>.\n" +
                    "      ?thing rdf:type ub:Person. \n" +
                    " }";

            String lubm5_clean2 = "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" +
                    " PREFIX ub: <urn:test:onto:univ#>\n" +
                    "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n" +
                    " SELECT * WHERE\n" +
                    " {\n" +
                    "      ?pred rdfs:subPropertyOf ub:memberOf . ?thing ?pred <http://www.Department0.University0.edu> . ?type rdfs:subClassOf ub:Person . ?thing rdf:type ?type \n" +
                    " }";

            String lubm3 = "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" +
                    "                 PREFIX ub: <urn:test:onto:univ#>\n" +
                    "                 SELECT * WHERE\n" +
                    "                 {\n" +
                    "                      ?x ub:publicationAuthor <http://www.Department0.University0.edu/AssistantProfessor3> .\n" +
                    "                      ?x rdf:type ub:Publication.\n" +
                    "                }";

            String lubm2 = "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" +
                    "                 PREFIX ub: <urn:test:onto:univ#>\n" +
                    "                 SELECT * WHERE\n" +
                    "                 {\n" +
                    "                       ?y rdf:type ub:University .\n" +
                    "                       ?z ub:subOrganizationOf ?y .\n" +
                    "                       ?z rdf:type ub:Department .\n" +
                    "                       ?x ub:memberOf ?z .\n" +
                    "                       ?x ub:undergraduateDegreeFrom ?y .\n" +
                    "                       ?x rdf:type ub:GraduateStudent .\n" +
                    "                }";

            String lubm2_a = "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" +
                    "                 PREFIX ub: <urn:test:onto:univ#>\n" +
                    "                 SELECT * WHERE\n" +
                    "                 {\n" +
//                    "                       ?y rdf:type ub:University .\n" +
                    "                       ?z ub:subOrganizationOf <http://www.University700.edu> .\n" +
                    "                       ?z rdf:type ub:Department .\n" +
                    "                       ?x ub:memberOf ?z .\n" +
                    "                       ?x ub:undergraduateDegreeFrom <http://www.University700.edu> .\n" +
                    "                       ?x rdf:type ub:GraduateStudent .\n" +
                    "                }";

            String hasAlum = "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" +
                    "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n" +
                    "PREFIX owl: <http://www.w3.org/2002/07/owl#>\n" +
                    " PREFIX ub: <urn:test:onto:univ#>\n" +
                    "SELECT * WHERE {\n" +
                    "        {<http://www.University1.edu> ub:hasAlumnus ?alum } \n" +
                    "        UNION \n" +
                    "        {ub:hasAlumnus owl:inverseOf ?invProp . \n" +
                    "      { ?pred rdfs:subPropertyOf ?invProp . ?alum ?pred <http://www.University1.edu> }\n" +
                    "         UNION \n" +
                    "      { ?alum ?invProp <http://www.University0.edu> }}\n" +
                    "      { ?type rdfs:subClassOf ub:Person . ?alum rdf:type ?type }\n" +
                    "       UNION       { ?alum rdf:type ub:Person }\n" +
                    "}";

            String hasAlum_clean = "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" +
                    "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n" +
                    "PREFIX owl: <http://www.w3.org/2002/07/owl#>\n" +
                    " PREFIX ub: <urn:test:onto:univ#>\n" +
                    "SELECT * WHERE {\n" +
                    "        <http://www.University1.edu> ub:hasAlumnus ?alum .\n" +
                    "}";

            String lubm7 = "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" +
                    "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n" +
                    " PREFIX ub: <urn:test:onto:univ#>\n" +
                    " SELECT DISTINCT ?course ?student WHERE\n" +
                    " {\n" +
                    " <http://www.Department0.University0.edu/AssociateProfessor0> ub:teacherOf ?course .\n" +
                    " ?student ub:takesCourse ?course .\n" +
                    "      { ?type rdfs:subClassOf ub:Student . ?student rdf:type ?type }\n" +
                    "       UNION       { ?student rdf:type ub:Student }\n" +
                    "      { ?type rdfs:subClassOf ub:Course . ?course rdf:type ?type }\n" +
                    "       UNION       { ?course rdf:type ub:Course }\n" +
                    " }";

            String lubm7_clean = "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" +
                    "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n" +
                    " PREFIX ub: <urn:test:onto:univ#>\n" +
                    " SELECT * WHERE\n" +
                    " {\n" +
                    " <http://www.Department0.University0.edu/AssociateProfessor0> ub:teacherOf ?course .\n" +
                    " ?student ub:takesCourse ?course .\n" +
                    " ?student rdf:type ub:Student .\n" +
                    " ?course rdf:type ub:Course .\n" +
                    " }";

            String lubm8_clean = "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" +
                    "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n" +
                    " PREFIX ub: <urn:test:onto:univ#>\n" +
                    " SELECT * WHERE\n" +
                    " {\n" +
                    "      ?suborg ub:subOrganizationOf <http://www.University0.edu> .\n" +
                    "      ?mem ub:memberOf ?suborg .\n" +
                    "      ?suborg rdf:type ub:Department .\n" +
                    "        ?mem ub:emailAddress ?email. \n" +
                    " }";

            String lubm9 = "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" +
                    "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n" +
                    " PREFIX ub: <urn:test:onto:univ#>\n" +
                    " SELECT * WHERE\n" +
                    " {\n" +
                    "       ?teach ub:teacherOf ?course .\n" +
                    "       ?teach ub:advisor ?student .\n" +
                    "        ?student ub:takesCourse ?course .\n" +
                    " }";

            String kolm = "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" +
                    "PREFIX mm: <http://mvm.com/owl/2010/10/mm.owl#>\n" +
                    "PREFIX kolm: <http://mvm.com/lrn/2010/11/kolm#>\n" +
                    "SELECT DISTINCT ?srv ?ncd WHERE {\n" +
                    "     ?pt kolm:serverRef <http://mvm.com/owl/2010/10/mm.owl#urn:mm:mvm:root/cimv2:PG_OperatingSystem:CIM_ComputerSystem:cirrus05.bullpen.net:Red_Hat_Enterprise_Linux_Server> .\n" +
                    "     ?cluster kolm:relatesTo ?pt .\n" +
                    "     ?cluster kolm:relatesTo ?pt2 .\n" +
                    "     ?pt2 kolm:serverRef ?srv .\n" +
                    "       ?pt2  kolm:ncd ?ncd .\n" +
                    "       ?cluster kolm:timestamp ?ts .\n" +
                    " } \n" +
                    " ORDER BY ?ncd";

            String kolm_tst = "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" +
                    "PREFIX mm: <http://mvm.com/owl/2010/10/mm.owl#>\n" +
                    "PREFIX kolm: <http://mvm.com/lrn/2010/11/kolm#>\n" +
                    "SELECT * WHERE {\n" +
                    "     ?s <http://mvm.com/lrn/2010/11/kolm#relatesTo> <http://mvm.com/lrn/2010/11/kolm#fef60314-78f1-4918-ad03-e1aff835e858>\n" +
                    " } ";

            String lubm9_tst = "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" +
                    "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n" +
                    " PREFIX ub: <urn:test:onto:univ#>\n" +
                    " SELECT * WHERE\n" +
                    " {\n" +
                    "       ?teach ub:teacherOf ?course .\n" +
//                    "       ?teach ub:teacherOf <http://www.Department0.University0.edu/Course3> .\n" +
                    "       ?student ub:advisor ?teach .\n" +
                    "        ?student ub:takesCourse ?course .\n" +
                    " }";

            String bsbm1 = "PREFIX bsbm-inst: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/>\n" +
                    "PREFIX bsbm: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/>\n" +
                    "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n" +
                    "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" +
                    "PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\n" +
                    "\n" +
                    "SELECT *\n" +
                    "WHERE {\n" +
                    "    ?product a <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/ProductType1315> .\n" +
                    "    ?product bsbm:productFeature <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/ProductFeature750> .\n" +
                    "    ?product bsbm:productPropertyNumeric1 ?value1 .\n" +
                    "    ?product rdfs:label ?label .\n" +
                    "        FILTER (?value1 > 933) .\n" +
                    "        }" +
                    "LIMIT 10" +
                    "";

            String hbs = "PREFIX hb: <http://here/2010/tracked-data-provenance/heartbeat/ns#>\n" +
                    "\n" +
                    "select * where {\n" +
                    " hb:f452f776-4994-43fc-ada0-4b60cc979dac ?p ?o .\n" +
                    "}";

            String hbmodel = "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" +
                    "PREFIX mod: <http://mvm.com/rdf/2011/02/model#>\n" +
                    "PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\n" +
                    "SELECT ?s (SUM(?ts) AS ?sum) WHERE\n" +
                    "{\n" +
                    "    ?s mod:key \"messageCount/urn:system:LTS-01/A/A_Sports1/ProvenanceAPI/2/12/\";\n" +
                    "        mod:model ?model;\n" +
                    "        mod:timestamp ?ts.\n" +
                    "}\n" +
                    "GROUP BY ?s\n" +
                    "ORDER BY DESC(?ts)\n";

            String hbagg = "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" +
                    " PREFIX hb: <http://here/2010/tracked-data-provenance/heartbeat/ns#>\n" +
                    " PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\n" +
                    " SELECT * WHERE\n" +
                    " {\n" +
                    "     ?systemName hb:reportedBy ?reportedBy;\n" +
                    "                         hb:systemType ?systemType;\n" +
                    "                         hb:heartbeat ?hbuuid.\n" +
                    "     ?hbuuid hb:timestamp ?timestamp;\n" +
                    "                         hb:messageCount ?messageCount.\n" +
                    " }";

            String runningProc = "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" +
                    " PREFIX mm: <http://mvm.com/owl/2010/10/mm.owl#>\n" +
                    " PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\n" +
                    " SELECT DISTINCT * WHERE\n" +
                    " {\n" +
                    "     <http://mvm.com/owl/2010/10/mm.owl#urn:mm:mvm:root/cimv2:PG_OperatingSystem:CIM_ComputerSystem:cirrus05.bullpen.net:Red_Hat_Enterprise_Linux_Server> mm:hasRunningProcess ?obj .\n" +
                    "     ?obj mm:parameters ?params .\n" +
                    "     ?obj mm:name ?name .\n" +
                    " }";

            String single = "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" +
                    " PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n" +
                    " PREFIX ub: <urn:test:onto:univ#>\n" +
                    " SELECT * WHERE\n" +
                    " {\n" +
                    "      <http://www.Department11.University370.edu/AssociateProfessor9> ?p ?o.\n" +
                    " }";

            String modelInnerSelect = "PREFIX nh: <http://mvm.com/rdf/2011/02/model#>\n" +
                    "\n" +
                    "SELECT * WHERE {\n" +
                    "\n" +
                    "{\n" +
                    "SELECT ?key ?modelType (MAX(?timestamp) as ?ts) WHERE {\n" +
                    "     ?modelUuid nh:key ?key;\n" +
                    "     FILTER regex(?key, \"messageCount(.*)/2/13\").\n" +
                    "     ?modelUuid nh:modelType ?modelType;\n" +
                    "                        nh:timestamp ?timestamp\n" +
                    "}\n" +
                    "GROUP BY ?key ?modelType\n" +
                    "}\n" +
                    "\n" +
                    "?muuid nh:key ?key;\n" +
                    "           nh:timestamp ?ts;\n" +
                    "           nh:modelType ?modelType;\n" +
                    "           nh:model ?model.\n" +
                    "\n" +
                    "}";

            String modelInnerSelect2 = "PREFIX nh: <http://mvm.com/rdf/2011/02/model#>\n" +
                    "\n" +
                    "SELECT ?key ?modelType (MAX(?timestamp) as ?ts) WHERE {\n" +
                    "     ?modelUuid nh:key ?key;\n" +
                    "     FILTER regex(?key, \"messageCount(.*)/2/13\").\n" +
                    "     ?modelUuid nh:modelType ?modelType;\n" +
                    "                        nh:timestamp ?timestamp\n" +
                    "}\n" +
                    "GROUP BY ?key ?modelType\n" +
                    "";

            String jim_query = "PREFIX tdp: <http://here/2010/tracked-data-provenance/ns#>\n" +
                    "\n" +
                    "select * where {\n" +
                    " ?eventUUID ?eventType <urn:tdo:88d4f31f-99dc-4527-8cfb-2c93583ad498> .\n" +
//                    " ?eventUUID tdp:performedBy ?systemName .\n" +
                    " ?eventUUID tdp:performedAt ?timeStamp .\n" +
                    "}\n" +
                    "ORDERBY ?timeStamp";

            String theWoman = "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" +
                    "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n" +
                    " PREFIX ub: <urn:test:onto:univ#>\n" +
                    " SELECT * WHERE\n" +
                    " {\n" +
                    " ub:TheWoman ?p ?o.\n" +
                    " }";

            String hbtimestamp = "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" +
                    " PREFIX hb: <http://here/2010/tracked-data-provenance/heartbeat/ns#>\n" +
                    " PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\n" +
                    " SELECT * WHERE\n" +
                    " {\n" +
                    "     ?hbuuid hb:timestamp ?timestamp.\n" +
                    " }";

            String tde = "PREFIX tdp: <http://here/2010/tracked-data-provenance/ns#>\n" +
                    "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" +
                    "PREFIX mvm: <urn:mvm.mmrts.rdfcloudstore/06/2011#>\n" +
                    "SELECT * WHERE\n" +
                    "{\n" +
                    "\t?id tdp:performedAt ?ts.\n" +
                    "\tFILTER(mvm:timeRange(?ts, tdp:performedAt, 'mvm.mmrts.api.date.DateTimeTtlValueConverter', 7200000, '1303911164088')).\n" +
                    "\t?id tdp:performedBy ?system;\n" +
                    "\t    rdf:type ?eventType.\n" +
                    "}\n";

            String tde_spec = "PREFIX tdp: <http://here/2010/tracked-data-provenance/ns#>\n" +
                    "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" +
                    "SELECT * WHERE\n" +
                    "{\n" +
                    "\t<urn:tde:0b19e224-7524-4ba0-880b-16d9ec735303> ?p ?o.\n" +
                    "}";

            String cimLatestMeasure_timeindex = "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" +
                    "PREFIX mm: <http://mvm.com/owl/2010/10/mm.owl#>\n" +
                    "PREFIX mvm: <urn:mvm.mmrts.rdfcloudstore/06/2011#>\n" +
                    "SELECT ?proc ?val ?time WHERE {\n" +
                    "     ?proc mm:loadPercentage ?val .\n" +
                    "     ?subj rdf:subject ?proc .\n" +
                    "     ?subj rdf:object ?val2 .\n" +
                    "     ?subj  rdf:type rdf:Statement ;\n" +
                    "     \t    mm:reportedAt ?time .\n" +
                    "\tFILTER (mvm:timeRange(?time, mm:reportedAt, 'mvm.mmrts.api.date.DateTimeTtlValueConverter', '86400000', '1295725236000')). " +
                    "}\n" +
//                    "ORDER BY DESC(?time)\n" +
//                    "LIMIT 25" +
                    "";

            String hbs_ti = "PREFIX hb: <http://here/2010/tracked-data-provenance/heartbeat/ns#>\n" +
                    "\n" +
                    "select * where {\n" +
                    " ?s ?p hb:f452f776-4994-43fc-ada0-4b60cc979dac .\n" +
                    "}";

            String hbtimestamp_ti = "PREFIX hb: <http://here/2010/tracked-data-provenance/heartbeat/ns#>\n" +
                    "PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\n" +
                    "PREFIX mvm: <urn:mvm.mmrts.rdfcloudstore/06/2011#>\n" +
                    "SELECT * WHERE\n" +
                    "{\n" +
                    "\t?id hb:timestamp ?timestamp.\n" +
                    "\tFILTER(mvm:timeRange(?timestamp, hb:timestamp, 'mvm.mmrts.api.date.TimestampTtlStrValueConverter', '72000000')).\n" +
                    "\t?id hb:systemName ?system;\n" +
                    "\t     hb:count ?count;\n" +
                    "}";

            String tst_qu = "select * where {\n" +
                    "\n" +
                    " ?s <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <urn:test2:lubm#GraduateStudent>.\n" +
                    " ?s <urn:test2:lubm#advisor> <http://www.Department4.University5155.edu/AssociateProfessor3>.\n" +
//                    " ?s ?p ?o.\n" +
                    "\n" +
                    "}";

            String tst_qu2 = "select * where {\n" +
                    "\n" +
                    "?s <urn:test2:lubm#takesCourse> <http://www.Department6.University4518.edu/GraduateCourse3>.\n" +
                    "?s <urn:test2:lubm#takesCourse> <http://www.Department6.University4518.edu/GraduateCourse28>.\n" +
                    "\n" +
                    "}";

            String timeQueryMike = "PREFIX tdp: <http://here/2010/tracked-data-provenance/ns#>\n" +
                    "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" +
                    "PREFIX mvm: <urn:mvm.mmrts.rdfcloudstore/06/2011#>\n" +
                    "SELECT * WHERE\n" +
                    "{\n" +
                    "?id tdp:reportedAt ?timestamp. \n" +
                    "FILTER(mvm:timeRange(?timestamp, tdp:reportedAt, 'mvm.mmrts.api.date.DateTimeTtlValueConverter', '14400000')).\n" +
                    "?id tdp:performedBy ?system;\n" +
                    "    rdf:type ?eventType.\n" +
                    "} ";

            String eventQ = "PREFIX tdp: <http://here/2010/tracked-data-provenance/ns#>\n" +
                    "select * where {\n" +
                    "?eventUUID tdp:performedBy ?systemName .\n" +
                    "} LIMIT 100";

            String allFullProf0 = "select * where {\n" +
                    "<http://www.Department0.University0.edu/FullProfessor0> ?p ?o.\n" +
                    "\n}";

            // Provenance Queries
            String prov_eventInfo = "select * where {\n" +
                    "<urn:tdo:7202e20d-d66c-469c-8a10-62e36e21d820> ?p ?o.\n" +
                    "}";

            String prov_objectInfo = "PREFIX ns:<http://here/2010/tracked-data-provenance/ns#>\n" +
                    "select * where {\n" +
                    "{" +
                    "   ?s ns:createdItem <urn:tdc:01a49336-d0e0-3cd6-9ddb-0e6b14369876>.\n" +
                    "   ?s ns:performedBy ?pb.\n" +
                    "   ?s ns:performedAt ?pa.\n" +
                    "}\n" +
                    "UNION {" +
                    "   ?s ns:clickedItem <urn:tdc:01a49336-d0e0-3cd6-9ddb-0e6b14369876>.\n" +
                    "   ?s ns:performedBy ?pb.\n" +
                    "   ?s ns:performedAt ?pa.\n" +
                    "}\n" +
                    "UNION {" +
                    "   ?s ns:deletedItem <urn:tdc:01a49336-d0e0-3cd6-9ddb-0e6b14369876>.\n" +
                    "   ?s ns:performedBy ?pb.\n" +
                    "   ?s ns:performedAt ?pa.\n" +
                    "}\n" +
                    "UNION {" +
                    "   ?s ns:droppedItem <urn:tdc:01a49336-d0e0-3cd6-9ddb-0e6b14369876>.\n" +
                    "   ?s ns:performedBy ?pb.\n" +
                    "   ?s ns:performedAt ?pa.\n" +
                    "}\n" +
                    "UNION {" +
                    "   ?s ns:receivedItem <urn:tdc:01a49336-d0e0-3cd6-9ddb-0e6b14369876>.\n" +
                    "   ?s ns:performedBy ?pb.\n" +
                    "   ?s ns:performedAt ?pa.\n" +
                    "}\n" +
                    "UNION {" +
                    "   ?s ns:storedItem <urn:tdc:01a49336-d0e0-3cd6-9ddb-0e6b14369876>.\n" +
                    "   ?s ns:performedBy ?pb.\n" +
                    "   ?s ns:performedAt ?pa.\n" +
                    "}\n" +
                    "UNION {" +
                    "   ?s ns:sentItem <urn:tdc:01a49336-d0e0-3cd6-9ddb-0e6b14369876>.\n" +
                    "   ?s ns:performedBy ?pb.\n" +
                    "   ?s ns:performedAt ?pa.\n" +
                    "}\n" +
                    "}\n";

            String prov_createdItems = "PREFIX ns: <http://here/2010/tracked-data-provenance/ns#>\n" +
                    "\n" +
                    "select * where {\n" +
                    "\n" +
                    "  ?s ns:createdItem ?i\n" +
                    " \n" +
                    "} limit 10";
            ////////////////////

            String rangeQuery = "PREFIX tdp: <http://here/2010/tracked-data-provenance/ns#>\n" +
                    "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" +
                    "PREFIX mvmpart: <urn:mvm.mmrts.partition.rdf/08/2011#>\n" +
                    "SELECT * WHERE\n" +
                    "{\n" +
                    "?id tdp:reportedAt ?timestamp.\n" +
                    "FILTER(mvmpart:timeRange(?id, tdp:reportedAt, 1314849589999 , 1314849599999 , 'XMLDATETIME')).\n" +
                    "?id tdp:performedBy ?system.\n" +
                    "?id rdf:type ?type.\n" +
                    "}";

            String issueMMRTS127 = "select * where \n" +
                    "{ \n" +
                    "<urn:system:null> ?p ?o. \n" +
                    "?o ?p2 ?o2. \n" +
                    "} ";

            String uuid1 = "select * where { " +
                    "?u <http://here/2010/tracked-data-provenance/ns#performedAt> ?pa; " +
                    "   <http://here/2010/tracked-data-provenance/ns#reportedAt> ?ra; " +
                    "   <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?t. " +
                    "}";

            String uuidAuth1 = "select * where { " +
                    "<http://here/2010/tracked-data-provenance/ns#uuidAuth1> ?p ?o. " +
                    "}";

            String selectAll = "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" +
                    " PREFIX ub: <urn:lubm:test#>\n" +
                    " SELECT * WHERE\n" +
                    " {\n" +
                    "     ?x ub:takesCourse ?c .\n" +
                    "     ?x rdf:type ?gs .\n" +
                    "     ?x ub:name 'UndergraduateStudent139'.\n" +
                    " }";

            String query = uuidAuth1;

            Calendar start_cal = Calendar.getInstance();
            start_cal.set(Calendar.MONTH, 7);
            start_cal.set(Calendar.DAY_OF_MONTH, 13);

            Calendar end_cal = Calendar.getInstance();
            end_cal.set(Calendar.MONTH, 7);
            end_cal.set(Calendar.DAY_OF_MONTH, 14);

            System.out.println(query);
            long start = System.currentTimeMillis();
            TupleQuery tupleQuery = conn.prepareTupleQuery(
                    QueryLanguage.SPARQL, query);
            ValueFactory vf = ValueFactoryImpl.getInstance();
//            tupleQuery.setBinding(NUMTHREADS_PROP, vf.createLiteral(10));
//            tupleQuery.setBinding(AUTHORIZATION_PROP, vf.createLiteral("C"));
//            tupleQuery.setBinding(START_BINDING, vf.createLiteral(start_cal.getTimeInMillis()));
//            tupleQuery.setBinding(END_BINDING, vf.createLiteral(end_cal.getTimeInMillis()));
//            tupleQuery.setBinding(START_BINDING, vf.createLiteral(1313779878435l));
//            tupleQuery.setBinding(END_BINDING, vf.createLiteral(1313779978435l));

//            TupleQueryResultHandler writer = new SPARQLResultsXMLWriter(new NullOutputStream());
            TupleQueryResultHandler writer = new SPARQLResultsXMLWriter(System.out);
//            tupleQuery.evaluate(writer);
            tupleQuery.evaluate(new TupleQueryResultHandler() {

                int count = 0;

                @Override
                public void startQueryResult(List<String> strings) throws TupleQueryResultHandlerException {
                }

                @Override
                public void endQueryResult() throws TupleQueryResultHandlerException {
                    System.out.println(count);
                }

                @Override
                public void handleSolution(BindingSet bindingSet) throws TupleQueryResultHandlerException {
                    count++;
                    System.out.println(bindingSet);
                }
            });
            System.out.println("Total query time: "
                    + (System.currentTimeMillis() - start));

            conn.close();
//			os.close();

            // for (String string : urls) {
            // ByteStreams
            // }
            myRepository.shutDown();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public static void main(String[] args) {
        try {
            new QueryPartitionData().run();
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.exit(0);
    }
}
