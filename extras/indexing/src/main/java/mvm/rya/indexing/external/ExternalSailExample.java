package mvm.rya.indexing.external;

/*
 * #%L
 * mvm.rya.indexing.accumulo
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

import java.util.List;

import mvm.rya.indexing.external.tupleSet.AccumuloIndexSet;
import mvm.rya.indexing.external.tupleSet.ExternalTupleSet;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.openrdf.model.URI;
import org.openrdf.model.impl.LiteralImpl;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.RDFS;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.algebra.helpers.QueryModelTreePrinter;
import org.openrdf.query.parser.ParsedQuery;
import org.openrdf.query.parser.sparql.SPARQLParser;
import org.openrdf.query.resultio.sparqlxml.SPARQLResultsXMLWriter;
import org.openrdf.repository.sail.SailRepository;
import org.openrdf.repository.sail.SailRepositoryConnection;
import org.openrdf.sail.Sail;
import org.openrdf.sail.memory.MemoryStore;

import com.google.common.collect.Lists;

public class ExternalSailExample {

    public static void main(String[] args) throws Exception {

        Sail s = new MemoryStore();
        SailRepository repo = new SailRepository(s);
        repo.initialize();
        SailRepositoryConnection conn = repo.getConnection();

        URI sub = new URIImpl("uri:entity");
        URI subclass = new URIImpl("uri:class");
        URI obj = new URIImpl("uri:obj");
        URI talksTo = new URIImpl("uri:talksTo");

        conn.add(sub, RDF.TYPE, subclass);
        conn.add(sub, RDFS.LABEL, new LiteralImpl("label"));
        conn.add(sub, talksTo, obj);

        URI sub2 = new URIImpl("uri:entity2");
        URI subclass2 = new URIImpl("uri:class2");
        URI obj2 = new URIImpl("uri:obj2");

        conn.add(sub2, RDF.TYPE, subclass2);
        conn.add(sub2, RDFS.LABEL, new LiteralImpl("label2"));
        conn.add(sub2, talksTo, obj2);

        // TODO Auto-generated method stub
        String indexSparqlString = ""//
                + "SELECT ?e ?l ?c " //
                + "{" //
                + "  ?e a ?c . "//
                + "  ?e <http://www.w3.org/2000/01/rdf-schema#label> ?l "//
                + "}";//

        conn.prepareTupleQuery(QueryLanguage.SPARQL, indexSparqlString).evaluate(new SPARQLResultsXMLWriter(System.out));

        SPARQLParser sp = new SPARQLParser();
        ParsedQuery pq = sp.parseQuery(indexSparqlString, null);
        System.out.println(pq);

        List<ExternalTupleSet> index = Lists.newArrayList();

        Connector accCon = new MockInstance().getConnector("root", "".getBytes());
        String tablename = "table";
        accCon.tableOperations().create(tablename);
        index.add(new AccumuloIndexSet(indexSparqlString, conn, accCon, tablename));

        String queryString = ""//
                + "SELECT ?e ?c ?l ?o " //
                + "{" //
                + "  ?e a ?c . "//
                + "  ?e <http://www.w3.org/2000/01/rdf-schema#label> ?l . "//
                + "  ?e <uri:talksTo> ?o . "//
                + "}";//

        conn.prepareTupleQuery(QueryLanguage.SPARQL, queryString).evaluate(new SPARQLResultsXMLWriter(System.out));

        pq = sp.parseQuery(queryString, null);
        QueryModelTreePrinter mp = new QueryModelTreePrinter();
        pq.getTupleExpr().visit(mp);
        System.out.println(mp.getTreeString());
        System.out.println(pq.getTupleExpr());

        System.out.println("++++++++++++");
        ExternalProcessor processor = new ExternalProcessor(index);
        System.out.println(processor.process(pq.getTupleExpr()));

        System.out.println("----------------");
        Sail processingSail = new ExternalSail(s, processor);
        SailRepository smartSailRepo = new SailRepository(processingSail);
        smartSailRepo.initialize();

        smartSailRepo.getConnection().prepareTupleQuery(QueryLanguage.SPARQL, queryString).evaluate(new SPARQLResultsXMLWriter(System.out));

    }

}
