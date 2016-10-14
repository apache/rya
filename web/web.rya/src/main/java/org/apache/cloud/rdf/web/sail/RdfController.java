package org.apache.cloud.rdf.web.sail;

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



import static org.apache.rya.api.RdfCloudTripleStoreConstants.VALUE_FACTORY;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;

import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.rya.api.security.SecurityProvider;
import org.apache.rya.api.RdfCloudTripleStoreConfiguration;
import org.apache.rya.rdftriplestore.RdfCloudTripleStoreConnection;

import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.query.BindingSet;
import org.openrdf.query.GraphQuery;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.QueryResultHandlerException;
import org.openrdf.query.TupleQuery;
import org.openrdf.query.TupleQueryResultHandler;
import org.openrdf.query.TupleQueryResultHandlerException;
import org.openrdf.query.Update;
import org.openrdf.query.UpdateExecutionException;
import org.openrdf.query.parser.ParsedGraphQuery;
import org.openrdf.query.parser.ParsedOperation;
import org.openrdf.query.parser.ParsedTupleQuery;
import org.openrdf.query.parser.ParsedUpdate;
import org.openrdf.query.parser.QueryParserUtil;
import org.openrdf.query.resultio.sparqljson.SPARQLResultsJSONWriter;
import org.openrdf.query.resultio.sparqlxml.SPARQLResultsXMLWriter;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.sail.SailRepository;
import org.openrdf.repository.sail.SailRepositoryConnection;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFHandler;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.RDFParseException;
import org.openrdf.rio.rdfxml.RDFXMLWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;

/**
 * Class RdfController
 * Date: Mar 7, 2012
 * Time: 11:07:19 AM
 */
@Controller
public class RdfController {
    
	private static final int QUERY_TIME_OUT_SECONDS = 120;

    @Autowired
    SailRepository repository;
    
    @Autowired   
    SecurityProvider provider;

    @RequestMapping(value = "/queryrdf", method = {RequestMethod.GET, RequestMethod.POST})
    public void queryRdf(@RequestParam("query") String query,
                         @RequestParam(value = RdfCloudTripleStoreConfiguration.CONF_QUERY_AUTH, required = false) String auth,
                         @RequestParam(value = RdfCloudTripleStoreConfiguration.CONF_CV, required = false) String vis,
                         @RequestParam(value = RdfCloudTripleStoreConfiguration.CONF_INFER, required = false) String infer,
                         @RequestParam(value = "nullout", required = false) String nullout,
                         @RequestParam(value = RdfCloudTripleStoreConfiguration.CONF_RESULT_FORMAT, required = false) String emit,
                         @RequestParam(value = "padding", required = false) String padding,
                         @RequestParam(value = "callback", required = false) String callback,
                         HttpServletRequest request,
                         HttpServletResponse response) {
        SailRepositoryConnection conn = null;
		final Thread queryThread = Thread.currentThread();
		auth = StringUtils.arrayToCommaDelimitedString(provider.getUserAuths(request));
		Timer timer = new Timer();
		timer.schedule(new TimerTask() {

			@Override
			public void run() {
				System.out.println("interrupting");
				queryThread.interrupt();

			}
		}, QUERY_TIME_OUT_SECONDS * 1000);
		
		try {
			ServletOutputStream os = response.getOutputStream();
            conn = repository.getConnection();

            Boolean isBlankQuery = StringUtils.isEmpty(query);
            ParsedOperation operation = QueryParserUtil.parseOperation(QueryLanguage.SPARQL, query, null);

            Boolean requestedCallback = !StringUtils.isEmpty(callback);
            Boolean requestedFormat = !StringUtils.isEmpty(emit);

            if (requestedCallback) {
                os.print(callback + "(");
            }

            if (!isBlankQuery) {
            	if (operation instanceof ParsedGraphQuery) {
            		// Perform Graph Query
                    RDFHandler handler = new RDFXMLWriter(os);
                    response.setContentType("text/xml");
                    performGraphQuery(query, conn, auth, infer, nullout, handler);
                } else if (operation instanceof ParsedTupleQuery) {
                    // Perform Tuple Query
                    TupleQueryResultHandler handler;

                    if (requestedFormat && emit.equalsIgnoreCase("json")) {
                        handler = new SPARQLResultsJSONWriter(os);
                        response.setContentType("application/json");
                    } else {
                        handler = new SPARQLResultsXMLWriter(os);
                        response.setContentType("text/xml");
                    }

                    performQuery(query, conn, auth, infer, nullout, handler);
                } else if (operation instanceof ParsedUpdate) {
                    // Perform Update Query
                    performUpdate(query, conn, os, infer, vis);
                } else {
                    throw new MalformedQueryException("Cannot process query. Query type not supported.");
                }
            }

            if (requestedCallback) {
                os.print(")");
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        } finally {
            if (conn != null) {
                try {
                    conn.close();
                } catch (RepositoryException e) {
                    e.printStackTrace();
                }
            }
        }
		
		timer.cancel();
    }
    
    private void performQuery(String query, RepositoryConnection conn, String auth, String infer, String nullout, TupleQueryResultHandler handler) throws RepositoryException, MalformedQueryException, QueryEvaluationException, TupleQueryResultHandlerException {
        TupleQuery tupleQuery = conn.prepareTupleQuery(QueryLanguage.SPARQL, query);
        if (auth != null && auth.length() > 0)
            tupleQuery.setBinding(RdfCloudTripleStoreConfiguration.CONF_QUERY_AUTH, VALUE_FACTORY.createLiteral(auth));
        if (infer != null && infer.length() > 0)
            tupleQuery.setBinding(RdfCloudTripleStoreConfiguration.CONF_INFER, VALUE_FACTORY.createLiteral(Boolean.parseBoolean(infer)));
        if (nullout != null && nullout.length() > 0) {
            //output nothing, but still run query
            tupleQuery.evaluate(new TupleQueryResultHandler() {
                @Override
                public void startQueryResult(List<String> strings) throws TupleQueryResultHandlerException {
                }

                @Override
                public void endQueryResult() throws TupleQueryResultHandlerException {
                }

                @Override
                public void handleSolution(BindingSet bindings) throws TupleQueryResultHandlerException {
                }

                @Override
                public void handleBoolean(boolean arg0) throws QueryResultHandlerException {
                }

                @Override
                public void handleLinks(List<String> arg0) throws QueryResultHandlerException {
                }
            });
        } else {
            CountingTupleQueryResultHandlerWrapper sparqlWriter = new CountingTupleQueryResultHandlerWrapper(handler);
            long startTime = System.currentTimeMillis();
            tupleQuery.evaluate(sparqlWriter);
            System.out.format("Query Time = %.3f\n", (System.currentTimeMillis() - startTime) / 1000.);
            System.out.format("Result Count = %s\n", sparqlWriter.getCount());
        }

    }
    
    private void performGraphQuery(String query, RepositoryConnection conn, String auth, String infer, String nullout, RDFHandler handler) throws RepositoryException, MalformedQueryException, QueryEvaluationException, RDFHandlerException {
        GraphQuery graphQuery = conn.prepareGraphQuery(QueryLanguage.SPARQL, query);
        if (auth != null && auth.length() > 0)
        	graphQuery.setBinding(RdfCloudTripleStoreConfiguration.CONF_QUERY_AUTH, VALUE_FACTORY.createLiteral(auth));
        if (infer != null && infer.length() > 0)
        	graphQuery.setBinding(RdfCloudTripleStoreConfiguration.CONF_INFER, VALUE_FACTORY.createLiteral(Boolean.parseBoolean(infer)));
        if (nullout != null && nullout.length() > 0) {
            //output nothing, but still run query
        	// TODO this seems like a strange use case.
        	graphQuery.evaluate(new RDFHandler() {
				@Override
				public void startRDF() throws RDFHandlerException {
				}

				@Override
				public void endRDF() throws RDFHandlerException {
				}

				@Override
				public void handleNamespace(String prefix, String uri)
						throws RDFHandlerException {
				}

				@Override
				public void handleStatement(Statement st)
						throws RDFHandlerException {
				}

				@Override
				public void handleComment(String comment)
						throws RDFHandlerException {
				}
            });
        } else {
            long startTime = System.currentTimeMillis();
            graphQuery.evaluate(handler);
            System.out.format("Query Time = %.3f\n", (System.currentTimeMillis() - startTime) / 1000.);
        }

    }
    private void performUpdate(String query, SailRepositoryConnection conn, ServletOutputStream os, String infer, String vis) throws RepositoryException, MalformedQueryException, IOException {
        Update update = conn.prepareUpdate(QueryLanguage.SPARQL, query);
        if (infer != null && infer.length() > 0)
            update.setBinding(RdfCloudTripleStoreConfiguration.CONF_INFER, VALUE_FACTORY.createLiteral(Boolean.parseBoolean(infer)));

        if (conn.getSailConnection() instanceof RdfCloudTripleStoreConnection && vis != null) {
            RdfCloudTripleStoreConnection sailConnection = (RdfCloudTripleStoreConnection) conn.getSailConnection();
            sailConnection.getConf().set(RdfCloudTripleStoreConfiguration.CONF_CV, vis);
        }

        long startTime = System.currentTimeMillis();

        try {
            update.execute();
        } catch (UpdateExecutionException e) {
            os.print(String.format("Update could not be successfully completed for query: %s\n\n", query));
            os.print(String.format("\n\n%s", e.getLocalizedMessage()));
        }

        System.out.format("Update Time = %.3f\n", (System.currentTimeMillis() - startTime) / 1000.);
    }    
    
    private static final class CountingTupleQueryResultHandlerWrapper implements TupleQueryResultHandler {
    	private TupleQueryResultHandler indir;
    	private int count = 0;
    	
    	public CountingTupleQueryResultHandlerWrapper(TupleQueryResultHandler indir){
    		this.indir = indir;
    	}
    	
    	public int getCount() { return count; }
    	
    	@Override
    	public void endQueryResult() throws TupleQueryResultHandlerException {
    		indir.endQueryResult();
    	}

    	@Override
    	public void handleSolution(BindingSet bindingSet) throws TupleQueryResultHandlerException {
    		count++;
    		indir.handleSolution(bindingSet);
    	}
    	@Override
    	public void startQueryResult(List<String> bindingNames) throws TupleQueryResultHandlerException {
    		count = 0;
    		indir.startQueryResult(bindingNames);
    	}

      @Override
      public void handleBoolean(boolean arg0) throws QueryResultHandlerException {
      }

      @Override
      public void handleLinks(List<String> arg0) throws QueryResultHandlerException {
      }
    }

    @RequestMapping(value = "/loadrdf", method = RequestMethod.POST)
    public void loadRdf(@RequestParam(required = false) String format,
            @RequestParam(value = RdfCloudTripleStoreConfiguration.CONF_CV, required = false) String cv,
            @RequestParam(required = false) String graph,
                        @RequestBody String body,
                        HttpServletResponse response)
            throws RepositoryException, IOException, RDFParseException {
        List<Resource> authList = new ArrayList<Resource>();
        RDFFormat format_r = RDFFormat.RDFXML;
        if (format != null) {
            format_r = RDFFormat.valueOf(format);
            if (format_r == null)
                throw new RuntimeException("RDFFormat[" + format + "] not found");
        }
        if (graph != null) {
        	authList.add(VALUE_FACTORY.createURI(graph));
        }
        SailRepositoryConnection conn = null;
        try {
            conn = repository.getConnection();
            
            if (conn.getSailConnection() instanceof RdfCloudTripleStoreConnection && cv != null) {
                RdfCloudTripleStoreConnection sailConnection = (RdfCloudTripleStoreConnection) conn.getSailConnection();
                sailConnection.getConf().set(RdfCloudTripleStoreConfiguration.CONF_CV, cv);
            }

            conn.add(new StringReader(body), "", format_r);
            conn.commit();
        } finally {
            if (conn != null) {
                conn.close();
            }
        }
    }
}
