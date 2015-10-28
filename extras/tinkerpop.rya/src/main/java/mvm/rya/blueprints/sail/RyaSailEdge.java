package mvm.rya.blueprints.sail;

/*
 * #%L
 * mvm.rya.tinkerpop.rya
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

import com.tinkerpop.blueprints.impls.sail.SailEdge;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.impl.ContextStatementImpl;
import org.openrdf.model.impl.StatementImpl;

/**
 * Blueprints Edge for Sail stores
 * outVertex edge inVertex
 * <br/>
 * Groovy is doing something funky with properties and such here
 * <p/>
 * Date: 5/9/12
 * Time: 9:03 AM
 */
public class RyaSailEdge extends SailEdge {

    public static final String SPLIT = "|";

    public RyaSailEdge(Statement rawEdge, RyaSailGraph graph) {
        super(rawEdge, graph);
    }

    @Override
    public Object getId() {
        Statement statement = this.getRawEdge();
        return formatId(statement);
    }

    /**
     * Returns a formatted id for a full statement.
     *
     * @param statement
     * @return
     */
    public static String formatId(Statement statement) {
        if (null != statement.getContext())
            return (new StringBuilder()).append(statement.getSubject()).append(SPLIT).append(statement.getPredicate()).append(SPLIT).append(statement.getObject()).append(SPLIT).append(statement.getContext()).toString();
        else
            return (new StringBuilder()).append(statement.getSubject()).append(SPLIT).append(statement.getPredicate()).append(SPLIT).append(statement.getObject()).toString();
    }

//    public static RyaSailEdge fromId(String id, RyaSailGraph graph) {
//        def decodedId = URLDecoder.decode(id)
//        def statement = RdfIO.readStatement(ByteStreams.newDataInput(decodedId.bytes), RdfCloudTripleStoreConstants.VALUE_FACTORY)
//        println statement
//        return new RyaSailEdge(statement, graph)
//    }

    /**
     * @param id    formatted from getId method
     * @param graph
     * @return
     */
    public static RyaSailEdge fromId(String id, RyaSailGraph graph) {
        assert id != null;
        String[] split = id.split("\\|");
        if (split.length < 3) {
            return null;
        }
        String subj_s = split[0].trim();
        Value subj = graph.createValue(subj_s);
        String pred_s = split[1].trim();
        Value pred = graph.createValue(pred_s);
        String obj_s = split[2].trim();
        Value obj = graph.createValue(obj_s);
        if (split.length == 4) {
            //context available
            Value context = graph.createValue(split[3]);
            return new RyaSailEdge(new ContextStatementImpl((Resource) subj, (URI) pred, obj, (Resource) context), graph);
        } else {
            return new RyaSailEdge(new StatementImpl((Resource) subj, (URI) pred, obj), graph);
        }
    }

}
