//package mvm.rya.blueprints.sail
//
//import com.tinkerpop.blueprints.pgm.impls.sail.SailEdge
//import org.openrdf.model.Statement
//import org.openrdf.model.impl.ContextStatementImpl
//import org.openrdf.model.impl.StatementImpl
//
///**
// * Blueprints Edge for Sail stores
// * outVertex edge inVertex
// *
// * Date: 5/9/12
// * Time: 9:03 AM
// */
//class RyaSailEdge extends SailEdge {
//
//    public static final String SPLIT = "|"
//
//    RyaSailEdge(Statement rawEdge, RyaSailGraph graph) {
//        super(rawEdge, graph)
//    }
//
//    @Override
//    Object getId() {
//        def statement = this.getRawEdge()
//        return formatId(statement);
//    }
//
//    /**
//     * Returns a formatted id for a full statement.
//     * @param statement
//     * @return
//     */
//    static String formatId(Statement statement) {
//        if (null != statement.getContext())
//            return (new StringBuilder()).append(statement.getSubject()).append(SPLIT).append(statement.getPredicate()).append(SPLIT).append(statement.getObject()).append(SPLIT).append(statement.getContext()).toString();
//        else
//            return (new StringBuilder()).append(statement.getSubject()).append(SPLIT).append(statement.getPredicate()).append(SPLIT).append(statement.getObject()).toString()
//    }
//
////    public static RyaSailEdge fromId(String id, RyaSailGraph graph) {
////        def decodedId = URLDecoder.decode(id)
////        def statement = RdfIO.readStatement(ByteStreams.newDataInput(decodedId.bytes), RdfCloudTripleStoreConstants.VALUE_FACTORY)
////        println statement
////        return new RyaSailEdge(statement, graph)
////    }
//
//    /**
//     *
//     * @param id formatted from getId method
//     * @param graph
//     * @return
//     */
//    public static RyaSailEdge fromId(String id, RyaSailGraph graph) {
//        assert id != null
//        def split = id.split("\\|")
//        if(split.length < 3) {
//            return null
//        }
//        String subj_s = split[0].trim()
//        def subj = graph.createValue(subj_s)
//        String pred_s = split[1].trim()
//        def pred = graph.createValue(pred_s)
//        String obj_s = split[2].trim()
//        def obj = graph.createValue(obj_s)
//        if(split.length == 4) {
//            //context available
//            def context = graph.createValue(split[3])
//            return new RyaSailEdge(new ContextStatementImpl(subj, pred, obj, context), graph);
//        } else {
//            return new RyaSailEdge(new StatementImpl(subj, pred, obj), graph);
//        }
//    }
//
//}
