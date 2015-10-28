//package mvm.mmrts.rdf.partition.query.evaluation.select;
//
//import cloudbase.core.data.Key;
//import cloudbase.core.data.Value;
//import org.openrdf.model.Statement;
//import org.openrdf.query.BindingSet;
//import org.openrdf.query.QueryEvaluationException;
//import org.openrdf.query.algebra.Var;
//import org.openrdf.query.algebra.evaluation.QueryBindingSet;
//
//import java.util.Iterator;
//import java.util.List;
//import java.util.Map;
//
///**
// * Class SubjectSelectIterator
// * Date: Jul 18, 2011
// * Time: 3:38:16 PM
// */
//public class SubjectSelectIterator extends SelectIterator {
//
//    private Var subjVar;
//    private List<Map.Entry<Var, Var>> select;
//
//    public SubjectSelectIterator(BindingSet bindings, Iterator<Map.Entry<Key, Value>> iter, Var subjVar, List<Map.Entry<Var, Var>> select) {
//        super(bindings, iter);
//        this.subjVar = subjVar;
//        this.select = select;
//    }
//
//    @Override
//    public BindingSet next() throws QueryEvaluationException {
//        List<Statement> document = nextDocument();
//        if(document.size() != 6) {
//            System.out.println("here");
//        }
//        return populateBindingSet(document, subjVar, this.select);
//
//    }
//}
