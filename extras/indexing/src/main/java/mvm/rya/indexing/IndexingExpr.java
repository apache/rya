package mvm.rya.indexing;

import java.util.Set;

import mvm.rya.indexing.accumulo.geo.GeoTupleSet;

import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.algebra.Var;

import com.google.common.collect.Sets;

public class IndexingExpr {

    private final URI function;
    private final Value[] arguments;
    private final StatementPattern spConstraint;

    public IndexingExpr(URI function, StatementPattern spConstraint, Value... arguments) {
        this.function = function;
        this.arguments = arguments;
        this.spConstraint = spConstraint;
    }

    public URI getFunction() {
        return function;
    }

    public Value[] getArguments() {
        return arguments;
    }

    public StatementPattern getSpConstraint() {
        return spConstraint;
    }


    public Set<String> getBindingNames() {
        //resource and match variable for search are already included as standard result-bindings
        Set<String> bindings = Sets.newHashSet();

        for(Var v: spConstraint.getVarList()) {
            if(!v.isConstant()) {
                bindings.add(v.getName());
            }
        }
        return bindings;
    }
    
    
    @Override
    public boolean equals(Object other) {
        if (!(other instanceof IndexingExpr)) {
            return false;
        }
        IndexingExpr arg = (IndexingExpr) other;
        return (this.function.equals(arg.function)) && (this.spConstraint.equals(arg.spConstraint))
                && (this.arguments.equals(arg.arguments));
    }
    
    
    @Override
    public int hashCode() {
        int result = 17;
        result = 31*result + function.hashCode();
        result = 31*result + spConstraint.hashCode();
        result = 31*result + arguments.hashCode();
        
        return result;
    }
    
}

    

