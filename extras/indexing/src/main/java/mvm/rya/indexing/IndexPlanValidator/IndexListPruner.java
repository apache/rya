package mvm.rya.indexing.IndexPlanValidator;

import java.util.List;



import java.util.Set;

import mvm.rya.indexing.external.tupleSet.ExternalTupleSet;

public interface IndexListPruner {

    public Set<ExternalTupleSet> getRelevantIndices(List<ExternalTupleSet> indexList);
        
}
