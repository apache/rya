package org.apache.rya.indexing.pcj.fluo.app.util;

import java.util.ArrayList;
import java.util.List;

import org.apache.rya.indexing.pcj.fluo.app.query.AggregationMetadata;
import org.apache.rya.indexing.pcj.fluo.app.query.ConstructQueryMetadata;
import org.apache.rya.indexing.pcj.fluo.app.query.FilterMetadata;
import org.apache.rya.indexing.pcj.fluo.app.query.FluoQuery;
import org.apache.rya.indexing.pcj.fluo.app.query.JoinMetadata;
import org.apache.rya.indexing.pcj.fluo.app.query.PeriodicQueryMetadata;
import org.apache.rya.indexing.pcj.fluo.app.query.ProjectionMetadata;
import org.apache.rya.indexing.pcj.fluo.app.query.QueryMetadata;
import org.apache.rya.indexing.pcj.fluo.app.query.QueryMetadataVisitorBase;
import org.apache.rya.indexing.pcj.fluo.app.query.StatementPatternMetadata;

/**
 * A visitor that does a pre-order traversal of the FluoQuery and
 * collects the ids of metadata query nodes along the way.
 *
 */
public class NodeIdCollector extends QueryMetadataVisitorBase {

    List<String> ids;
    
    public NodeIdCollector(FluoQuery fluoQuery ) {
        super(fluoQuery);
        ids = new ArrayList<>();
    }
    
    public List<String> getNodeIds() {
        return ids;
    }
    
    public void visit(QueryMetadata metadata) {
        ids.add(metadata.getNodeId());
        super.visit(metadata);
    }
    
    public void visit(ProjectionMetadata metadata) {
        ids.add(metadata.getNodeId());
        super.visit(metadata);
    }
    
    public void visit(ConstructQueryMetadata metadata) {
        ids.add(metadata.getNodeId());
        super.visit(metadata);
    }
    
    public void visit(FilterMetadata metadata) {
        ids.add(metadata.getNodeId());
        super.visit(metadata);
    }
    
    public void visit(JoinMetadata metadata) {
        ids.add(metadata.getNodeId());
        super.visit(metadata);
    }
    
    public void visit(StatementPatternMetadata metadata) {
        ids.add(metadata.getNodeId());
    }
    
    public void visit(PeriodicQueryMetadata metadata) {
        ids.add(metadata.getNodeId());
        super.visit(metadata);
    }
    
    public void visit(AggregationMetadata metadata) {
        ids.add(metadata.getNodeId());
        super.visit(metadata);
    }
    
}
