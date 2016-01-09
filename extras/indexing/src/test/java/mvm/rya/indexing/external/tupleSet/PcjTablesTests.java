package mvm.rya.indexing.external.tupleSet;

import static org.junit.Assert.assertEquals;

import java.util.Set;

import org.junit.Test;

import com.google.common.collect.Sets;

import mvm.rya.indexing.external.tupleSet.PcjTables.PcjMetadata;
import mvm.rya.indexing.external.tupleSet.PcjTables.ShiftVarOrderFactory;
import mvm.rya.indexing.external.tupleSet.PcjTables.VariableOrder;

/**
 * Tests the classes and methods of {@link PcjTables}.
 */
public class PcjTablesTests {

    @Test
    public void variableOrder_hashCode() {
        assertEquals(new VariableOrder("a", "b", "C").hashCode(), new VariableOrder("a", "b", "C").hashCode());
    }

    @Test
    public void variableOrder_equals() {
        assertEquals(new VariableOrder("a", "b", "C"), new VariableOrder("a", "b", "C"));
    }

    @Test
    public void variableOrder_fromString() {
        assertEquals(new VariableOrder("a", "b", "c"), new VariableOrder("a;b;c"));
    }

    @Test
    public void variableORder_toString() {
        assertEquals("a;b;c", new VariableOrder("a", "b", "c").toString());
    }

    @Test
    public void pcjMetadata_hashCode() {
        PcjMetadata meta1 = new PcjMetadata("A SPARQL string.", 5, Sets.newHashSet(new VariableOrder("a", "b", "c"), new VariableOrder("d", "e", "f")));
        PcjMetadata meta2 = new PcjMetadata("A SPARQL string.", 5, Sets.newHashSet(new VariableOrder("a", "b", "c"), new VariableOrder("d", "e", "f")));
        assertEquals(meta1.hashCode(), meta2.hashCode());
    }

    @Test
    public void pcjMetadata_equals() {
        PcjMetadata meta1 = new PcjMetadata("A SPARQL string.", 5, Sets.newHashSet(new VariableOrder("a", "b", "c"), new VariableOrder("d", "e", "f")));
        PcjMetadata meta2 = new PcjMetadata("A SPARQL string.", 5, Sets.newHashSet(new VariableOrder("a", "b", "c"), new VariableOrder("d", "e", "f")));
        assertEquals(meta1, meta2);
    }

    @Test
    public void shiftVarOrdersFactory() {
        Set<VariableOrder> expected = Sets.newHashSet(
                new VariableOrder("a;b;c"),
                new VariableOrder("b;c;a"),
                new VariableOrder("c;a;b"));

        Set<VariableOrder> varOrders = new ShiftVarOrderFactory().makeVarOrders(new VariableOrder("a;b;c"));
        assertEquals(expected, varOrders);
    }

}