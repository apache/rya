package org.apache.rya.api.model;

import static org.junit.Assert.assertNotEquals;

import org.junit.Test;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.query.impl.MapBindingSet;

/**
 * Unit tests the methods of {@link VisibilityBindingSet}.
 */
public class VisibilityBindingSetTest {

    @Test
    public void hashcode() {
        // Create a BindingSet, decorate it, and grab its hash code.
        final ValueFactory vf = new ValueFactoryImpl();
        final MapBindingSet bSet = new MapBindingSet();
        bSet.addBinding("name", vf.createLiteral("alice"));

        final VisibilityBindingSet visSet = new VisibilityBindingSet(bSet);
        final int origHash = visSet.hashCode();

        // Add another binding to the binding set and grab the new hash code.
        bSet.addBinding("age", vf.createLiteral(37));
        final int updatedHash = visSet.hashCode();

        // Show those hashes are different.
        assertNotEquals(origHash, updatedHash);
    }
}