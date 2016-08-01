package org.apache.rya.export.api;

/**
 * Handles merging the database instances based on the type of parent DB
 * instance and child DB instance involved.
 */
public interface Merger {
    /**
     * Runs the merging job based on the type of parent DB instance and child DB
     * instance involved.
     */
    public void runJob();
}