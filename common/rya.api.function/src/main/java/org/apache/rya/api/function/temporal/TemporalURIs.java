package org.apache.rya.api.function.temporal;

/**
 * Constants for the Temporal Functions used in rya.
 */
public class TemporalURIs {
    /**
     * All temporal functions have the namespace (<tt>http://rya.apache.org/ns/temporal#</tt>).
     */
    public static final String NAMESPACE = "http://rya.apache.org/ns/temporal#";

    /** <tt>http://rya.apache.org/ns/temporal#equals</tt> */
    public final static String EQUALS = NAMESPACE + "equals";

    /** <tt>http://rya.apache.org/ns/temporal#before</tt> */
    public final static String BEFORE = NAMESPACE + "before";

    /** <tt>http://rya.apache.org/ns/temporal#after</tt> */
    public final static String AFTER = NAMESPACE + "after";

    /** <tt>http://rya.apache.org/ns/temporal#within</tt> */
    public final static String WITHIN = NAMESPACE + "within";
}
