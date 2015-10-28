package mvm.rya.joinselect.mr;

import static org.junit.Assert.*;

import org.junit.Test;

import java.io.IOException;

import mvm.rya.joinselect.mr.JoinSelectProspectOutput;
import mvm.rya.joinselect.mr.utils.CardinalityType;
import mvm.rya.joinselect.mr.utils.CompositeType;
import mvm.rya.joinselect.mr.utils.TripleCard;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Test;

public class JoinSelectProspectOutputTest {

    private static final String DELIM = "\u0000";

    public enum TripleValueType {
        subject, predicate, object, subjectpredicate, predicateobject, subjectobject
    }

    @Test
    public void testOutput() throws InterruptedException, IOException {

        String s = "urn:gem:etype#1234";
        String p = "urn:gem#pred";

        String ts = "798497748386999999";
        
        Text t1 = new Text(TripleValueType.subject.name() + DELIM + s + DELIM + 1);
        Text t2 = new Text(TripleValueType.predicate.name() + DELIM + p + DELIM + 2);
        Text t3 = new Text(TripleValueType.subjectpredicate.name() + DELIM + s + DELIM + p + DELIM + ts);

        byte[] b = new byte[0];
        byte[] c = "25".getBytes();
        byte[] d = "47".getBytes();
        byte[] e = "15".getBytes();

        Key key1 = new Key(t1.getBytes(), b, b, b, 1);
        Key key2 = new Key(t2.getBytes(), b, b, b, 1);
        Key key3 = new Key(t3.getBytes(), b, b, b, 1);
        Value val1 = new Value(c);
        Value val2 = new Value(d);
        Value val3 = new Value(e);
        
       

        // System.out.println("Keys are " + key1 + " and " + key2);

        new MapDriver<Key, Value, CompositeType, TripleCard>()
                .withMapper(new JoinSelectProspectOutput.CardinalityMapper())
                .withInput(key1, val1)
                .withInput(key2, val2)
                .withInput(key3, val3)
                .withOutput(new CompositeType(s, 1), new TripleCard(new CardinalityType(25, "subject", 1)))
                .withOutput(new CompositeType(p, 1), new TripleCard(new CardinalityType(47, "predicate", 2)))
                .withOutput(new CompositeType(s + DELIM + p, 1),
                        new TripleCard(new CardinalityType(15, "subjectpredicate", Long.parseLong(ts)))).runTest();

    }

}
