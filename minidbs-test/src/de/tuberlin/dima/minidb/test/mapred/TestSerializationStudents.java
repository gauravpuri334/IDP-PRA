package de.tuberlin.dima.minidb.test.mapred;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.junit.Assert;
import org.junit.Test;

import de.tuberlin.dima.minidb.core.BigIntField;
import de.tuberlin.dima.minidb.core.CharField;
import de.tuberlin.dima.minidb.core.DataFormatException;
import de.tuberlin.dima.minidb.core.DataTuple;
import de.tuberlin.dima.minidb.core.DateField;
import de.tuberlin.dima.minidb.core.DoubleField;
import de.tuberlin.dima.minidb.core.FloatField;
import de.tuberlin.dima.minidb.core.IntField;
import de.tuberlin.dima.minidb.core.RID;
import de.tuberlin.dima.minidb.core.SmallIntField;
import de.tuberlin.dima.minidb.core.TimeField;
import de.tuberlin.dima.minidb.core.TimestampField;
import de.tuberlin.dima.minidb.core.VarcharField;
import de.tuberlin.dima.minidb.mapred.SerializationUtils;
import de.tuberlin.dima.minidb.parser.Predicate.Operator;
import de.tuberlin.dima.minidb.qexec.LowLevelPredicate;
import de.tuberlin.dima.minidb.qexec.predicate.JoinPredicate;
import de.tuberlin.dima.minidb.qexec.predicate.JoinPredicateAtom;
import de.tuberlin.dima.minidb.qexec.predicate.JoinPredicateConjunction;
import de.tuberlin.dima.minidb.qexec.predicate.JoinPredicateDisjunction;
import de.tuberlin.dima.minidb.qexec.predicate.LocalPredicate;
import de.tuberlin.dima.minidb.qexec.predicate.LocalPredicateConjunction;
import de.tuberlin.dima.minidb.qexec.predicate.LocalPredicateDisjunction;

public class TestSerializationStudents {

	/**
	 * Ensures that we can correctly serialize and deserialize DataTuples.
	 * 
	 * @throws DataFormatException
	 * @throws IOException
	 */
	@Test
	public void testDataTupleSerialization() throws DataFormatException, IOException {
		// Build a test tuple.
		DataTuple sent_tuple = new DataTuple(12);
		sent_tuple.assignDataField(new SmallIntField((short) 10), 0);
		sent_tuple.assignDataField(new IntField(10), 1);
		sent_tuple.assignDataField(new IntField(20), 2);
		sent_tuple.assignDataField(new BigIntField(40), 3);
		sent_tuple.assignDataField(new FloatField((float) 4.2), 4);
		sent_tuple.assignDataField(new DoubleField(7.4), 5);
		sent_tuple.assignDataField(new CharField("Hello"), 6);
		sent_tuple.assignDataField(new VarcharField("World"), 7);
		sent_tuple.assignDataField(new DateField(10, 11, 1992), 8);
		sent_tuple.assignDataField(new TimeField(12, 23, 54), 9);
		sent_tuple.assignDataField(new TimestampField(4, 10, 2004, 10, 16, 20, 152), 10);
		sent_tuple.assignDataField(new RID(245), 11);
		
		// Try serializing this tuple.
		ByteArrayOutputStream byte_stream = new ByteArrayOutputStream(128);
		DataOutputStream out = new DataOutputStream(byte_stream);
		sent_tuple.write(out);
		
		// Now read the content into a new tuple.
		DataInputStream in = new DataInputStream(new ByteArrayInputStream(byte_stream.toByteArray()));
		DataTuple received_tuple = new DataTuple();
		received_tuple.readFields(in);
		
		// And ensure that they are identical.
		Assert.assertEquals(sent_tuple, received_tuple);
	}
	
	/**
	 * Ensures that we can write local predicates into strings.
	 * 
	 * @throws IOException
	 */
	@Test
	public void testLocalPrediacateSerialization() throws IOException {
		// Build a test predicate.
		LocalPredicate[] preds = new LocalPredicate[2];
		preds[0] = new LowLevelPredicate(Operator.SMALLER,new IntField(10), 4);
		preds[1] = new LowLevelPredicate(Operator.GREATER_OR_EQUAL,new CharField("Hello"), 2);
		LocalPredicateDisjunction dis_pred = new LocalPredicateDisjunction(preds);
		preds = new LocalPredicate[2];
		preds[0] = dis_pred;
		preds[1] = new LowLevelPredicate(Operator.NOT_EQUAL, new DoubleField(2.4), 1);
		LocalPredicateConjunction conj_pred = new LocalPredicateConjunction(preds);
		
		// Serialize it into a string.
		String pred_str = SerializationUtils.writeLocalPredicateToString(conj_pred);
		
		// De-serialize it from the string.
		LocalPredicate pred = SerializationUtils.readLocalPredicateFromString(pred_str);
		
		Assert.assertEquals(pred.toString(), conj_pred.toString());
	}
	
	/**
	 * Ensures that we can write join predicates into strings.
	 * @throws IOException 
	 */
	@Test
	public void testJoinPredicateSerialization() throws IOException {
		// Build a test predicate.
		JoinPredicate[] preds = new JoinPredicate[2];
		preds[0] = new JoinPredicateAtom(Operator.SMALLER, 0, 3);
		preds[1] = new JoinPredicateAtom(Operator.GREATER, 0, 2);
		JoinPredicateConjunction conj_pred = new JoinPredicateConjunction(preds);
		preds = new JoinPredicate[2];
		preds[0] = conj_pred;
		preds[1] = new JoinPredicateAtom(Operator.EQUAL, 4, 1);
		JoinPredicateDisjunction disj_pred = new JoinPredicateDisjunction(preds);
		
		// Serialize it into a string.
		String pred_str = SerializationUtils.writeJoinPredicateToString(disj_pred);
		
		// De-serialize it from the string.
		JoinPredicate pred = SerializationUtils.readJoinPredicateFromString(pred_str);
		
		Assert.assertEquals(pred.toString(), disj_pred.toString());
	}
	
	@Test
	public void testIntegerArraySerialization() throws IOException {
		// Build a new test array.
		int[] array = new int[23];
		for (int i=0; i<array.length; ++i) {
			array[i] = i;
		}
		
		// Serialize it to a string.
		String array_str = SerializationUtils.writeIntArrayToString(array);
		
		// De-serialize it from the string.
		int[] rec_array = SerializationUtils.readIntArrayFromString(array_str);
		
		Assert.assertArrayEquals(array, rec_array);
	}
	
}
