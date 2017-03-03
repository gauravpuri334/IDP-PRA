package de.tuberlin.dima.minidb.test.mapred.qexec;


import java.io.IOException;
import java.util.List;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import de.tuberlin.dima.minidb.DBInstance;
import de.tuberlin.dima.minidb.api.AbstractExtensionFactory;
import de.tuberlin.dima.minidb.catalogue.TableDescriptor;
import de.tuberlin.dima.minidb.core.DataTuple;
import de.tuberlin.dima.minidb.core.IntField;
import de.tuberlin.dima.minidb.io.cache.PageExpiredException;
import de.tuberlin.dima.minidb.io.manager.BufferPoolException;
import de.tuberlin.dima.minidb.io.tables.PageTupleAccessException;
import de.tuberlin.dima.minidb.mapred.qexec.BulkProcessingOperator;
import de.tuberlin.dima.minidb.mapred.qexec.TableInputOperator;
import de.tuberlin.dima.minidb.mapred.qexec.examples.CountTuplesOperator;
import de.tuberlin.dima.minidb.mapred.qexec.examples.ForwardLeftInputOperator;
import de.tuberlin.dima.minidb.mapred.qexec.examples.IdentityOperator;
import de.tuberlin.dima.minidb.test.mapred.TestHadoopIntegrationStudents;
import de.tuberlin.dima.minidb.test.mapred.TestUtils;

/**
 * This test verifies that the exemplary Hadoop operators all work as
 * expected.
 * 
 * This test should pass once you correctly implemented the InputFormat.
 * 
 * @author mheimel
 *
 */
public class TestExampleHadoopOperatorsStudents {

	/**
	 * The path to the file with the configuration.
	 */
	private static final String CONFIG_FILE_NAME = TestHadoopIntegrationStudents.class.getResource("/config.xml").getPath();
	
	/**
	 * The path to the file with the catalogue.
	 */
	private static final String CATALOGUE_FILE_NAME = TestHadoopIntegrationStudents.class.getClass().getResource("/catalogue.xml").getPath();
	
	/**
	 * The database instance to perform the tests on.
	 */
	protected DBInstance dbInstance = null;
	
	@BeforeClass
	public static void initExtensionFactory() throws Exception {
		// initialize the extension factory to have access to the user methods
		AbstractExtensionFactory.initializeDefault();
	}
	
	@Before
	public void setUp() throws Exception {
		// initialize a database instance and start it
		this.dbInstance = new DBInstance(CONFIG_FILE_NAME, CATALOGUE_FILE_NAME);
		int returncode = this.dbInstance.startInstance(); 
		if (returncode != DBInstance.RETURN_CODE_OKAY) {
			throw new Exception("DBInstance could not be started: " + returncode);
		}
	}

	@After
	public void tearDown() throws Exception {
		// check if instance is running
		if (this.dbInstance != null && this.dbInstance.isRunning()) {
			// stop running instance
			this.dbInstance.shutdownInstance(false);
		}
	}

	@Test
	public void testIdentityOperator() throws IOException, ClassNotFoundException, 
											  InterruptedException, BufferPoolException,
											  PageExpiredException, PageTupleAccessException {
		BulkProcessingOperator op = new TableInputOperator(dbInstance, "lineitem");
		op = new IdentityOperator(dbInstance, op);
		
		// Run the operator.
		Assert.assertTrue(op.run());
		
		// And make sure that the output is as expected.
		TestUtils.compareTables(dbInstance, "lineitem", op.getResultTableName());
	}
	
	@Test
	public void testForwardLeftInputOperator() throws IOException, ClassNotFoundException,
	                                                  InterruptedException, BufferPoolException,
	                                                  PageExpiredException, PageTupleAccessException {
		BulkProcessingOperator left_in = new TableInputOperator(dbInstance, "lineitem");
		BulkProcessingOperator right_in = new TableInputOperator(dbInstance, "region");
		BulkProcessingOperator op = new ForwardLeftInputOperator(dbInstance, left_in, right_in);
		
		// Run the operator.
		Assert.assertTrue(op.run());
		
		// And make sure that the output is as expected.
		TestUtils.compareTables(dbInstance, "lineitem", op.getResultTableName());
	}
	
	@Test
	public void testCountTuplesOperator() throws IOException, ClassNotFoundException,
                                                 InterruptedException, BufferPoolException,
                                                 PageExpiredException, PageTupleAccessException {
		BulkProcessingOperator op = new TableInputOperator(dbInstance, "lineitem");
		op = new CountTuplesOperator(dbInstance, op);
		
		Assert.assertTrue(op.run());
		
		// Now make sure the tuple is correct.
		int expected = (int)dbInstance.getCatalogue().getTable("lineitem").getStatistics().getCardinality();
		
		// Open the result table and look at the value.
		TableDescriptor result_desc = dbInstance.getCatalogue().getTable(op.getResultTableName());
		List<DataTuple> result = TestUtils.readTableToList(result_desc);
		Assert.assertEquals(result.size(), 1);
		Assert.assertEquals(((IntField)(result.get(0).getField(0))).getValue(), expected);
	}
	
}
