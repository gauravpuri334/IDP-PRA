package de.tuberlin.dima.minidb.test.mapred;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import de.tuberlin.dima.minidb.DBInstance;
import de.tuberlin.dima.minidb.api.AbstractExtensionFactory;
import de.tuberlin.dima.minidb.core.DataTuple;
import de.tuberlin.dima.minidb.io.cache.PageExpiredException;
import de.tuberlin.dima.minidb.io.cache.PageFormatException;
import de.tuberlin.dima.minidb.io.manager.BufferPoolException;
import de.tuberlin.dima.minidb.io.tables.PageTupleAccessException;
import de.tuberlin.dima.minidb.mapred.TableInputFormat;
import de.tuberlin.dima.minidb.mapred.TableOutputFormat;

public class TestHadoopIntegrationStudents {
	
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
	
	/**
	 * Helper function to test the IO formats on a given table.
	 * @param table
	 * @throws IOException 
	 * @throws BufferPoolException 
	 * @throws InterruptedException 
	 * @throws ClassNotFoundException 
	 * @throws PageTupleAccessException 
	 * @throws PageExpiredException 
	 */
	public void testTableIOFormat(String table) throws IOException, BufferPoolException, ClassNotFoundException, InterruptedException, PageExpiredException, PageTupleAccessException {
		Job job = new Job();
		Configuration jobConf = job.getConfiguration();
		
		// Prepare the table input format.
		TableInputFormat.registerDBInstance(this.dbInstance);
		TableInputFormat.addInputTable(jobConf, table);
		TableInputFormat.setNrOfInputSplitsPerTable(jobConf, 4);
		job.setInputFormatClass(AbstractExtensionFactory.getExtensionFactory().
				getTableInputFormat());
		
		// Prepare the table output format.
		String dummyTable = TestUtils.addDummyTable(this.dbInstance, table, "dummy");
		TableOutputFormat.registerDBInstance(this.dbInstance);
		TableOutputFormat.setOutputTable(jobConf, dummyTable);
		job.setOutputFormatClass(TableOutputFormat.class);
		
		// Configure a map-only task that simply forwards the output of the inputformat.
	    job.setMapperClass(Mapper.class);	// Use the identity mapper.
	    job.setNumReduceTasks(0);	// Disable the reducer.
	    job.setMapOutputKeyClass(LongWritable.class);
	    job.setMapOutputValueClass(DataTuple.class);
	    job.setOutputKeyClass(LongWritable.class);
	    job.setOutputValueClass(DataTuple.class);
	    
	    // Ensure that the job runs succesfully.
	    Assert.assertTrue(job.waitForCompletion(true));
	    
	    // And ensure that the output is identical.
	    TestUtils.compareTables(this.dbInstance, table, dummyTable);
	}
	
	/**
	 * Ensures that Hadoop jobs can correctly read from / write to the MiniDB table files.
	 * 
	 * @throws IOException 
	 * @throws InterruptedException 
	 * @throws ClassNotFoundException 
	 * @throws PageFormatException 
	 * @throws BufferPoolException 
	 * @throws PageTupleAccessException 
	 * @throws PageExpiredException 
	 */
	@Test
	public void testTableIOFormatsOnSmallTable()
			throws IOException, ClassNotFoundException, InterruptedException, 
			PageFormatException, BufferPoolException, PageExpiredException, 
			PageTupleAccessException {
		testTableIOFormat("region");
	}
	
	/**
	 * Make sure that we can correctly read to / write from large MinIDB table files.
	 * 
	 * @throws IOException
	 * @throws ClassNotFoundException
	 * @throws InterruptedException
	 * @throws PageFormatException
	 * @throws BufferPoolException
	 * @throws PageExpiredException
	 * @throws PageTupleAccessException
	 */
	@Test
	public void testTableIOFormatsOnLargeTable() 
			throws IOException, ClassNotFoundException, InterruptedException, 
			PageFormatException, BufferPoolException, PageExpiredException, 
			PageTupleAccessException {
		testTableIOFormat("lineitem");
	}
}
