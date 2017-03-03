package de.tuberlin.dima.minidb.test.mapred;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.collections.Bag;
import org.apache.commons.collections.bag.HashBag;
import org.junit.Assert;

import de.tuberlin.dima.minidb.DBInstance;
import de.tuberlin.dima.minidb.catalogue.TableDescriptor;
import de.tuberlin.dima.minidb.catalogue.TableSchema;
import de.tuberlin.dima.minidb.core.BasicType;
import de.tuberlin.dima.minidb.core.DataField;
import de.tuberlin.dima.minidb.core.DataTuple;
import de.tuberlin.dima.minidb.core.DoubleField;
import de.tuberlin.dima.minidb.core.FloatField;
import de.tuberlin.dima.minidb.io.cache.PageExpiredException;
import de.tuberlin.dima.minidb.io.manager.BufferPoolException;
import de.tuberlin.dima.minidb.io.tables.PageTupleAccessException;
import de.tuberlin.dima.minidb.io.tables.TablePage;
import de.tuberlin.dima.minidb.io.tables.TableResourceManager;
import de.tuberlin.dima.minidb.io.tables.TupleIterator;
import de.tuberlin.dima.minidb.qexec.PhysicalPlanOperator;
import de.tuberlin.dima.minidb.qexec.QueryExecutionException;

/**
 * A small collection of test helper utilities for the MapReduce tests.
 * 
 * @author mheimel
 *
 */
public class TestUtils {

	/**
	 * Helper function to copy the schema of a given table.
	 * 
	 * @param instance The MiniDBs instance that owns the table.
	 * @param tableName
	 * @return A copy of the schema for said table, or null if the given table does not exist.
	 */
	private static TableSchema copyTableSchema(DBInstance instance, String tableName) {
		// Make sure that the table exists.
		TableDescriptor desc = instance.getCatalogue().getTable(tableName);
		if (desc == null) return null;
		// Now fetch the original schema.
		TableSchema schema = desc.getSchema();
		// And build a copy.
		TableSchema new_schema = new TableSchema(schema.getPageSize());
		for (int i=0; i < schema.getNumberOfColumns(); ++i) {
			new_schema.addColumn(schema.getColumn(i));
		}
		return new_schema;
	}
	
	/**
	 * Helper function to add an empty copy of a given table to the provided 
	 * database instance. The copy will be named sourceTable_postfix.
	 * 
	 * @param instance
	 * @param sourceTable
	 * @param postfix
	 * 
	 * @return The name of the newly generated dummy table.
	 * @throws IOException
	 * @throws BufferPoolException
	 */
	public static String addDummyTable(DBInstance instance, String sourceTable, 
			String postfix) throws IOException, BufferPoolException {
		String tableName = sourceTable + "_" + postfix;
		// First, we need to copy the schema.
		TableSchema schema = copyTableSchema(instance, sourceTable);
		// Prepare the table file, and ensure it is empty.
		File tableFile = new File(System.getProperty("java.io.tmpdir"), tableName + ".tbl");
		if (tableFile.exists()) tableFile.delete();
		// Now, allocate a new TableResourceManager.
		TableResourceManager res = TableResourceManager.createTable(tableFile, schema);
		// And register the table.
		TableDescriptor desc = new TableDescriptor(tableName, tableFile.getAbsolutePath());
		instance.getCatalogue().addTable(desc);
		int resID = instance.getCatalogue().reserveNextId();
		instance.getBufferPool().registerResource(resID, res);
		desc.setResourceProperties(res, resID);
		// We are done.
		return tableName;
	}
	
	/**
	 * Hashes each tuple and inserts it into a bag.
	 * 
	 * @param table
	 * @return
	 * @throws PageExpiredException
	 * @throws PageTupleAccessException
	 * @throws IOException
	 */
	public static Bag readTableToHashBag(TableDescriptor table) throws PageExpiredException, PageTupleAccessException, IOException {
		Bag result = new HashBag();
		
		// Open the resource manager and allocate a page buffer.
		TableResourceManager res = table.getResourceManager();
		byte[] buffer = new byte[res.getPageSize().getNumberOfBytes()];

		// Now walk over all pages.
		for (int pageNr = res.getFirstDataPageNumber(); 
				pageNr <= res.getLastDataPageNumber(); pageNr++) {
			// Open a Tuple iterator for this page.
			TablePage page = res.readPageFromResource(buffer, pageNr);
			TupleIterator it = page.getIterator(
					table.getSchema().getNumberOfColumns(), 
					Long.MAX_VALUE);
			while (it.hasNext()) {
				ByteArrayOutputStream byte_stream = new ByteArrayOutputStream(1024);
				// Serialize the tuple into the byte stream.
				DataTuple tuple = it.next();
				tuple.write(new DataOutputStream(byte_stream));
				// And insert the hash value into the bag.
				result.add(Arrays.hashCode(byte_stream.toByteArray()));
			}
		}
		return result;
	}
	
	/**
	 * Convenience function to materialize the provided query plan, hash each
	 * tuple and insert it into a bag.
	 * 
	 * @param root
	 * @return
	 * @throws QueryExecutionException
	 * @throws IOException
	 */
	public static Bag readQueryPlanToHashBag(PhysicalPlanOperator root) 
			throws QueryExecutionException, IOException {
		Bag result = new HashBag();
		
		root.open(null);
		
		DataTuple tuple;
		while ((tuple = root.next()) != null) {
			ByteArrayOutputStream byte_stream = new ByteArrayOutputStream(1024);
			tuple.write(new DataOutputStream(byte_stream));
			result.add(Arrays.hashCode(byte_stream.toByteArray()));
		}
		
		root.close();
		
		return result;
	}
	
	public static List<DataTuple> readQueryPlanToList(
			PhysicalPlanOperator root) throws QueryExecutionException {
		List<DataTuple> result = new LinkedList<DataTuple>();
		
		root.open(null);
		DataTuple tuple;
		while ((tuple = root.next()) != null) {
			result.add(tuple);
		}
		root.close();
		
		return new ArrayList<DataTuple>(result);
	}
	
	/**
	 * Convenience function to read a complete relation into a list of data tuples.
	 * 
	 * @param table
	 * @return
	 * @throws IOException
	 * @throws PageExpiredException
	 * @throws PageTupleAccessException
	 */
	public static List<DataTuple> readTableToList(TableDescriptor table) 
			throws IOException, PageExpiredException, 
				   PageTupleAccessException {
		List<DataTuple> result = new LinkedList<DataTuple>();
		
		// Open the resource manager and allocate a page buffer.
		TableResourceManager res = table.getResourceManager();
		byte[] buffer = new byte[res.getPageSize().getNumberOfBytes()];
		

		// Now walk over all pages.
		for (int pageNr = res.getFirstDataPageNumber(); 
				pageNr <= res.getLastDataPageNumber(); pageNr++) {
			// Open a Tuple iterator for this page.
			TablePage page = res.readPageFromResource(buffer, pageNr);
			TupleIterator it = page.getIterator(
					table.getSchema().getNumberOfColumns(), 
					Long.MAX_VALUE);
			while (it.hasNext()) {
				result.add(it.next());
			}
		}
		return new ArrayList<DataTuple>(result);
	}
	
	
	/**
	 * Helper function to compare two tables for equality.
	 * 
	 * @param instance
	 * @param table1
	 * @param table2
	 * @throws IOException 
	 * @throws PageTupleAccessException 
	 * @throws PageExpiredException 
	 */
	public static void compareTables(DBInstance instance, String table1, 
			String table2) throws PageExpiredException, 
								  PageTupleAccessException, IOException {
		// Fetch the tables.
		TableDescriptor desc1 = instance.getCatalogue().getTable(table1);
		TableDescriptor desc2 = instance.getCatalogue().getTable(table2);
		Assert.assertNotNull(desc1);
		Assert.assertNotNull(desc2);
		// First, compare the table schemas.
		TableSchema schema1 = desc1.getSchema();
		TableSchema schema2 = desc2.getSchema();
		Assert.assertEquals(schema1.getNumberOfColumns(), schema2.getNumberOfColumns());
		for (int i = 0; i < schema1.getNumberOfColumns(); ++i) {
			Assert.assertEquals(schema1.getColumn(i), schema2.getColumn(i));
		}
		// Now compare the actual table content (for now we only do the 
		// comparison hash-based).
		Bag bag1 = readTableToHashBag(desc1);
		Bag bag2 = readTableToHashBag(desc2);
		Assert.assertEquals(bag1, bag2);
	}
	
	/**
	 * Compare the output of a physical plan to a materialized table.
	 * 
	 * @param instance
	 * @param table
	 * @param plan
	 * @throws PageExpiredException
	 * @throws IOException
	 * @throws PageTupleAccessException
	 * @throws QueryExecutionException
	 */
	public static void compareTableToQueryPlan(DBInstance instance, 
			String table, PhysicalPlanOperator plan) 
					throws PageExpiredException, IOException, 
					       PageTupleAccessException, QueryExecutionException {
		TableDescriptor desc = instance.getCatalogue().getTable(table);
		Assert.assertNotNull(desc);
		// Now read the both the table and the operator into lists.
		List<DataTuple> table_list = readTableToList(desc);
		List<DataTuple> plan_list = readQueryPlanToList(plan);
		Assert.assertEquals(table_list.size(), plan_list.size());
		// Sort both to ensure they have the same order.
		Collections.sort(table_list);
		Collections.sort(plan_list);
		
		for (int i=0; i<table_list.size(); ++i) {
			// Compare each field of the Datatuples individually.
			Assert.assertEquals(table_list.get(i).getNumberOfFields(),
					plan_list.get(i).getNumberOfFields());
			for (int j=0; j<table_list.get(i).getNumberOfFields(); ++j) {
				DataField field1 = table_list.get(i).getField(j);
				DataField field2 = plan_list.get(i).getField(j);
				Assert.assertEquals(field1.getBasicType(),
						field2.getBasicType());
				if (field1.getBasicType() == BasicType.DOUBLE) {
					Assert.assertEquals(
							((DoubleField)field1).asDouble(),
							((DoubleField)field2).asDouble(),
							1.0f);
				} else if (field1.getBasicType() == BasicType.FLOAT) {
					Assert.assertEquals(
							((FloatField)field1).asDouble(),
							((FloatField)field2).asDouble(),
							1.0f);					
				} else {
					Assert.assertEquals(field1, field2);
				}
			}
		}
	}
}
