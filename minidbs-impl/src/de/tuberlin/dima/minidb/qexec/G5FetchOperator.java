package de.tuberlin.dima.minidb.qexec;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

import de.tuberlin.dima.minidb.core.DataTuple;
import de.tuberlin.dima.minidb.core.RID;
import de.tuberlin.dima.minidb.io.cache.PageExpiredException;
import de.tuberlin.dima.minidb.io.manager.BufferPoolException;
import de.tuberlin.dima.minidb.io.manager.BufferPoolManager;
import de.tuberlin.dima.minidb.io.tables.PageTupleAccessException;
import de.tuberlin.dima.minidb.io.tables.TablePage;

public class G5FetchOperator implements FetchOperator {

	private PhysicalPlanOperator child;
	private BufferPoolManager bufferPool;
	private int tableResourceId;
	private int[] outputColumnMap;
	private int colBitmap;
	private ArrayList<Integer> columnIndexes;

	public G5FetchOperator(PhysicalPlanOperator child, BufferPoolManager bufferPool, int tableResourceId, int[] outputColumnMap) {
		
		this.child = child;
		this.bufferPool = bufferPool;
		this.tableResourceId = tableResourceId;
		this.outputColumnMap = outputColumnMap;
		
		

		// Column bitmap used to tell which column to retrieve from the table
		// Ex : col 1,2,4 : 10110 = 22
		this.colBitmap = 0;
		
		// Array of the indexes retrieved -- redundant with the column bitmap but make the normalization easier
		// Ex : col 1, 2, 4 : [1, 2, 4]
		this.columnIndexes = new ArrayList<Integer>();
		
		
		for (int i = 0; i < outputColumnMap.length; i++) {
			
			if(!columnIndexes.contains(outputColumnMap[i])) {
				
				this.colBitmap += Math.pow(2, outputColumnMap[i]);
				columnIndexes.add(outputColumnMap[i]);						
			}
		}
		
		Collections.sort(columnIndexes);
		
	}

	@Override
	public void open(DataTuple correlatedTuple) throws QueryExecutionException {
		
		child.open(correlatedTuple);

	}

	@Override
	public DataTuple next() throws QueryExecutionException {
		
		DataTuple childTuple;
		if ((childTuple = child.next()) != null) {
			RID rid = (RID) childTuple.getField(0);
			
			TablePage page;
			try {
				page = (TablePage) bufferPool.getPageAndPin(tableResourceId, rid.getPageIndex());
				
				DataTuple result = normalize(page.getDataTuple(rid.getTupleIndex(), colBitmap, columnIndexes.size()));
				
				bufferPool.unpinPage(tableResourceId, rid.getPageIndex());
				
				return result;
				
			} catch (BufferPoolException | IOException | PageExpiredException | PageTupleAccessException e) {
				throw new QueryExecutionException(e);
			}
			

		}
		
		return null;
	}

	@Override
	public void close() throws QueryExecutionException {
		child.close();

	}
	
	
	private DataTuple normalize(DataTuple tuple) {
		
		/*
		 * The input tuple follows the indexes in columnIndexes 
		 * and has to be mapped to the result tuple following producedColumnIndexes
		 * 
		 */
		DataTuple result = new DataTuple(outputColumnMap.length);

		/* Ex : columnIndexes : [1,2,4]
		 * 		producedColumnIndexes : [2, 4, 4, 1]
		 * 
		 */
		
		// Loop through columnIndexes
		for(int i = 0; i < columnIndexes.size(); i++) {
			
			int index = columnIndexes.get(i);
			
			// Loop through producedColumnIndexes to find matching columns
			for(int col = 0; col < outputColumnMap.length; col ++) {
					
				// Fill result tuple according to producedColumnIndexes
				if(outputColumnMap[col] == index)	
					result.assignDataField(tuple.getField(i), col);
					
			}				
		}		
		return result;
	}

}
