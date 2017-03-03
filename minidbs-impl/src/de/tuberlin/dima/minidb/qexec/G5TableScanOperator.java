package de.tuberlin.dima.minidb.qexec;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

import de.tuberlin.dima.minidb.core.DataTuple;
import de.tuberlin.dima.minidb.io.cache.PageExpiredException;
import de.tuberlin.dima.minidb.io.manager.BufferPoolException;
import de.tuberlin.dima.minidb.io.manager.BufferPoolManager;
import de.tuberlin.dima.minidb.io.tables.PageTupleAccessException;
import de.tuberlin.dima.minidb.io.tables.TablePage;
import de.tuberlin.dima.minidb.io.tables.TableResourceManager;
import de.tuberlin.dima.minidb.io.tables.TupleIterator;

public class G5TableScanOperator implements TableScanOperator {

	private BufferPoolManager bufferPool;
	private TableResourceManager tableManager;
	private int resourceId;	
	private LowLevelPredicate[] predicate;
	private int prefetchWindowLength;
	private int currentPageNumber;
	private int[] producedColumnIndexes;
	private long colBitmap;
	private ArrayList<Integer> columnIndexes;

	private TablePage currentPage;
	private TupleIterator iterator;


	public G5TableScanOperator(BufferPoolManager bufferPool, TableResourceManager tableManager, int resourceId,
			int[] producedColumnIndexes, LowLevelPredicate[] predicate,	int prefetchWindowLength) {
		
				this.bufferPool = bufferPool;
				this.tableManager = tableManager;
				this.resourceId = resourceId;
				this.producedColumnIndexes = producedColumnIndexes;
				this.predicate = predicate;
				this.prefetchWindowLength = prefetchWindowLength;
				
				
				// Column bitmap used to tell which column to retrieve from the table
				// Ex : col 1,2,4 : 10110 = 22
				this.colBitmap = 0;
				
				// Array of the indexes retrieved -- redundant with the column bitmap but make the normalization easier
				// Ex : col 1, 2, 4 : [1, 2, 4]
				this.columnIndexes = new ArrayList<Integer>();
				
				
				for (int i = 0; i < producedColumnIndexes.length; i++) {
					
					if(!columnIndexes.contains(producedColumnIndexes[i])) {
						
						this.colBitmap += Math.pow(2, producedColumnIndexes[i]);
						columnIndexes.add(producedColumnIndexes[i]);						
					}
				}
				
				Collections.sort(columnIndexes);
	}

	@Override
	public void open(DataTuple correlatedTuple) throws QueryExecutionException {
		
		currentPageNumber = tableManager.getFirstDataPageNumber();
	
		try {
			
			currentPage = (TablePage) bufferPool.getPageAndPin(resourceId, currentPageNumber);
			
			iterator = currentPage.getIterator(predicate, columnIndexes.size(), colBitmap);
			
		
			int endPrefetchPageNumber = Math.min(currentPageNumber + prefetchWindowLength, tableManager.getLastDataPageNumber());
			
			
			bufferPool.prefetchPages(resourceId, currentPageNumber + 1, endPrefetchPageNumber);
		} catch (BufferPoolException | PageExpiredException | PageTupleAccessException | IOException e) {		
			throw new QueryExecutionException(e);
		}
	}

	@Override
	public DataTuple next() throws QueryExecutionException {		
		
		
		
		try {
			if(iterator.hasNext()) {
				return normalize(iterator.next());
				
			} else {
				while(currentPageNumber != tableManager.getLastDataPageNumber()) {
					
					currentPageNumber++;
					//System.out.print("[" + currentPageNumber + "]");
					currentPage = (TablePage) bufferPool.unpinAndGetPageAndPin(resourceId, currentPageNumber - 1, currentPageNumber);
					iterator = currentPage.getIterator(predicate, columnIndexes.size(), colBitmap);

					if(iterator.hasNext())
						return normalize(iterator.next());					
				}
			}
			
			
			
			
			
		} catch (BufferPoolException | IOException | PageTupleAccessException e) {
			e.printStackTrace();
			throw new QueryExecutionException(e);
		} 
		
		return null;
	}

	@Override
	public void close() throws QueryExecutionException {
		
		iterator = null;
		
		bufferPool.unpinPage(resourceId, currentPageNumber);
	}
	
	
	
	private DataTuple normalize(DataTuple tuple) {
		
		/*
		 * The input tuple follows the indexes in columnIndexes 
		 * and has to be mapped to the result tuple following producedColumnIndexes
		 * 
		 */
		DataTuple result = new DataTuple(producedColumnIndexes.length);

		/* Ex : columnIndexes : [1,2,4]
		 * 		producedColumnIndexes : [2, 4, 4, 1]
		 * 
		 */
		
		// Loop through columnIndexes
		for(int i = 0; i < columnIndexes.size(); i++) {
			
			int index = columnIndexes.get(i);
			
			// Loop through producedColumnIndexes to find matching columns
			for(int col = 0; col < producedColumnIndexes.length; col ++) {
					
				// Fill result tuple according to producedColumnIndexes
				if(producedColumnIndexes[col] == index)	
					result.assignDataField(tuple.getField(i), col);
					
			}				
		}	
		return result;
	}
}
