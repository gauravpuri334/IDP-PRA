package de.tuberlin.dima.minidb.qexec;

import java.io.IOException;

import de.tuberlin.dima.minidb.core.BigIntField;
import de.tuberlin.dima.minidb.core.DataTuple;
import de.tuberlin.dima.minidb.core.RID;
import de.tuberlin.dima.minidb.io.cache.PageExpiredException;
import de.tuberlin.dima.minidb.io.cache.PageFormatException;
import de.tuberlin.dima.minidb.io.index.BTreeIndex;
import de.tuberlin.dima.minidb.io.manager.BufferPoolException;
import de.tuberlin.dima.minidb.io.manager.BufferPoolManager;
import de.tuberlin.dima.minidb.io.tables.TablePage;
import de.tuberlin.dima.minidb.io.tables.TableResourceManager;

public class G5InsertOperator implements InsertOperator {

	private BufferPoolManager bufferPool;
	private TableResourceManager tableManager;
	private int resourceId;
	private BTreeIndex[] indexes;
	private int[] columnNumbers;
	private PhysicalPlanOperator child;
	
	private TablePage currentPage;

	public G5InsertOperator(BufferPoolManager bufferPool, TableResourceManager tableManager, int resourceId,
			BTreeIndex[] indexes, int[] columnNumbers, PhysicalPlanOperator child) {
		
			this.bufferPool = bufferPool;
			this.tableManager = tableManager;
			this.resourceId = resourceId;
			this.indexes = indexes;
			this.columnNumbers = columnNumbers;
			this.child = child;
	}

	@Override
	public void open(DataTuple correlatedTuple) throws QueryExecutionException {
		
		child.open(correlatedTuple);
		
		int currentPageNumber = tableManager.getLastDataPageNumber();
		
		try {
			currentPage = (TablePage) bufferPool.getPageAndPin(resourceId, currentPageNumber);
		} catch (BufferPoolException | IOException e) {
			throw new QueryExecutionException(e);
		}

	}

	@Override
	public DataTuple next() throws QueryExecutionException {
		
		DataTuple tuple;
		
		int inserted = 0;
		try {
				
			while( (tuple = child.next()) != null) {
				
				if(!currentPage.insertTuple(tuple)) {
					
					bufferPool.unpinPage(resourceId, currentPage.getPageNumber());
					currentPage = (TablePage) bufferPool.createNewPageAndPin(resourceId);
					currentPage.insertTuple(tuple);
					
					
				}
								
				for( int i = 0; i < indexes.length; i++) {
					
					RID rid = new RID(currentPage.getPageNumber(), currentPage.getNumRecordsOnPage());
					indexes[i].insertEntry(tuple.getField(columnNumbers[i]), rid);
				}
				
				inserted++;
			}
			
		} catch (PageExpiredException | PageFormatException | BufferPoolException | IOException e) {
			throw new QueryExecutionException(e);
		}
		
		
		DataTuple result = null;
		
		if (inserted > 0) {
			result = new DataTuple(1);
			result.assignDataField(new BigIntField(inserted), 0);
		}
		
		return result;
	}

	@Override
	public void close() throws QueryExecutionException {
		
		bufferPool.unpinPage(resourceId, currentPage.getPageNumber());
		child.close();

	}

}
