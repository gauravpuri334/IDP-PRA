package de.tuberlin.dima.minidb.qexec;

import java.io.IOException;

import de.tuberlin.dima.minidb.core.BigIntField;
import de.tuberlin.dima.minidb.core.DataTuple;
import de.tuberlin.dima.minidb.core.RID;
import de.tuberlin.dima.minidb.io.cache.PageExpiredException;
import de.tuberlin.dima.minidb.io.manager.BufferPoolException;
import de.tuberlin.dima.minidb.io.manager.BufferPoolManager;
import de.tuberlin.dima.minidb.io.tables.PageTupleAccessException;
import de.tuberlin.dima.minidb.io.tables.TablePage;

public class G5DeleteOperator implements DeleteOperator {

	private BufferPoolManager bufferPool;
	private int resourceId;
	private PhysicalPlanOperator child;

	public G5DeleteOperator(BufferPoolManager bufferPool, int resourceId, PhysicalPlanOperator child) {
		
		this.bufferPool = bufferPool;
		this.resourceId = resourceId;
		this.child = child;
	}

	@Override
	public void open(DataTuple correlatedTuple) throws QueryExecutionException {
		
		child.open(correlatedTuple);

	}

	@Override
	public DataTuple next() throws QueryExecutionException {
		
		DataTuple tuple;
		
		int deleted = 0;
		try {
				
			while( (tuple = child.next()) != null) {
				
				RID rid = (RID) tuple.getField(0);
				
				TablePage page = (TablePage) bufferPool.getPageAndPin(resourceId, rid.getPageIndex());
				
				page.deleteTuple(rid.getTupleIndex());
				
				deleted++;
				
			}
		} catch (PageExpiredException | BufferPoolException | IOException | PageTupleAccessException e) {
			throw new QueryExecutionException(e);
		}
		
		
		DataTuple result = null;
		
		if (deleted > 0) {
			result = new DataTuple(1);
			result.assignDataField(new BigIntField(deleted), 0);
		}
		
		return result;
	}

	@Override
	public void close() throws QueryExecutionException {

		child.close();
	}

}
