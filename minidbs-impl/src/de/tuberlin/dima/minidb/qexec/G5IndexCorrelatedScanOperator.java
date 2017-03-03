package de.tuberlin.dima.minidb.qexec;

import java.io.IOException;

import de.tuberlin.dima.minidb.core.DataField;
import de.tuberlin.dima.minidb.core.DataTuple;
import de.tuberlin.dima.minidb.core.RID;
import de.tuberlin.dima.minidb.io.cache.PageFormatException;
import de.tuberlin.dima.minidb.io.index.BTreeIndex;
import de.tuberlin.dima.minidb.io.index.IndexFormatCorruptException;
import de.tuberlin.dima.minidb.io.index.IndexResultIterator;

public class G5IndexCorrelatedScanOperator implements
		IndexCorrelatedLookupOperator {

	private BTreeIndex index;
	private int correlatedColumnIndex;
	private IndexResultIterator<RID> iterator;

	public G5IndexCorrelatedScanOperator(BTreeIndex index,	int correlatedColumnIndex) {
		
		this.index = index;
		this.correlatedColumnIndex = correlatedColumnIndex;
	}

	@Override
	public void open(DataTuple correlatedTuple) throws QueryExecutionException {
		
		try {
			
			DataField key = correlatedTuple.getField(correlatedColumnIndex);
			
			iterator = index.lookupRids(key);
			
			
		} catch (IndexFormatCorruptException | PageFormatException
				| IOException e) {
			throw new QueryExecutionException(e);
		}

	}

	@Override
	public DataTuple next() throws QueryExecutionException {

		
		RID rid;
		
		try {
			if(iterator.hasNext()) {
				rid = iterator.next();
				
				DataTuple result = new DataTuple(1);
				
				result.assignDataField( (DataField) rid, 0);
				
				return result;
			}
			
		} catch (IndexFormatCorruptException | IOException
				| PageFormatException e) {
			throw new QueryExecutionException(e);
		}
		
		return null;
	}

	@Override
	public void close() throws QueryExecutionException {
		
		iterator = null;
	}

}
