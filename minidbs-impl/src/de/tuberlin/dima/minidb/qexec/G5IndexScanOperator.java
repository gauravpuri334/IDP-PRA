package de.tuberlin.dima.minidb.qexec;

import java.io.IOException;

import de.tuberlin.dima.minidb.core.DataField;
import de.tuberlin.dima.minidb.core.DataTuple;
import de.tuberlin.dima.minidb.io.cache.PageFormatException;
import de.tuberlin.dima.minidb.io.index.BTreeIndex;
import de.tuberlin.dima.minidb.io.index.IndexFormatCorruptException;
import de.tuberlin.dima.minidb.io.index.IndexResultIterator;

public class G5IndexScanOperator implements IndexScanOperator{
	

	private BTreeIndex index;
	private DataField startKey;
	private DataField stopKey;
	private boolean startKeyIncluded;
	private boolean stopKeyIncluded;
	
	private IndexResultIterator<DataField> iterator;

	public G5IndexScanOperator(BTreeIndex index, DataField startKey, DataField stopKey,
				boolean startKeyIncluded, boolean stopKeyIncluded) {
		
					this.index = index;
					this.startKey = startKey;
					this.stopKey = stopKey;
					this.startKeyIncluded = startKeyIncluded;
					this.stopKeyIncluded = stopKeyIncluded;
	}
	

	@Override
	public void open(DataTuple correlatedTuple) throws QueryExecutionException {
		
		
			
		try {
			iterator = index.lookupKeys(startKey, stopKey, startKeyIncluded, stopKeyIncluded);
			
		} catch (IndexFormatCorruptException | PageFormatException
				| IOException e) {
			throw new QueryExecutionException(e);
		}

	
	}

	@Override
	public DataTuple next() throws QueryExecutionException {
		
		DataField key;
		

			try {
				if(iterator.hasNext()) {
					key = iterator.next();
					
					DataTuple result = new DataTuple(1);
					
					result.assignDataField(key, 0);
					
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