package de.tuberlin.dima.minidb.qexec;

import java.io.IOException;

import de.tuberlin.dima.minidb.core.DataField;
import de.tuberlin.dima.minidb.core.DataTuple;
import de.tuberlin.dima.minidb.core.RID;
import de.tuberlin.dima.minidb.io.cache.PageFormatException;
import de.tuberlin.dima.minidb.io.index.BTreeIndex;
import de.tuberlin.dima.minidb.io.index.IndexFormatCorruptException;
import de.tuberlin.dima.minidb.io.index.IndexResultIterator;

public class G5IndexLookupOperator implements IndexLookupOperator {

	private BTreeIndex index;
	private DataField equalityLiteral;
	private DataField lowerBound;
	private boolean lowerIncluded;
	private DataField upperBound;
	private boolean upperIncluded;
	
	
	private IndexResultIterator<RID> iterator;

	public G5IndexLookupOperator(BTreeIndex index, DataField equalityLiteral) {
		
		this.index = index;
		this.equalityLiteral = equalityLiteral;
	}

	public G5IndexLookupOperator(BTreeIndex index, DataField lowerBound, boolean lowerIncluded,
													DataField upperBound, boolean upperIncluded) {
		this.equalityLiteral = null;
		this.index = index;
		this.lowerBound = lowerBound;
		this.lowerIncluded = lowerIncluded;
		this.upperBound = upperBound;
		this.upperIncluded = upperIncluded;
	}

	@Override
	public void open(DataTuple correlatedTuple) throws QueryExecutionException {

		
		try {
			
			if (this.equalityLiteral != null) {
				iterator = index.lookupRids(equalityLiteral);
			} else {
				iterator = index.lookupRids(lowerBound, upperBound, lowerIncluded, upperIncluded);
			}
			
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
