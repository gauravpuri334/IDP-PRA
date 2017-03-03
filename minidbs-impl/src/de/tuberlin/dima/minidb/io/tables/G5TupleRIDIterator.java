package de.tuberlin.dima.minidb.io.tables;

import de.tuberlin.dima.minidb.core.DataTuple;
import de.tuberlin.dima.minidb.core.RID;
import de.tuberlin.dima.minidb.qexec.LowLevelPredicate;
import de.tuberlin.dima.minidb.util.Pair;

public class G5TupleRIDIterator implements TupleRIDIterator {

	int position;
	int numCols;
	long columnBitmap;
	LowLevelPredicate[] preds;
	
	G5TablePage page;
	
	public G5TupleRIDIterator(G5TablePage page, int numCols, long columnBitmap) {
		
		this.page = page;
		this.numCols = numCols;
		this.columnBitmap = columnBitmap;
		
		position = -1;
		
	}
	
	public G5TupleRIDIterator(G5TablePage page, LowLevelPredicate[] preds, int numCols, long columnBitmap) {
		
		this.page = page;
		this.numCols = numCols;
		this.columnBitmap = columnBitmap;
		this.preds = preds;
		position = -1;
		
	}

	@Override
	public boolean hasNext() throws PageTupleAccessException {
		
		int numRecords = page.getNumRecordsOnPage();
		
		
		
		for (int i = position +1; i < numRecords; i++){
			
			if ((page.getTombstone(i) & 0x1) == 0) {
				
				if (preds == null)		
					return true;	
				
				if (page.getDataTuple(preds, i, columnBitmap, numCols) != null) 					
					return true;

			}
		}
		return false;
	}

	
	
	
	@Override
	public Pair<DataTuple, RID> next() throws PageTupleAccessException {
		
		int numRecords = page.getNumRecordsOnPage();
		
		Pair<DataTuple, RID> result = new Pair<DataTuple, RID>();
		

		
		for (int i = position +1; i < numRecords; i++){
			
			if ((page.getTombstone(i) & 0x1) == 0) {
				if (preds == null) {
					position = i;
					result.setFirst(page.getDataTuple( i, columnBitmap, numCols));
					result.setSecond(new RID(page.getPageNumber(), i));
					
					return result;
				}
				
				
				DataTuple tuple = page.getDataTuple(preds, i, columnBitmap, numCols);
				
				if (tuple != null) {
					position = i;
					result.setFirst(tuple);
					result.setSecond(new RID(page.getPageNumber(), i));
					
					return result;	
				}
			}
		}
		return null;
	}

}

