package de.tuberlin.dima.minidb.qexec;

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;

import de.tuberlin.dima.minidb.core.DataField;
import de.tuberlin.dima.minidb.core.DataTuple;
import de.tuberlin.dima.minidb.core.DataType;
import de.tuberlin.dima.minidb.qexec.heap.ExternalTupleSequenceIterator;
import de.tuberlin.dima.minidb.qexec.heap.QueryHeap;
import de.tuberlin.dima.minidb.qexec.heap.QueryHeapException;

public class G5SortOperator implements SortOperator {

	private PhysicalPlanOperator child;
	private QueryHeap queryHeap;
	private DataType[] columnTypes;
	private int estimatedCardinality;
	
	private int heapId;
	private TupleComparator comparator;
	private DataTuple[] listHeads;
	private boolean internal;
	private ExternalTupleSequenceIterator[] externalSortedLists;
	
	
	private int internalPos, internalLength;
	private DataTuple[] sortArray;

	public G5SortOperator(PhysicalPlanOperator child, QueryHeap queryHeap,	DataType[] columnTypes, int estimatedCardinality,
			int[] sortColumns, boolean[] columnsAscending) {
		
				this.child = child;
				this.queryHeap = queryHeap;
				this.columnTypes = columnTypes;
				this.estimatedCardinality = estimatedCardinality;
				
				this.comparator = new TupleComparator(sortColumns, columnsAscending);

	}

	@Override
	public void open(DataTuple correlatedTuple) throws QueryExecutionException {
		
		child.open(correlatedTuple);
		
		try {
			heapId = queryHeap.reserveSortHeap(columnTypes, estimatedCardinality);
			sortArray = queryHeap.getSortArray(heapId);
			
			DataTuple tuple;
			int pos = 0;
			internal = true;
	
			while((tuple = child.next()) != null) {
				sortArray[pos] = tuple;
				pos++;
				
				
				// when the sort array is full : sort it !
				if(pos == queryHeap.getMaximalTuplesForInternalSort(heapId)) {
					Arrays.sort(sortArray, comparator);
					
					queryHeap.writeTupleSequencetoTemp(heapId, sortArray, queryHeap.getMaximalTuplesForInternalSort(heapId));
					internal = false;
					pos = 0;
				}
			}
			
			child.close();
			
			
			//At this point we have a partially filled array in memory (which might be the only list)

			
			
				Arrays.sort(sortArray, 0,  pos , comparator);
				
				internalPos = 0;
				internalLength = pos;
				
				

			if(!internal) {
				

				externalSortedLists = queryHeap.getExternalSortedLists(heapId);
				listHeads = new DataTuple[externalSortedLists.length];
				
				for (int i = 0; i < externalSortedLists.length; i++) {
					if (externalSortedLists[i].hasNext())
						listHeads[i] = externalSortedLists[i].next();
					else {
						listHeads[i] = null;
					}
				}	
			}
		
		} catch (QueryHeapException | IOException e) {
			throw new QueryExecutionException(e);
		}
	}

	@Override
	public DataTuple next() throws QueryExecutionException {

		if (internal) {
			
			return internalPos < internalLength ? sortArray[internalPos++] : null;
		
		} else {
			
			try {

				DataTuple minTuple = listHeads[0];
				int minPos = 0;

				for (int i = 1; i < listHeads.length; i++) {

					if (comparator.compare(listHeads[i], minTuple) < 0) {
						minTuple = listHeads[i];
						minPos = i;
	
					}	
				}

				if(internalPos < internalLength) {
					if (comparator.compare(sortArray[internalPos], minTuple) < 0) {
						minTuple = sortArray[internalPos];
						internalPos++;
						return minTuple;

					}
				}
			
					if(externalSortedLists[minPos].hasNext())
						listHeads[minPos] = externalSortedLists[minPos].next();
					else
						listHeads[minPos] = null;
	
				return minTuple;
						
			} catch (QueryHeapException | IOException e) {
				throw new QueryExecutionException(e);
			}

		}
	}

	@Override
	public void close() throws QueryExecutionException {

			queryHeap.releaseSortHeap(heapId);
			child.close();
	}
	
	
	
	private class TupleComparator implements Comparator<DataTuple> {

		private int[] sortColumns;
		private boolean[] columnsAscending;

		public TupleComparator(int[] sortColumns, boolean[] columnsAscending) {
			this.sortColumns = sortColumns;
			this.columnsAscending = columnsAscending;			
			
		}

		@Override
		public int compare(DataTuple tuple1, DataTuple tuple2) {
			
			
			if(tuple1 == tuple2) 
				return 0;
			
			if(tuple1 == null)
				return 1;
			
			if(tuple2 == null)
				return -1;
			
			DataField field1, field2;
			
			for(int i = 0; i < sortColumns.length; i++) {
				
				field1 = tuple1.getField(sortColumns[i]);
				field2 = tuple2.getField(sortColumns[i]);
			
				if(field1.compareTo(field2) < 0)
					return columnsAscending[i] ? -1 : 1;
				
				if(field1.compareTo(field2) > 0)
					return columnsAscending[i] ? 1 : -1;			
			}
					
			return 0;
		}		
	}

}
