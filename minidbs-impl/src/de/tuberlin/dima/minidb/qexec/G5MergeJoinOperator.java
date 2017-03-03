package de.tuberlin.dima.minidb.qexec;

import java.util.ArrayList;

import de.tuberlin.dima.minidb.core.DataField;
import de.tuberlin.dima.minidb.core.DataTuple;

public class G5MergeJoinOperator implements MergeJoinOperator {

	private PhysicalPlanOperator leftChild;
	private PhysicalPlanOperator rightChild;
	private int[] leftJoinColumns;
	private int[] rightJoinColumns;
	private int[] columnMapLeftTuple;
	private int[] columnMapRightTuple;
	
	private DataTuple currentLeft, currentRight, nextLeft, nextRight;
	
	private ArrayList<DataTuple> rightList, leftList;
	private int posRightList, posLeftList;


	public G5MergeJoinOperator(PhysicalPlanOperator leftChild,	PhysicalPlanOperator rightChild, int[] leftJoinColumns,
			int[] rightJoinColumns, int[] columnMapLeftTuple, int[] columnMapRightTuple) {
		
				this.leftChild = leftChild;
				this.rightChild = rightChild;
				this.leftJoinColumns = leftJoinColumns;
				this.rightJoinColumns = rightJoinColumns;
				this.columnMapLeftTuple = columnMapLeftTuple;
				this.columnMapRightTuple = columnMapRightTuple;
	}

	@Override
	public void open(DataTuple correlatedTuple) throws QueryExecutionException {
		
		leftChild.open(correlatedTuple);
		rightChild.open(correlatedTuple);
		
		currentLeft = leftChild.next();
		currentRight = rightChild.next();
		
		nextLeft = leftChild.next();
		nextRight = rightChild.next();
		
		leftList = new ArrayList<DataTuple>();
		posLeftList = 0;
				
		rightList = new ArrayList<DataTuple>();
		posRightList = 0;


	}

	@Override
	public DataTuple next() throws QueryExecutionException {
		
		
		
		if (posLeftList < leftList.size()) {
			
			posRightList++;
			
			if (posRightList < rightList.size()) {

				return concatenate(leftList.get(posLeftList), rightList.get(posRightList));
			} else {
				posRightList = 0;
				posLeftList++;
				
				if (posLeftList < leftList.size())
					return concatenate(leftList.get(posLeftList), rightList.get(posRightList));
			}
				
		}
		
		while (currentLeft != null && currentRight != null) {
			
			DataTuple result;
			
			
			boolean goLeft = compare(currentLeft, nextRight, true, false) < 0;
			boolean goRight = compare(currentRight, nextLeft, false, true) < 0;
			
			 
			// Multiple time the same key on both side : cartesian product !
			if (!goRight && ! goLeft) {
			
				
				leftList = new ArrayList<DataTuple>();
				posLeftList = 0;						
				rightList = new ArrayList<DataTuple>();
				posRightList = 0;
				
				leftList.add(nextLeft);
				
				while (compare(currentLeft, nextLeft, true, true) == 0) {
					leftList.add(currentLeft);
					currentLeft = leftChild.next();
				}
				
				nextLeft = leftChild.next();
				
				
				rightList.add(nextRight);
				
				while (compare(currentRight, nextRight, false, false) == 0) {
					rightList.add(currentRight);
					currentRight = rightChild.next();
				}
				nextRight = rightChild.next();
				
				
				// return first pair

				return concatenate(leftList.get(0), rightList.get(0));
				 
			} else {
				 
				result =  concatenate(currentLeft, currentRight);
				
				boolean isMatching = compare(currentLeft, currentRight, true, false) == 0;
				
				if (goLeft) {
					currentLeft = nextLeft;
					nextLeft = leftChild.next();
				}
						 
				if (goRight) {
					currentRight = nextRight;
					nextRight = rightChild.next();
				}
						 
				if (isMatching) {

					return result;
				}
			}		
		}
		
		
		return null;
	}

	@Override
	public void close() throws QueryExecutionException {
		
		leftChild.close();
		rightChild.close();

	}
	
	
	private int compare(DataTuple leftTuple, DataTuple rightTuple, boolean isFirstLeft, boolean isSecondLeft) {
		
		DataField leftKey, rightKey;
		int compare = 0;
		
		
		if (leftTuple == null || rightTuple == null)
			return -1;
		
		int[] leftColumns = isFirstLeft ? leftJoinColumns : rightJoinColumns;
		int[] rightColumns = isSecondLeft ? leftJoinColumns : rightJoinColumns;
		
		for( int i = 0; i < leftColumns.length; i++) {
			leftKey = leftTuple.getField(leftColumns[i]);
			rightKey = rightTuple.getField(rightColumns[i]);
			
			compare = leftKey.compareTo(rightKey);
			
			if (compare != 0)
				break;
		}
		
		return compare;
	}

	private DataTuple concatenate(DataTuple leftTuple, DataTuple rightTuple) {
		
		DataTuple outputTuple = new DataTuple(columnMapLeftTuple.length);
		
		for (int i = 0; i < columnMapLeftTuple.length; i++) {
			int index = columnMapLeftTuple[i];
			if (index != -1) {
				outputTuple.assignDataField(leftTuple.getField(index), i);
			}
			
			index = columnMapRightTuple[i];
			if (index != -1) {
				outputTuple.assignDataField(rightTuple.getField(index), i);
			}
			
			
		}

		return outputTuple;
	}

}
