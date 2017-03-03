package de.tuberlin.dima.minidb.qexec;

import de.tuberlin.dima.minidb.core.DataTuple;
import de.tuberlin.dima.minidb.qexec.predicate.JoinPredicate;

public class G5NestedLoopJoinOperator implements NestedLoopJoinOperator {

	private PhysicalPlanOperator outerChild;
	private PhysicalPlanOperator innerChild;
	private JoinPredicate joinPredicate;
	private int[] columnMapOuterTuple;
	private int[] columnMapInnerTuple;
	
	private DataTuple outerTuple;

	public G5NestedLoopJoinOperator(PhysicalPlanOperator outerChild, PhysicalPlanOperator innerChild,
			JoinPredicate joinPredicate, int[] columnMapOuterTuple, int[] columnMapInnerTuple) {
			
				this.outerChild = outerChild;
				this.innerChild = innerChild;
				this.joinPredicate = joinPredicate;
				this.columnMapOuterTuple = columnMapOuterTuple;
				this.columnMapInnerTuple = columnMapInnerTuple;
	}

	@Override
	public void open(DataTuple correlatedTuple) throws QueryExecutionException {
		
		outerChild.open(correlatedTuple);
		
		outerTuple = outerChild.next();
		
		innerChild.open(outerTuple);
		

	}

	@Override
	public DataTuple next() throws QueryExecutionException {
		
		
		DataTuple innerTuple, result;
		
		do {
		
			while((innerTuple = innerChild.next()) == null) {
				
				innerChild.close();
				
				if ((outerTuple = outerChild.next()) == null)
					return null;
	
				innerChild.open(outerTuple);
				
			}
			
		} while (joinPredicate != null && !joinPredicate.evaluate(outerTuple, innerTuple));
		
		
		
		
		
		
		int resultSize = columnMapOuterTuple.length;
		
		result = new DataTuple(resultSize);
		
		for (int i = 0; i < resultSize; i++) {
			
			int pos = columnMapOuterTuple[i];
			
			if (pos != -1)
				result.assignDataField(outerTuple.getField(pos), i);
			
			pos = columnMapInnerTuple[i];
			
			if (pos != -1)
				result.assignDataField(innerTuple.getField(pos), i);
		}
		
		return result;
	}

	@Override
	public void close() throws QueryExecutionException {
		
		outerChild.close();

	}

	@Override
	public PhysicalPlanOperator getOuterChild() {
		
		return this.outerChild;
	}

	@Override
	public PhysicalPlanOperator getInnerChild() {
		
		return this.innerChild;
	}

	@Override
	public JoinPredicate getJoinPredicate() {
		
		return this.joinPredicate;
	}

}
