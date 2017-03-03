package de.tuberlin.dima.minidb.qexec;

import de.tuberlin.dima.minidb.core.DataTuple;
import de.tuberlin.dima.minidb.qexec.predicate.JoinPredicate;

public class G5FilterCorrelatedOperator implements FilterCorrelatedOperator {

	private PhysicalPlanOperator child;
	private JoinPredicate correlatedPredicate;
	private DataTuple correlatedTuple;

	public G5FilterCorrelatedOperator(PhysicalPlanOperator child, JoinPredicate correlatedPredicate) {
		
		this.child = child;
		this.correlatedPredicate = correlatedPredicate;
	}

	@Override
	public void open(DataTuple correlatedTuple) throws QueryExecutionException {
		
		this.correlatedTuple = correlatedTuple;
		child.open(null);
		

	}

	@Override
	public DataTuple next() throws QueryExecutionException {
		DataTuple childTuple;
		
		while ((childTuple = child.next()) != null) {
			if (correlatedPredicate.evaluate(childTuple, correlatedTuple))
				return childTuple;
		}
		
		return null;
	}

	@Override
	public void close() throws QueryExecutionException {
		
		child.close();
	}

	@Override
	public JoinPredicate getCorrelatedPredicate() {
		
		return this.correlatedPredicate;
	}

}
