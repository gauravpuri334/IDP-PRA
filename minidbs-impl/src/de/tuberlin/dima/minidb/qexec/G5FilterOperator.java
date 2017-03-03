package de.tuberlin.dima.minidb.qexec;

import de.tuberlin.dima.minidb.core.DataTuple;
import de.tuberlin.dima.minidb.qexec.predicate.LocalPredicate;

public class G5FilterOperator implements FilterOperator {

	private PhysicalPlanOperator child;
	private LocalPredicate predicate;

	public G5FilterOperator(PhysicalPlanOperator child, LocalPredicate predicate) {
		this.child = child;
		this.predicate = predicate;
	}

	@Override
	public void open(DataTuple correlatedTuple) throws QueryExecutionException {
		child.open(correlatedTuple);

	}

	@Override
	public DataTuple next() throws QueryExecutionException {
		
		DataTuple childTuple;
		
		while ((childTuple = child.next()) != null) {
			if (predicate.evaluate(childTuple))
				return childTuple;
		}
		
		return null;
	}

	@Override
	public void close() throws QueryExecutionException {

		child.close();
	}

	@Override
	public LocalPredicate getPredicate() {
		
		return this.predicate;
	}

}
