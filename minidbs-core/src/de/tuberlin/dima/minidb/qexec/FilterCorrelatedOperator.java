package de.tuberlin.dima.minidb.qexec;


import de.tuberlin.dima.minidb.qexec.predicate.JoinPredicate;


/**
 * Interface describing a FILTER operator that filters its tuples by comparing them
 * against the current correlated tuple.
 * 
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public interface FilterCorrelatedOperator extends PhysicalPlanOperator
{
	/**
	 * Gets the predicate applied by this filter.
	 * 
	 * @return This filter's predicate.
	 */
	public JoinPredicate getCorrelatedPredicate();
}