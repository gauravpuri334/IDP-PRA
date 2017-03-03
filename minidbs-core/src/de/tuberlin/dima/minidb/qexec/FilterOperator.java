package de.tuberlin.dima.minidb.qexec;


import de.tuberlin.dima.minidb.qexec.predicate.LocalPredicate;


/**
 * Interface describing a FILTER operator that applies a simple predicate on the incoming
 * tuples.
 * 
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public interface FilterOperator extends PhysicalPlanOperator
{
	/**
	 * Gets the predicate applied by this filter.
	 * 
	 * @return This filter's predicate.
	 */
	public LocalPredicate getPredicate();
}