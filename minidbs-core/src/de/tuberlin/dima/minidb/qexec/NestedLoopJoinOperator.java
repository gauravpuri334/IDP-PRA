package de.tuberlin.dima.minidb.qexec;


import de.tuberlin.dima.minidb.qexec.predicate.JoinPredicate;


/**
 * Interface representing a nested loop join operator. For every tuple on the outer
 * side, the operator opens the plan on the inner side correlated to that tuple 
 * and takes all of the tuples, evaluating them against the tuple from the
 * outer side.
 * 
 * The join optionally evaluates a join predicate. Optionally means here that
 * in some cases, no join predicate exists (Cartesian Join), or the join predicate
 * is already represented through the correlation (such as that the inner side in
 * known to produce only tuples that match the current outer tuple.)
 * 
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public interface NestedLoopJoinOperator extends PhysicalPlanOperator
{
	/**
	 * Gets the operator rooting the outer sub-plan.
	 * 
	 * @return The outer child operator.
	 */
	public PhysicalPlanOperator getOuterChild();
	
	/**
	 * Gets the operator rooting the inner sub-plan.
	 * 
	 * @return The inner child operator.
	 */
	public PhysicalPlanOperator getInnerChild();
	
	/**
	 * Gets the join predicate applied in this join.
	 * 
	 * @return This joins join predicate.
	 */
	public JoinPredicate getJoinPredicate();
	
}
