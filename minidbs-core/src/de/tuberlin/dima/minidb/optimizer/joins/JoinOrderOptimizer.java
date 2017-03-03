package de.tuberlin.dima.minidb.optimizer.joins;


import de.tuberlin.dima.minidb.optimizer.OptimizerPlanOperator;
import de.tuberlin.dima.minidb.semantics.JoinGraphEdge;
import de.tuberlin.dima.minidb.semantics.Relation;


/**
 * Interface for the class implementing the algorithm to pick the best join order for the
 * query execution plan.
 * 
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public interface JoinOrderOptimizer
{
	/**
	 * Finds the best join order for a query represented as the given graph.
	 * 
	 * The input is an array of <tt>TableScanPlanOperator</tt>, which is sorted
	 * by the <i>Scan-Id</i> of the operator, to establish an internal ordering, which
	 * can be exploited to more efficiently implement some algorithms.
	 * 
	 * Similarly, every <tt>JoinGraphEdge</tt> created such that the table scan with
	 * the lower <i>Scan-Id</i> is always on the left side. The array of the edges is
	 * sorted primarily after the <i>Scan-Id</i> of its left node and secondarily after
	 * the <i>Scan-Id</i> of its right node. 
	 * 
	 * @param tables The tables, forming the nodes in the join graph.
	 * @param joins The joins, forming the edges in the join graph. 
	 * @return The abstract best plan, restricted to the join operators.
	 */
	public OptimizerPlanOperator findBestJoinOrder(Relation[] relations, JoinGraphEdge[] joins);
	
}
