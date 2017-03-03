package de.tuberlin.dima.minidb.qexec;


/**
 * Operator representing the access to an index in a correlated fashion. The
 * index evaluates only equality predicates against one column of the current
 * correlated tuple.
 * 
 * This interface is empty and serves only as a marker. The methods are all
 * specified in <tt>PhysicalPlanOperator</tt>.
 * 
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public interface IndexCorrelatedLookupOperator extends PhysicalPlanOperator
{
	
}
