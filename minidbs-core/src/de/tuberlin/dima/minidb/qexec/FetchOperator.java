package de.tuberlin.dima.minidb.qexec;


/**
 * Interface describing a physical plan operator that performs the FETCH operation.
 * The operator receives tuples containing a single RID field and used this RID
 * to fetch tuples from a table, which the operator then returns.
 *
 * This interface is empty and serves only as a marker. All relevant methods
 * are specified in the interface <tt>PhysicalPlanOperator</tt>.
 * 
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public interface FetchOperator extends PhysicalPlanOperator
{
}
