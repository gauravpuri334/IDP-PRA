package de.tuberlin.dima.minidb.qexec;


/**
 * Operator representing the access to an index in an uncorrelated fashion.
 * The index returns RIDs for the keys fulfilling some condition. That
 * condition is either the equality to some value:
 * <code> key = "Value" </code>
 * or the fact of being within the range between two keys:
 * <code> "val1" &lt;(=) key &lt;(=) "val2" </code>.
 * 
 * The interface is empty, it serves only as a marker. All methods
 * for the operator are already specified in the interface 
 * <tt>PhysicalPlanOperator</tt>.
 * 
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public interface IndexLookupOperator extends PhysicalPlanOperator
{

}
