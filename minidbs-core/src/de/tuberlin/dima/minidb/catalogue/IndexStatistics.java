package de.tuberlin.dima.minidb.catalogue;


/**
 * A simple bean that describes the statistics of an index. A plain bean
 * assumes the default values for the index which are:
 * <ul>
 *   <li>Tree-Depth: 3</li>
 *   <li>Number of Leaf Pages: 500 pages</li>
 * </ul>
 * 
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public class IndexStatistics
{
	/**
	 * The number of levels that the B-Tree has (including root and leafs).
	 */
	private int treeDepth;
	
	/**
	 * The number of leaf pages in the B-Tree.
	 */
	private int numberOfLeafs;
	
	
	
	/**
	 * Creates a new IndexStatistics bean with the default values.
	 */
	public IndexStatistics()
	{
		this.treeDepth = 3;
		this.numberOfLeafs = 500;
	}



	/**
     * Gets the treeDepth from this IndexStatistics.
     *
     * @return The treeDepth.
     */
    public int getTreeDepth()
    {
    	return this.treeDepth;
    }

	/**
     * Gets the numberOfLeafs from this IndexStatistics.
     *
     * @return The numberOfLeafs.
     */
    public int getNumberOfLeafs()
    {
    	return this.numberOfLeafs;
    }

	/**
     * Sets the treeDepth for this IndexStatistics.
     *
     * @param treeDepth The treeDepth to set.
     */
    public void setTreeDepth(int treeDepth)
    {
    	this.treeDepth = treeDepth;
    }

	/**
     * Sets the numberOfLeafs for this IndexStatistics.
     *
     * @param numberOfLeafs The numberOfLeafs to set.
     */
    public void setNumberOfLeafs(int numberOfLeafs)
    {
    	this.numberOfLeafs = numberOfLeafs;
    }
    
    /* (non-Javadoc)
     * @see java.lang.Object#toString()
     */
    @Override
	public String toString()
    {
    	return "Tree levels: " + this.treeDepth + ", Number of Leafs: " + this.numberOfLeafs;
    }
}
