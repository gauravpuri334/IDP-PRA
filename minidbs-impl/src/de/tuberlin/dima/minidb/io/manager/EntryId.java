package de.tuberlin.dima.minidb.io.manager;


/**
 * Helper class to track resource ids and page numbers of cache entries.
 */
public class EntryId
{
	private int id;
	private int pagenumber;
	
	public EntryId(int id, int pagenumber)
    {
        this.id = id;
        this.pagenumber = pagenumber;
    }

	public int getPageNumber() 
	{
		return this.pagenumber;
	}


	public int getResourceId() 
	{
		return this.id;
	}


	@Override
    public int hashCode()
    {
        final int prime = 31;
        int result = 1;
        result += prime * result + this.id;
        result += prime * result + this.pagenumber;
        return result;
    }

	@Override
    public boolean equals(Object obj)
    {
        if (this == obj)
	        return true;
        if (obj == null)
	        return false;
        if (getClass() != obj.getClass())
	        return false;
        EntryId other = (EntryId) obj;
        if (this.id != other.id)
	        return false;
        if (this.pagenumber != other.pagenumber)
	        return false;
        return true;
    }		
}

