package de.tuberlin.dima.minidb.io.cache;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map.Entry;
import java.util.Set;

public class G5PageCache implements PageCache {
	
	
	private PageSize pageSize;
	
	private int numPages;
	private int targetSize;
	private boolean pagesExpelled;

	

	LinkedHashMap<EntryId, CacheEntry> recent;
	LinkedHashMap<EntryId, CacheEntry> frequent;
	
	Set<EntryId> bottomRecent;
	Set<EntryId> bottomFrequent;
	
	
	public G5PageCache(PageSize pageSize, int numPages) {
		
		this.pageSize = pageSize;
		this.numPages = numPages;
		this.pagesExpelled = false;
		
		
		targetSize = numPages/2;
		
		
		recent = new LinkedHashMap<EntryId, CacheEntry>();
		frequent = new LinkedHashMap<EntryId, CacheEntry>();
			
		bottomRecent = new HashSet<EntryId>();
		bottomFrequent = new HashSet<EntryId>();
		
	}
	
	
	
	
	@Override
	public CacheableData getPage(int resourceId, int pageNumber) {

		EntryId id = new EntryId(resourceId, pageNumber);
		
		CacheEntry page = recent.remove(id);
		
		// Page found in the recent list 
		if (page != null && !page.isExpelled()) {
			
			page.hit();
			
			if (page.gethit() > 1) {
				// move to frequent if already hit before
				frequent.put(id, page);
			} else {
				// Or put on top (MRU side) if not
				recent.put(id, page);
			}
			return page.getPage();
		}	
		
			
		page = frequent.remove(id);	
		
		// Page found in the recent list 
		if (page != null && !page.isExpelled()){	
			
			page.hit();
			// Put on top (MRU side)
			frequent.put(id, page);
			return page.getPage();
		}
		

		
		return null;
	}

	@Override
	public CacheableData getPageAndPin(int resourceId, int pageNumber) {

		
		EntryId id = new EntryId(resourceId, pageNumber);
		
		CacheEntry page = recent.remove(id);
		
		// Page found in the recent list 
		if (page != null && !page.isExpelled()) {
			
			page.hit();
			page.pin();
			
			if (page.gethit() > 1) {
				// move to frequent if already hit before
				frequent.put(id, page);
			} else {
				// Or put on top (MRU side) if not
				recent.put(id, page);
			}
			return page.getPage();
		}	
			
		page = frequent.remove(id);	
			
		// Page found in the recent list 
		if (page != null && !page.isExpelled()){	
			
			page.hit();
			page.pin();
			
			// Put on top (MRU side)
			frequent.put(id, page);
			return page.getPage();
		}
		return null;
	}

	
	@Override
	public EvictedCacheEntry addPage(CacheableData newPage, int resourceId)
			throws CachePinnedException, DuplicateCacheEntryException {

		return addPage(newPage, resourceId, false);
	
	}

	@Override
	public EvictedCacheEntry addPageAndPin(CacheableData newPage, int resourceId)
			throws CachePinnedException, DuplicateCacheEntryException {
	
		return addPage(newPage, resourceId, true);
	}
	
	
	
	private EvictedCacheEntry addPage(CacheableData newPage, int resourceId, boolean pinned)
			throws CachePinnedException, DuplicateCacheEntryException {
		
	

	
		EntryId id = new EntryId(resourceId, newPage.getPageNumber());	
		

		
		// Page shouldn't already be in the cache
		if (recent.containsKey(id) || frequent.containsKey(id)) 
			throw new DuplicateCacheEntryException(resourceId, newPage.getPageNumber());
		
		

		

		
		// Case 1 : page is in recent bottom list : add it directly to frequent list
		if (bottomRecent.remove(id)) {
		


		  int b1 = bottomRecent.size();
		  int b2 = bottomFrequent.size();
		 	
		  int delta = ( b1 >= b2 || b1 == 0 ? 1 : b2/b1);
		 	
		 	
		 	targetSize  = Math.min(targetSize + delta, numPages/2);


			EvictedCacheEntry evicted = evictPage();
			
			
			frequent.put(id, new CacheEntry(newPage, 1, pinned));

			
			
		
			return evicted;
			
		// Case 2 : page is in frequent bottom list : add it directly to frequent list
		} else if (bottomFrequent.remove(id)) {
			
		  
		 int b1 = bottomRecent.size();
		  int b2 = bottomFrequent.size();
		  
		  int delta = ( b2 >= b1 || b2 == 0 ? 1 : b1/b2);
		 
		 	
		  targetSize  = Math.min(targetSize - delta, 0);
		 


			EvictedCacheEntry evicted = evictPage();

			frequent.put(id, new CacheEntry(newPage, 1, pinned));
			
			return evicted;
			 
		// Case 3 : Page was not used recently : add it to recent list (without hit)
		} else {
			EvictedCacheEntry evicted = evictPage();
			recent.put(id, new CacheEntry(newPage, 0, pinned));	
			
			return evicted;
		}
	}

	@Override
	public void unpinPage(int resourceId, int pageNumber) {
		
			
		EntryId id = new EntryId(resourceId, pageNumber);
		
		CacheEntry page = recent.get(id);
		
		if (page != null) {			
			page.unpin();
		} else {
			
			page = frequent.get(id);	
			
			if (page != null){				
				page.unpin();
			}
		}	
	}

	@Override
	public CacheableData[] getAllPagesForResource(int resourceId) {
		
		ArrayList<CacheableData> results = new ArrayList<CacheableData>();
		
		Iterator<Entry<EntryId, CacheEntry>> it = recent.entrySet().iterator();;
		
		while (it.hasNext())
		{
			Entry<EntryId, CacheEntry> entry = it.next();
		  
			if (entry.getKey().getResourceId() == resourceId) {
				results.add(entry.getValue().getPage());
			}
		}
		
		it = frequent.entrySet().iterator();;
		
		while (it.hasNext())
		{
			Entry<EntryId, CacheEntry> entry = it.next();
		  
			if (entry.getKey().getResourceId() == resourceId) {
				results.add(entry.getValue().getPage());
			}
		}
		
		CacheableData[] result = new CacheableData[results.size()];
		results.toArray(result);

		return result;
	}

	@Override
	public void expellAllPagesForResource(int resourceId) {
		
		Iterator<Entry<EntryId, CacheEntry>> it = recent.entrySet().iterator();
		
		pagesExpelled = true;
		
		while (it.hasNext())
		{
			Entry<EntryId, CacheEntry> entry = it.next();
		  
			if (entry.getKey().getResourceId() == resourceId) {
				entry.getValue().expell();
			}
		}
		
		it = frequent.entrySet().iterator();;
		
		while (it.hasNext())
		{
			Entry<EntryId, CacheEntry> entry = it.next();
		  
			if (entry.getKey().getResourceId() == resourceId) {
			  entry.getValue().expell();
			}
		}
	}

	@Override
	public int getCapacity() {
		return numPages;
	}

	@Override
	public void unpinAllPages() {
		
		
		
		
		Iterator<CacheEntry> it = recent.values().iterator();;
		
		
		while (it.hasNext())
		{
			CacheEntry entry = it.next();
			entry.unpinAll();		  
		}
		
		it = frequent.values().iterator();;
	
		while (it.hasNext())
		{
			CacheEntry entry = it.next();
			entry.unpinAll();	
		}		
	}
	
	
	
	public EvictedCacheEntry evictPage() throws CachePinnedException{

		Iterator<Entry<EntryId, CacheEntry>> it;
		boolean fromFrequent;
		
		
		
		if ((frequent.size() + recent.size()) < numPages)  
			return new EvictedCacheEntry(new byte[pageSize.getNumberOfBytes()]);
		
		
		
		
		
		// Check first if expelled pages are available

		if (pagesExpelled) {
			it = recent.entrySet().iterator();	
	
			while (it.hasNext())
			{
				Entry<EntryId, CacheEntry> entry = it.next();
			  
				if (entry.getValue().isExpelled()) {
					CacheableData page = entry.getValue().getPage();
					it.remove();
					return new EvictedCacheEntry(page.getBuffer(), page, entry.getKey().getResourceId());
				}		  
			}
			
			it = frequent.entrySet().iterator();	
	
			while (it.hasNext())
			{
				Entry<EntryId, CacheEntry> entry = it.next();
			  
				if (entry.getValue().isExpelled()) {
					CacheableData page = entry.getValue().getPage();
					it.remove();
					return new EvictedCacheEntry(page.getBuffer(), page, entry.getKey().getResourceId());
				}	  
			}
			pagesExpelled = false;
		}
		


		
		
		
		if (recent.size() >= targetSize) {
			it = recent.entrySet().iterator();	
			fromFrequent = false;
		} else {
			it = frequent.entrySet().iterator();
			fromFrequent = true;
		}
		
		
		Entry<EntryId, CacheEntry> entry;
		

		
		while (it.hasNext())
		{
			
			entry = it.next();

			if (!entry.getValue().isPinned()) {
				CacheableData page = entry.getValue().getPage();
				
				
				if (fromFrequent) {
				  
					if (bottomFrequent.size() >= targetSize)
						bottomFrequent.remove(0);
					
					bottomFrequent.add(entry.getKey());
				  
				} else {
					if (bottomRecent.size() >= numPages - targetSize)
					  	bottomRecent.remove(0);
					
					bottomRecent.add(entry.getKey());
				}
			  
				
			  	it.remove();
			  	
			  	return new EvictedCacheEntry(page.getBuffer(), page, entry.getKey().getResourceId());
			}
			
		
		}

		
		// The frequent (resp. recent) part of the cache is entirely pinned, check the other part
		
		if (fromFrequent) {
			it = recent.entrySet().iterator();	
			fromFrequent = false;
		} else {
			it = frequent.entrySet().iterator();
			fromFrequent = true;
		}
		
		while (it.hasNext())
		{
			entry = it.next();
		  
			if (!entry.getValue().isPinned()) {
				CacheableData page = entry.getValue().getPage();
			  
			  	if (fromFrequent) {
			  		if (bottomFrequent.size() >= targetSize)
			  			bottomFrequent.remove(0);
			  		
				  bottomFrequent.add(entry.getKey());
				  
			  	} else {
			  		if (bottomRecent.size() >= numPages - targetSize)
			  			bottomRecent.remove(0);
			  		
			  		bottomRecent.add(entry.getKey());
			  	}
			  
			  	it.remove();
			  	return new EvictedCacheEntry(page.getBuffer(), page, entry.getKey().getResourceId());
		  	}
		}
		
		throw new CachePinnedException();	

	}
	
	
	private class CacheEntry {
		
		private CacheableData page;
		private int hit;
		private int pinned;
		private boolean expelled;
		
		public CacheEntry(CacheableData page,  int hit, boolean pinned)
	    {
	        this.page = page;
	        this.hit = hit;
	        this.pinned = pinned ? 1 : 0;
	        this.expelled = false;
	    }


		/**
		 * Returns the page number of this entry id.
		 * 
		 * @return The page number.
		 */
		public CacheableData getPage() 
		{
			return this.page;
		}

		public int gethit() 
		{
			return this.hit;
		}
		
		public void hit() {
			
			this.hit++;		
		}
		
		public boolean isPinned() {
			return this.pinned > 0;
		}
		
		public void pin() {
			this.pinned++;
		}
		
		public void unpin() {
			this.pinned--;
		}
		
		public void unpinAll() {
			this.pinned = 0;
		}
		
		public boolean isExpelled() {
			return this.expelled;
		}
		
		public void expell() {
			this.expelled = true;;
		}

	}
	
	
	/**
	 * Helper class to track resource ids and page numbers of cache entries.
	 */
	private class EntryId
	{
		private int id;
		private int pagenumber;
		

		/**
		 * Ctr.
		 * 
		 * @param id The resource id.
		 * @param pagenumber The page number.
		 */
		public EntryId(int id, int pagenumber)
        {
	        this.id = id;
	        this.pagenumber = pagenumber;
        }


		/**
		 * Returns the page number of this entry id.
		 * 
		 * @return The page number.
		 */

		
		@SuppressWarnings("unused")
		public int getPageNumber() 
		{
			return this.pagenumber;
		}


		/**
		 * Returns the resource id of this entry id.
		 * 
		 * @return The resource id.
		 */
		public int getResourceId() 
		{
			return this.id;
		}


		/*
		 * (non-Javadoc)
		 * @see java.lang.Object#hashCode()
		 */
		@Override
        public int hashCode()
        {
	        final int prime = 31;
	        int result = 1;
	        result += prime * result + this.id;
	        result += prime * result + this.pagenumber;
	        return result;
        }

		/*
		 * (non-Javadoc)
		 * @see java.lang.Object#equals(java.lang.Object)
		 */
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
}
