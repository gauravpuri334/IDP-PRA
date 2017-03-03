package de.tuberlin.dima.minidb.io.manager;

import de.tuberlin.dima.minidb.io.cache.CacheableData;

public class G5ReadRequest {
	
	
	private byte[] buffer;
	private CacheableData wrapper;
	private ResourceManager manager;
	private boolean prefetch;
	private boolean done;
	private int pageNumber;
	private int resourceId;
	
	
	
	public G5ReadRequest(ResourceManager manager, byte[] buffer, int pageNumber, int resourceId, boolean prefetch) {
		
		this.manager = manager;
		this.buffer = buffer;
		this.pageNumber = pageNumber;
		this.resourceId = resourceId;
		this.prefetch = prefetch;
		this.done = false;
		
	}
	
	
	public ResourceManager getManager() {
		return this.manager;
	}
	
	public CacheableData getWrapper() {
		return this.wrapper;
	}	
	
	public void setWrapper(CacheableData wrapper) {
		this.wrapper = wrapper;
	}	
	
	public byte[] getBuffer() {
		return this.buffer;
	}
	
	public int getPageNumber() {
		return this.pageNumber;
	}
	
	public int getResourceId() {
		return this.resourceId;
	}
	
	public boolean isPrefetch() {
		return this.prefetch;
	}
		
	public boolean isDone() {
		return this.done;
	}
	
	public void done() {
		this.done = true;;
	}
	
	
	


}
