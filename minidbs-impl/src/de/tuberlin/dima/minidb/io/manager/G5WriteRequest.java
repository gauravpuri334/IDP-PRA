package de.tuberlin.dima.minidb.io.manager;

import de.tuberlin.dima.minidb.io.cache.CacheableData;

public class G5WriteRequest {
	
	private byte[] buffer;
	private CacheableData wrapper;
	private ResourceManager resource;
	private int resourceId;
	
	
	
	public G5WriteRequest(int resourceId, ResourceManager resource, byte[] buffer, CacheableData wrapper) {
		
		this.resource = resource;
		this.buffer = buffer;
		this.wrapper = wrapper;
		this.resourceId = resourceId;
		
	}
	
	
	public ResourceManager getManager() {
		return this.resource;
	}
	
	public int getResourceId() {
		return this.resourceId;
	}
	
	public CacheableData getWrapper() {
		return this.wrapper;
	}	
	
	public byte[] getBuffer() {
		return this.buffer;
	}
	
	
	

}
