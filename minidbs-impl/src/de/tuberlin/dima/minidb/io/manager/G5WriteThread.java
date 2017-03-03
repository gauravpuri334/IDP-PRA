package de.tuberlin.dima.minidb.io.manager;

import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.ConcurrentLinkedQueue;

import de.tuberlin.dima.minidb.io.cache.CacheableData;
import de.tuberlin.dima.minidb.io.cache.PageSize;

public class G5WriteThread extends Thread {

	interface FreeBufferCallback   
	{  
	    void freeBuffer(PageSize pageSize);
	}  
	
	public ConcurrentLinkedQueue<G5WriteRequest> requests;
	
	private final FreeBufferCallback callback;

	private volatile boolean alive;
	
	public G5WriteThread(FreeBufferCallback callback) {
		
		this.callback = callback;
		this.requests = new ConcurrentLinkedQueue<G5WriteRequest>();
		this.alive = true;	

	}
	
	
	@Override
	public void run() {
		
		while(this.alive) {
			
			
			
			while (!requests.isEmpty()) {	
				
				
				G5WriteRequest request = requests.remove();
				
				CacheableData wrapper;
				byte[] buffer;
				ResourceManager resource;
				
				synchronized(request) {
					
					wrapper = request.getWrapper();
					buffer = request.getBuffer();
					resource = request.getManager();
				}
				
					
						
				try {
					synchronized(resource) {
						
						
						resource.writePageToResource(buffer, wrapper);
						
					//System.out.println("W");

					}
					
				} catch (IOException ioe) {
					System.out.println("Write thread IOException : " + ioe.getMessage());
				} finally {
					callback.freeBuffer(resource.getPageSize());
				}
			}
			
		}
		
	}
	
	
	
	public synchronized void request(G5WriteRequest request) {
		
	//	System.out.println("Rq");
		requests.add(request);
	}
	
	public synchronized CacheableData getRequest(int resourceId, int pageNumber) {
		
		Iterator<G5WriteRequest> it = requests.iterator();
		
		while (it.hasNext()) {
			
			G5WriteRequest request = it.next();
			
			synchronized(request) {		
					if (request.getResourceId() == resourceId && request.getWrapper().getPageNumber() == pageNumber)
						return request.getWrapper();
			}
		}
		return null;
		
	}
	
	
	public void stopThread()
	{
		this.alive = false;
	}
	
	
}
