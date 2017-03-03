package de.tuberlin.dima.minidb.io.manager;

import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.ConcurrentLinkedQueue;

import de.tuberlin.dima.minidb.io.cache.CacheableData;

public class G5ReadThread extends Thread {
	
	
	interface PrefetchCallback {
		
		void addPageInCache(int resourceId, CacheableData page, boolean pin);
	}
	
	public ConcurrentLinkedQueue<G5ReadRequest> requests;
	
	private PrefetchCallback callback;

	private volatile boolean alive;
	
	
	
	
	
	
	public G5ReadThread(PrefetchCallback callback) {
		
		
		this.requests = new ConcurrentLinkedQueue<G5ReadRequest>();
		this.callback = callback;
		this.alive = true;
		
	}
	
	
	@Override
	public void run() {
		
		while(this.alive) {
			
			if (!requests.isEmpty()) {
				
				G5ReadRequest request = requests.peek();


				synchronized (request) {
				
					ResourceManager resource = request.getManager();
					byte[] buffer = request.getBuffer();
					int pageNumber = request.getPageNumber();
					
					try {
						CacheableData page;
						
						synchronized(resource) {
							 page = resource.readPageFromResource(buffer, pageNumber);	
						}
						
						if (request.isPrefetch())
							callback.addPageInCache(request.getResourceId(), page, false);
						
						request.setWrapper(page);
						
					} catch (IOException ioe) {
						System.out.println("Read IO Exception : " + ioe.getMessage());
						System.out.println(resource.getClass());
						
						
					} finally {
						
						request.done();
						requests.remove();
						request.notifyAll();
					}
					
				}
			}	
		}		
	}
	
	
	
	public synchronized void request(G5ReadRequest request) {
		
		requests.add(request);		
	}
	
	
	public synchronized G5ReadRequest getRequest(int resourceId, int pageNumber) {
		
		Iterator<G5ReadRequest> it = requests.iterator();
		
		while (it.hasNext()) {
			
			G5ReadRequest request = it.next();
			

					if (request.getResourceId() == resourceId && request.getPageNumber() == pageNumber)
						return request;
				}
		
		return null;
	}
	
	

	public boolean isRunning() {
		return this.alive;
	}
	
	public void stopThread()
	{
		
		this.alive = false;
		
		while (!requests.isEmpty()) {
			G5ReadRequest request = requests.remove();
			synchronized(request) {
				request.notifyAll();
			}
		}
		
	}
	
	

	
	
}
