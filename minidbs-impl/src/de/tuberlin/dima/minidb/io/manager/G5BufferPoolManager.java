package de.tuberlin.dima.minidb.io.manager;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.logging.Logger;

import de.tuberlin.dima.minidb.Config;
import de.tuberlin.dima.minidb.api.AbstractExtensionFactory;
import de.tuberlin.dima.minidb.io.cache.CachePinnedException;
import de.tuberlin.dima.minidb.io.cache.CacheableData;
import de.tuberlin.dima.minidb.io.cache.DuplicateCacheEntryException;
import de.tuberlin.dima.minidb.io.cache.EvictedCacheEntry;
import de.tuberlin.dima.minidb.io.cache.PageCache;
import de.tuberlin.dima.minidb.io.cache.PageFormatException;
import de.tuberlin.dima.minidb.io.cache.PageSize;
import de.tuberlin.dima.minidb.io.manager.G5ReadThread.PrefetchCallback;
import de.tuberlin.dima.minidb.io.manager.G5WriteThread.FreeBufferCallback;





public class G5BufferPoolManager implements BufferPoolManager, FreeBufferCallback, PrefetchCallback {
	
	
	
	
	
	private HashMap<Integer, ResourceManager> resources; // 
	
	private HashMap<PageSize, PageCache> caches;
	
	private HashMap<PageSize, LinkedList<byte[]>> buffers;	
	
	private G5ReadThread readThread;
	private G5WriteThread writeThread;
	
	private Config config;
	private int nbrIOBuffers;
	private boolean opened;

	
	
	public G5BufferPoolManager(Config config, Logger logger) {
		
		this.config = config;

		
		resources = new HashMap<Integer, ResourceManager>();
		
		caches = new HashMap<PageSize, PageCache>();
		
		buffers = new HashMap<PageSize,LinkedList<byte[]>>();

		
		nbrIOBuffers = config.getNumIOBuffers();

		
		readThread = new G5ReadThread(this);
		
		writeThread = new G5WriteThread(this);

		
		this.opened = false;
		
	}

	@Override
	public void startIOThreads() throws BufferPoolException {
		readThread.start();
		writeThread.start();
		
		this.opened = true;
		
	}

	
	/**
	 * This method closes the buffer pool. It causes the following actions:
	 * <ol>
	 *   <li>It prevents further requests from being processed</li>
	 *   <li>Its stops the reading thread from fetching any further pages. Requests that are currently in the
	 *       queues to be read are discarded.</li>
	 *   <li>All methods that are currently blocked and waiting on a synchronous page request will be waken up and
	 *       must throw a BufferPoolException as soon as they wake up.</li>
	 *   <li>It closes all resources, meaning that it gets all their pages from the cache, and queues the modified
	 *       pages to be written out.</li>
	 *   <li>It dereferences the caches (setting them to null), allowing the garbage collection to
	 *       reclaim the memory.</li>
	 * </ol>
	 */
	@SuppressWarnings("unused")
	@Override
	public void closeBufferPool() {
		
		this.opened = false;
		
		
		readThread.stopThread();
		
		
		for(Entry<Integer, ResourceManager> entry : resources.entrySet()) {
			try {
				int resourceId = entry.getKey();
				ResourceManager resource = entry.getValue();
				
				PageCache cache = caches.get(resource.getPageSize());
				
				CacheableData[] pages = cache.getAllPagesForResource(resourceId);
				
				for (CacheableData page : pages) {
			
					if (page != null && page.hasBeenModified()) {

						byte[] writeBuffer = getBuffer(resource.getPageSize());
						 
						 
						 System.arraycopy(page.getBuffer(), 0, writeBuffer, 0, writeBuffer.length);
			
						 G5WriteRequest writeRequest = new G5WriteRequest(resourceId, resource, writeBuffer, page);
						 writeThread.request(writeRequest);	
					}
				}
			} catch (BufferPoolException e) {
				// Well .. can't do much
			}
		}
		
		writeThread.stopThread();
		
		try {
			writeThread.join();
		} catch (InterruptedException e) {
			// Well .. can't do much
		}
		
		
		for(PageCache cache : caches.values()) {
			cache = null;
		}

		
		// The bufferPool shouldn't close the resource managers himself
		/*	for(ResourceManager resource : resources.values()) {
				try {
					resource.closeResource();
					
				} catch (IOException ioe) {
					// Well .. can't do much
				}
			}*/
		
		
		
		
	}

	
	/**
	 * Registers a resource with this buffer pool. All requests for this resource are then served through this buffer pool.
	 * <p>
	 * If the buffer pool has already a cache for the page size used by that resource, the cache will
	 * be used for the resource. Otherwise, a new cache will be created for that page size. The size
	 * of the cache that is to be created is read from the <tt>Config</tt> object. In addition to the
	 * cache, the buffer pool must also create the I/O buffers for that page size.
	 * 
	 * @param id The id of the resource.
	 * @param manager The ResourceManager through which pages of the resource are read from and 
	 *                written to secondary storage.
	 * 
	 * @throws BufferPoolException Thrown, if the buffer pool has been closed, the resource is already
	 *                             registered with another handler or a cache needs to be created but
	 *                             the creation fails.
	 */
	@Override
	public synchronized void registerResource(int id, ResourceManager manager)
			throws BufferPoolException {
		
		if (!this.opened)
			throw new BufferPoolException("The Buffer Pool Manager is closed");
		
		if (resources.containsKey(id))
			throw new BufferPoolException("This resource is already registered");

		resources.put(id, manager);		
		
		PageSize pageSize = manager.getPageSize();
		
		if (!caches.containsKey(pageSize)) {
			
			PageCache cache = AbstractExtensionFactory.getExtensionFactory().createPageCache(pageSize, config.getCacheSize(pageSize));
			
			caches.put(pageSize, cache);
			
			
			
			LinkedList<byte[]> bufferList = new LinkedList<byte[]>();
			
			for (int i = 0; i < nbrIOBuffers; i++) {
								
				bufferList.add(new byte[pageSize.getNumberOfBytes()] );
							
				
			}
			
			buffers.put(pageSize, bufferList);
			
		}
		
	}

	
	/**
	 * Transparently fetches the page defined by the page number for the given resource.
	 * If the cache contains the page, it will be fetched from there, otherwise it will
	 * be loaded from secondary storage. The page is pinned in order to prevent eviction from
	 * the cache.
	 * <p>
	 * The binary page will be wrapped by a page object depending on the resource type.
	 * If the resource type is for example a table, then the returned object is a <code>TablePage</code>.
	 * 
	 * @param resourceId The id of the resource.
	 * @param pageNumber The page number of the page to fetch.
	 * @return The requested page, wrapped by a structure to make it accessible.
	 * 
	 * @throws BufferPoolException Thrown, if the given resource it not registered at the buffer pool,
	 *                             the buffer pool is closed, or an internal problem occurred.
	 * @throws IOException Thrown, if the page had to be loaded from secondary storage and the loading
	 *                     failed due to an I/O problem.
	 */
	@Override
	public CacheableData getPageAndPin(int resourceId, int pageNumber)
			throws BufferPoolException, IOException {
		
		if (!this.opened)
			throw new BufferPoolException("The Buffer Pool Manager is closed");
		
		ResourceManager resource = resources.get(resourceId);
		
		if (resource == null) 
			throw new BufferPoolException("Resource not registered");
		
		
		
		
		// Look in the cache first
		PageCache cache = caches.get(resource.getPageSize());
		G5ReadRequest request;
		
		synchronized (cache) {
		
			CacheableData page = cache.getPageAndPin(resourceId, pageNumber);
		
			if (page != null)
				return page;		
		
			
			 request = readThread.getRequest(resourceId, pageNumber);

		}
		
		
		
			
			// Page not in cache : is it currently in one request queue ? (useful ??)
			
			// - write queue : use the version waiting to be read
			
			CacheableData page = writeThread.getRequest(resourceId, pageNumber);
			
			if (page != null) {
				addPageInCache(resourceId, page, true);
				//System.out.println("Write request found");
				return page;
			}
			
			
			// - Read queue : wait for the request to complete and return the page (+ hit ! )
			
			
			if (request != null) {
				
				//System.out.println("read request found");
				try {
					synchronized (request) {
						
						while (!request.isDone()) {
							request.wait();
							if(!readThread.isRunning())
								throw new BufferPoolException("Buffer Pool is closing");
						}
					}			
				} catch (InterruptedException ie) {
					throw new IOException("Request interrupted");
				}
				
				synchronized (cache) {
					
					page = cache.getPageAndPin(resourceId, pageNumber); // Just to hit it, should be the same as in the request wrapper
				
					if (page != null)
						return page;		
				}
				
				
				return request.getWrapper();
			}
		
		
		
		
		// Page not in cache : emit a read request
		byte[] readBuffer;
		
		
		
			
			readBuffer = getBuffer(resource.getPageSize());
		
			request = new G5ReadRequest(resource, readBuffer, pageNumber, resourceId, false);
			readThread.request(request);
			
			CacheableData wrapper;
			
			
		try {
			synchronized (request) {
				
				while (!request.isDone()) {
					request.wait();
					if(!readThread.isRunning())
						throw new BufferPoolException("Buffer Pool is closing");
				}
				
				wrapper = request.getWrapper();
			}			
		} catch (InterruptedException ie) {
			throw new IOException("Request interrupted");
		}
			
			
			// Read request complete : add page to cache
			addPageInCache(resourceId, wrapper, true);
			
		
				
		return wrapper;
	}

	
	/**
	 * Unpins a given page and in addition fetches another page from the same resource. This method works exactly
	 * like the method {@link de.tuberlin.dima.minidb.io.BufferPoolManager#getPageAndPin(int, int)}, only that it
	 * unpins a page before beginning the request for the new page.
	 * <p>
	 * Similar to the behavior of {@link de.tuberlin.dima.minidb.io.BufferPoolManager#unpinPage(int, int)}, no exception
	 * should be thrown, if the page to be unpinned was already unpinned or no longer in the cache.
	 * <p>
	 * This method should perform better than isolated calls to unpin a page and getting a new one.
	 * It should, for example, only acquire the lock/monitor on the cache once, instead of twice.
	 * 
	 * @param resourceId The id of the resource.
	 * @param unpinPageNumber The page number of the page to be unpinned.
	 * @param getPageNumber The page number of the page to get and pin.
	 * @return The requested page, wrapped by a structure to make it accessible.
	 * 
	 * @throws BufferPoolException Thrown, if the given resource it not registered at the buffer pool,
	 *                             the buffer pool is closed, or an internal problem occurred.
	 * @throws IOException Thrown, if the page had to be loaded from secondary storage and the loading
	 *                     failed due to an I/O problem.
	 */
	@Override
	public CacheableData unpinAndGetPageAndPin(int resourceId,
			int unpinPageNumber, int getPageNumber) throws BufferPoolException,
			IOException {
		
		
		if (!this.opened)
			throw new BufferPoolException("The Buffer Pool Manager is closed");
		
		ResourceManager resource = resources.get(resourceId);
		
		if (resource == null) 
			throw new BufferPoolException("Resource not registered");
		
		
		// Look first in cache
		PageCache cache = caches.get(resource.getPageSize());
		G5ReadRequest request;
		synchronized (cache) {
			cache.unpinPage(resourceId, unpinPageNumber);
			CacheableData page = cache.getPageAndPin(resourceId, getPageNumber);
		
			if (page != null)
				return page;
		
			 request = readThread.getRequest(resourceId, getPageNumber);
			
		}
		

		
		// Page not in cache : emit a read request
		byte[] buffer;
		
		buffer = getBuffer(resource.getPageSize());
		
		
		
		
		if (request != null) {
			synchronized (request) {
				while (!request.isDone()) {
					try {
						request.wait();
					} catch (InterruptedException e) {
						throw new IOException("Request interrupted");
					}
					if(!readThread.isRunning())
						throw new BufferPoolException("Buffer Pool is closing");
				}			
			}	
			return request.getWrapper();
			
		}

	
		request = new G5ReadRequest(resource, buffer, getPageNumber, resourceId, false);

		readThread.request(request);
		
		CacheableData wrapper;
		
		
		try {			
			synchronized (request) {
				while (!request.isDone()) {
					request.wait();
					if(!readThread.isRunning())
						throw new BufferPoolException("Buffer Pool is closing");
				}
				
				wrapper = request.getWrapper();
			}	
			
		} catch (InterruptedException ie) {
			throw new IOException("Request interrupted");
		}
		

		if (wrapper.getPageNumber() != getPageNumber)
			throw new BufferPoolException("Read page number different from the one requested");
		
		// Add page to cache
		addPageInCache(resourceId, wrapper, true);

		
		return wrapper;
	}

	
	
	

	/**
	 * Unpins a page so that it can again be evicted from the cache. This method works after the principle of 
	 * <i>best effort</i>: It will try to unpin the page, but will not throw any exception if the page is not
	 * contained in the cache or if it is not pinned.
	 * 
	 * @param resourceId The id of the resource.
	 * @param pageNumber The page number of the page to unpin.
	 */
	@Override
	public void unpinPage(int resourceId, int pageNumber) {
		
		ResourceManager resource = resources.get(resourceId);
		
		if (resource != null)  {
		
			PageCache cache = caches.get(resource.getPageSize());
			
			synchronized (cache) {
			
				cache.unpinPage(resourceId, pageNumber);				
			}
		}		
	}

	
	
	
	/**
	 * Prefetches a page. If the page is currently in the cache, this method causes a hit on the page.
	 * If not, an asynchronous request to load the page is issued. When the asynchronous request has loaded the
	 * page, it adds it to the cache without causing it to be considered hit.
	 * <p>
	 * The rational behind that behavior is the following: Pages that are already in the cache are hit again and
	 * become frequent, which is desirable, since it is in fact accessed multiple times. (A prefetch is typically
	 * followed by a getAndPin request short after). Any not yet contained page is added to the cache and not hit,
	 * so that the later getAndPin request is in fact the first request and keeps the page among the recent items
	 * rather than among the frequent.
	 * <p>
	 * This function returns immediately and does not wait for any I/O operation to complete.
	 * 
	 * @param resourceId The id of the resource.
	 * @param pageNumber The page number of the page to fetch.
	 * @throws BufferPoolException If the buffer pool is closed, or the resource is not registered.
	 */
	@Override
	public void prefetchPage(int resourceId, int pageNumber)
			throws BufferPoolException {
		
		if (!this.opened)
			throw new BufferPoolException("The Buffer Pool Manager is closed");
		
		ResourceManager resource = resources.get(resourceId);
		
		if (resource == null)
			throw new BufferPoolException();
		
		
		PageCache cache = caches.get(resource.getPageSize());
	
		
		// Look in cache first
		CacheableData page;
		
		synchronized(cache) {
			 page = cache.getPage(resourceId, pageNumber);
		}
		
		if (page != null) {	
			return;
		}
		
		// Page not in cache : emit a read request
		byte[] readBuffer;
		G5ReadRequest request;

			
			readBuffer = getBuffer(resource.getPageSize());
		
			// prefetch = true : the read request takes care of adding the page to cache to allow the prefetch function to return immediately
			request = new G5ReadRequest(resource, readBuffer, pageNumber, resourceId, true);
			readThread.request(request);					
	}

	
	
	/**
	 * Prefetches a sequence of pages. Behaves exactly like
	 * {@link de.tuberlin.dima.minidb.io.BufferPoolManager#prefetchPage(int, int)}, only that it prefetches
	 * multiple pages.
	 * 
	 * @param resourceId The id of the resource.
	 * @param startPageNumber The page number of the first page to prefetch.
	 * @param endPageNumber The page number of the last page to prefetch.
	 * @throws BufferPoolException If the buffer pool is closed, or the resource is not registered.
	 */
	@Override
	public void prefetchPages(int resourceId, int startPageNumber,
			int endPageNumber) throws BufferPoolException {
		
		if (!this.opened)
			throw new BufferPoolException("The Buffer Pool Manager is closed");
		
		
		ResourceManager resource = resources.get(resourceId);
		
		ArrayList<Integer> toLoad = new ArrayList<Integer>();
		
		if (resource == null)
			throw new BufferPoolException();
		
		
		PageCache cache = caches.get(resource.getPageSize());
		
		CacheableData page;
		
		// List the pages not in cache
		
		synchronized(cache) {
			for (int i = startPageNumber; i <= endPageNumber; i++ ) {
				page = cache.getPage(resourceId, i);
				
				if (page == null)
					toLoad.add(i);
				
			}
		}
		

		
		byte[] readBuffer;
		G5ReadRequest request;
		
		// Emit a read request for each page not in cache
		for (Integer pageNumber : toLoad) {

				
			readBuffer = getBuffer(resource.getPageSize());
			
			// prefetch = true : the read request takes care of adding the page to cache to allow the prefetch function to return immediately
			request = new G5ReadRequest(resource, readBuffer, pageNumber, resourceId, true);

			readThread.request(request);
		}	
	}

	@Override
	public CacheableData createNewPageAndPin(int resourceId)
			throws BufferPoolException, IOException {
		
		if (!this.opened)
			throw new BufferPoolException("The Buffer Pool Manager is closed");
	
		ResourceManager resource = resources.get(resourceId);
		
		if (resource == null)
			throw new BufferPoolException();
				

		byte[] buffer;
		
		try {
			buffer = getBuffer(resource.getPageSize());
			 
			CacheableData page;
			
			synchronized(resource) {
				page = resource.reserveNewPage(buffer);
			}
			
			addPageInCache(resourceId, page, true);
			
			return page;
				 
		} catch (NoSuchElementException nsee) {
			throw new BufferPoolException("No more buffers available");
			
		} catch (PageFormatException pfe) {
			throw new BufferPoolException("Page couldn't be reserved");
				
		}
	}

	
	
	/**
	 * Creates a new empty page for the resource described by the id and pins the new page. This method is called
	 * whenever during the insertion into a table a new page is needed, or index insertion requires an additional
	 * page. See {@link de.tuberlin.dima.minidb.io.BufferPoolManager#createNewPageAndPin(int)} for details.
	 * <p>
	 * This method takes a parameter that lets the caller specify the type of page to be created. That is important
	 * for example for indexes, which have multiple types of pages (leaf pages, inner node pages). The parameter need
	 * not be interpreted by the buffer pool manager, but it may rather be passed to the <tt>ResourceManager</tt> of the
	 * given resource, who will interpret it and make sure that the correct page type is instantiated.
	 * 
	 * @param type The resource type, e.g. table or index.
	 * @param resourceId The id of the resource for which the page is to be created.
	 * @param parameters Parameters for the page creation. Not all resource types will interpret those.
	 * @return A new empty page that is part of the given resource.
	 * @throws BufferPoolException Thrown, if the given resource it not registered at the buffer pool,
	 *                             the buffer pool is closed, or an internal problem occurred.
	 * @throws IOException Thrown, if the page had to be loaded from secondary storage and the loading
	 *                     failed due to an I/O problem.
	 */
	@Override
	public CacheableData createNewPageAndPin(int resourceId, Enum<?> type)
			throws BufferPoolException, IOException {
		
		if (!this.opened)
			throw new BufferPoolException("The Buffer Pool Manager is closed");
		
		
		ResourceManager resource = resources.get(resourceId);
		
		if (resource == null)
			throw new BufferPoolException("Resource not registered");
				

		byte[] buffer;
		
		try {
			 buffer = getBuffer(resource.getPageSize());

			CacheableData page;
			synchronized(resource) {
				page = resource.reserveNewPage(buffer, type);				
			}
							
			addPageInCache(resourceId, page, true);
					
			return page;
				 
		} catch (NoSuchElementException nsee) {
			throw new BufferPoolException("No more buffers available");
			
		} catch (PageFormatException pfe) {
			throw new BufferPoolException("Page couldn't be reserved");
			
		}
	}

	

	@Override
	public synchronized void freeBuffer(PageSize pageSize) {
		
		LinkedList<byte[]> bufferQueue = buffers.get(pageSize);
		
		synchronized (bufferQueue) {
			
			bufferQueue.add(new byte[pageSize.getNumberOfBytes()]);		
			bufferQueue.notifyAll();      
		}		
	}	
	
	private byte[] getBuffer(PageSize pageSize) throws BufferPoolException {
		
		LinkedList<byte[]> bufferQueue = buffers.get(pageSize);
		byte[] buffer;
		
	//	System.out.println(bufferQueue.size() + " buffers in queue " + pageSize);
		
		try {
			synchronized (bufferQueue) {
				while (bufferQueue.isEmpty()) {
			        bufferQueue.wait();
		        }
			 
		        buffer = bufferQueue.pop();
		        bufferQueue.notifyAll();	
			 }
			
		} catch (InterruptedException ie) {
			throw new BufferPoolException();
		}
					
		return buffer;
	}

	@Override
	public void addPageInCache(int resourceId, CacheableData page, boolean pin) {
				
		ResourceManager resource = resources.get(resourceId);
		PageSize pageSize = resource.getPageSize();
		
		PageCache cache = caches.get(pageSize);


		EvictedCacheEntry evicted;
		try {
			synchronized(cache) {
				if (pin) {
					evicted = cache.addPageAndPin(page, resourceId);
				} else {
					evicted = cache.addPage(page, resourceId);
				}

			}
			
			
			
			
			freeBuffer(pageSize);	
			
			CacheableData evictedPage = evicted.getWrappingPage();
			
			
			int evictedResourceId = evicted.getResourceID();
			ResourceManager evictedResource = resources.get(evictedResourceId);
	
			if (evictedPage != null && evictedPage.hasBeenModified()) {

				byte[] writeBuffer = getBuffer(pageSize);
				 
				 
				 System.arraycopy(evictedPage.getBuffer(), 0, writeBuffer, 0, writeBuffer.length);
	
				 G5WriteRequest writeRequest = new G5WriteRequest(evictedResourceId, evictedResource, writeBuffer, evictedPage);
				 writeThread.request(writeRequest);	

			}
			
		} catch (DuplicateCacheEntryException dcee) {
			
			dcee.printStackTrace();
			System.out.println("Page : " + dcee.getPageNumber());
		} catch (CachePinnedException cpe) {
			cpe.printStackTrace();
		} catch (NoSuchElementException nsee) {			
			nsee.printStackTrace();
		} catch (BufferPoolException ie) {
			ie.printStackTrace();
		}
	}		
	
	
	
	

	
}
