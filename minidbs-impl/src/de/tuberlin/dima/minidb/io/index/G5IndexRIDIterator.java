package de.tuberlin.dima.minidb.io.index;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import de.tuberlin.dima.minidb.catalogue.IndexSchema;
import de.tuberlin.dima.minidb.core.DataField;
import de.tuberlin.dima.minidb.core.RID;
import de.tuberlin.dima.minidb.io.cache.PageFormatException;
import de.tuberlin.dima.minidb.io.manager.BufferPoolException;
import de.tuberlin.dima.minidb.io.manager.BufferPoolManager;

public class G5IndexRIDIterator implements IndexResultIterator<RID> {
	
	BufferPoolManager bufferPool;
	IndexSchema schema;
	
	int resourceId;
	
	BTreeLeafPage currentPage;
	
	DataField key;
	
	List<RID> currentElements;
	
	int nextPageNumber;

	public G5IndexRIDIterator(BufferPoolManager bufferPool, IndexSchema schema,  int resourceId, BTreeLeafPage firstPage, DataField key) {
		
		this.bufferPool = bufferPool;
		this.schema = schema;
		
		this.resourceId = resourceId;
		
		this.currentPage = firstPage;
		
		this.key = key;
		
		currentElements = new ArrayList<RID>();
		
			
		try {
			firstPage.getAllsRIDsForKey(key, this.currentElements);
		
			bufferPool.unpinPage(resourceId, firstPage.getPageNumber());
			
			
			// If the key continues to the next page, prefetch it
			nextPageNumber = -1;
			
			if (firstPage.isLastKeyContinuingOnNextPage() && firstPage.getLastKey().equals(key)) {
				
				nextPageNumber = firstPage.getNextLeafPageNumber();
				bufferPool.prefetchPage(resourceId, nextPageNumber);	
			}
		
		} catch (PageFormatException | BufferPoolException e) {
			e.printStackTrace();
		}		
	}
	
	
	/**
	 * This method checks, if further elements are available from this iterator.
	 * 
	 * @return true, if there are more elements available, false if not.
	 * @throws IOException Thrown, if the method fails due to an I/O problem.
	 * @throws IndexFormatCorruptException Thrown, if the method fails because the index is in an inconsistent state.
	 * @throws PageFormatException Thrown, if a corrupt page prevents execution of this method.
	 */
	@Override

	public boolean hasNext() throws IOException, IndexFormatCorruptException, PageFormatException {
		
		if (!currentElements.isEmpty() || (nextPageNumber != -1)) 
			return true;
		
		return false;
	}
	
	/**
	 * This gets the next element from the iterator, moving the iterator forward.
	 * This method should succeed, if a prior call to hasNext() returned true.
	 * 
	 * @return The next element in the sequence. 
	 * @throws IOException Thrown, if the method fails due to an I/O problem.
	 * @throws IndexFormatCorruptException Thrown, if the method fails because the index is in an inconsistent state.
	 * @throws PageFormatException Thrown, if a corrupt page prevents execution of this method.
	 */
		
	@Override
	public RID next() throws IOException, IndexFormatCorruptException, PageFormatException {
		
		
		// Return element of the current page
		if (!currentElements.isEmpty()) {			
			return currentElements.remove(0);
				
			
		// get next page otherwise
		} else if (nextPageNumber != -1) {
				
			try {

								
				currentPage = (BTreeLeafPage) bufferPool.getPageAndPin(resourceId, nextPageNumber);
				
				currentPage.getAllsRIDsForKey(key, this.currentElements);	
					
				bufferPool.unpinPage(resourceId, currentPage.getPageNumber());
				
				
				// If the key continues to the next page, prefetch it
				nextPageNumber = -1;
				
				if (currentPage.isLastKeyContinuingOnNextPage() && currentPage.getLastKey().equals(key)) {
					
					nextPageNumber = currentPage.getNextLeafPageNumber();					
					bufferPool.prefetchPage(resourceId, nextPageNumber);
				}
				
				return currentElements.remove(0);
				
				
			} catch (BufferPoolException e) {
				e.printStackTrace();
			}		
		}
		
		return null;
	}
}



