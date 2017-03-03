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

public class G5IndexRIDRangeIterator implements IndexResultIterator<RID> {
	
	BufferPoolManager bufferPool;
	IndexSchema schema;
	
	int resourceId;
	
	BTreeLeafPage currentPage;
	
	DataField startKey, stopKey;
	
	boolean startKeyIncluded, stopKeyIncluded;
	
	List<RID> currentElements;
	
	int nextPageNumber;
	
	
	public G5IndexRIDRangeIterator(BufferPoolManager bufferPool, IndexSchema schema,  int resourceId, BTreeLeafPage firstPage,
			DataField startKey, DataField stopKey, boolean startKeyIncluded, boolean stopKeyIncluded) {
		
		this.bufferPool = bufferPool;
		this.schema = schema;
		
		this.resourceId = resourceId;
		
		this.currentPage = firstPage;
		
		this.startKey = startKey;
		this.stopKey = stopKey;
		this.startKeyIncluded = startKeyIncluded;
		this.stopKeyIncluded = stopKeyIncluded;
		
		
		
		currentElements = new ArrayList<RID>();
				
		try {
			
			
			// Fetch all RIDs in this page corresponding to the range
			fetchRIDs();
			
		
			updateNextPageNumber();
		
		} catch (BufferPoolException e) {
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
		
		if (!currentElements.isEmpty()) 
			return true;
		
		while (nextPageNumber != -1 && currentElements.isEmpty()) {
			
			loadNextPage();		
		}
		
		if (!currentElements.isEmpty())
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
		
		
		if (!currentElements.isEmpty()) {
			
			return currentElements.remove(0);
											
		} else if (nextPageNumber != -1) {
							
			loadNextPage();				

			return currentElements.remove(0);
				
		}
		
		return null;
	}
	
	
	
	private void loadNextPage() throws IOException, IndexFormatCorruptException, PageFormatException{
		
		try {	
							
			this.currentPage = (BTreeLeafPage) bufferPool.getPageAndPin(resourceId, nextPageNumber);
			
			fetchRIDs();
			
			bufferPool.unpinPage(resourceId, nextPageNumber);
			
			updateNextPageNumber();
						
		} catch (BufferPoolException bpe) {
			bpe.printStackTrace();
		}
	}
	
	
	private void fetchRIDs() {
		
		
		DataField currentKey;
		
		for (int i = this.currentPage.getPositionForKey(startKey); i < this.currentPage.getNumberOfEntries(); i++) {
			
			currentKey = this.currentPage.getKey(i);
			

			if (currentKey.equals(startKey) && !startKeyIncluded)
				continue;
			if ((currentKey.equals(stopKey) && !stopKeyIncluded) || currentKey.compareTo(stopKey) > 0)
				break;
			
			currentElements.add(this.currentPage.getRidAtPosition(i));		
		}
	}
	
	
	
	// Check if the next page may contain RIDs, prefetch it and update  next page number
	private void updateNextPageNumber() throws BufferPoolException{
		
		nextPageNumber = -1;

		
		int compare = currentPage.getLastKey().compareTo(stopKey);

		if (compare < 0 || (compare == 0 && currentPage.isLastKeyContinuingOnNextPage())) {
			
			nextPageNumber = currentPage.getNextLeafPageNumber();
			
			bufferPool.prefetchPage(resourceId, nextPageNumber);
		}
	}
}



