package de.tuberlin.dima.minidb.io.index;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import de.tuberlin.dima.minidb.catalogue.IndexSchema;
import de.tuberlin.dima.minidb.core.DataField;
import de.tuberlin.dima.minidb.io.cache.PageFormatException;
import de.tuberlin.dima.minidb.io.manager.BufferPoolException;
import de.tuberlin.dima.minidb.io.manager.BufferPoolManager;

public class G5IndexKeyIterator implements IndexResultIterator<DataField> {
	
	BufferPoolManager bufferPool;
	IndexSchema schema;
	
	int resourceId;
	
	BTreeLeafPage currentPage;
	
	DataField startKey, stopKey;
	
	boolean startKeyIncluded, stopKeyIncluded;
	
	List<DataField> currentElements;
	
	int nextPageNumber;
	
	public G5IndexKeyIterator(BufferPoolManager bufferPool, IndexSchema schema,  int resourceId, BTreeLeafPage firstPage,
			DataField startKey, DataField stopKey, boolean startKeyIncluded, boolean stopKeyIncluded) {
		
		
		this.bufferPool = bufferPool;
		this.schema = schema;
		
		this.resourceId = resourceId;
		
		this.currentPage = firstPage;
		
		this.startKey = startKey;
		this.stopKey = stopKey;
		this.startKeyIncluded = startKeyIncluded;
		this.stopKeyIncluded = stopKeyIncluded;
				
		currentElements = new ArrayList<DataField>();
		
		try {
			
			// Fetch all RIDs in this page corresponding to the range	
			fetchKeys();
		
			updateNextPageNumber();
			

		
		} catch (BufferPoolException e) {
			e.printStackTrace();
		}
		
	}


	@Override
	public boolean hasNext() throws IOException, IndexFormatCorruptException,
			PageFormatException {
		
		
		if (!currentElements.isEmpty()) 
			return true;
				
		
		
		// while loop needed in case page only contains again a previous key
		while (nextPageNumber != -1 && currentElements.isEmpty()) {
			
			loadNextPage();		
		}
		
		if (!currentElements.isEmpty())
			return true;
		
		return false;
	}
	
	
	

	@Override
	public DataField next() throws IOException, IndexFormatCorruptException,
			PageFormatException {
		
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
			
			fetchKeys();
			
			bufferPool.unpinPage(resourceId, nextPageNumber);
			
			updateNextPageNumber();
		
		} catch (BufferPoolException bpe) {
			bpe.printStackTrace();
		}
	}
	
	
	
	
	
	private void fetchKeys() {	
		
		ArrayList<DataField> tmp = new ArrayList<DataField>();
		
		currentPage.getAllKeys(tmp, 0);
		
		
		
		for (int i = 0; i < tmp.size(); i++) {
			DataField key = tmp.get(i);
			
			int compareStop = key.compareTo(stopKey);
			int compareStart = key.compareTo(startKey);
			
			if (		(compareStart > 0 || (compareStart == 0 && startKeyIncluded))
					&& (compareStop < 0 || (compareStop == 0 && stopKeyIncluded))) {			
				currentElements.add(key);
			}
		}	
	}
	
	
	
	
	// Check if the next page may contain RIDs, prefetch it and update  next page number
	private void updateNextPageNumber() throws BufferPoolException{
		
		nextPageNumber = -1;
		
		int compare = currentPage.getLastKey().compareTo(stopKey);

		if (compare < 0 ||  (compare == 0 && currentPage.isLastKeyContinuingOnNextPage()) && stopKeyIncluded) {
			
			nextPageNumber = currentPage.getNextLeafPageNumber();
			bufferPool.prefetchPage(resourceId, nextPageNumber);
		}
	}
}
