package de.tuberlin.dima.minidb.io.index;

import java.io.IOException;

import de.tuberlin.dima.minidb.catalogue.IndexSchema;
import de.tuberlin.dima.minidb.core.DataField;
import de.tuberlin.dima.minidb.core.DuplicateException;
import de.tuberlin.dima.minidb.core.RID;
import de.tuberlin.dima.minidb.io.cache.PageFormatException;
import de.tuberlin.dima.minidb.io.manager.BufferPoolException;
import de.tuberlin.dima.minidb.io.manager.BufferPoolManager;

public class G5BTreeIndex implements BTreeIndex{
	
	
	
	
	IndexSchema schema;
	BufferPoolManager bufferPool;
	int resourceId;
	
	
	
	public G5BTreeIndex(IndexSchema schema, BufferPoolManager bufferPool, int resourceId) {
		
		this.schema = schema;
		this.bufferPool = bufferPool;
		this.resourceId = resourceId;		
		
		try {
			bufferPool.getPageAndPin(resourceId, schema.getRootPageNumber());
		} catch (IOException ioe) {
			
		} catch (BufferPoolException bpe) {
			
		}	
	}
	
	
	/**
	 * Gets the schema of the index represented by this instance.
	 * 
	 * @return The schema of the index.
	 */
	@Override
	public IndexSchema getIndexSchema() {
		
		return this.schema;		
	}
	
	
	/**
	 * Gets all RIDs for the given key. If the key is not found, then the returned iterator will
	 * not return any element (the first call to hasNext() is false).
	 * <p>
	 * This method should in general not get all RIDs for that key from the index at once, but only
	 * some. If the sequence of RIDs for that key spans multiple pages, these pages should be loaded gradually
	 * and the RIDs should be extracted when needed. It makes sense to always extract all RIDs from one page
	 * in one step, in order to be able to unpin that page again. 
	 * <p>
	 * Consider an example, where the key has many RIDs, spanning three pages. The iterator should load those
	 * from the first page first. When they are all returned, it loads the next page, extracting all RIDs there,
	 * returning them one after the other, and so on. It makes sense to issue prefetch requests for the next leaf
	 * pages ahead. 
	 * 
	 * @param key The key to get the RIDs for.
	 * @return An Iterator over of all RIDs for key.
	 * @throws PageFormatException Thrown if during processing a page's layout was found to be
	 *                             found to be corrupted.
	 * @throws IndexFormatCorruptException Throws, if the evaluation failed because condition
	 *                                     of the BTree were found to be invalid.
	 * @throws IOException Thrown, if a page could not be loaded.
	 */
	@Override
	public IndexResultIterator<RID> lookupRids(DataField key)
	throws PageFormatException, IndexFormatCorruptException, IOException {
				
		BTreeLeafPage leaf = findLeaf(key);
		
		if (leaf == null)
			throw new IndexFormatCorruptException();
				
		return new G5IndexRIDIterator(bufferPool, schema, resourceId, leaf, key);

	}
	
	
	/**
	 * Gets all RIDs in a given key-range. The rage is defined by the start key <code>l</code> (lower bound) 
	 * and the stop key <code>u</code> (upper bound), where both <code>l</code> and <code>u</code> can be
	 * optionally included or excluded from the interval, e.g. [l, u) or [l, u].
	 * <p>
	 * This method should obey the same on-demand-loading semantics as the {@link #lookupRids(DataField)} method. I.e. it
	 * should NOT first retrieve all RIDs and then return an iterator over an internally kept list.
	 * 
	 * @param startKey The lower boundary of the requested interval.
	 * @param stopKey The upper boundary of the requested interval.
	 * @param startKeyIncluded A flag indicating whether the lower boundary is inclusive. True indicates an inclusive boundary. 
	 * @param stopKeyIncluded A flag indicating whether the upper boundary is inclusive. True indicates an inclusive boundary.
	 * @return An Iterator over of all RIDs for the given key range.
	 * @throws PageFormatException Thrown if during processing a page's layout was found to be
	 *                             found to be corrupted.
	 * @throws IndexFormatCorruptException Throws, if the evaluation failed because condition
	 *                                     of the BTree were found to be invalid.
	 * @throws IOException Thrown, if a page could not be loaded.
	 */	
	@Override
	public IndexResultIterator<RID> lookupRids(DataField startKey, DataField stopKey, boolean startKeyIncluded, boolean stopKeyIncluded)
	throws PageFormatException, IndexFormatCorruptException, IOException {
		
		
		BTreeLeafPage leaf = findLeaf(startKey);
		
		if (leaf == null)
			throw new IndexFormatCorruptException();
			
		return new G5IndexRIDRangeIterator(bufferPool, schema, resourceId, leaf, startKey, stopKey, startKeyIncluded, stopKeyIncluded);

	}
	
	
	/**
	 * Gets all Keys that are contained in the given key-range. The rage is defined by the start key <code>l</code> (lower bound) 
	 * and the stop key <code>u</code> (upper bound), where both <code>l</code> and <code>u</code> can be
	 * optionally included or excluded from the interval, e.g. [l, u) or [l, u].
	 * <p>
	 * This method should obey the same on-demand-loading semantics as the {@link #lookupRids(DataField)} method. I.e. it
	 * should NOT first retrieve all RIDs and then return an iterator over an internally kept list.
	 * 
	 * @param startKey The lower boundary of the requested interval.
	 * @param stopKey The upper boundary of the requested interval.
	 * @param startKeyIncluded A flag indicating whether the lower boundary is inclusive. True indicates an inclusive boundary. 
	 * @param stopKeyIncluded A flag indicating whether the upper boundary is inclusive. True indicates an inclusive boundary.
	 * @return An Iterator over of all RIDs for the given key range.
	 * @throws PageFormatException Thrown if during processing a page's layout was found to be
	 *                             found to be corrupted.
	 * @throws IndexFormatCorruptException Throws, if the evaluation failed because condition
	 *                                     of the BTree were found to be invalid.
	 * @throws IOException Thrown, if a page could not be loaded.
	 */
	@Override
	public IndexResultIterator<DataField> lookupKeys(DataField startKey, DataField stopKey, boolean startKeyIncluded, boolean stopKeyIncluded)
	throws PageFormatException, IndexFormatCorruptException, IOException {
		
	
		BTreeLeafPage leaf = findLeaf(startKey);
		
		if (leaf == null)
			throw new IndexFormatCorruptException();
			
		return new G5IndexKeyIterator(bufferPool, schema, resourceId, leaf, startKey, stopKey, startKeyIncluded, stopKeyIncluded);
	}
	
	
	/**
	 * Inserts a pair of (key/RID) into the index. For unique indexes, this method must throw
	 * a DuplicateException, if the key is already contained.
	 * <p>
	 * If the page number of the root node or the first leaf node changes during the operation,
	 * then this method must notify the schema to reflect this change.
	 * 
	 * @param key The key of the pair to be inserted.
	 * @param rid The RID of the pair to be inserted.
	 * @throws PageFormatException Thrown if during processing a page's layout was found to be
	 *                             found to be corrupted.
	 * @throws IndexFormatCorruptException Throws, if the evaluation failed because condition
	 *                                     of the BTree were found to be invalid.
	 * @throws DuplicateException Thrown, if the key is already contained and the index is defined to be unique.
	 * @throws IOException Thrown, if a page could not be read or written.
	 */
	@Override
	public void insertEntry(DataField key, RID rid)
	throws PageFormatException, IndexFormatCorruptException, DuplicateException, IOException {
		
		
		BTreeLeafPage leaf = findLeaf(key);		
		
		// Try to insert in the leaf		
		if ( ! leaf.insertKeyRIDPair(key, rid)) {
			
			// Leaf is full : need to split			
			try {
				// Create a new leaf
				BTreeLeafPage rightLeaf = (BTreeLeafPage) bufferPool.createNewPageAndPin(resourceId, BTreeIndexPageType.LEAF_PAGE);
				
				// Move half of the pair from full leaf to new one
				rightLeaf.prependEntriesFromOtherPage(leaf, schema.getMaximalLeafEntries()/2);
				
				// Insert the entry in the corresponding leaf
				int compare = leaf.getLastKey().compareTo(key);
				if (compare >= 0) {
					leaf.insertKeyRIDPair(key, rid);
				} else {
					rightLeaf.insertKeyRIDPair(key, rid);
				}
				
				
				// Link the leaves	and set continuing flags
				rightLeaf.setNextLeafPageNumber(leaf.getNextLeafPageNumber());
				rightLeaf.setLastKeyContinuingOnNextPage(leaf.isLastKeyContinuingOnNextPage());
				
				boolean keyContinuing = rightLeaf.getFirstKey().equals(leaf.getLastKey());

				leaf.setNextLeafPageNumber(rightLeaf.getPageNumber());
				leaf.setLastKeyContinuingOnNextPage(keyContinuing);		

				
				// Insert key/ pointer pair in parent				
				BTreeInnerNodePage parent = findParent(leaf.getPageNumber(), key);
				
				insertInParent(parent, leaf.getLastKey(), leaf.getPageNumber(), rightLeaf.getPageNumber());

				
				// and unpin the pages used
				bufferPool.unpinPage(resourceId, rightLeaf.getPageNumber());
				bufferPool.unpinPage(resourceId, leaf.getPageNumber());
				
				if (parent != null)
					bufferPool.unpinPage(resourceId, parent.getPageNumber());
				
			} catch (BufferPoolException e) {
				e.printStackTrace();
			}
			
			
		}
		
		
		
		
	}
	
	
	
	/**
	 * Insert a key/pointer pair in a the given inner node
	 * The leftPageNumber is only used if no parent exists and a new root needs to be created
	 * @param parent The parent in which the pair should be inserted
	 * @param key The key to insert
	 * @param leftPageNumber The page number used in case a new has to be created
	 * @param rightPageNumberthe page number to insert
	 * @throws IOException
	 * @throws BufferPoolException
	 * @throws PageFormatException
	 */
	private void insertInParent(BTreeInnerNodePage parent, DataField key, int leftPageNumber, int rightPageNumber)
				throws IOException, BufferPoolException, PageFormatException {
		
		
		/**
		 * 
		 * |4|5|8|
		 * | | | | |
		 * 
		 * 			|6|
		 * 			  | |
		 */
		
		
		// The root was split : create a new root with the given key & pointers
		if (parent == null) {
			
			BTreeInnerNodePage newRoot = (BTreeInnerNodePage) bufferPool.createNewPageAndPin(resourceId, BTreeIndexPageType.INNER_NODE_PAGE);

			
			newRoot.initRootState(key, leftPageNumber, rightPageNumber);
			schema.setRootPageNumber(newRoot.getPageNumber());
			
			bufferPool.unpinPage(resourceId, newRoot.getPageNumber());
			
		} else {
		
			// Try to insert in the parent
			if (!parent.insertKeyPageNumberPair(key, rightPageNumber)) {
				
				// Parent is full
				
				
				// Move half of the pair from full leaf to new one
				
				/**
				 *  |4|			|5| (dropped)			|8|
				 *  | | |								| | |
				 * 
				 */
				

				
				// Insert the key-value pair
				
				/**										 
				 * 										 ↓
				 *  |4|			|5| (dropped)			|6|8|
				 *  | | |								| | | |
				 * 
				 */
				
				// Insert dropped key in parent
				
				/**
				 * 
				 * 				|1|5|8|
				 * 				| | | | |
				 * 				   ↑
				 * 						   
				 *  |4|					|6|8|
				 *  | | |				| | | |
				 * 
				 */
				
				
					
				// Split	
				BTreeInnerNodePage rightParent = (BTreeInnerNodePage) bufferPool.createNewPageAndPin(resourceId, BTreeIndexPageType.INNER_NODE_PAGE);
	
				
				
				
				// Look where the new key has to be inserted (compare with key in the middle of the node)
				
				int compare = key.compareTo(parent.getKey(schema.getFanOut()/2 - 1));
				
				DataField dropped;
				
				if (compare < 0) {
					// Key will be inserted in left node : move N/2+1 key in new right node
					
					dropped = parent.moveLastToNewPage(rightParent, schema.getFanOut()/2 + 1);
					
					// Insert key in first node
					parent.insertKeyPageNumberPair(key, rightPageNumber);

					
				} else {
					// Key will be inserted in right node : move N/2 key in new right node
					
					dropped = parent.moveLastToNewPage(rightParent, schema.getFanOut()/2);
					
					// Insert key in new right node
					rightParent.insertKeyPageNumberPair(key, rightPageNumber);

					
					
				}
				
				

				
				BTreeInnerNodePage grandParent = findParent(parent.getPageNumber(), key);
				
				insertInParent(grandParent, dropped, parent.getPageNumber(), rightParent.getPageNumber());
	

				// Unpin pages
				
				bufferPool.unpinPage(resourceId, rightParent.getPageNumber());
				
				if (grandParent != null)
					bufferPool.unpinPage(resourceId, grandParent.getPageNumber());				
			}			
		}
	}
	
	
	
	
	/**
	 * Find and retrieve the leaf of the B-Tree containting the first occurence of the given key
	 * @param key to find
	 * @return The first leaf containing the given key
	 */
	private BTreeLeafPage findLeaf(DataField key) throws IOException, PageFormatException {
		
		
		
		try {			
			
			BTreeIndexPage child = (BTreeIndexPage) bufferPool.getPageAndPin(resourceId, schema.getRootPageNumber());
			
			BTreeInnerNodePage parent = null;
			

			
			// Loop through the inner nodes until we get to the leaf corresponding to the given key
			while (child instanceof BTreeInnerNodePage) {
				
				// unpin the page before getting the next one
				if (parent != null)
					bufferPool.unpinPage(resourceId, parent.getPageNumber());
							
				parent = (BTreeInnerNodePage) child;
	
				int childPageNumber = parent.getChildPageForKey(key);
			
				child = (BTreeIndexPage) bufferPool.getPageAndPin(resourceId, childPageNumber);
						
			}
			
			if (parent != null)
				bufferPool.unpinPage(resourceId, parent.getPageNumber());
			
			
			return (BTreeLeafPage) child;
			

		} catch (BufferPoolException e) {

			throw new IndexFormatCorruptException(e);
		}			
	}
	
	
	/**
	 * Find and retrieve the leaf of the B-Tree containting the first occurence of the given key
	 * @param key to find
	 * @return The first leaf containing the given key
	 */
	private BTreeInnerNodePage findParent(int pageNumber, DataField key) throws IOException, PageFormatException {
		
		
		
		try {
			
			BTreeIndexPage child = (BTreeIndexPage) bufferPool.getPageAndPin(resourceId, schema.getRootPageNumber());
			
			
			BTreeInnerNodePage parent = null;
		
			while (child instanceof BTreeInnerNodePage) {
				
				
				if (parent != null)
					bufferPool.unpinPage(resourceId, parent.getPageNumber());

				parent = (BTreeInnerNodePage) child;
				
				
				int childPageNumber = parent.getChildPageForKey(key);
				
				if (childPageNumber == pageNumber)
					return parent;
				
				child = (BTreeIndexPage) bufferPool.getPageAndPin(resourceId, childPageNumber);
				
			} 

		} catch (BufferPoolException e) {

			throw new IndexFormatCorruptException(e);
		}		
		return null;
			
	}
}
