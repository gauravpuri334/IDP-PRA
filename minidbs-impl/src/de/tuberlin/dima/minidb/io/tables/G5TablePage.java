package de.tuberlin.dima.minidb.io.tables;

import de.tuberlin.dima.minidb.catalogue.TableSchema;
import de.tuberlin.dima.minidb.core.DataField;
import de.tuberlin.dima.minidb.core.DataTuple;
import de.tuberlin.dima.minidb.core.DataType;
import de.tuberlin.dima.minidb.io.cache.PageExpiredException;
import de.tuberlin.dima.minidb.io.cache.PageFormatException;
import de.tuberlin.dima.minidb.qexec.LowLevelPredicate;

public class G5TablePage implements TablePage {
	
	public static final int HEADER_POS_MAGIC_NUMBER = 0;
	public static final int HEADER_POS_PAGE_NUMBER = 4;
	public static final int HEADER_POS_NUMBER_RECORDS = 8;
	public static final int HEADER_POS_RECORD_WIDTH = 12;
	public static final int HEADER_POS_CHUNK_OFFSET = 16;
	
	private byte[] binaryPage;
	private TableSchema schema;
	
	private boolean expired;
	private boolean modified;
	
	public static int debug = 0;
	

	
	/**
	 * Create a page from an existing bufer
	 * 
	 * @param schema The schema of the table
	 * @param binaryPage The existing buffer to build the TablePage from
	 * @throws PageFormatException if the buffer is not correctly formed
	 */
	public G5TablePage(TableSchema schema, byte[] binaryPage) throws PageFormatException {
		
		this.binaryPage = binaryPage;
		this.schema = schema;
		
		expired = false;
		modified = false; // Page already exists, not modified
		
		
		/* Check if page is well-formed */
		
		if (readIntByteArray(binaryPage, HEADER_POS_MAGIC_NUMBER) != TablePage.TABLE_DATA_PAGE_HEADER_MAGIC_NUMBER) {
			throw new PageFormatException("Magic number doesn't match");
		}

	}
	
	/**
	 * Create a new page and initialize its header
	 * @param schema
	 * @param binaryPage
	 * @param newPageNumber
	 */
	public G5TablePage(TableSchema schema, byte[] binaryPage, int newPageNumber) throws PageFormatException {
		
		
		/* TODO Should check if the array size does not match the page size for this table schema. */
		
		
		this.binaryPage = binaryPage;
		expired = false;
		modified = true;	// new pages are considered already modified
		
		int recordWidth = 4;	// 4 bytes for metadata
		
		
		// Compute the whole records length
		
		for (int i = 0; i < schema.getNumberOfColumns(); i++) {
			
			
			DataType dataType = schema.getColumn(i).getDataType();
			
			if (dataType.isFixLength()) {
				
				recordWidth += dataType.getNumberOfBytes();
			} else {
				recordWidth += 8;
			}			
		}

		
		// Write the header page (32 bytes)
		writeIntByteArray(binaryPage, HEADER_POS_MAGIC_NUMBER, TablePage.TABLE_DATA_PAGE_HEADER_MAGIC_NUMBER);
		writeIntByteArray(binaryPage, HEADER_POS_PAGE_NUMBER, newPageNumber);
		writeIntByteArray(binaryPage, HEADER_POS_NUMBER_RECORDS, 0);
		writeIntByteArray(binaryPage, HEADER_POS_RECORD_WIDTH, recordWidth);
		writeIntByteArray(binaryPage, HEADER_POS_CHUNK_OFFSET, binaryPage.length);
			
		this.schema = schema;
		
	}
	

	@Override
	public boolean hasBeenModified() throws PageExpiredException {

		if (expired) throw new PageExpiredException();
		return modified;
	}

	@Override
	public void markExpired() {
		expired = true;
		
	}

	@Override
	public boolean isExpired() {
		return expired;
	}

	@Override
	public byte[] getBuffer() {
		
		return binaryPage;
	}

	@Override
	public int getPageNumber() throws PageExpiredException {
		
		if (expired) throw new PageExpiredException();
		
		return readIntByteArray(binaryPage, HEADER_POS_PAGE_NUMBER);
	}

	@Override
	public int getNumRecordsOnPage() throws PageExpiredException {
		
		if (expired) throw new PageExpiredException();
		
		return readIntByteArray(binaryPage, HEADER_POS_NUMBER_RECORDS);
	}
	
	private int getRecordWidth() throws PageExpiredException {
		
		if (expired) throw new PageExpiredException();
		
		return readIntByteArray(binaryPage, HEADER_POS_RECORD_WIDTH);
	}
	
	private int getChunkOffset() throws PageExpiredException {
		
		if (expired) throw new PageExpiredException();
		
		return readIntByteArray(binaryPage, HEADER_POS_CHUNK_OFFSET);
	}
	
	public int getTombstone(int position) throws PageExpiredException {
		
		if (expired) throw new PageExpiredException();
		
		int offset = 32 + position * readIntByteArray(binaryPage, HEADER_POS_RECORD_WIDTH);
		
		return readIntByteArray(binaryPage, offset);
	}
	
	

	@Override
	public boolean insertTuple(DataTuple tuple) throws PageFormatException,
			PageExpiredException {
		
		if (expired) throw new PageExpiredException();

		/* TODO : Throw exception, if the format of the page is invalid, such as that
		 *  current offset to the variable-length-chunk is invalid. */
		
		int chunkWidth = 0;
		int recordOffset = getNumRecordsOnPage()*getRecordWidth() + 32;

		
	for (int i = 0; i < tuple.getNumberOfFields(); i++) {
			
			
			DataField field = tuple.getField(i);
			
			if (!field.getBasicType().equals(schema.getColumn(i).getDataType().getBasicType()))
				throw new PageFormatException("Tuple and page format not coherent");
			
			
			
			
			if (!field.getBasicType().isFixLength()) {
				
				chunkWidth += field.getNumberOfBytes();
			}
			
			//System.out.println(schema.getColumn(i).getColumnName() + " : " + schema.getColumn(i).getDataType() + ", "  + schema.getColumn(i).getDataType().getNumberOfBytes() + ", fixed : " + (schema.getColumn(i).getDataType().isFixLength() ? "yes" : "no"));	
		}

	if (chunkWidth + getRecordWidth()  > getChunkOffset() - recordOffset)
		return false;
		
		
	
	writeIntByteArray(binaryPage, recordOffset, 0);
	
	int currentRecordOffset = recordOffset +4;
	int currentChunkOffset = getChunkOffset();
	
	for (int i = 0; i < tuple.getNumberOfFields(); i++) {
		
		
		DataField field = tuple.getField(i);
		
		
		if (field.getBasicType().isFixLength()) {

			field.encodeBinary(binaryPage, currentRecordOffset);
			currentRecordOffset += schema.getColumn(i).getDataType().getNumberOfBytes();
			
		} else {
			
			
			if (field.isNULL()) {
				writeIntByteArray(binaryPage, currentRecordOffset, 0);
				writeIntByteArray(binaryPage, currentRecordOffset + 4, 0);
			} else {
				currentChunkOffset -= field.getNumberOfBytes();
				
				
					field.encodeBinary(binaryPage, currentChunkOffset);
	
				
				
				
				writeIntByteArray(binaryPage, currentRecordOffset, currentChunkOffset);
				writeIntByteArray(binaryPage, currentRecordOffset + 4, field.getNumberOfBytes());
								
			}
			currentRecordOffset += 8;	
		}
		
		//System.out.println(schema.getColumn(i).getColumnName() + " : " + schema.getColumn(i).getDataType() + ", "  + schema.getColumn(i).getDataType().getNumberOfBytes() + ", fixed : " + (schema.getColumn(i).getDataType().isFixLength() ? "yes" : "no"));	
	}
	
	writeIntByteArray(binaryPage, HEADER_POS_NUMBER_RECORDS, getNumRecordsOnPage()+1);
	writeIntByteArray(binaryPage, HEADER_POS_CHUNK_OFFSET, currentChunkOffset);

		this.modified = true;
		
		return true;
	}

	@Override
	public void deleteTuple(int position) throws PageTupleAccessException,
			PageExpiredException {
		if (expired) throw new PageExpiredException();

		
		int recordOffset = readIntByteArray(binaryPage, HEADER_POS_RECORD_WIDTH) * position + 32;
		
		writeIntByteArray(binaryPage, recordOffset, 1);
		
		this.modified = true;
		
	}

	@Override
	public DataTuple getDataTuple(int position, long columnBitmap, int numCols)
			throws PageTupleAccessException, PageExpiredException {
		
		if (expired) throw new PageExpiredException();
		
		if (position > readIntByteArray(binaryPage, HEADER_POS_NUMBER_RECORDS) || position < 0) 
			throw new PageTupleAccessException(position, "index negative or larger than the number of tuple on the page");

		
		int recordOffset = readIntByteArray(binaryPage, HEADER_POS_RECORD_WIDTH) * position + 32;
		
		if (( readIntByteArray(binaryPage, recordOffset) & 0x1) == 1)
			return null;
		
		
		

		recordOffset += 4;
		
		DataTuple tuple = new DataTuple(numCols);
		DataType type;
		DataField field;
		int currentColumn = 0;
		
		

		for (int i =0; i < schema.getNumberOfColumns(); i++) {
			
			type = schema.getColumn(i).getDataType();

			
			
			if ((columnBitmap & 0x1) == 0) {
			
				if (type.isFixLength()) {
					recordOffset += type.getNumberOfBytes();
					
				} else {					
					recordOffset += 8;
				}

			} else {

				
			
			
			
			
			
			
				if (type.isFixLength()) {
				
					field = type.getFromBinary(binaryPage, recordOffset);
				
					recordOffset += type.getNumberOfBytes();

					if (field.getNumberOfBytes() != type.getNumberOfBytes() ) {

				
					}
				
				} else  {
					int start = readIntByteArray(binaryPage, recordOffset);
					int length = readIntByteArray(binaryPage, recordOffset+4);
					
					if(start == 0 && length ==0) {
						field = type.getNullValue();
						
					} else {
						
	
							field = type.getFromBinary(binaryPage, start, length);	
							
					}				
					recordOffset += 8;
							
				}

				tuple.assignDataField(field, currentColumn);
				
				currentColumn++;
			}	
			
			columnBitmap >>>=1;
		}
		

		return tuple;
	}

	@Override
	public DataTuple getDataTuple(LowLevelPredicate[] preds, int position,
			long columnBitmap, int numCols) throws PageTupleAccessException,
			PageExpiredException {
		
		/* TODO should be optimized and not just use the normal method */
				
		if (expired) throw new PageExpiredException();
		
		if (position > readIntByteArray(binaryPage, HEADER_POS_NUMBER_RECORDS) || position < 0) 
			throw new PageTupleAccessException(position, "index negative or larger than the number of tuple on the page");

		
		int recordOffset = readIntByteArray(binaryPage, HEADER_POS_RECORD_WIDTH) * position + 32;
		
		if (( readIntByteArray(binaryPage, recordOffset) & 0x1) == 1)
			return null;
		
		

		recordOffset += 4;
	
		
		DataTuple tuple = new DataTuple(numCols);
		DataType type;
		DataField field;
		int currentColumn = 0;
		
		

		for (int i =0; i < schema.getNumberOfColumns(); i++) {
			
			type = schema.getColumn(i).getDataType();
			
			
			
				if (type.isFixLength()) {
				
					field = type.getFromBinary(binaryPage, recordOffset);

				
					recordOffset += type.getNumberOfBytes();

				
				} else  {
					int start = readIntByteArray(binaryPage, recordOffset);
					int length = readIntByteArray(binaryPage, recordOffset+4);
					
					if(start == 0 && length ==0) {
						field = type.getNullValue();
						
					} else {
						
	
							field = type.getFromBinary(binaryPage, start, length);	
							
					}				
					recordOffset += 8;							
				}
				
				
				
				for(int j = 0; j < preds.length; j++) {
					if (preds[j].getColumnIndex() == i) {						
						if (!preds[j].evaluateWithNull(field))
							return null;
					}
				}
					

					
				if ((columnBitmap & 0x1) == 1) {
					tuple.assignDataField(field, currentColumn);
					
					currentColumn++;
				}
				
			
			columnBitmap >>>=1;
		}
		

		return tuple;
		
		
	}

	@Override
	public TupleIterator getIterator(int numCols, long columnBitmap)
			throws PageTupleAccessException, PageExpiredException {
		
		if (expired) throw new PageExpiredException();

		
		return new G5TupleIterator(this, numCols, columnBitmap);
	}

	@Override
	public TupleIterator getIterator(LowLevelPredicate[] preds, int numCols,
			long columnBitmap) throws PageTupleAccessException,
			PageExpiredException {
		
		if (expired) throw new PageExpiredException();

		return new G5TupleIterator(this, preds, numCols, columnBitmap);
	}

	@Override
	public TupleRIDIterator getIteratorWithRID()
			throws PageTupleAccessException, PageExpiredException {
		
		if (expired) throw new PageExpiredException();

		int numCols = schema.getNumberOfColumns();
		
		return new G5TupleRIDIterator(this, numCols, Long.MAX_VALUE);
	}
	
	

	public void writeIntByteArray(byte[] buffer, int offset, int value) {

		
		buffer[offset    ] = (byte) (value       );
		buffer[offset + 1] = (byte) (value >>>  8);
		buffer[offset + 2] = (byte) (value >>> 16);
		buffer[offset + 3] = (byte) (value >>> 24);
		
	}
	
	public int readIntByteArray(byte[] byteArray, int offset) {
		
		int value = (byteArray[offset    ]        & 0x000000ff) |
		         ((byteArray[offset + 1] <<  8) & 0x0000ff00) |
		         ((byteArray[offset + 2] << 16) & 0x00ff0000) |
			     ((byteArray[offset + 3] << 24) & 0xff000000);
		
		return value;
	}

}
