package de.tuberlin.dima.minidb.mapred;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import de.tuberlin.dima.minidb.core.DataTuple;
import de.tuberlin.dima.minidb.io.cache.PageExpiredException;
import de.tuberlin.dima.minidb.io.manager.BufferPoolException;
import de.tuberlin.dima.minidb.io.manager.BufferPoolManager;
import de.tuberlin.dima.minidb.io.tables.PageTupleAccessException;
import de.tuberlin.dima.minidb.io.tables.TablePage;
import de.tuberlin.dima.minidb.io.tables.TableResourceManager;
import de.tuberlin.dima.minidb.io.tables.TupleIterator;

public class G5TableInputFormat extends TableInputFormat {

	@Override
	public RecordReader<Text, DataTuple> createRecordReader(InputSplit split,
			TaskAttemptContext context) throws IOException,
			InterruptedException {
		
		final int prefetchWindow = context.getConfiguration().getInt(INPUT_PREFETCH_WINDOW, 0);
		

		
		
		final BufferPoolManager bufferPool = TableInputFormat.getDBInstance().getBufferPool();
		

		
		return new RecordReader<Text, DataTuple>() {

			TableResourceManager table;
			int resId;
			Text key;
			int firstPage, lastPage;
			
			
			int pageNbr;
			TablePage page;
			TupleIterator iterator;
			

			
			
			DataTuple value;
			
			@Override
			public void close() throws IOException {
				
				bufferPool.unpinPage(resId, pageNbr);
				
			}

			@Override
			public Text getCurrentKey() throws IOException,
					InterruptedException {
				if (key == null)
					System.out.println("Key is null");
				return key;
			}

			@Override
			public DataTuple getCurrentValue() throws IOException,
					InterruptedException {
				
				if (value == null)
					System.out.println("Value is null");
				return value;
			}

			@Override
			public float getProgress() throws IOException, InterruptedException {
				
				return (1.0f*pageNbr - firstPage)/(lastPage - firstPage);
			}

			@Override
			public void initialize(InputSplit split, TaskAttemptContext context)
					throws IOException, InterruptedException {


				
				
				if (! (split instanceof G5InputSplit))
					throw new IOException("Bad input split type : " + split.getClass());
				
				table = TableInputFormat.getDBInstance().getCatalogue().getTable(((G5InputSplit)split).getTableName()).getResourceManager();
				resId = TableInputFormat.getDBInstance().getCatalogue().getTable(((G5InputSplit)split).getTableName()).getResourceId();

				key = new Text(((G5InputSplit)split).getTableName());
				
				firstPage = ((G5InputSplit)split).getFirstPageNumber();
				lastPage = ((G5InputSplit)split).getLastPageNumber();
				
				pageNbr = firstPage;
				
				
				
				try {
					page = (TablePage) bufferPool.getPageAndPin(resId, pageNbr);
					iterator = page.getIterator(table.getSchema().getNumberOfColumns(), Long.MAX_VALUE);
					bufferPool.prefetchPages(resId, pageNbr+1, pageNbr + prefetchWindow + 1);
					
					
					
				} catch (BufferPoolException | PageExpiredException | PageTupleAccessException e) {
					throw new IOException(e);
				}
				
				
				
			}

			@Override
			public boolean nextKeyValue() throws IOException,
					InterruptedException {

				try {

					
					if (iterator.hasNext()) {
						value = iterator.next();
						return true;
					} else {
											
						pageNbr++;
						
						// If no page left : go to next table
						if (pageNbr > lastPage)
							return false;
						
						
						
						// Get next page
						page = (TablePage) bufferPool.unpinAndGetPageAndPin(resId, pageNbr - 1, pageNbr);
						
						if (pageNbr + prefetchWindow < lastPage);
							bufferPool.prefetchPage(resId, pageNbr + prefetchWindow);
							
						// Get next page tuple iterator
											
						iterator = page.getIterator(table.getSchema().getNumberOfColumns(), Long.MAX_VALUE);
						
						value = iterator.next();
						return true;
					}
					
				} catch (BufferPoolException | PageExpiredException | PageTupleAccessException e) {
					throw new IOException(e);
				}
			}		
		};
	}

	@Override
	public List<InputSplit> getSplits(JobContext context) throws IOException,
			InterruptedException {
		
		String[] tableNames = context.getConfiguration().get(INPUT_TABLE_NAMES).split(",");
	
		int splitSize = context.getConfiguration().getInt(INPUT_SPLIT_SIZE, -1);
		
		int nbrSplits= context.getConfiguration().getInt(SPLITS_PER_TABLE, 0);
		
		
		ArrayList<InputSplit> splits = new ArrayList<InputSplit>();
		
		
		System.out.println("Creating splits");
		// Split the tables in splits of "splitSize" pages
		if (splitSize != -1) {
			
			System.out.println("By split size");
			for (String tableName : tableNames) {
				TableResourceManager table = TableInputFormat.getDBInstance().getCatalogue().getTable(tableName).getResourceManager();
				
				int firstPage = table.getFirstDataPageNumber();
				int lastPage = table.getLastDataPageNumber();
				nbrSplits = (int) Math.ceil((lastPage - firstPage + 1) / (1.0f*splitSize));
				
				for (int i = 0; i < nbrSplits; i++) {
					splits.add(new G5InputSplit(tableName, firstPage + i*splitSize, Math.min(firstPage + (i+1) * splitSize - 1, lastPage)));
				}			
			}	
			
		// Split the tables in "nbrSplits" per table
		} else {
			
			System.out.println("By split number : " + nbrSplits);
			for (String tableName : tableNames) {
				
				TableResourceManager table = TableInputFormat.getDBInstance().getCatalogue().getTable(tableName).getResourceManager();

				int firstPage = table.getFirstDataPageNumber(); // 1
				int lastPage = table.getLastDataPageNumber(); // 2 
				
				System.out.println("First : " + firstPage + ", last : " + lastPage);
				
				
				if (nbrSplits > lastPage - firstPage + 1)
					nbrSplits = lastPage - firstPage + 1; // 2
				
				splitSize = (int)Math.ceil((lastPage - firstPage + 1)/(1.0f * nbrSplits));		
				
				for (int i = 0; i < nbrSplits; i++) {
					System.out.println(firstPage + i*splitSize + " - " + Math.min(firstPage + (i+1) * splitSize - 1, lastPage));
					splits.add(new G5InputSplit(tableName, firstPage + i*splitSize, Math.min(firstPage + (i+1) * splitSize - 1, lastPage)));
				}
			}
		}
		return splits;
	}

}
