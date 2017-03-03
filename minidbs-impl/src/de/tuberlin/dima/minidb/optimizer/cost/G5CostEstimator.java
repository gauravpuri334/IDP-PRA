package de.tuberlin.dima.minidb.optimizer.cost;

import de.tuberlin.dima.minidb.catalogue.IndexDescriptor;
import de.tuberlin.dima.minidb.catalogue.TableDescriptor;
import de.tuberlin.dima.minidb.core.DataType;
import de.tuberlin.dima.minidb.io.cache.PageSize;
import de.tuberlin.dima.minidb.optimizer.OptimizerPlanOperator;
import de.tuberlin.dima.minidb.qexec.heap.QueryHeap;
import de.tuberlin.dima.minidb.semantics.Column;
import de.tuberlin.dima.minidb.semantics.predicate.LocalPredicate;

public class G5CostEstimator implements CostEstimator {

	private long readCost;
	private long writeCost;
	private long randomReadOverhead;
	private long randomWriteOverhead;
	
	

	public G5CostEstimator(long readCost, long writeCost,
			long randomReadOverhead, long randomWriteOverhead) {
		
			this.readCost = readCost;
			this.writeCost = writeCost;
			this.randomReadOverhead = randomReadOverhead;
			this.randomWriteOverhead = randomWriteOverhead;
	}

	@Override
	public long computeTableScanCosts(TableDescriptor table) {
		
		PageSize pageSize = table.getSchema().getPageSize();
		
		int pageNbr = table.getStatistics().getNumberOfPages();
		
		return randomReadOverhead + pageNbr * getReadCost(pageSize);
	}

	@Override
	public long computeIndexLookupCosts(IndexDescriptor index,
			TableDescriptor baseTable, long cardinality) {
		
		PageSize indexPageSize = index.getSchema().getPageSize();
		PageSize tablePageSize = baseTable.getSchema().getPageSize();
		
		long BTreeCost = (index.getStatistics().getTreeDepth() - 2) * (getReadCost(indexPageSize) + randomReadOverhead);
		
		long pageNbr;
		
		if (cardinality == baseTable.getStatistics().getCardinality())
			pageNbr = index.getStatistics().getNumberOfLeafs() + 1;
		else
			pageNbr = (long) Math.ceil(index.getStatistics().getNumberOfLeafs() * 1.0f * cardinality / baseTable.getStatistics().getCardinality());
		
		long scanCost = randomReadOverhead + pageNbr * getReadCost(tablePageSize);

		return BTreeCost + scanCost;
	}

	@Override
	public long computeSortCosts(Column[] columnsInTuple, long numTuples) {
		
		
		
		DataType[] types = new DataType[columnsInTuple.length];
		
		for(int i = 0; i < columnsInTuple.length; i++) {			
			types[i] = columnsInTuple[i].getDataType();
		}
		
		long tupleSize = QueryHeap.getTupleBytes(types);
		
		PageSize pageSize = QueryHeap.getPageSize();
		
		long tuplePerBlock = (long) Math.floor(1.0f * (pageSize.getNumberOfBytes() -32) / tupleSize);
		
		long nbrOfBlocks = (long) Math.max(Math.floor(1.0f * numTuples / tuplePerBlock), 1);
		
		long wCost = randomWriteOverhead + nbrOfBlocks * getWriteCost(pageSize);
		
		long rCost = nbrOfBlocks * (getReadCost(pageSize) + randomReadOverhead / 16);
		
		
	/*	System.out.println("Num tuple : " + numTuples);
		System.out.println("Size : " + tupleSize);
		System.out.println("per Block : " + tuplePerBlock);
		System.out.println("nbr blocks : " + nbrOfBlocks);
		System.out.println("Write : " + wCost);
		System.out.println("Read : " + rCost);
	*/
		return wCost + rCost;
	}

	@Override
	public long computeFetchCosts(TableDescriptor fetchedTable,
			long cardinality, boolean sequential) {
		
		
		PageSize pageSize = fetchedTable.getSchema().getPageSize();
		int totalPages = fetchedTable.getStatistics().getNumberOfPages();
		

		

		
		if (sequential) {

			
			float k = 1.0f*(totalPages - 1) / totalPages;
			
		//	double nbrPages =  (1/k - 1/(k- Math.pow(k, 2))) * Math.pow(k, cardinality)+ 1/(1-k);
			
			double nbrPages = Math.floor((1 - Math.pow(k, cardinality))/(1-k));
			//nbrPages = 249.05f;
			
			double ratio = 1 - (1.0f*nbrPages / totalPages) * (7.0f / 8.0f);
			
			
			/*System.out.println("k : " + k);
			System.out.println("total pages : " + totalPages);
			System.out.println("pages : " + nbrPages);
			System.out.println("ratio : " + ratio);
			System.out.println("PageSize : " + pageSize.toString());
		*/	

			return (long)Math.floor(nbrPages * (getReadCost(pageSize) + ratio * randomReadOverhead));
			
		} else {
			
			double B = Math.pow(10 * totalPages, 1.0 / 0.66);
			double H = Math.min(Math.log(cardinality) / Math.log(B), 0.8);
			
			return (long) Math.floor(cardinality * (1-H) * (getReadCost(pageSize) + randomReadOverhead));
		}
		
	}

	@Override
	public long computeFilterCost(LocalPredicate pred, long cardinality) {
		return 0;
	}

	@Override
	public long computeMergeJoinCost() {
		return 0;
	}

	@Override
	public long computeNestedLoopJoinCost(long outerCardinality,
			OptimizerPlanOperator innerOp) {
		
		return innerOp.getCumulativeCosts() * outerCardinality;
	}


	
	private long getReadCost(PageSize pageSize) {
		
		return (readCost * pageSize.getNumberOfBytes()) / PageSize.getDefaultPageSize().getNumberOfBytes();		
	}
	
private long getWriteCost(PageSize pageSize) {
		
		return (writeCost * pageSize.getNumberOfBytes()) / PageSize.getDefaultPageSize().getNumberOfBytes();		
	}
}
