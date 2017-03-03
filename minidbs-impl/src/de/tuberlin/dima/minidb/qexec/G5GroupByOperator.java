package de.tuberlin.dima.minidb.qexec;

import de.tuberlin.dima.minidb.core.DataTuple;
import de.tuberlin.dima.minidb.core.DataType;
import de.tuberlin.dima.minidb.parser.OutputColumn.AggregationType;

public class G5GroupByOperator implements GroupByOperator {

	private PhysicalPlanOperator child;
	private int[] groupColumnIndices;
	private int[] aggColumnIndices;
	private int[] groupColumnOutputPositions;
	private int[] aggregateColumnOutputPosition;
	
	private Aggregate[] aggregates;
	
	
	private DataTuple groupTuple;
	
	private boolean hasNext;

	public G5GroupByOperator(PhysicalPlanOperator child, int[] groupColumnIndices, int[] aggColumnIndices,
						AggregationType[] aggregateFunctions, DataType[] aggColumnTypes,
						int[] groupColumnOutputPositions, int[] aggregateColumnOutputPosition) {
				
		this.child = child;
		this.groupColumnIndices = groupColumnIndices;
		this.aggColumnIndices = aggColumnIndices;
		this.groupColumnOutputPositions = groupColumnOutputPositions;
		this.aggregateColumnOutputPosition = aggregateColumnOutputPosition;
		
		aggregates = new Aggregate[aggColumnIndices.length];
		for( int i = 0; i < aggColumnIndices.length; i++) {
			aggregates[i] = new Aggregate(aggregateFunctions[i], aggColumnTypes[i]);
		}
	}

	@Override
	public void open(DataTuple correlatedTuple) throws QueryExecutionException {
		
		child.open(correlatedTuple);
		
		groupTuple = child.next();

		hasNext = groupTuple != null || groupColumnIndices.length == 0;
		 
		
		if (groupTuple != null) {
			for( int i = 0; i <aggColumnIndices.length; i++) {					
				aggregates[i].aggregate(groupTuple.getField(aggColumnIndices[i]));					
			}
		}


	}

	@Override
	public DataTuple next() throws QueryExecutionException {
		
		DataTuple currentTuple;
		
		if (!hasNext)
			return null;
			
		
		while ((currentTuple = child.next()) != null) {
			
			boolean sameGroup = true;
			
			for( int i : groupColumnIndices) {
				if ( ! currentTuple.getField(i).equals(groupTuple.getField(i)))
					sameGroup = false;
			}
			
			
			if( !sameGroup) {

				DataTuple outputTuple = getOutputTuple();
				
				groupTuple = currentTuple;

				for( int i = 0; i <aggColumnIndices.length; i++) {	
					aggregates[i].reset();
					aggregates[i].aggregate(currentTuple.getField(aggColumnIndices[i]));					
				}
				
				return outputTuple;
				
				
			} else {
				for( int i = 0; i <aggColumnIndices.length; i++) {					
					aggregates[i].aggregate(currentTuple.getField(aggColumnIndices[i]));					
				}
			}
			
			
		}
		
		
		hasNext = false;
		
		return getOutputTuple();
	}

	@Override
	public void close() throws QueryExecutionException {
		// TODO Auto-generated method stub

	}
	
	
	
	private DataTuple getOutputTuple() {

		DataTuple outputTuple = new DataTuple(groupColumnOutputPositions.length);
		
		for (int i = 0; i < groupColumnOutputPositions.length; i++) {
			int index = groupColumnOutputPositions[i];
				if (index != -1) {
					outputTuple.assignDataField(groupTuple.getField(groupColumnIndices[index]), i);
				}
		}
		
		for (int i = 0; i < aggregateColumnOutputPosition.length; i++) {
			int index = aggregateColumnOutputPosition[i];
			if (index != -1) {
				outputTuple.assignDataField(aggregates[index].getAggregate(), i);
			}
		}
		
		return outputTuple;
	}

}
