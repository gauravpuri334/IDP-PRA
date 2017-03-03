package de.tuberlin.dima.minidb.qexec;

import de.tuberlin.dima.minidb.core.ArithmeticType;
import de.tuberlin.dima.minidb.core.DataField;
import de.tuberlin.dima.minidb.core.DataType;
import de.tuberlin.dima.minidb.core.IntField;
import de.tuberlin.dima.minidb.parser.OutputColumn.AggregationType;

public class Aggregate {
	
	AggregationType aggregation;
	ArithmeticType<DataField> sum;
	IntField count;
	DataField min, max;
	private DataType dataType;
	
	/*
	 * Aggregation types : 
	 * 		NONE, COUNT, SUM, AVG, MIN, MAX
	 */
	
	
	public Aggregate(AggregationType aggregation, DataType dataType) {
		this.aggregation = aggregation;
		this.dataType = dataType;
		this.count = new IntField(0);
		this.min = dataType.getNullValue();
		this.max = dataType.getNullValue();
		
		if(dataType.isArithmeticType()) {
			sum = dataType.createArithmeticZero();
		}
		
	}
	
	public void reset() {
		this.count = new IntField(0);
		this.min = dataType.getNullValue();
		this.max = dataType.getNullValue();
		
		if(dataType.isArithmeticType()) {
			sum = dataType.createArithmeticZero();
		}
	}
	
	public void aggregate(DataField field) {
		
		if(aggregation == AggregationType.COUNT) {
			count.add(1);
			
		} else if (aggregation == AggregationType.SUM) {
			sum.add(field);
			
		} else if (aggregation == AggregationType.AVG) {
			count.add(1);
			sum.add(field);
			
		} else if (aggregation == AggregationType.MIN) {
			if(min.isNULL() || min.compareTo(field) > 0) {
				min = field;
			}
			
		} else if (aggregation == AggregationType.MAX) {
			
			if (max.isNULL() || max.compareTo(field) < 0) {
				max = field;
			}
		}
		
		
		
	}
	
	public DataField getAggregate() {
		
		if(aggregation == AggregationType.COUNT) {
			return count;
			
		} else if (aggregation == AggregationType.SUM) {
			return (DataField) sum;
			
		} else if (aggregation == AggregationType.AVG) {
			if (count.isZero())
				return dataType.getNullValue();
			
			ArithmeticType<DataField> avg = sum;
			avg.divideBy(count.getValue());
			
			return (DataField) avg;
			
		} else if (aggregation == AggregationType.MIN) {
			return min;
			
		} else if (aggregation == AggregationType.MAX) {
			return max;
		} else {
			return dataType.getNullValue();
		}
	}
	
	
	public static DataType getResultingType(AggregationType aggregation, DataType type) {
		if(aggregation == AggregationType.COUNT) {
			return DataType.intType();
			
		} else if (aggregation == AggregationType.SUM) {
			return type;
			
		} else if (aggregation == AggregationType.AVG) {

			return DataType.floatType();
			
		} else if (aggregation == AggregationType.MIN) {
			return type;
			
		} else if (aggregation == AggregationType.MAX) {
			return type;
		} else {
			return type;
		}
	}

}
