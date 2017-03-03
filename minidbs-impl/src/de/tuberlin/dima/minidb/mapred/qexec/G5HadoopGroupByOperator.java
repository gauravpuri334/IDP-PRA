package de.tuberlin.dima.minidb.mapred.qexec;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import de.tuberlin.dima.minidb.DBInstance;
import de.tuberlin.dima.minidb.catalogue.ColumnSchema;
import de.tuberlin.dima.minidb.catalogue.TableSchema;
import de.tuberlin.dima.minidb.core.DataTuple;
import de.tuberlin.dima.minidb.core.DataType;
import de.tuberlin.dima.minidb.mapred.SerializationAggUtils;
import de.tuberlin.dima.minidb.mapred.SerializationUtils;
import de.tuberlin.dima.minidb.parser.OutputColumn.AggregationType;
import de.tuberlin.dima.minidb.qexec.Aggregate;

public class G5HadoopGroupByOperator extends HadoopOperator<DataTuple, DataTuple> {
	
	private final static String CONF_GROUP_COLS= "de.tuberlin.dima.minidb.mapred.qexec.G5HadoopGroupByOperator.groupCols";
	private final static String CONF_GROUP_OUT_COLS= "de.tuberlin.dima.minidb.mapred.qexec.G5HadoopGroupByOperator.groupOutCols";
	
	private final static String CONF_AGG_COLS= "de.tuberlin.dima.minidb.mapred.qexec.G5HadoopGroupByOperator.aggCols";
	private final static String CONF_AGG_TYPES = "de.tuberlin.dima.minidb.mapred.qexec.G5HadoopGroupByOperator.aggTypes";
	private final static String CONF_AGG_DATA_TYPES = "de.tuberlin.dima.minidb.mapred.qexec.G5HadoopGroupByOperator.aggDataTypes";
	private final static String CONF_AGG_OUT_COLS = "de.tuberlin.dima.minidb.mapred.qexec.G5HadoopGroupByOperator.aggOutCols";

	
	public static class GroupByMapper extends Mapper<Text, DataTuple, 
													DataTuple, DataTuple> {
		private int[] groupCols;
		
		public static void specifyParams(Configuration jobConf, int[] groupCols) {
			
			try {
				jobConf.set(CONF_GROUP_COLS, SerializationUtils.writeIntArrayToString(groupCols));
				
			} catch (IOException e) {
				e.printStackTrace();
			}
			}
		
		@Override
		public void setup(Context context) {
			try {

				System.out.println("Setup mapper");
				groupCols = SerializationUtils.readIntArrayFromString(context.getConfiguration().get(CONF_GROUP_COLS));

			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
		@Override
		public void map(Text key, DataTuple value, 
							Context context) throws IOException, InterruptedException {
			
			DataTuple keyTuple = new DataTuple(groupCols.length);

			//System.out.println("Map group by");
			for (int i = 0 ; i < groupCols.length; i++) {
				keyTuple.assignDataField(value.getField(groupCols[i]), i);
			}	
			context.write(keyTuple, value);
		}
	}
	
	
	public static class GroupByReducer extends Reducer<DataTuple, DataTuple, 
														NullWritable, DataTuple> {
		
		private int[] aggCols;
		private AggregationType[] aggFunctions;
		private DataType[] types;
		private int[] groupOutCols;
		private int[] aggOutCols;
		
		public static void specifyParams(Configuration jobConf, int[] aggCols, AggregationType[] aggFunctions,
										DataType[] types, int[] groupColsOutputPos, int[] aggColsOutputPos) {
		
			try {
				jobConf.set(CONF_AGG_TYPES, SerializationAggUtils.writeAggTypeArrayToString(aggFunctions));
				jobConf.set(CONF_AGG_DATA_TYPES, SerializationAggUtils.writeDataTypeArrayToString(types));
				jobConf.set(CONF_AGG_COLS, SerializationUtils.writeIntArrayToString(aggCols));

				jobConf.set(CONF_GROUP_OUT_COLS, SerializationUtils.writeIntArrayToString(groupColsOutputPos));
				jobConf.set(CONF_AGG_OUT_COLS, SerializationUtils.writeIntArrayToString(aggColsOutputPos));
			
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
		@Override
		public void setup(Context context) {
			try {
				
				System.out.println("Setup reducer");
				aggFunctions = SerializationAggUtils.readAggTypeArrayFromString(context.getConfiguration().get(CONF_AGG_TYPES));
				types = SerializationAggUtils.readDataTypeArrayFromString(context.getConfiguration().get(CONF_AGG_DATA_TYPES));
				aggCols = SerializationUtils.readIntArrayFromString(context.getConfiguration().get(CONF_AGG_COLS));
				
				groupOutCols = SerializationUtils.readIntArrayFromString(context.getConfiguration().get(CONF_GROUP_OUT_COLS));
				aggOutCols = SerializationUtils.readIntArrayFromString(context.getConfiguration().get(CONF_AGG_OUT_COLS));
			
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
		@Override
		public void reduce(DataTuple key, Iterable<DataTuple> values, 
				Context context) throws IOException, InterruptedException {
			

			//System.out.println("Reduce group by");
			
			Aggregate[] aggregates = new Aggregate[aggCols.length];
			
			for (int i = 0; i < aggCols.length; i++) {
				
				aggregates[i] = new Aggregate(aggFunctions[i], types[i]);
			}
		
			for (DataTuple tuple : values) {
				for (int i = 0; i < aggCols.length; i++) {
					aggregates[i].aggregate(tuple.getField(aggCols[i]));
				}
			}
			
			
			
			DataTuple result = new DataTuple(groupOutCols.length);
		
				
			for (int i = 0; i < groupOutCols.length; i++) {
				int index = groupOutCols[i];
				if (index != -1) {
					result.assignDataField(key.getField(index), i);
				}
			}
				
			for (int i = 0; i < aggOutCols.length; i++) {
				int index = aggOutCols[i];
				if (index != -1) {
					System.out.println("Type : " + aggregates[index].getAggregate().getBasicType());
					result.assignDataField(aggregates[index].getAggregate(), i);
				}
			}
				
				context.write(NullWritable.get(), result);		
		}
	}


	private int[] groupColsOutputPos;
	private int[] aggColsOutputPos;
	private BulkProcessingOperator child;
	private int[] groupCols;
	private AggregationType[] aggFunctions;
	private DataType[] aggTypes;
	

	public G5HadoopGroupByOperator(DBInstance instance,
			BulkProcessingOperator child) throws IOException {
		super(instance, child);
	}

	public G5HadoopGroupByOperator(DBInstance instance,
			BulkProcessingOperator child, int[] groupCols, int[] aggCols,
			AggregationType[] aggFunctions, DataType[] aggTypes,
			int[] groupColsOutputPos, int[] aggColsOutputPos) throws IOException {
		
		super(instance, child);
		this.child = child;
		this.groupCols = groupCols;
		this.aggFunctions = aggFunctions;
		this.aggTypes = aggTypes;
		this.groupColsOutputPos = groupColsOutputPos;
		this.aggColsOutputPos = aggColsOutputPos;
		
		
		GroupByMapper.specifyParams(job.getConfiguration(), groupCols);
		GroupByReducer.specifyParams(job.getConfiguration(), aggCols, aggFunctions, aggTypes, groupColsOutputPos, aggColsOutputPos);
		
		
		this.configureMapReduceJob(GroupByMapper.class, GroupByReducer.class, DataTuple.class, DataTuple.class);
		
		
	}

	@Override
	protected TableSchema createResultSchema() {
		TableSchema result_schema = new TableSchema();
		
		TableSchema child_schema = child.getResultSchema();
		
		for (int i = 0; i < groupColsOutputPos.length; i++) {
			int groupOutIndex = groupColsOutputPos[i];

			if (groupOutIndex != -1) {
				int groupIndex = groupCols[groupOutIndex];
				result_schema.addColumn(child_schema.getColumn(groupIndex));
				
			} else {
				int aggOutIndex = aggColsOutputPos[i];
				
				DataType type = Aggregate.getResultingType(aggFunctions[aggOutIndex], aggTypes[aggOutIndex]);
				
				result_schema.addColumn(ColumnSchema.createColumnSchema(aggFunctions[aggOutIndex].name(),type));				
			}			
		}
		return result_schema;
	}

}
