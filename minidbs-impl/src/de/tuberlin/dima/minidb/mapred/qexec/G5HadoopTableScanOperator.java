package de.tuberlin.dima.minidb.mapred.qexec;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import de.tuberlin.dima.minidb.DBInstance;
import de.tuberlin.dima.minidb.catalogue.TableSchema;
import de.tuberlin.dima.minidb.core.DataTuple;
import de.tuberlin.dima.minidb.mapred.SerializationUtils;
import de.tuberlin.dima.minidb.qexec.QueryExecutionException;
import de.tuberlin.dima.minidb.qexec.predicate.LocalPredicate;

public class G5HadoopTableScanOperator extends HadoopOperator<Text, DataTuple> {
	
	
	private final static String CONF_PREDICATE = "de.tuberlin.dima.minidb.mapred.qexec.G5HadoopTableScanOperator.Predicate";
	
	
	/**
	 * Custom Mapper that emit only if predicate is fulfilled
	 * @author mheimel
	 *
	 */
	public static class ScanMapper extends Mapper<Text, DataTuple, 
    												Text, DataTuple> {
		private LocalPredicate predicate;
		
		public static void specifySelectedKey(Configuration jobConf, LocalPredicate predicate) {
			try {
				if (predicate != null)
					jobConf.set(CONF_PREDICATE, SerializationUtils.writeLocalPredicateToString(predicate));
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
		@Override
		public void setup(Context context) {
			try {
				String predString = context.getConfiguration().get(CONF_PREDICATE);
				if (predString == null) {
					predicate = null;
				} else {
					predicate = SerializationUtils.readLocalPredicateFromString(predString);
				}
				
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
		@Override
		public void map(Text key, DataTuple value, 
				Context context) throws IOException, InterruptedException {


			try {
				if (predicate == null || predicate.evaluate(value)) {
					context.write(key, value);
				}
			} catch (QueryExecutionException e) {
				e.printStackTrace();
			}
		}
	}

	private BulkProcessingOperator child;

	public G5HadoopTableScanOperator(DBInstance instance,
			BulkProcessingOperator child, LocalPredicate predicate) throws IOException {
		
		super(instance, child);
		
		this.child = child;
			
		this.configureMapOnlyJob(ScanMapper.class);
		ScanMapper.specifySelectedKey(
				job.getConfiguration(), 
				predicate);
				
	}

	@Override
	protected TableSchema createResultSchema() {
		
		return child.getResultSchema();
	}

}
