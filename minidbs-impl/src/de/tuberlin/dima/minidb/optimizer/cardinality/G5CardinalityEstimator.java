package de.tuberlin.dima.minidb.optimizer.cardinality;

import java.util.ArrayList;
import java.util.List;

import de.tuberlin.dima.minidb.catalogue.ColumnStatistics;
import de.tuberlin.dima.minidb.catalogue.TableStatistics;
import de.tuberlin.dima.minidb.core.ArithmeticType;
import de.tuberlin.dima.minidb.core.DataField;
import de.tuberlin.dima.minidb.optimizer.AbstractJoinPlanOperator;
import de.tuberlin.dima.minidb.optimizer.OptimizerPlanOperator;
import de.tuberlin.dima.minidb.parser.Predicate;
import de.tuberlin.dima.minidb.parser.Predicate.Operator;
import de.tuberlin.dima.minidb.semantics.BaseTableAccess;
import de.tuberlin.dima.minidb.semantics.Column;
import de.tuberlin.dima.minidb.semantics.Relation;
import de.tuberlin.dima.minidb.semantics.predicate.JoinPredicate;
import de.tuberlin.dima.minidb.semantics.predicate.JoinPredicateAtom;
import de.tuberlin.dima.minidb.semantics.predicate.JoinPredicateConjunct;
import de.tuberlin.dima.minidb.semantics.predicate.LocalPredicate;
import de.tuberlin.dima.minidb.semantics.predicate.LocalPredicateAtom;
import de.tuberlin.dima.minidb.semantics.predicate.LocalPredicateBetween;
import de.tuberlin.dima.minidb.semantics.predicate.LocalPredicateConjunct;

public class G5CardinalityEstimator implements CardinalityEstimator {

	@SuppressWarnings("unchecked")
	@Override
	public void estimateTableAccessCardinality(BaseTableAccess tableAccess) {
		
		
		
		TableStatistics stats = tableAccess.getTable().getStatistics();
		
		if (stats == null) {
			tableAccess.setOutputCardinality(0);
			return;
		}
		long inRows = stats.getCardinality();
		
		
		
		
		
		LocalPredicate pred = tableAccess.getPredicate();
		
		ArrayList<LocalPredicate> predList = new ArrayList<LocalPredicate>();
		
		
		if (pred == null) {
			tableAccess.setOutputCardinality(inRows);
			tableAccess.setOperatorCosts(inRows);
			tableAccess.setCumulativeCosts(inRows);

		//	System.out.println(tableAccess.getName() + " : " + inRows);
			return;
			
		} else if (pred instanceof LocalPredicateConjunct) {
			for(LocalPredicate locPred : ((LocalPredicateConjunct) pred).getPredicates()) {
			
				predList.add(locPred);				
			}			
		} else {
			
			predList.add(pred);
			
		}
		
		
		
		
		float selectivity = 1.0f;
		
		
		
		for(LocalPredicate locPred : predList) {
					
			if (locPred instanceof LocalPredicateAtom) {
				
				LocalPredicateAtom predAtom = (LocalPredicateAtom) locPred;
				
				int colIndex = predAtom.getColumn().getColumnIndex();
				ColumnStatistics colStats = tableAccess.getTable().getStatistics().getColumnStatistics(colIndex);
				long colCard = colStats.getCardinality();
							
				
				Operator op = predAtom.getParsedPredicate().getOp();
				
				// Column = value
				if (op.equals(Operator.EQUAL))  {
					selectivity *= 1.0f/colCard;
					
				} else if (op.equals(Operator.NOT_EQUAL)) {
					selectivity *= 1.0f - 1.0f/colCard;
					
				// Column =<, <, =>, > value
				} else {
					
					DataField literal = predAtom.getLiteral();
					
					if (literal instanceof ArithmeticType) {
						
						DataField highKey = colStats.getHighKey();
						DataField lowKey = colStats.getLowKey();
						
						long value = ((ArithmeticType<DataField>) literal).asLong();
						long high = ((ArithmeticType<DataField>) highKey).asLong();
						long low = ((ArithmeticType<DataField>) lowKey).asLong();
					
						if (op.equals(Operator.GREATER) || op.equals(Operator.GREATER_OR_EQUAL)) {							
							selectivity *= 1.0f*(high - value) / (high - low);
						} else {
							selectivity *= 1.0f*(value - low) / (high - low);
						}
						
					// If not arithmetic type : use default value
					} else {
						selectivity *= DEFAULT_RANGE_PREDICATE_SELECTIVITY;
					}
					
					
		
				}
			
			} else if (locPred instanceof LocalPredicateBetween) {
				LocalPredicateBetween predBetween = (LocalPredicateBetween) locPred;
				
				int colIndex = predBetween.getColumn().getColumnIndex();
				ColumnStatistics colStats = tableAccess.getTable().getStatistics().getColumnStatistics(colIndex);
				
				DataField highKey = colStats.getHighKey();

				
				if (highKey instanceof ArithmeticType) {
					
					DataField lowKey = colStats.getLowKey();
					DataField highValue = predBetween.getUpperBoundLiteral();
					DataField lowValue = predBetween.getLowerBoundLiteral();
					
					
					long high = ((ArithmeticType<DataField>) highKey).asLong();
					long low = ((ArithmeticType<DataField>) lowKey).asLong();
					long highVal = ((ArithmeticType<DataField>) highValue).asLong();
					long lowVal = ((ArithmeticType<DataField>) lowValue).asLong();
				
				
					selectivity *= 1.0f*(highVal - lowVal)/(high - low);
										
				// If not arithmetic : default value (should be 1/4 as in paper ..)
				} else {					
					selectivity *= DEFAULT_RANGE_PREDICATE_SELECTIVITY;
				}
				
			}			
		}

		tableAccess.setOutputCardinality((long)(inRows*selectivity));
		tableAccess.setOperatorCosts((long)(inRows*selectivity));
		tableAccess.setCumulativeCosts((long)(inRows*selectivity));

	//	System.out.println(tableAccess.getName() + " : " + (inRows*selectivity));
		
	}

	@Override
	public void estimateJoinCardinality(AbstractJoinPlanOperator operator) {
		// TODO Auto-generated method stub
		
		OptimizerPlanOperator childLeft = operator.getLeftChild();
		OptimizerPlanOperator childRight = operator.getRightChild();

		
	    long leftCard = childLeft.getOutputCardinality();
	    long rightCard = childRight.getOutputCardinality();
	    float selectivity = 1.0f;
	    JoinPredicate joinPred;
	    
	    if ((joinPred = operator.getJoinPredicate()) == null) {
	    	selectivity = 1.0f;
	      
	    } else if ((joinPred instanceof JoinPredicateAtom)) {
	    	selectivity = getSelectivity((JoinPredicateAtom)joinPred);
	    }
	    else if ((joinPred instanceof JoinPredicateConjunct)) {
	    	selectivity = 1.0F;

	    	List<JoinPredicateAtom> predList = ((JoinPredicateConjunct)joinPred).getConjunctiveFactors();

	    	for (int i = 0; i < predList.size(); i++) {
	    		JoinPredicateAtom pred = (JoinPredicateAtom)predList.get(i);
	    		float f2 = getSelectivity((JoinPredicateAtom)pred);
	    		selectivity = Math.min(selectivity, f2);
	    	}
	    }
	    
	    
	    long outputCard  = leftCard * rightCard;
	    
	    /*if ((outputCard = leftCard * rightCard) <=  0L) {
	    	outputCard = 9223372036854775807L;
	    }*/
	    


	    if ((outputCard = (long)((float)outputCard * selectivity)) < 1L) {
	    	outputCard = 1L;
	    }

	    operator.setOutputCardinality(outputCard);
	    
	    
		operator.setOperatorCosts(outputCard);
		
		operator.setCumulativeCosts(outputCard +  childLeft.getCumulativeCosts() + childRight.getCumulativeCosts());
	}

	private float getSelectivity(JoinPredicateAtom pred) {

		  
		if (pred.getParsedPredicate().getOp()  ==  Predicate.Operator.EQUAL) {

			long l1;
	      
			if ((l1 = getColCard(pred.getLeftHandColumn())) < 
	        1L)
	        return 0.1F;
	      long l2;
	      if ((
	        l2 = getColCard(pred.getRightHandColumn())) < 
	        1L) {
	        return 0.1F;
	      }

	      return 1.0F / (float)Math.max(l1, l2);
	    }

	    return 0.33333F;
	  }

	private long getColCard(Column column) {

		Relation relation = column.getRelation();

		if (relation instanceof BaseTableAccess) {
			
			TableStatistics stats = ((BaseTableAccess)relation).getTable().getStatistics();
			
	        if (stats != null) {
	        	ColumnStatistics colStats = stats.getColumnStatistics(column.getColumnIndex());
	        	
	        	if (colStats != null)
	        		return colStats.getCardinality();
	        }
		}
		return -1L;
	}
}
