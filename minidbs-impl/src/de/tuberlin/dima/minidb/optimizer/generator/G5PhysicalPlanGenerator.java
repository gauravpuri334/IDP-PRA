package de.tuberlin.dima.minidb.optimizer.generator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import de.tuberlin.dima.minidb.catalogue.Catalogue;
import de.tuberlin.dima.minidb.catalogue.IndexDescriptor;
import de.tuberlin.dima.minidb.optimizer.AbstractJoinPlanOperator;
import de.tuberlin.dima.minidb.optimizer.FetchPlanOperator;
import de.tuberlin.dima.minidb.optimizer.FilterPlanOperator;
import de.tuberlin.dima.minidb.optimizer.GroupByPlanOperator;
import de.tuberlin.dima.minidb.optimizer.IndexLookupPlanOperator;
import de.tuberlin.dima.minidb.optimizer.InterestingOrder;
import de.tuberlin.dima.minidb.optimizer.MergeJoinPlanOperator;
import de.tuberlin.dima.minidb.optimizer.NestedLoopJoinPlanOperator;
import de.tuberlin.dima.minidb.optimizer.OptimizerException;
import de.tuberlin.dima.minidb.optimizer.OptimizerPlanOperator;
import de.tuberlin.dima.minidb.optimizer.OrderedColumn;
import de.tuberlin.dima.minidb.optimizer.RequestedOrder;
import de.tuberlin.dima.minidb.optimizer.SortPlanOperator;
import de.tuberlin.dima.minidb.optimizer.TableScanPlanOperator;
import de.tuberlin.dima.minidb.optimizer.cardinality.CardinalityEstimator;
import de.tuberlin.dima.minidb.optimizer.cost.CostEstimator;
import de.tuberlin.dima.minidb.optimizer.generator.util.PhysicalPlanCostUpdater;
import de.tuberlin.dima.minidb.optimizer.generator.util.PhysicalPlanGeneratorUtils;
import de.tuberlin.dima.minidb.parser.OutputColumn;
import de.tuberlin.dima.minidb.semantics.AnalyzedSelectQuery;
import de.tuberlin.dima.minidb.semantics.BaseTableAccess;
import de.tuberlin.dima.minidb.semantics.Column;
import de.tuberlin.dima.minidb.semantics.Order;
import de.tuberlin.dima.minidb.semantics.ProducedColumn;
import de.tuberlin.dima.minidb.semantics.Relation;
import de.tuberlin.dima.minidb.semantics.predicate.JoinPredicate;
import de.tuberlin.dima.minidb.semantics.predicate.JoinPredicateAtom;
import de.tuberlin.dima.minidb.semantics.predicate.JoinPredicateConjunct;
import de.tuberlin.dima.minidb.semantics.predicate.LocalPredicate;
import de.tuberlin.dima.minidb.semantics.predicate.LocalPredicateAtom;
import de.tuberlin.dima.minidb.semantics.predicate.LocalPredicateBetween;

public class G5PhysicalPlanGenerator implements PhysicalPlanGenerator {

	private Catalogue catalogue;
	private CardinalityEstimator cardEstimator;
	private PhysicalPlanCostUpdater costUpdater;
	private CostEstimator costEstimator;

	public G5PhysicalPlanGenerator(Catalogue catalogue, CardinalityEstimator cardEstimator, CostEstimator costEstimator) {
		
		this.catalogue = catalogue;
		this.cardEstimator = cardEstimator;
		this.costEstimator = costEstimator;
		this.costUpdater = new PhysicalPlanCostUpdater(costEstimator);
	}

	@Override
	public OptimizerPlanOperator generatePhysicalPlan(
			AnalyzedSelectQuery query, OptimizerPlanOperator joinPlan)
			throws OptimizerException {
		
	//	PhysicalPlanPrinter printer = new PhysicalPlanPrinter();
	//	printer.print(joinPlan);
		
		return buildBestOrderByPlans(query, joinPlan)[0];
	}

	@Override
	public OptimizerPlanOperator[] buildBestOrderByPlans(
			AnalyzedSelectQuery query, OptimizerPlanOperator joinPlan)
			throws OptimizerException {
		
		

		ProducedColumn[] outCols = query.getOutputColumns();
		
		
		// Count the number of order to request
		int producedOrderCount = 0;

		for (ProducedColumn col : outCols) {
			if (col.getOrder() != Order.NONE) {
				producedOrderCount++;
			}
		}
	
		// Generate the requested orders
		RequestedOrder[]  reqOrders = new RequestedOrder[producedOrderCount];
		int[] orderColIndices = new int[producedOrderCount];
		boolean[] sortAscending = new boolean[producedOrderCount];


		if (producedOrderCount > 0) {
			
			int colPos = 0;
			for (int i = 0; i < outCols.length; i++) {
				if (outCols[i].getOrder() != Order.NONE) {
		
					reqOrders[colPos] = new RequestedOrder(outCols[i], outCols[i].getOrder());
					orderColIndices[colPos] = i;
					sortAscending[colPos] = (outCols[i].getOrder() == Order.ASCENDING ? true : false);
					colPos++;
				}
			}
		}
	
		// build the plans
		OptimizerPlanOperator[] plans = buildBestGroupByPlans(query.getOutputColumns(), query.isGrouping(), query.getPredicate(), joinPlan, reqOrders, orderColIndices);

		
		// Add sort
		
		OptimizerPlanOperator[] sortedPlans = new OptimizerPlanOperator[plans.length];
		// For each plan, check if a sort is needed and add it
		if (reqOrders != null && reqOrders.length > 0) {
			for (int n = 0; n < plans.length; n++) {
				
				OptimizerPlanOperator plan = plans[n];
				
				boolean needSort = true;

		        OrderedColumn[] orderCols = plan.getColumnOrder();
		        
		        
		        
		        // Check if need to add a sort
		        if (orderCols.length >= reqOrders.length) {
		        	needSort = false;

		        	for (int i = 0; i < reqOrders.length; i++) {
		        		if (!reqOrders[i].isMetBy(orderCols[i])) {
		        			needSort = true;
		        			break;
		        		}
		        	}
		        }
		        
		        if (needSort) {
		        	SortPlanOperator sortedPlan = new SortPlanOperator(plan, orderColIndices, sortAscending);

		        	this.costUpdater.costGenericOperator(sortedPlan);
		        	sortedPlans[n] = sortedPlan;
		        } else {
		        	sortedPlans[n] = plan;
		        }
			}

			return PhysicalPlanGeneratorUtils.prunePlans(sortedPlans, null);
		}

		return PhysicalPlanGeneratorUtils.prunePlans(plans, null);
	}
	
	
	
	@Override
	public OptimizerPlanOperator[] buildBestGroupByPlans(
			ProducedColumn[] outCols, boolean grouping,
			LocalPredicate havingPred, OptimizerPlanOperator joinPlan,
			RequestedOrder[] order, int[] orderColIndices)
			throws OptimizerException {	
		
		
		if (grouping) {
			
			
			// Generate the requested order to pass to the next level
			
			// Add the requested order from parent
			ArrayList<RequestedOrder> reqOrderList = new ArrayList<RequestedOrder>();

			if ((order != null) && (order.length > 0)) {
		        for (RequestedOrder reqOrder : order) {
		        	reqOrderList.add(reqOrder);
		        }
			}
			

			// Add the requested order for the group by level
			for (int i = 0; i < outCols.length; i++) {

				ProducedColumn outCol= outCols[i];
				
				if (outCol.getAggregationFunction() == OutputColumn.AggregationType.NONE) {
					if (order == null || order.length == 0 || outCol.getOrder() == Order.NONE)
						reqOrderList.add(new RequestedOrder(outCol, Order.ASCENDING));
				}
			}

			RequestedOrder[] reqOrders = reqOrderList.toArray(new RequestedOrder[reqOrderList.size()]);


			

			// Add order information
			
			int[] sortColumns = new int[reqOrderList.size()];
					
			boolean[] direction = new boolean[reqOrderList.size()];

			if (order != null && order.length != 0) {
				
				for (int i = 0; i < orderColIndices.length; i++) {
		        	sortColumns[i] = orderColIndices[i];
		        	direction[i] = (order[i].getOrder() == Order.ASCENDING ? true : false);
		        }
			}
			

			// Split between group and aggregates
			ArrayList<Integer> groupColList = new ArrayList<Integer>();
			ArrayList<Integer> aggColList = new ArrayList<Integer>();
			
			for (int i = 0; i < outCols.length; i++) {
				ProducedColumn outCol = outCols[i];
		        if (outCol.getAggregationFunction() == OutputColumn.AggregationType.NONE) {
	        	
		        	groupColList.add(i);
		        } else {
		        	aggColList.add(i);
		        }
			}
			
			
			// Convert to array
			int[] groupColIndices = new int[groupColList.size()];
			
			for (int i = 0; i < groupColList.size(); i++) {
				groupColIndices[i] = groupColList.get(i);
			}
			

			int[] aggColIndices = new int[aggColList.size()];
			
			for (int i = 0; i < aggColList.size(); i++) {
				aggColIndices[i] = aggColList.get(i);
			}

			long sortCost = this.costEstimator.computeSortCosts(outCols, joinPlan.getOutputCardinality());


			InterestingOrder[] intOrders = { new InterestingOrder(reqOrders, sortCost) };
			
			OptimizerPlanOperator[] plans = buildBestSubPlans(outCols, joinPlan, intOrders);

			
			// Generate groupBy and Filter
			for (int i = 0; i < plans.length; i++) {
				
				if (groupColIndices.length > 0) {
					plans[i] = new GroupByPlanOperator(
							PhysicalPlanGeneratorUtils.addSortIfNecessary(
									plans[i], reqOrders, sortColumns, direction), outCols, groupColIndices, aggColIndices, (int)joinPlan.getOutputCardinality());
		        } else {
		        	plans[i] = new GroupByPlanOperator(plans[i], outCols, groupColIndices, aggColIndices, (int)joinPlan.getOutputCardinality());
		        }

		        if (havingPred != null) {
		        	plans[i] = new FilterPlanOperator(plans[i], havingPred);
		        }

			}

			
			// Update costs
			for (int i = 0; i < plans.length; i++) {
		        this.costUpdater.costGenericOperator(plans[i]);
			}

			return PhysicalPlanGeneratorUtils.prunePlans(plans, intOrders);
		}
		
		
		// No grouping done

		long outputCard = joinPlan.getOutputCardinality();
		InterestingOrder[] intOrders = new InterestingOrder[0];

		if (outCols != null) {
			long sortCost = this.costEstimator.computeSortCosts(outCols, outputCard);

			intOrders = new InterestingOrder[] { new InterestingOrder(order, sortCost) };
		}
		
		return buildBestSubPlans(outCols, joinPlan, intOrders);
	}
	
	
	@Override
	public OptimizerPlanOperator[] buildBestSubPlans(Column[] neededCols,
			OptimizerPlanOperator abstractJoinPlan, InterestingOrder[] intOrders)
			throws OptimizerException {

		if (abstractJoinPlan instanceof AbstractJoinPlanOperator) {
			AbstractJoinPlanOperator joinPlan = (AbstractJoinPlanOperator) abstractJoinPlan;
			return buildBestConcreteJoinPlans(neededCols, joinPlan, intOrders);
		} else {
			Relation relation = (Relation) abstractJoinPlan;
			return buildBestRelationAccessPlans(neededCols, relation, intOrders);
		}
	}
	
	
	@Override
	public OptimizerPlanOperator[] buildBestConcreteJoinPlans(
			Column[] neededCols, AbstractJoinPlanOperator join,
			InterestingOrder[] intOrders) throws OptimizerException {
		
		
		OptimizerPlanOperator rightChild;
		OptimizerPlanOperator leftChild;
		
		JoinPredicate joinPred;
		
		// Put the smaller subplan in the inner side
		// (Should maybe have been done in JoinOrderOptimizer ?)
		if (join.getLeftChild().getCumulativeCosts() > join.getRightChild().getCumulativeCosts()) {
			leftChild  = join.getLeftChild();
			rightChild = join.getRightChild();
			joinPred = join.getJoinPredicate();
		} else {
			leftChild = join.getRightChild();
			rightChild = join.getLeftChild();
			joinPred = join.getJoinPredicate().createSideSwitchedCopy();
		}
		
		
		// Generate the output column maps for each side
		int[] outerOutColMap = new int[neededCols.length];
		int[] innerOutColMap = new int[neededCols.length];

		ArrayList<Column> outerColList = new ArrayList<Column>();
		ArrayList<Column> innerColList = new ArrayList<Column>();
		    
	    for (int i = 0; i < neededCols.length; i++) {
	    	Column col = neededCols[i];
	    	if (rightChild.getInvolvedRelations().contains(col.getRelation())) {
	    		outerOutColMap[i] = -1;
	    		innerOutColMap[i] = innerColList.size();
		        innerColList.add(col);
	    	} else {
		    	  outerOutColMap[i] = outerColList.size();
		    	  innerOutColMap[i] = -1;
		    	  outerColList.add(col);
	    	}
	    }
	    
	    
	    
	    // Adapt predicates
	    int[] leftColIndices;
	    int[] rightColIndices;
	       
	    JoinPredicate swJoinPred, swAdaptedPred;
	    JoinPredicate adaptedPred;
	    
	    if (joinPred instanceof JoinPredicateAtom) {
	    	
	    	leftColIndices = new int[1];
	    	rightColIndices = new int[1];
	    	 adaptedPred = PhysicalPlanGeneratorUtils.addKeyColumnsAndAdaptPredicate((JoinPredicateAtom)joinPred, outerColList, innerColList, leftChild.getInvolvedRelations(), rightChild.getInvolvedRelations(), leftColIndices, rightColIndices, 0);

		     swJoinPred = joinPred.createSideSwitchedCopy();
		     swAdaptedPred = adaptedPred.createSideSwitchedCopy();
	    } else {
	    	
	    	JoinPredicateConjunct predConjunct = (JoinPredicateConjunct) joinPred;

	    	adaptedPred = new JoinPredicateConjunct();
	    	List<JoinPredicateAtom> preds = predConjunct.getConjunctiveFactors();
	    	
	    	leftColIndices = new int[preds.size()];
	    	rightColIndices = new int[preds.size()];
		      
	    	for (int j = 0; j < preds.size(); j++) {
	            ((JoinPredicateConjunct)adaptedPred).addJoinPredicate(PhysicalPlanGeneratorUtils.addKeyColumnsAndAdaptPredicate(preds.get(j), outerColList, innerColList, leftChild.getInvolvedRelations(), rightChild.getInvolvedRelations(), leftColIndices, rightColIndices, j));

	    	}

	    	swJoinPred = predConjunct.createSideSwitchedCopy();
	    	swAdaptedPred = adaptedPred.createSideSwitchedCopy();
	    }
	    
	    
	    
	    // Generate interesting order for each side
	    Column[] outerCols = outerColList.toArray(new Column[outerColList.size()]);
	    Column[] innerCols = innerColList.toArray(new Column[innerColList.size()]);
	    
	    
	    ArrayList<InterestingOrder> leftIntOrderList = new ArrayList<InterestingOrder>();
	    ArrayList<InterestingOrder> rightIntOrderList = new ArrayList<InterestingOrder>();

	    long leftSortCost = this.costEstimator.computeSortCosts(outerCols, leftChild.getOutputCardinality());
	    long rightSortCost = this.costEstimator.computeSortCosts(innerCols, rightChild.getOutputCardinality());

	    
	    RequestedOrder[] leftReqOrders = new RequestedOrder[leftColIndices.length];
	    RequestedOrder[] rightReqOrders = new RequestedOrder[rightColIndices.length];
	    
	    
	    for (int k = 0; k < leftColIndices.length; k++) {
	    	leftReqOrders[k] = new RequestedOrder(outerCols[leftColIndices[k]], Order.ASCENDING);
	    }
	    
	    for (int k = 0; k < rightColIndices.length; k++) {
	    	rightReqOrders[k] = new RequestedOrder(innerCols[rightColIndices[k]], Order.ASCENDING);
	    }
	    
	    
	    InterestingOrder leftIntOrder = new InterestingOrder(leftReqOrders, leftSortCost);
	    InterestingOrder rightIntOrder = new InterestingOrder(rightReqOrders, rightSortCost);	    
	  
	    leftIntOrderList.add(leftIntOrder);
	    rightIntOrderList.add(rightIntOrder);
	    
	    
	    // Add the interesting orders from parent
	    if (intOrders != null) {
	    	
	    	for (InterestingOrder intOrder : intOrders) {
	    	
		        RequestedOrder[] reqOrders = intOrder.getOrderColumns();
		        
		        if (reqOrders.length > 0) {
			        if (rightChild.getInvolvedRelations().contains(intOrder.getOrderColumns()[0])) {
			        	rightIntOrderList.add(intOrder);
			        } else {
			        	leftIntOrderList.add(intOrder);
			        }
		        }
	    	}
	    }
	    
	    

	    // Convert to array
	    InterestingOrder[] leftInOrders = leftIntOrderList.toArray(new InterestingOrder[leftIntOrderList.size()]);
	    InterestingOrder[] rightIntOrders = rightIntOrderList.toArray(new InterestingOrder[rightIntOrderList.size()]);
	    
	    
	    
	    // Create sort directions array
	    boolean[] leftDirection = new boolean[leftColIndices.length];
	    boolean[] rightDirection = new boolean[leftColIndices.length];
	    Arrays.fill(leftDirection, true);
	    Arrays.fill(rightDirection, true);

	    OptimizerPlanOperator[] leftPlans = buildBestSubPlans(outerCols, leftChild,  leftInOrders);

	    OptimizerPlanOperator[] rightPlans = buildBestSubPlans(innerCols, rightChild, rightIntOrders);

	    
	    
	    

	    
	    // Construct inner sides for NL Join
	    OptimizerPlanOperator rightInnerSide = PhysicalPlanGeneratorUtils.createIndexNestedLoopJoinInner(this.cardEstimator, rightChild, joinPred, this.catalogue, innerCols, leftColIndices);

	    OptimizerPlanOperator leftInnerSide = PhysicalPlanGeneratorUtils.createIndexNestedLoopJoinInner(this.cardEstimator, leftChild, swJoinPred, this.catalogue, outerCols, rightColIndices);

	    
	    // Generate plans
	    ArrayList<OptimizerPlanOperator> planList = new ArrayList<OptimizerPlanOperator>();
	

	    // All NL Joins with right child in inner side
	    if (rightInnerSide != null) {
	    	for (int i = 0; i < leftPlans.length; i++) {
	    	
	    		OptimizerPlanOperator leftPlan= leftPlans[i];    	
	    		OptimizerPlanOperator NLJoin = new NestedLoopJoinPlanOperator(leftPlan, rightInnerSide, null, outerOutColMap, innerOutColMap, join.getOutputCardinality());

	    		planList.add(NLJoin);
	    	}
	    }
	    
	 // All NL Joins with left child in inner side
        if (leftInnerSide != null) {
	    	for (int i = 0; i < rightPlans.length; i++) {
	    		OptimizerPlanOperator rightPlan= rightPlans[i]; 
	    		OptimizerPlanOperator NLJoin = new NestedLoopJoinPlanOperator(rightPlan, leftInnerSide, null, innerOutColMap, outerOutColMap, join.getOutputCardinality());

	    		planList.add(NLJoin);
	    	}
        }

        
        // All other plans
	    for (int i = 0; i < leftPlans.length; i++) {
	    	for (int j = 0; j < rightPlans.length; j++) {
	    		

	    		OptimizerPlanOperator leftPlan= leftPlans[i];
	    		OptimizerPlanOperator rightPlan = rightPlans[j];

	    		// Merge possible ?
		        if (PhysicalPlanGeneratorUtils.isMergeJoinPossible(joinPred)) {
		        	OptimizerPlanOperator mergeJoin = new MergeJoinPlanOperator(
		        		  PhysicalPlanGeneratorUtils.addSortIfNecessary(leftPlan, leftReqOrders, leftColIndices, leftDirection),
		        		  PhysicalPlanGeneratorUtils.addSortIfNecessary(rightPlan, rightReqOrders, rightColIndices, rightDirection),
		        		  adaptedPred, leftColIndices, rightColIndices, outerOutColMap, innerOutColMap, join.getOutputCardinality());

		        	planList.add(mergeJoin);
		        	
		        // NL Joins
		        } else {
		        	// Left - right
		        	OptimizerPlanOperator NLJoin = new NestedLoopJoinPlanOperator(leftPlan, rightPlan, adaptedPred, outerOutColMap, innerOutColMap, join.getOutputCardinality());

		        	planList.add(NLJoin);

		        	// Right - left
		        	OptimizerPlanOperator NLJoin2 = new NestedLoopJoinPlanOperator(rightPlan, leftPlan, swAdaptedPred, innerOutColMap, outerOutColMap, join.getOutputCardinality());

		        	planList.add(NLJoin2);
		        }
	    	}
	    }
	    	
	    
	    // Update costs
	    for(OptimizerPlanOperator plan : planList) {
		      this.costUpdater.costGenericOperator(plan);
	    }

	    OptimizerPlanOperator[] planArray = planList.toArray(new OptimizerPlanOperator[planList.size()]);
	    
	    return PhysicalPlanGeneratorUtils.prunePlans(planArray, intOrders);		
	}
	


	@Override
	public OptimizerPlanOperator[] buildBestRelationAccessPlans(
			Column[] neededCols, Relation toAccess, InterestingOrder[] intOrders)
			throws OptimizerException {
	
		LocalPredicate pred = toAccess.getPredicate();

		BaseTableAccess baseAccess;
		
		if (toAccess instanceof BaseTableAccess) {
			baseAccess = (BaseTableAccess) toAccess;
		} else {
			baseAccess = new BaseTableAccess(catalogue.getTable(toAccess.getName()));
		}
		cardEstimator.estimateTableAccessCardinality(baseAccess);
		
		
		
		
		// Enumerating possible plans
		ArrayList<OptimizerPlanOperator> plans =new ArrayList<OptimizerPlanOperator>();
		
		// Table Scan
		TableScanPlanOperator scanOperator = new TableScanPlanOperator(baseAccess, neededCols);

		costUpdater.costGenericOperator(scanOperator);
		
		plans.add(scanOperator);
		
		
		
		
		ArrayList<IndexDescriptor> indexes = new ArrayList<IndexDescriptor>();
		
		indexes.addAll(catalogue.getAllIndexesForTable(baseAccess.getTable().getTableName()));
		
		int predCol = -1;
		if (pred instanceof LocalPredicateAtom) {
			predCol = ((LocalPredicateAtom) pred).getColumn().getColumnIndex();
			
		} else if (pred instanceof LocalPredicateBetween) {
			predCol = ((LocalPredicateBetween) pred).getColumn().getColumnIndex();
		}
		
		IndexLookupPlanOperator IXLookup = PhysicalPlanGeneratorUtils.createIndexLookup(baseAccess, baseAccess.getOutputCardinality(), pred, predCol, indexes);
		
		if (IXLookup != null) {
		
			FetchPlanOperator fetchOperator = new FetchPlanOperator(IXLookup, baseAccess, neededCols);

			costUpdater.costGenericOperator(fetchOperator);
			plans.add(fetchOperator);
		}
	
		OptimizerPlanOperator[] planArray = plans.toArray(new OptimizerPlanOperator[plans.size()]);
		
		return PhysicalPlanGeneratorUtils.prunePlans(planArray, intOrders);		
	}
}
