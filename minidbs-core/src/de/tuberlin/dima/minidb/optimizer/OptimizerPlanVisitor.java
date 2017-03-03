package de.tuberlin.dima.minidb.optimizer;

public interface OptimizerPlanVisitor
{
        public void preVisit(OptimizerPlanOperator operator);
        
        public void postVisit(OptimizerPlanOperator operator);
}