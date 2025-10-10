#include "duckdb/optimizer/join_order/join_order_optimizer.hpp"

#include "duckdb/common/enums/join_type.hpp"
#include "duckdb/common/limits.hpp"
#include "duckdb/common/pair.hpp"
#include "duckdb/optimizer/join_order/cost_model.hpp"
#include "duckdb/optimizer/join_order/plan_enumerator.hpp"
#include "duckdb/planner/expression/list.hpp"
#include "duckdb/planner/operator/list.hpp"

#include "duckdb/optimizer/setting.hpp"

namespace duckdb {

//JoinOrderOptimizer::JoinOrderOptimizer(ClientContext &context) : context(context), query_graph_manager(context) {}

JoinOrderOptimizer JoinOrderOptimizer::CreateChildOptimizer() {
	JoinOrderOptimizer child_optimizer(context);
	child_optimizer.materialized_cte_stats = materialized_cte_stats;
	child_optimizer.delim_scan_stats = delim_scan_stats;
	return child_optimizer;
}

unique_ptr<LogicalOperator> JoinOrderOptimizer::Optimize(unique_ptr<LogicalOperator> plan,
                                                         optional_ptr<RelationStats> stats) {

	// make sure query graph manager has not extracted a relation graph already
	LogicalOperator *op = plan.get();

	// extract the relations that go into the hyper graph.
	// We optimize the children of any non-reorderable operations we come across.
	bool reorderable = query_graph_manager.Build(*this, *op);

	// get relation_stats here since the reconstruction process will move all relations.
	auto relation_stats = query_graph_manager.relation_manager.GetRelationStats();
	unique_ptr<LogicalOperator> new_logical_plan = nullptr;

	if (reorderable) {
		// query graph now has filters and relations
		auto cost_model = CostModel(query_graph_manager);

		// Initialize a plan enumerator.
		auto plan_enumerator =
		    PlanEnumerator(query_graph_manager, cost_model, query_graph_manager.GetQueryGraphEdges());

		// Initialize the leaf/single node plans
		plan_enumerator.InitLeafPlans();
		plan_enumerator.SolveJoinOrder();
		// now reconstruct a logical plan from the query graph plan
		query_graph_manager.plans = &plan_enumerator.GetPlans();

		new_logical_plan = query_graph_manager.Reconstruct(std::move(plan));
	} else {
		new_logical_plan = std::move(plan);
		if (relation_stats.size() == 1) {
			new_logical_plan->estimated_cardinality = relation_stats.at(0).cardinality;
			new_logical_plan->has_estimated_cardinality = true;
		}
	}

	// Propagate up a stats object from the top of the new_logical_plan if stats exist.
	if (stats) {
		auto cardinality = new_logical_plan->EstimateCardinality(context);
		auto bindings = new_logical_plan->GetColumnBindings();
		auto new_stats = RelationStatisticsHelper::CombineStatsOfReorderableOperator(bindings, relation_stats);
		new_stats.cardinality = cardinality;
		RelationStatisticsHelper::CopyRelationStats(*stats, new_stats);
	} else {
		// starts recursively setting cardinality
		new_logical_plan->EstimateCardinality(context);
	}

	if (new_logical_plan->type == LogicalOperatorType::LOGICAL_EXPLAIN) {
		new_logical_plan->SetEstimatedCardinality(3);
	}

	return new_logical_plan;
}

void JoinOrderOptimizer::AddMaterializedCTEStats(idx_t index, RelationStats &&stats) {
	materialized_cte_stats.emplace(index, std::move(stats));
}

RelationStats JoinOrderOptimizer::GetMaterializedCTEStats(idx_t index) {
	auto it = materialized_cte_stats.find(index);
	if (it == materialized_cte_stats.end()) {
		throw InternalException("Unable to find materialized CTE stats with index %llu", index);
	}
	return it->second;
}

void JoinOrderOptimizer::AddDelimScanStats(RelationStats &stats) {
	delim_scan_stats = &stats;
}

RelationStats JoinOrderOptimizer::GetDelimScanStats() {
	if (!delim_scan_stats) {
		throw InternalException("Unable to find delim scan stats!");
	}
	return *delim_scan_stats;
}

// Use for two case: 1. GYO 2. Arrange the plan as we have the exec_order for select * query
unique_ptr<LogicalOperator> JoinOrderOptimizer::CallSolveJoinOrderFixed(unique_ptr<LogicalOperator> plan, vector<LogicalOperator*> &exec_order) {
	// Store a clean copy before any modifications
    auto plan_backup = plan->Copy(context);

	// make sure query graph manager has not extracted a relation graph already
	LogicalOperator *op = plan.get();
	// extract the relations that go into the hyper graph.
	// We optimize the children of any non-reorderable operations we come across.
	bool reorderable = query_graph_manager.Build(*this, *op, false);
	// get relation_stats here since the reconstruction process will move all of the relations.
	auto relation_stats = query_graph_manager.relation_manager.GetRelationStats();
	unique_ptr<LogicalOperator> new_logical_plan = nullptr;

#ifdef PLAN_DEBUG
	// std::cout << "Print Relations in JoinOrderOptimizer::CallSolveJoinOrderFixed: " << std::endl;
	// query_graph_manager.relation_manager.PrintRelations();
	// Debug: Print all edges in query graph
    // std::cout << "Dumping all query graph edges in CallSolveJoinOrderFixed:" << std::endl;
    // const auto &query_graph = query_graph_manager.GetQueryGraphEdges();
    // std::cout << query_graph.ToString() << std::endl; // Use ToString() which is const-qualified
	// std::cout << "Relations in CallSolveJoinOrderFixed: " << std::endl;
	// for (idx_t i = 0; i < query_graph_manager.relation_manager.NumRelations(); i++) {
	// 	auto &relation = query_graph_manager.set_manager.GetJoinRelation(i);
	//	std::cout << relation.ToString() << std::endl;
	// }
#endif

	// NOTE: Fallback to DuckDB plan when #tables <= 5
	if (query_graph_manager.relation_manager.NumRelations() <= 5) {
		GYO = false;
	}

	if (!exec_order.empty() || GYO) {
		// query graph now has filters and relations
		auto cost_model = CostModel(query_graph_manager);
		// Initialize a plan enumerator.
		auto plan_enumerator = PlanEnumerator(query_graph_manager, cost_model, query_graph_manager.GetQueryGraphEdges());

		if (!exec_order.empty()) {
			plan_enumerator.InitLeafPlans();
			plan_enumerator.SolveJoinOrderFixed(exec_order);
			// now reconstruct a logical plan from the query graph plan
			query_graph_manager.plans = &plan_enumerator.GetPlans();
			new_logical_plan = query_graph_manager.Reconstruct(std::move(plan));
		} else {
			try {
				plan_enumerator.root_op = op;
				plan_enumerator.SolveJoinOrderGYO();
				// now reconstruct a logical plan from the query graph plan
				query_graph_manager.plans = &plan_enumerator.GetPlans();
				new_logical_plan = query_graph_manager.Reconstruct(std::move(plan));
#ifdef PLAN_DEBUG 
				std::cout << "GYO join tree found and reconstructed! " << std::endl;
#endif
			} catch (...) {
				// Unable to handle with GYO
#ifdef PLAN_DEBUG
				std::cout << "GYO join tree not found! " << std::endl;
#endif
				GYO = false;
			}
		}
	}

	if (!GYO || !new_logical_plan){
#ifdef PLAN_DEBUG
		std::cout << "CallSolveJoinOrderFixed Failed and fall back to DuckDB implementation! " << std::endl;
#endif
		// Create a completely fresh optimizer with clean state
        JoinOrderOptimizer fallback_optimizer(context);  // false = no GYO for fallback
        return fallback_optimizer.Optimize(std::move(plan_backup));
	}
	
	return new_logical_plan;
}

} // namespace duckdb
