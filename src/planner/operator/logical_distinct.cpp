#include "duckdb/common/string_util.hpp"
#include "duckdb/planner/operator/logical_distinct.hpp"

namespace duckdb {

LogicalDistinct::LogicalDistinct(DistinctType distinct_type)
    : LogicalOperator(LogicalOperatorType::LOGICAL_DISTINCT), distinct_type(distinct_type) {
}
LogicalDistinct::LogicalDistinct(vector<unique_ptr<Expression>> targets, DistinctType distinct_type)
    : LogicalOperator(LogicalOperatorType::LOGICAL_DISTINCT), distinct_type(distinct_type),
      distinct_targets(std::move(targets)) {
}
/*
InsertionOrderPreservingMap<string> LogicalDistinct::ParamsToString() const {
	auto result = LogicalOperator::ParamsToString();
	if (!distinct_targets.empty()) {
		result["Distinct Targets"] =
		    StringUtil::Join(distinct_targets, distinct_targets.size(), "\n",
		                     [](const unique_ptr<Expression> &child) { return child->GetName(); });
	}
	SetParamsEstimatedCardinality(result);
	return result;
}*/
InsertionOrderPreservingMap<string> LogicalDistinct::ParamsToString() const {
    InsertionOrderPreservingMap<string> result;
    
    // Add distinct type information
    result["Distinct Type"] = distinct_type == DistinctType::DISTINCT ? "DISTINCT" : "DISTINCT ON";
    
    // Add distinct targets if any (similar to LogicalAggregate's groups handling)
    if (!distinct_targets.empty()) {
        string targets_info;
        for (idx_t i = 0; i < distinct_targets.size(); i++) {
            if (i > 0) {
                targets_info += "\n";
            }
            targets_info += distinct_targets[i]->GetName();
        }
        result["Distinct Targets"] = targets_info;
    }
    
    // Add column bindings information (manual string building like LogicalAggregate)
    auto column_bindings = children[0]->GetColumnBindings();
    if (!column_bindings.empty()) {
        string bindings_info;
        for (idx_t i = 0; i < column_bindings.size(); i++) {
            if (i > 0) {
                bindings_info += ", ";
            }
            bindings_info += column_bindings[i].ToString();
        }
        result["Column Bindings"] = bindings_info;
    }
    
    SetParamsEstimatedCardinality(result);
    return result;
}

void LogicalDistinct::ResolveTypes() {
	types = children[0]->types;
}

} // namespace duckdb
