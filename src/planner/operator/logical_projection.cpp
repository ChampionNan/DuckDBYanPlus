#include "duckdb/planner/operator/logical_projection.hpp"

#include "duckdb/main/config.hpp"

namespace duckdb {

LogicalProjection::LogicalProjection(idx_t table_index, vector<unique_ptr<Expression>> select_list)
    : LogicalOperator(LogicalOperatorType::LOGICAL_PROJECTION, std::move(select_list)), table_index(table_index) {
}

vector<ColumnBinding> LogicalProjection::GetColumnBindings() {
	return GenerateColumnBindings(table_index, expressions.size());
}

void LogicalProjection::ResolveTypes() {
	for (auto &expr : expressions) {
		types.push_back(expr->return_type);
	}
}

vector<idx_t> LogicalProjection::GetTableIndex() const {
	return vector<idx_t> {table_index};
}

string LogicalProjection::GetName() const {
#ifdef DEBUG
	if (DBConfigOptions::debug_print_bindings) {
		return LogicalOperator::GetName() + StringUtil::Format(" #%llu", table_index);
	}
#endif
	return LogicalOperator::GetName();
}

InsertionOrderPreservingMap<string> LogicalProjection::ParamsToString() const {
    InsertionOrderPreservingMap<string> result;
    
    // Build expressions info similar to base class, but add bindings
    string expressions_info;
    
    for (idx_t i = 0; i < expressions.size(); i++) {
        if (i > 0) {
            expressions_info += "\n";
        }
        // Get expression name like base class
        expressions_info += expressions[i]->GetName();
        
        // Add binding information using table_index and column position
        expressions_info += StringUtil::Format(" [%llu.%llu]", table_index, i);
    }
    
    result["Expressions"] = expressions_info;
    result["Table Index"] = StringUtil::Format("%llu", table_index);
    
    // Keep the estimated cardinality from base class
    SetParamsEstimatedCardinality(result);
    
    return result;
}


} // namespace duckdb
