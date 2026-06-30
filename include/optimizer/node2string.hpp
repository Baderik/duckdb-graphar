#pragma once

#include "duckdb/optimizer/optimizer_extension.hpp"

#include "utils/benchmark.hpp"
#include "utils/global_log_manager.hpp"

#include "duckdb/execution/column_binding_resolver.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/common/enums/logical_operator_type.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/joinside.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"

namespace duckdb {

std::string LogBound(BoundColumnRefExpression &expr) {
    string result;
    return ExpressionTypeToString(expr.GetExpressionType()) + ": " + expr.GetName() + " " + expr.binding.ToString();
}

template<typename elT>
std::string GetStringL(const vector<elT> &elements, std::function<string(const elT&)>format_func) {
    std::string result = "size=" + std::to_string(elements.size()) + " [";
    
    for (idx_t i = 0; i < elements.size(); ++i) {
        if (i > 0) result += ", ";
        result += format_func(elements[i]);
    }
    
    result += "]";
    return result;
}

std::string GetStringE(const unique_ptr<Expression> &expr) {
    if (expr == nullptr) {
        return "nullptr";
    } else {
    switch (expr->GetExpressionClass()) {
	// case ExpressionClass::BOUND_AGGREGATE:
	// 	return LogBound(expr->Cast<BoundAggregateExpression>());
	// 	break;
	// case ExpressionClass::BOUND_BETWEEN:
	// 	return LogBound(expr->Cast<BoundBetweenExpression>());
	// 	break;
	// case ExpressionClass::BOUND_CASE:
	// 	return LogBound(expr->Cast<BoundCaseExpression>());
	// 	break;
	// case ExpressionClass::BOUND_CAST:
	// 	return LogBound(expr->Cast<BoundCastExpression>());
	// 	break;
	case ExpressionClass::BOUND_COLUMN_REF:
		return LogBound(expr->Cast<BoundColumnRefExpression>());
	// case ExpressionClass::BOUND_COMPARISON:
	// 	return LogBound(expr->Cast<BoundComparisonExpression>());
	// 	break;
	// case ExpressionClass::BOUND_CONJUNCTION:
	// 	return LogBound(expr->Cast<BoundConjunctionExpression>());
	// 	break;
	// case ExpressionClass::BOUND_CONSTANT:
	// 	return LogBound(expr->Cast<BoundConstantExpression>());
	// 	break;
	// case ExpressionClass::BOUND_FUNCTION:
	// 	return LogBound(expr->Cast<BoundFunctionExpression>());
	// 	break;
	// case ExpressionClass::BOUND_SUBQUERY:
	// 	return LogBound(expr->Cast<BoundSubqueryExpression>());
	// 	break;
	// case ExpressionClass::BOUND_OPERATOR:
	// 	return LogBound(expr->Cast<BoundOperatorExpression>());
	// 	break;
	// case ExpressionClass::BOUND_PARAMETER:
	// 	return LogBound(expr->Cast<BoundParameterExpression>());
	// 	break;
	// case ExpressionClass::BOUND_REF:
	// 	return LogBound(expr->Cast<BoundReferenceExpression>());
	// 	break;
	// case ExpressionClass::BOUND_DEFAULT:
	// 	return LogBound(expr->Cast<BoundReferenceExpression>());
	// 	break;
	// case ExpressionClass::BOUND_WINDOW:
	// 	return LogBound(expr->Cast<BoundWindowExpression>());
	// 	break;
	// case ExpressionClass::BOUND_UNNEST:
	// 	return LogBound(expr->Cast<BoundUnnestExpression>());
	// 	break;
	default:
        return ExpressionClassToString(expr->GetExpressionClass()) + ": " + expr->ToString();
		// throw InternalException("Unrecognized expression type in logical operator visitor");
	}
    //     expr->
        // return expr->ToString();
    }
}

std::string GetStringJ(const unique_ptr<JoinFilterPushdownInfo> &pushdown_info) {
    if (pushdown_info == nullptr) {
        return "nullptr";
    }
    std::string result;
    result += "JoinCond: " + GetStringL<idx_t>(pushdown_info->join_condition, [](const auto &idx){return std::to_string(idx);}) + '\n';
    result += "MinMaxAggregates: " + GetStringL<unique_ptr<Expression>>(pushdown_info->min_max_aggregates, [](const auto &el){return GetStringE(el);}) + '\n';
    
    result += "has filter: " + std::to_string(pushdown_info->build_side_has_filter);
    return result;
}

std::string GetInfoLogicalOperator(LogicalOperator &op) {
    std::string result;
    result += "Type: " + LogicalOperatorToString(op.type) + '\n';
    result += "Name: " + op.GetName() + '\n';

    result += "Children: " + GetStringL<unique_ptr<LogicalOperator>>(op.children, [](const auto &child){return child->GetName() + "<" + LogicalOperatorToString(child->type)+ ">";}) + '\n';
    result += "Expressions: " + GetStringL<unique_ptr<Expression>>(op.expressions, [](const auto &expr){return GetStringE(expr);}) + '\n';
    result += "Types: " + GetStringL<LogicalType>(op.types, [](const auto &type){return type.ToString();}) + '\n';

    result += "Estimated_cardinality: has=" + std::to_string(op.has_estimated_cardinality) + " val=" + std::to_string(op.estimated_cardinality) + '\n';
    
    const auto binds = op.GetColumnBindings();
    result += "Bindings: size=" + std::to_string(binds.size()) + " " + op.ColumnBindingsToString(binds) + '\n';
    // if (!binds.empty()) {
    //     result += "root idx: " + std::to_string(op.GetRootIndex()) + '\n';
    // }
    const auto params = op.ParamsToString();
    result += "params: size=" + std::to_string(params.size()) + " ";
    for (const auto &param : params) {
        result += "," + param.first + "=" + param.second;
    }

    return result;
}

std::string GetInfoLogicalJoin(LogicalJoin &op) {
    std::string result;
    result += "JoinType: " + JoinTypeToString(op.join_type) + "\n";
    result += "mark_idx: " + std::to_string(op.mark_index) + "\n";
    result += "Left proj map: " + GetStringL<idx_t>(op.left_projection_map, [](const auto &idx){return std::to_string(idx);}) + '\n';
    result += "Right proj map: " + GetStringL<idx_t>(op.right_projection_map, [](const auto &idx){return std::to_string(idx);}) + '\n';
    result += "Join stats: " + GetStringL<unique_ptr<BaseStatistics>>(op.join_stats, [](const auto &stat){return stat->ToString();}) + '\n';

    const auto table_idx = op.GetTableIndex();
    result += "Table idx: " + GetStringL<idx_t>(table_idx, [](const auto &idx){return std::to_string(idx);}) + '\n';

    return result;
}

std::string GetInfoComparisonJoin(LogicalComparisonJoin &op) {
    std::string result;

    result += "Conditions: " + GetStringL<duckdb::JoinCondition>(op.conditions, [](const auto &cond){return GetStringE(cond.left) + " " + ExpressionTypeToOperator(cond.comparison) + " " + GetStringE(cond.right);}) + '\n';

    result += "MarkTypes: " + GetStringL<duckdb::LogicalType>(op.mark_types, [](const auto &type){return type.ToString();}) + '\n';
    
    result += "DuplicateRliminatedCol: " + GetStringL<unique_ptr<Expression>>(op.duplicate_eliminated_columns, [](const auto &expr){return GetStringE(expr);}) + '\n';

    result += "Delim flipped: " + std::to_string(op.delim_flipped) + '\n';
    result += "Convert_mark_to_semi: " + std::to_string(op.convert_mark_to_semi) + '\n';

    result += GetStringJ(op.filter_pushdown) + '\n';

    result += "Predicate: " + GetStringE(op.predicate) + '\n';

    const auto params = op.ParamsToString();
    result += "Params: size=" + std::to_string(params.size()) + " ";
    for (const auto &param : params) {
        result += "," + param.first + "=" + param.second;
    }
    result += '\n';

    return result;
}

std::string GetInfoLogicalGet(LogicalGet &op) {
    std::string result;
    result += "Table index: " + std::to_string(op.table_index) + '\n';

    result += "Names: " + GetStringL<std::string>(op.names, [](const auto &name){return name;}) + '\n';
    result += "Projections: " + GetStringL<idx_t>(op.projection_ids, [](const auto &el){return std::to_string(el);}) + '\n';
    result += "InputTableNames: " + GetStringL<string>(op.input_table_names, [](const auto &el){return el;}) + '\n';
    result += "InputTableTypes: " + GetStringL<LogicalType>(op.input_table_types, [](const auto &el){return el.ToString();}) + '\n';
    result += "Projections input: " + GetStringL<idx_t>(op.projected_input, [](const auto &el){return std::to_string(el);}) + '\n';

    return result;
}

std::string GetInfoLogicalAggregate(LogicalAggregate &op) {
    std::string result;
    result += "GroupIdx: " + std::to_string(op.group_index) + '\n';
    result += "AggregateIdx: " + std::to_string(op.aggregate_index) + '\n';
    result += "GroupingsIdx: " + std::to_string(op.groupings_index) + '\n';
    result += "Groups: " + GetStringL<unique_ptr<Expression>>(op.groups, [](const auto &expr){return GetStringE(expr);}) + '\n';
    result += "GroupStats: " + GetStringL<unique_ptr<BaseStatistics>>(op.group_stats, [](const auto &stat){return stat->ToString();}) + '\n';
    result += "Validity: " + std::to_string(op.distinct_validity == TupleDataValidityType::CANNOT_HAVE_NULL_VALUES) + '\n';

    return result;
}

std::string GetInfoLogical(LogicalOperator &op) {
    std::string result = "LO:\n";
    result += GetInfoLogicalOperator(op);

    switch (op.type) {
        case LogicalOperatorType::LOGICAL_GET: {
            auto &get = op.Cast<LogicalGet>();
            result += GetInfoLogicalGet(get);
            break;
        }
        case LogicalOperatorType::LOGICAL_JOIN: {
            auto &join = op.Cast<LogicalJoin>();
            result += GetInfoLogicalJoin(join);
            break;
        }
        case LogicalOperatorType::LOGICAL_COMPARISON_JOIN: {
            auto &join = op.Cast<LogicalJoin>();
            result += GetInfoLogicalJoin(join) + '\n';
            auto &comp_join = op.Cast<LogicalComparisonJoin>();
            result += GetInfoComparisonJoin(comp_join);
            break;
        }
        case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY: {
            auto &agg = op.Cast<LogicalAggregate>();
            result += GetInfoLogicalAggregate(agg);
            break;
        }
        default: {
            result += "Special operator";
            break;
        }
    }
    return result; 
}

std::string GetGraphArFunctionName(const LogicalOperator &op) {
    if (op.type != LogicalOperatorType::LOGICAL_GET) {
        return "";
    }
    const auto &get = op.Cast<LogicalGet>();
    if (get.function.name != "read_edges" && get.function.name != "read_vertices") {
        return "";
    }
    return get.function.name;
}

std::string node_str(unique_ptr<LogicalOperator> &op, const int i, const int depth) {
    return "d=" + std::to_string(depth) + " i=" + std::to_string(i) + " c=" + std::to_string(op->children.size());
} 

}  // namespace duckdb
