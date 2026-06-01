#include "optimizer/graphar_optimizer.hpp"

#include "utils/global_log_manager.hpp"

#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/common/enums/logical_operator_type.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"

namespace duckdb {

static int64_t GetOperatorTree(LogicalOperator &op) {
    int64_t result = 0;

    for (auto &child : op.children) {
        result = std::max(GetOperatorTree(*child), result);
    }
    return result + 1;
}

static void GetOperatorTree(LogicalOperator &op, std::string &result) {
    if (!op.GetName().empty()) {
        result += op.GetName();
    } else {
        result += "UNKNOWN<" + LogicalOperatorToString(op.type) + ">";
        if (op.type == LogicalOperatorType::LOGICAL_GET) {
            auto &get = op.Cast<LogicalGet>();
            result += "'" + get.function.name + "'";
        }
    }
    result += "(";
    bool first = true;
    for (auto &child : op.children) {
        if (!first) {
            result += ", ";
        }
        first = false;
        if (child) {
            GetOperatorTree(*child, result);
        } else {
            result += "NULL_CHILD";
        }
    }
    result += ")";
}

static bool HasGraphArScan(LogicalOperator &op) {
    if (op.type == LogicalOperatorType::LOGICAL_GET) {
        auto &get = op.Cast<LogicalGet>();
        return get.function.name == "read_edges" || get.function.name == "read_vertices";
    }
    for (auto &child : op.children) {
        if (HasGraphArScan(*child)) {
            return true;
        }
    }
    return false;
}

static void GraphArPreOptimize(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan) {
    DUCKDB_GRAPHAR_LOG_TRACE("GraphArPreOptimize");

    // Перед тем как утка оптимизирует запрос

    DUCKDB_GRAPHAR_LOG_DEBUG("Has graphAr before optimize: " + std::to_string(HasGraphArScan(*plan)));

    std::string tree;
    GetOperatorTree(*plan, tree);
    DUCKDB_GRAPHAR_LOG_DEBUG("before operators: " + tree);
}

static void GraphArOptimize(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan) {
    DUCKDB_GRAPHAR_LOG_TRACE("GraphArOptimize");

    // После оптимизации уже

    DUCKDB_GRAPHAR_LOG_DEBUG("Has graphAr after optimize: " + std::to_string(HasGraphArScan(*plan)));
    DUCKDB_GRAPHAR_LOG_DEBUG("after optimize:\n" + plan->ToString());
    DUCKDB_GRAPHAR_LOG_DEBUG("after depth: " + std::to_string(GetOperatorTree(*plan)))
    std::string tree;
    GetOperatorTree(*plan, tree);
    DUCKDB_GRAPHAR_LOG_DEBUG("after operators: " + tree);

    if (HasGraphArScan(*plan)) {
        DUCKDB_GRAPHAR_LOG_DEBUG("Adding is_graphar column to GraphAR query");
        
        bool already_added = false;
        LogicalOperator* current = plan.get();
        if (current->type == LogicalOperatorType::LOGICAL_PROJECTION && current->children.size() == 1) {
            auto& proj = current->Cast<LogicalProjection>();
            if (proj.expressions.size() > 0) {
                auto& last_expr = proj.expressions.back();
                if (last_expr->GetAlias() == "is_graphar") {
                    already_added = true;
                }
            }
        }
        
        if (!already_added) {
            vector<unique_ptr<Expression>> select_list;
            
            for (idx_t i = 0; i < plan->types.size(); i++) {
                auto ref = make_uniq<BoundReferenceExpression>(plan->types[i], i);
                select_list.push_back(std::move(ref));
            }
            
            auto is_graphar_const = make_uniq<BoundConstantExpression>(Value::INTEGER(1));
            is_graphar_const->SetAlias("is_graphar");
            select_list.push_back(std::move(is_graphar_const));
            
            auto projection = make_uniq<LogicalProjection>(0, std::move(select_list));
            projection->AddChild(std::move(plan));
            projection->ResolveOperatorTypes();
            
            plan = std::move(projection);
            
            DUCKDB_GRAPHAR_LOG_DEBUG("After adding is_graphar column:\n" + plan->ToString());
            DUCKDB_GRAPHAR_LOG_DEBUG("New plan types count: " + std::to_string(plan->types.size()));
        } else {
            DUCKDB_GRAPHAR_LOG_DEBUG("is_graphar column already added");
        }
    }
}

GraphArOptimizerExtension::GraphArOptimizerExtension() {
    DUCKDB_GRAPHAR_LOG_TRACE("GraphArOptimizerExtension");

    optimize_function = GraphArOptimize;
    pre_optimize_function = GraphArPreOptimize;
}

}  // namespace duckdb
