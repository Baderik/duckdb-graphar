#include "optimizer/graphar_optimizer.hpp"

#include "optimizer/node2string.hpp"

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

struct OptimizeResult {
    bool optimized = false;
    vector<idx_t> new_indexes;
};

static std::string map2str(replace_col_map &col_map) {
	std::string ss;
	for (auto &[key, value] : col_map) {
		ss += key + ":" + value.ToString() + ",";
	}
	return ss;
}

static bool IsGraphArScan(const LogicalOperator &op) {
    if (op.type != LogicalOperatorType::LOGICAL_GET) {
        return false;
    }
    const auto &get = op.Cast<LogicalGet>();
    return get.function.name == "read_edges" || get.function.name == "read_vertices";
}

static pair<ColumnBinding, ColumnBinding> GetReplaceBinding(const JoinCondition &condition, const idx_t vertex_table) {
    if (condition.left->type != ExpressionType::BOUND_COLUMN_REF || condition.right->type != ExpressionType::BOUND_COLUMN_REF) {
        throw InternalException("invalid join condition types");
    }
    auto &left = condition.left->Cast<BoundColumnRefExpression>();
    auto &right = condition.right->Cast<BoundColumnRefExpression>();
    if (left.binding.table_index == vertex_table) {
        return {left.binding, right.binding};
    } else if (right.binding.table_index == vertex_table) {
        return {right.binding, left.binding};
    } else {
        throw InternalException("invalid join condition table indexes");
    }
}

static bool replaceColumnsInOperator(unique_ptr<LogicalOperator> &op, replace_col_map &replace_columns) {
    switch (op->type) {
	    case LogicalOperatorType::LOGICAL_COMPARISON_JOIN: {
            auto &join = op->Cast<LogicalComparisonJoin>();
            for (auto &condition : join.conditions) {
                if (condition.left->type == ExpressionType::BOUND_COLUMN_REF) {
                    auto &col = condition.left->Cast<BoundColumnRefExpression>();
                    
                    auto it = replace_columns.find(col.binding.ToString());
                    while (it != replace_columns.end()) {
                        col.binding = it->second;
                        it = replace_columns.find(col.binding.ToString());
                    }
                }
                if (condition.right->type == ExpressionType::BOUND_COLUMN_REF) {
                    auto &col = condition.right->Cast<BoundColumnRefExpression>();
                    auto it = replace_columns.find(col.binding.ToString());
                    while (it != replace_columns.end()) {
                        col.binding = it->second;
                        it = replace_columns.find(col.binding.ToString());
                    }
                }
            }
            break;
        }
        default: {
            for (auto &exp : op->expressions) {
                switch (exp->type) {
                    case (ExpressionType::BOUND_COLUMN_REF):
                        auto &col = exp->Cast<BoundColumnRefExpression>();
                        auto it = replace_columns.find(col.binding.ToString());
                        while (it != replace_columns.end()) {
                            col.binding = it->second;
                            it = replace_columns.find(col.binding.ToString());
                        }
                        break;
                }
                
            }
        }
    }
}

static OptimizeResult TryOptimizeVertexEdgeJoin(unique_ptr<LogicalOperator> &op, replace_col_map &replace_columns) {
    OptimizeResult result;

    if (op->type != LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
        return result;
    }

    auto &join = op->Cast<LogicalComparisonJoin>();
    if (join.children.size() != 2) {
        return result;
    }
    auto &left = join.children[0];
    auto &right = join.children[1];

    const auto left_name = GetGraphArFunctionName(*left);
    const auto right_name = GetGraphArFunctionName(*right);
    
    if (left_name.empty() || right_name.empty()) {
        DUCKDB_GRAPHAR_LOG_DEBUG("unchanged join\n" + join.ToString());
        return result;
    }
    
    if (left_name == right_name) {
        DUCKDB_GRAPHAR_LOG_DEBUG("Found Join with 2 GraphAr Scans with equal scans");
        DUCKDB_GRAPHAR_LOG_DEBUG("saved join\n" + join.ToString());
        return result;
    }

    result.optimized = true;

    DUCKDB_GRAPHAR_LOG_DEBUG("ln: " + left_name + " rn: " + right_name);
    idx_t vertex_table;
    if (left_name == "read_edges") {
        auto vertex_binds = right->GetColumnBindings();
        if (vertex_binds.size() != 1) {
            return result;
        }
        vertex_table = vertex_binds[0].table_index;
    } else {
        auto vertex_binds = left->GetColumnBindings();
        if (vertex_binds.size() != 1) {
            return result;
        }
        vertex_table = vertex_binds[0].table_index;
    }
    if (join.conditions.size() != 1) {
        return result;
    }

    auto replace = GetReplaceBinding(join.conditions[0], vertex_table);
    replace_columns[replace.first.ToString()] = replace.second;

    if (left_name == "read_edges") {
        if (join.left_projection_map.empty()) {
            auto left_bindings_size = left->GetColumnBindings().size();
            for (idx_t i = 0; i < left_bindings_size; ++i) {
                result.new_indexes.push_back(i);
            }
        } else {
            for (const auto value : join.left_projection_map) {
                result.new_indexes.push_back(value);
            }
        }
        const auto right_binding = right->GetColumnBindings()[0];
        result.new_indexes.push_back(replace_columns[right_binding.ToString()].column_index);
        op = std::move(left);
    } else {
        const auto left_binding = left->GetColumnBindings()[0];
        result.new_indexes.push_back(replace_columns[left_binding.ToString()].column_index);
        if (join.right_projection_map.empty()) {
            auto right_bindings_size = right->GetColumnBindings().size();
            for (idx_t i = 0; i < right_bindings_size; ++i) {
                result.new_indexes.push_back(i);
            }
        } else {
            for (const auto value : join.right_projection_map) {
                result.new_indexes.push_back(value);
            }
        }
        op = std::move(right);
    }

    DUCKDB_GRAPHAR_LOG_DEBUG("join optimized");

    return result;
}

static OptimizeResult OptimizeJoins(unique_ptr<LogicalOperator> &op, replace_col_map &replace_columns, int &i, int depth = 1) {
    OptimizeResult result;
    int cur_i = i;

    DUCKDB_GRAPHAR_LOG_DEBUG("open: " + node_str(op, cur_i, depth) + "\n" + op->ToString());
    DUCKDB_GRAPHAR_LOG_DEBUG(GetInfoLogical(*op));

    bool is_join = op->type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN;

    for (int child_i = 0; child_i < op->children.size(); ++child_i) {
        ++i;
        if (op->children[child_i]) {
            DUCKDB_GRAPHAR_LOG_DEBUG(node_str(op, cur_i, depth) + " go to child child_i=" + std::to_string(child_i) + " " + node_str(op->children[child_i], i, depth + 1)); 
            auto child_result = OptimizeJoins(op->children[child_i], replace_columns, i, depth + 1);
            result.optimized = result.optimized || child_result.optimized;

            if (is_join && !child_result.new_indexes.empty()) {
                auto &join = op->Cast<LogicalComparisonJoin>();
                if (child_i == 0) {
                    if (join.left_projection_map.empty()) {
                        for (auto &index : child_result.new_indexes) {
                            join.left_projection_map.push_back(index);
                        }
                    } else {
                        for (auto &index : join.left_projection_map) {
                            index = child_result.new_indexes[index];
                        }
                    }
                } else {
                    if (join.right_projection_map.empty()) {
                        for (auto &index : child_result.new_indexes) {
                            join.right_projection_map.push_back(index);
                        }
                    } else {
                        for (auto &index : join.right_projection_map) {
                            index = child_result.new_indexes[index];
                        }
                    }
                }
            }
        } else {
            DUCKDB_GRAPHAR_LOG_DEBUG("child is nullptr " + node_str(op, cur_i, depth));
        }
    }
    if (result.optimized) {
        DUCKDB_GRAPHAR_LOG_DEBUG("child was optimized " + node_str(op, cur_i, depth) + ";\n" + op->ToString());
        DUCKDB_GRAPHAR_LOG_DEBUG("LO:\n" + GetInfoLogicalOperator(*op) + '\n');
        if (op->type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
            auto &join = op->Cast<LogicalComparisonJoin>();
            DUCKDB_GRAPHAR_LOG_DEBUG("LJ:\n" + GetInfoLogicalJoin(join));
            DUCKDB_GRAPHAR_LOG_DEBUG("CJ:\n" + GetInfoComparisonJoin(join));
        }
        if (op->type == LogicalOperatorType::LOGICAL_GET) {
            auto &get = op->Cast<LogicalGet>();
            DUCKDB_GRAPHAR_LOG_DEBUG("LG:\n" + GetInfoLogicalGet(get));
        }
    }
    auto cur_result = TryOptimizeVertexEdgeJoin(op, replace_columns);
    DUCKDB_GRAPHAR_LOG_DEBUG("child was optimized: " + std::to_string(result.optimized) + " cur optimized: " + std::to_string(cur_result.optimized));
    if (cur_result.optimized || result.optimized) {
        result.optimized = true;
        result.new_indexes = cur_result.new_indexes;
        DUCKDB_GRAPHAR_LOG_DEBUG("resolve " + node_str(op, cur_i, depth));
        DUCKDB_GRAPHAR_LOG_DEBUG(GetInfoLogical(*op));

        replaceColumnsInOperator(op, replace_columns);
    }
    if (cur_result.optimized) {
        DUCKDB_GRAPHAR_LOG_DEBUG("optimized " + node_str(op, cur_i, depth) + "curr;\n" + op->ToString());
    }
    return result;
}


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
    result += "{c:";
    result += std::to_string(op.children.size());
    result += ",e:";
    result += std::to_string(op.expressions.size());
    result += ",t:";
    result += std::to_string(op.types.size());
    result += "}(";
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

static void FinalWalk(LogicalOperator &op) {
    DUCKDB_GRAPHAR_LOG_DEBUG("LO:\n" + GetInfoLogicalOperator(op) + '\n');
    if (op.type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
        auto &join = op.Cast<LogicalComparisonJoin>();
        DUCKDB_GRAPHAR_LOG_DEBUG("LJ:\n" + GetInfoLogicalJoin(join));
        DUCKDB_GRAPHAR_LOG_DEBUG("CJ:\n" + GetInfoComparisonJoin(join));
    }
    if (op.type == LogicalOperatorType::LOGICAL_GET) {
        auto &get = op.Cast<LogicalGet>();
        DUCKDB_GRAPHAR_LOG_DEBUG("LG:\n" + GetInfoLogicalGet(get));
    }
    for (auto &child : op.children) {
        if (child) {
            FinalWalk(*child);
        } 
    } 
}

static void GraphArPreOptimize(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan) {
    DUCKDB_GRAPHAR_LOG_TRACE("GraphArPreOptimize");

    DUCKDB_GRAPHAR_LOG_DEBUG("Has graphAr before optimize: " + std::to_string(HasGraphArScan(*plan)));
    DUCKDB_GRAPHAR_LOG_DEBUG("PRE OPTIMIZE:\n" + plan->ToString());
    // FinalWalk(*plan);

    DUCKDB_GRAPHAR_LOG_DEBUG("FINISHED");
}

static void GraphArOptimize(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan) {
    DUCKDB_GRAPHAR_LOG_TRACE("GraphArOptimize");

    // После оптимизации уже

    const bool hasGraphArScan = HasGraphArScan(*plan);

    DUCKDB_GRAPHAR_LOG_DEBUG("Has graphAr after optimize: " + std::to_string(hasGraphArScan));
    DUCKDB_GRAPHAR_LOG_DEBUG("after optimize:\n" + plan->ToString());
    DUCKDB_GRAPHAR_LOG_DEBUG("after depth: " + std::to_string(GetOperatorTree(*plan)))
    std::string tree;
    GetOperatorTree(*plan, tree);
    DUCKDB_GRAPHAR_LOG_DEBUG("after operators: " + tree);

    bool use_optimize = GraphArSettings::use_optimize(input.context);

    if (hasGraphArScan && use_optimize) {
        int i = 0;
        replace_col_map replace_columns;

        if (OptimizeJoins(plan, replace_columns, i).optimized) {
            DUCKDB_GRAPHAR_LOG_DEBUG("✓ Join optimization applied")
            plan->ResolveOperatorTypes();
            DUCKDB_GRAPHAR_LOG_DEBUG("Final plan:\n" + plan->ToString());
            FinalWalk(*plan);
            DUCKDB_GRAPHAR_LOG_DEBUG("ReplacedMap:\n" + map2str(replace_columns));
        }
        tree.clear();
        GetOperatorTree(*plan, tree);
        DUCKDB_GRAPHAR_LOG_DEBUG("FINAL PLAN\n" + tree);
    }
    if (!use_optimize) {
        DUCKDB_GRAPHAR_LOG_DEBUG("OPT x:\n" + plan->ToString());
    }
    
}

GraphArOptimizerExtension::GraphArOptimizerExtension() {
    DUCKDB_GRAPHAR_LOG_TRACE("GraphArOptimizerExtension");

    optimize_function = GraphArOptimize;
    pre_optimize_function = GraphArPreOptimize;
}

}  // namespace duckdb
