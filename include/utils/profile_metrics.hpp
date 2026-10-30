#pragma once

#include <duckdb/main/client_config.hpp>
#include <duckdb/main/client_context.hpp>
#include <duckdb/main/profiler/gathered_metrics.hpp>
#include <duckdb/main/profiler/metrics_manager.hpp>
#include <duckdb/main/profiler/profiling_utils.hpp>
#include <duckdb/main/query_profiler.hpp>

namespace GraphArMetrics {
struct State {
	static constexpr std::string SetBindData = "SetBindData";
	static constexpr std::string Bind = "Bind";
	static constexpr std::string Init = "Init";
	static constexpr std::string InitLocal = "InitLocal";
	static constexpr std::string Execute = "Execute";
};
} // namespace GraphArMetrics

namespace duckdb {


inline bool GraphArMetricIsTracked(ClientContext &context, const string &key) {
	auto &profiler = QueryProfiler::Get(context);
	if (!profiler.IsEnabled()) {
		return false;
	}
	GatheredMetrics tracked_metrics(ClientConfig::GetConfig(context).tracked_metrics);
	return tracked_metrics.MetricIsTracked(key);
}

class GraphArQueryTimer {
public:
	GraphArQueryTimer(ClientContext &context, const string &key) {
		if (!GraphArMetricIsTracked(context, key)) {
			return;
		}
		timer = QueryProfiler::Get(context).StartTimerInternal(key);
	}

private:
	MetricsTimer timer;
};

// MetricsManager so they are discoverable via duckdb_available_metrics()
inline void RegisterGraphArMetrics(MetricsManager &manager) {
	auto register_double_metric = [&](const string &name, const string &description) {
		MetricInfo info;
		info.name = name;
		info.metric_type = "double";
		info.description = description;
		info.unit = "seconds";
		manager.RegisterMetric(std::move(info));
	};
}

// function, e.g. "graphar.read_vertices.Execute_time".
inline string GraphArMetricName(const string &function, const string &phase) {
	return "graphar." + function + "." + phase + "_time";
}

// Register the bind/init/execute timing metrics for one table function.
inline void RegisterGraphArFunctionMetrics(MetricsManager &manager, const string &function) {
	auto register_double_metric = [&](const string &name, const string &description) {
		MetricInfo info;
		info.name = name;
		info.metric_type = "double";
		info.description = description;
		info.unit = "seconds";
		manager.RegisterMetric(std::move(info));
	};

	register_double_metric(GraphArMetricName(function, GraphArMetrics::State::SetBindData), "Time spent set bind state the " + function + " table function");
	register_double_metric(GraphArMetricName(function, GraphArMetrics::State::Bind), "Time spent binding the " + function + " table function");
	register_double_metric(GraphArMetricName(function, GraphArMetrics::State::Init), "Time spent initializing global state the " + function + " table function");
	register_double_metric(GraphArMetricName(function, GraphArMetrics::State::InitLocal), "Time spent initializing local state the " + function + " table function");
	register_double_metric(GraphArMetricName(function, GraphArMetrics::State::Execute), "Time spent executing the " + function + " table function");
}

}  // namespace duckdb