#include "func.hpp"

#include "duckdb/common/types.hpp"
#include "graphar/types.h"


namespace duckdb {

LogicalTypeId GrapharFunctions::grapharT2duckT(const std::string &name) {
	if (name == "bool") {
		return LogicalTypeId::BOOLEAN;
	} else if (name == "int32") {
		return LogicalTypeId::INTEGER;
	} else if (name == "int64") {
		return LogicalTypeId::BIGINT;
	} else if (name == "float") {
		return LogicalTypeId::FLOAT;
	} else if (name == "double") {
		return LogicalTypeId::DOUBLE;
	} else if (name == "string") {
		return LogicalTypeId::VARCHAR;
	} else if (name == "date") {
		return LogicalTypeId::DATE;
	} else if (name == "timestamp") {
		return LogicalTypeId::TIMESTAMP;
	}
	//	else if (name == "list<int32>") {
	//		return list(int32());
	//	} else if (name == "list<int64>") {
	//		return list(int64());
	//	} else if (name == "list<float>") {
	//		return list(float32());
	//	} else if (name == "list<double>") {
	//		return list(float64());
	//	} else if (name == "list<string>") {
	//		return list(string());
	//	}
}

}