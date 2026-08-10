#include "loom/dag/parser.h"
#include <stdexcept>

namespace loom {

dag::DAG DAGParser::parse_file(const std::string& path) {
    throw std::runtime_error("DAGParser::parse_file not yet implemented");
}

dag::DAG DAGParser::parse_string(const std::string& json) {
    throw std::runtime_error("DAGParser::parse_string not yet implemented");
}

}  // namespace loom
