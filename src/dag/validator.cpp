#include "loom/dag/validator.h"
#include "loom/dag/parser.h"

namespace loom {
namespace dag {

std::vector<ValidationError> DAGValidator::validate(const DAG& /*dag*/) {
    return {};
}

bool DAGValidator::is_valid(const DAG& dag) {
    return validate(dag).empty();
}

void DAGValidator::validate_structure(const DAG& /*dag*/, std::vector<ValidationError>& /*errors*/) {}
void DAGValidator::validate_connections(const DAG& /*dag*/, std::vector<ValidationError>& /*errors*/) {}
void DAGValidator::validate_steps(const DAG& /*dag*/, std::vector<ValidationError>& /*errors*/) {}
void DAGValidator::validate_dependencies(const DAG& /*dag*/, std::vector<ValidationError>& /*errors*/) {}
void DAGValidator::validate_types(const DAG& /*dag*/, std::vector<ValidationError>& /*errors*/) {}

}  // namespace dag
}  // namespace loom
