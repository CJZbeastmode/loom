#pragma once

#include <string>
#include <vector>

namespace loom {
namespace dag {

struct DAG;
struct ValidationError {
    std::string field;
    std::string message;
    enum Severity { Warning, Error } severity = Error;
};

class DAGValidator {
public:
    DAGValidator() = default;

    std::vector<ValidationError> validate(const DAG& dag);
    bool is_valid(const DAG& dag);

private:
    void validate_structure(const DAG& dag, std::vector<ValidationError>& errors);
    void validate_connections(const DAG& dag, std::vector<ValidationError>& errors);
    void validate_steps(const DAG& dag, std::vector<ValidationError>& errors);
    void validate_dependencies(const DAG& dag, std::vector<ValidationError>& errors);
    void validate_types(const DAG& dag, std::vector<ValidationError>& errors);
};

}  // namespace dag
}  // namespace loom
