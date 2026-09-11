# Sprint 1 — DAG Parser

**Files:** `include/loom/dag/parser.h`, `src/dag/parser.cpp`, `tests/test_parser.cpp`
**Status:** Implemented. The `DAGValidator` header is scaffolded but its logic is
a later sprint.

---

## TL;DR ("the aha")

> **A parser is just a translator between two languages:** the DAG *JSON*
> language (a text spec humans write) and the DAG *struct* language (a typed,
> in-memory representation the engine actually runs). Everything in this sprint
> is answering the question: *"given this JSON shape, which C++ struct field
> does it map to, and how do I convert the string/enum/list?"*

Once you internalize that, every function in `parser.cpp` is the same pattern:

```
read a JSON key → convert to the right C++ type → stuff it into a struct field
```

There is **no clever logic** in a parser. There is only a large, boring,
*systematic* mapping. The difficulty is in the details:

- JSON values come in many shapes (`"5"`, `5`, `5.0`, `true`) — the parser must
  tolerate them.
- Enums are written as strings (`"generate"`) but stored as C++ enums.
- Config strings may contain `${ENV_VAR}` or `${ENV_VAR:-default}` — these must
  be resolved against the environment.
- The same logical concept can appear under two different key names across
  example files (`condition` vs `conditions`, `template` vs `template_`) — the
  parser must accept both.

---

## Objective

Read a `.dag.json` file (e.g. `pipelines/jira_scraper.dag.json`) and produce a
**validated, type-safe internal DAG IR** (intermediate representation) that the
rest of the engine consumes.

**Deliverable:** the example DAG parses without error; malformed DAGs are
rejected with clear `std::runtime_error` messages (missing `name`, missing
`start_at`, empty `steps`, invalid JSON, unknown step type).

---

## The domain model (what we translate INTO)

`include/loom/dag/parser.h` defines the target. It's a tree:

```
DAG
├── name, version, start_at          (top-level strings)
├── error_handling                   (max_attempts, backoff, base_ms)
├── connections: vector<Connection>  (name, type Http/S3, base_url, rate_limit,
│                                     max_inflight, backpressure, auth, ...)
└── steps: vector<Step>              (id, type, input, parallel, + type-specific fields)
    ├── Generate  → template_, request_queue, parameters
    ├── Transform → function
    ├── Filter    → conditions
    ├── Aggregate → group_by, aggregates
    ├── Join      → join_config
    ├── Unnest    → unnest
    ├── Sink      → output
    └── Stub      → reason
```

Every enum in the JSON is written as a lowercase string and must be mapped to a
`enum class`:

| JSON string | C++ enum |
|---|---|
| `"generate"` | `StepType::Generate` |
| `"http"` / `"s3"` | `ConnectionType::Http` / `ConnectionType::S3` |
| `"batch_complete"` / `"buffer_size"` / `"interval"` | `FlushTrigger::...` |
| `"inner"` / `"left"` | `JoinType::...` |
| `"dead_letter"` / `"drop"` / `"emit_null"` | `OnTimeout::...` |
| `"literal"` / `"list"` / `"range"` / `"column"` | `ParameterSourceKind::...` |

---

## The code, layer by layer

### Layer 0 — the JSON library (jsoncpp)

The only third-party dependency. `Json::CharReaderBuilder` parses a text stream
into a `Json::Value` tree:

```cpp
Json::Value root;
Json::CharReaderBuilder builder;
std::string errors;
std::istringstream stream(json);
if (!Json::parseFromStream(builder, stream, &root, &errors)) {
    throw std::runtime_error("JSON parse error: " + errors);
}
if (!root.isObject()) { throw ...; }   // top level must be { ... }
```

`Json::Value` is a dynamic tree: `.isObject()`, `.isArray()`, `.isMember(key)`,
`obj[key]`, `.isString()`, `.isInt()`, `.asInt()`, `.get(key, default)`.

### Layer 1 — env-var substitution (the "magic" that runs on every string)

```cpp
static std::string substitute_env_vars(const std::string& input) {
    static std::regex env_re(R"(\$\{([A-Za-z_][A-Za-z0-9_]*)(?::-([^}]*))?\})");
    // matches  ${NAME}            → group 1 = NAME
    //           ${NAME:-default}  → group 1 = NAME, group 2 = default
    ...
    std::smatch m;
    while (std::regex_search(it, end, m, env_re)) {
        result += m.prefix();                    // text before the match
        const char* val = std::getenv(m[1].str().c_str());
        if (val && val[0] != '\0') result += val;         // env var wins
        else if (m[2].matched)  result += m[2].str();     // else the default
        it = m.suffix().first;
    }
    result.append(it, end);
    return result;
}
```

**Aha:** this is applied inside `get_str()`, so **every string** read from the
JSON gets environment substitution for free. A connection's
`"base_url": "${MINIO_ENDPOINT:-http://localhost:9000}"` becomes its real value
at parse time.

### Layer 2 — the typed getters (the conversion workhorses)

These five functions are the entire "tolerate JSON's many shapes" story:

```cpp
static std::string get_str(const Json::Value& obj, const std::string& key,
                           const std::string& def = "") {
    if (!obj.isMember(key)) return substitute_env_vars(def);  // missing → default
    const auto& v = obj[key];
    if (v.isString()) return substitute_env_vars(v.asString());  // string → string
    if (v.isInt() || v.isInt64() || v.isUInt64()) return std::to_string(v.asInt64());
    if (v.isDouble()) return std::to_string(v.asDouble());   // number → string
    if (v.isBool()) return v.asBool() ? "true" : "false";    // bool → string
    return def;
}
```

`get_int`, `get_bool`, `get_double`, `get_int64` follow the same shape but
convert **toward** their type (e.g. `"5"` → `5`, `true` → `1`). Write these once,
then every struct parser is a single line per field.

### Layer 3 — enum mappers

One function per enum, mapping the JSON string to the C++ value, throwing on
unknown input:

```cpp
dag::StepType parse_step_type(const std::string& s) {
    if (s == "generate")  return dag::StepType::Generate;
    if (s == "transform") return dag::StepType::Transform;
    ... 8 cases ...
    throw std::runtime_error("Unknown step type: '" + s + "'");
}
```

### Layer 4 — struct parsers (the boring-but-complete part)

One function per struct, each following the exact same recipe:

```cpp
static dag::Connection parse_connection(const std::string& name, const Json::Value& obj) {
    dag::Connection conn;
    conn.name = name;
    conn.type = parse_connection_type(get_str(obj, "type", "http"));
    conn.base_url = get_str(obj, "base_url");
    if (obj.isMember("rate_limit") && obj["rate_limit"].isObject())
        conn.rate_limit = parse_rate_limit(obj["rate_limit"]);   // nested objects recurse
    conn.max_inflight = get_int(obj, "max_inflight", 10);
    ...
    return conn;
}
```

Notice: nested JSON objects → nested parser calls (recursion). Missing optional
fields → the struct's C++ default value. Arrays → a `for` loop pushing elements.

### Layer 5 — two "dual-format" special cases

**Connections can be an array OR an object:**

```json
// array form
"connections": [ { "name": "jira_api", "type": "http", ... } ]

// object form (name is the KEY, not a field)
"connections": { "jira_api": { "type": "http", ... } }
```

```cpp
static std::vector<dag::Connection> parse_connections(const Json::Value& val) {
    std::vector<dag::Connection> conns;
    if (val.isArray()) {
        for (const auto& c : val) conns.push_back(parse_connection(get_str(c, "name"), c));
    } else if (val.isObject()) {
        for (auto it = val.begin(); it != val.end(); ++it)
            conns.push_back(parse_connection(it.name(), *it));   // key = name
    }
    return conns;
}
```

**Step fields have tolerated aliases:** `condition` OR `conditions`, and
`template` OR `template_` (the trailing underscore exists because `template` is
a C++ keyword). Both are checked.

### Layer 6 — `parse_step` (type dispatch)

A step is a union: most fields exist on all steps, but the *meaningful* ones
depend on `type`. So: parse the common fields, then dispatch:

```cpp
static dag::Step parse_step(const Json::Value& obj) {
    dag::Step step;
    step.id          = get_str(obj, "id");
    step.type        = parse_step_type(get_str(obj, "type", "stub"));
    step.description = get_str(obj, "description");
    step.connection  = get_str(obj, "connection");
    if (obj.isMember("input") && obj["input"].isObject())
        step.input = parse_step_input(obj["input"]);
    if (obj.isMember("parameters") && obj["parameters"].isArray())
        for (const auto& p : obj["parameters"]) step.parameters.push_back(parse_parameter(p));
    step.parallel = get_int(obj, "parallel", 1);

    if (obj.isMember("template") && obj["template"].isObject())
        step.template_ = parse_request_template(obj["template"]);
    else if (obj.isMember("template_") && obj["template_"].isObject())
        step.template_ = parse_request_template(obj["template_"]);

    if (obj.isMember("function")  && obj["function"].isObject())
        step.function = parse_transform_function(obj["function"]);
    ... conditions / group_by / aggregates / join_config / unnest / output ...
    step.reason = get_str(obj, "reason");   // stub
    return step;
}
```

### Layer 7 — the public entry points

```cpp
dag::DAG DAGParser::parse_string(const std::string& json) {
    // 1. jsoncpp → Json::Value tree
    // 2. reject non-objects
    // 3. name = get_str(root,"name"); version = ...; start_at = ...
    // 4. throw if name or start_at missing
    // 5. if root has "error_handling" → parse_error_handling (flat OR {"retry":{...}})
    // 6. if root has "connections" → parse_connections
    // 7. if root has "steps" array → parse_step each
    // 8. throw if steps empty
    return dag;
}

dag::DAG DAGParser::parse_file(const std::string& path) {
    std::ifstream file(path);
    if (!file.is_open()) throw std::runtime_error("Cannot open file: " + path);
    std::ostringstream buf; buf << file.rdbuf();
    return parse_string(buf.str());   // read file, then reuse parse_string
}
```

---

## The "aha" patterns to internalize

1. **Parser = mapping.** Every function maps one JSON shape to one struct.
   No business logic, no cleverness — just systematic conversion + defaults.
2. **`get_str/get_int/get_bool` are the foundation.** Write them once, robustly,
   and every field becomes a one-liner.
3. **Recursion for nesting.** Nested objects → nested parser calls. Arrays →
   loops.
4. **Tolerate aliases.** Real-world files are inconsistent
   (`condition`/`conditions`); parse both.
5. **Validate early, throw with context.** `"DAG missing required field: 'name'"`
   beats a silent empty struct.

## Replicate it yourself (recipe)

1. Define your structs in a header (DAG, Step, Connection, + all enums/structs).
2. Include jsoncpp; write `get_str/get_int/get_bool/get_double/get_int64` with
   `isMember` guards and defaults.
3. Write `substitute_env_vars` and call it inside `get_str`.
4. Write one `parse_X` per struct; recurse for nested objects; loop for arrays.
5. Handle the two dual-formats (connections array-vs-object; field aliases).
6. `parse_string`: parse → extract scalars → validate required → optional blocks
   → steps → return.
7. `parse_file`: open file, read to string, call `parse_string`.
8. Test with a minimal DAG, the real `pipelines/jira_scraper.dag.json`, and a
   handful of deliberately-broken DAGs (each must throw a clear error).
