#include <gtest/gtest.h>
#include "loom/dag/parser.h"

TEST(DAGParserTest, ParseEmptyStringThrows) {
    EXPECT_THROW(loom::DAGParser::parse_string("{}"), std::runtime_error);
}

TEST(DAGParserTest, ParseFileThrows) {
    EXPECT_THROW(loom::DAGParser::parse_file("/nonexistent/path"), std::runtime_error);
}
