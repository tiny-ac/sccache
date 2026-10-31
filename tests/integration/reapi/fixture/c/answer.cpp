#include "answer_base.h"

namespace {
constexpr int offset() { return 2; }
}

extern "C" int fixture_cpp_answer() { return ANSWER_BASE + offset(); }
