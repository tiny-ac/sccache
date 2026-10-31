#include <stddef.h>
#include "answer_base.h"

int fixture_c_answer(void) { return ANSWER_BASE + 1; }

const char *fixture_c_file(void) { return __FILE__; }
