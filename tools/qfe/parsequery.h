#ifndef QFE_PARSEQUERY_H__
#define QFE_PARSEQUERY_H__

#include <unistd.h>
#include <string>

#include "calpontselectexecutionplan.h"

namespace qfe
{

execplan::CalpontSelectExecutionPlan* parseQuery(const std::string&, uint32_t);

}

#endif
