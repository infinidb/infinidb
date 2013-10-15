#ifndef QFE_SENDCSEP_H__
#define QFE_SENDCSEP_H__

#include "messagequeue.h"
#include "calpontselectexecutionplan.h"

namespace qfe
{

//Takes ownership of the alloc'd ptr
//Returns an alloc'd mqc
messageqcpp::MessageQueueClient* sendCSEP(execplan::CalpontSelectExecutionPlan*);

}

#endif

