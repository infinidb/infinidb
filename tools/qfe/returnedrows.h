#ifndef QFE_RETURNEDROWS_H__
#define QFE_RETURNEDROWS_H__

#include "socktype.h"
#include "messagequeue.h"

namespace qfe
{

void processReturnedRows(messageqcpp::MessageQueueClient*, SockType);

}

#endif

