#ifndef QFE_DDLSTMTS_H__
#define QFE_DDLSTMTS_H__

#include <unistd.h>
#include <string>

namespace qfe
{
void processCreateStmt(const std::string&, uint32_t);
void processDropStmt(const std::string&, uint32_t);
}

#endif

