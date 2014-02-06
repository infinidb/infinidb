#include <unistd.h>
#include <string>
using namespace std;

#include "constantcolumn.h"
#include "simplefilter.h"
#include "calpontsystemcatalog.h"
#include "parsetree.h"
#include "simplecolumn.h"
#include "calpontselectexecutionplan.h"
using namespace execplan;

#include "cseputils.h"


namespace qfe
{
extern string DefaultSchema;

namespace utils
{

ConstantColumn* createConstCol(const string& valstr)
{
	ConstantColumn* cc= new ConstantColumn(valstr);
	cc->alias(valstr);
	return cc;
}

template <typename T>
ConstantColumn* createConstCol(const string& valstr, T val)
{
	ConstantColumn* cc= new ConstantColumn(valstr, val);
	cc->alias(valstr);
	return cc;
}

SimpleFilter* createSimpleFilter
				(
				boost::shared_ptr<CalpontSystemCatalog>& csc,
				const CalpontSystemCatalog::TableColName& tcn,
				const string& opstr,
				ConstantColumn* cc
				)
{
	SimpleFilter* lsf = new SimpleFilter();

	Operator* op = new Operator();
	op->data(opstr);
	CalpontSystemCatalog::ColType ccct;
	ccct = op->resultType();
	ccct.colDataType = cc->resultType().colDataType;
	op->operationType(ccct);

	SOP sop(op);
	lsf->op(sop);

	CalpontSystemCatalog::OID oid = csc->lookupOID(tcn);
	CalpontSystemCatalog::ColType ct = csc->colType(oid);

	SimpleColumn* sc = new SimpleColumn();
	sc->schemaName(tcn.schema);
	sc->tableName(tcn.table);
	sc->tableAlias(tcn.table);
	sc->columnName(tcn.column);
	sc->oid(oid);
	sc->resultType(ct);
	sc->alias(tcn.toString());

	lsf->lhs(sc);
	lsf->rhs(cc);

	return lsf;
}

void appendSimpleFilter
				(
				ParseTree*& ptree,
				SimpleFilter* filter
				)
{
	if( ptree->data() == 0 )
	{
		// degenerate case, this filter goes at this node
		ptree->data( filter );
	}
	else if( ptree->right() == 0 && ptree->left() == 0 )
	{
		// this will be the case when there is a single node in the tree
		// that contains a filter.  Here we want to make the root node an
		// 'and' operator, push the existing down to the lhs and make a
		// new node for the new filter
		ParseTree* newLhs = new ParseTree( ptree->data() );
		ParseTree* newRhs = new ParseTree( filter );

		Operator* op = new Operator();
		op->data("and");

		ptree->data( op );
		ptree->left( newLhs );
		ptree->right( newRhs );
	}
	else
	{
		// this will be the case once we have a tree with an 'and' at the
		// root node, a filter in the lhs, and an arbitrary height tree
		// with the same properties on the rhs.  Because all operators
		// are guaranteed to be and for now we simply insert a new rhs
		// node and "push down" the existing tree
		Operator* op = new Operator();
		op->data("and");

		ParseTree* newRhs = new ParseTree( op );
		newRhs->left( new ParseTree( filter ) );
		newRhs->right( ptree->right() );
		ptree->right( newRhs );
	}
}

void updateParseTree(boost::shared_ptr<execplan::CalpontSystemCatalog>& csc,
	execplan::CalpontSelectExecutionPlan*& csep,
	execplan::SimpleColumn* sc,
	const std::string& relop, pair<int, string> cval)
{
	execplan::ConstantColumn* cc=0;
	if (cval.first == 0)
		cc = createConstCol(cval.second, static_cast<int64_t>(atoll(cval.second.c_str())));
	else
		cc = createConstCol(cval.second);
	if (sc->schemaName() == "infinidb_unknown" && !DefaultSchema.empty())
		sc->schemaName(DefaultSchema);
	execplan::SimpleFilter* sf=0;
	sf = createSimpleFilter(csc,
		execplan::make_tcn(sc->schemaName(), sc->tableName(), sc->columnName()),
		relop, cc);
	execplan::ParseTree* ptp=0;
	ptp = csep->filters();
	if (ptp == 0)
		ptp = new execplan::ParseTree();
	appendSimpleFilter(ptp, sf);
	csep->filters(ptp);
}

//template instantiations
template ConstantColumn* createConstCol<int64_t>(const string& valstr, int64_t val);

} //namespace qfe::utils
} //namespace qfe

