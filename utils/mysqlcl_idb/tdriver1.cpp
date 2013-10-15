#include <iostream>
//#define NDEBUG
#include <cassert>
using namespace std;

#include "libdrizzle-2.0/drizzle.h"
#include "libdrizzle-2.0/drizzle_client.h"

int main(int argc, char** argv)
{
	drizzle_st* drzp=0;
	drzp = drizzle_create();
	assert(drzp);

	drizzle_set_verbose(drzp, DRIZZLE_VERBOSE_DEBUG);

	drizzle_con_st* drzcp=0;
	drzcp = drizzle_con_create(drzp);
	assert(drzcp);

	//drizzle_con_add_tcp(drzp, "127.0.0.1", 13306, "rjd", "", "tpch1", DRIZZLE_CON_MYSQL);
	drizzle_con_set_tcp(drzcp, "127.0.0.1", 13306);
	drizzle_con_set_auth(drzcp, "rjd", "");
	drizzle_con_set_db(drzcp, "tpch1");
	drizzle_con_add_options(drzcp, DRIZZLE_CON_MYSQL);

	drizzle_con_connect(drzcp);

	drizzle_result_st* drzrp=0;
	drzrp = drizzle_result_create(drzcp);
	assert(drzrp);

	const char* query = "select n_nationkey from nation";
	drizzle_return_t drztr;
	drizzle_return_t* drztrp=0;
	drztrp = &drztr;
	drzrp = drizzle_query_str(drzcp, drzrp, query, drztrp);
	assert(drzrp);
	cout << "return_t = " << drztr << endl;

	uint64_t nrows=0;
	nrows = drizzle_row_read(drzrp, drztrp);
	cout << "return_t = " << drztr << endl;
	cout << "rows returned = " << nrows << endl;

	nrows = drizzle_result_row_count(drzrp);
	cout << "rows returned = " << nrows << endl;

	drizzle_con_close(drzcp);

	return 0;
}

