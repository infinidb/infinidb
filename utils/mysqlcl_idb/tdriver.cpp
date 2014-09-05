#include <iostream>
//#define NDEBUG
#include <cassert>
using namespace std;

#include "libdrizzle-2.0/drizzle.h"
#include "libdrizzle-2.0/drizzle_client.h"

namespace
{
int doit()
{
	drizzle_st* drzp=0;
	drzp = drizzle_create();
	assert(drzp);

	//drizzle_set_verbose(drzp, DRIZZLE_VERBOSE_DEBUG);

	drizzle_con_st* drzcp=0;
	drzcp = drizzle_con_add_tcp(drzp, "127.0.0.1", 13306, "rjd", "", "tpch1", DRIZZLE_CON_MYSQL);

	drizzle_return_t drztr;
	drztr = drizzle_con_connect(drzcp);
	assert(drztr==0);

	//const char* query = "select * from nation";
	const char* query = "select * from region";
	drizzle_return_t* drztrp=0;
	drztrp = &drztr;
	drizzle_result_st* drzrp=0;
	//drzrp=drizzle_result_create(drzcp);
	//assert(drzrp);
	drzrp = drizzle_query_str(drzcp, drzrp, query, drztrp);
	assert(drzrp);
	cout << "return_t = " << drztr << endl;

	drztr = drizzle_result_buffer(drzrp);
	cout << "return_t = " << drztr << endl;

	drizzle_row_t row;
	row = drizzle_row_next(drzrp);
	while (row)
	{
		//cout << "got row: " << row[0] << ',' << row[1] << ',' << row[2] << '.' << row[3] << endl;
		cout << "got row: " << row[0] << ',' << row[1] << ',' << row[2] << endl;
		//drizzle_row_free(drzrp, row);
		row = drizzle_row_next(drzrp);
	}
	drizzle_result_free(drzrp);
	drzrp = 0;

	drizzle_con_close(drzcp);
	drizzle_con_free(drzcp);
	drzcp = 0;
	drizzle_free(drzp);
	drzp = 0;

	return 0;
}

int insertit()
{
	drizzle_st* drzp=0;
	drzp = drizzle_create();
	assert(drzp);

	//drizzle_set_verbose(drzp, DRIZZLE_VERBOSE_DEBUG);

	drizzle_con_st* drzcp=0;
	drzcp = drizzle_con_add_tcp(drzp, "127.0.0.1", 13306, "rjd", "", "tpch1", DRIZZLE_CON_MYSQL);

	drizzle_return_t drztr;
	drztr = drizzle_con_connect(drzcp);
	assert(drztr==0);

	//const char* query = "select * from nation";
	const char* query = "insert into foo values (1)";
	drizzle_return_t* drztrp=0;
	drztrp = &drztr;
	drizzle_result_st* drzrp=0;
	//drzrp=drizzle_result_create(drzcp);
	//assert(drzrp);
	drzrp = drizzle_query_str(drzcp, drzrp, query, drztrp);
	assert(drzrp);
	cout << "return_t = " << drztr << endl;

	drztr = drizzle_result_buffer(drzrp);
	cout << "return_t = " << drztr << endl;

	drizzle_result_free(drzrp);
	drzrp = 0;

	drizzle_con_close(drzcp);
	drizzle_con_free(drzcp);
	drzcp = 0;
	drizzle_free(drzp);
	drzp = 0;

	return 0;
}

}

int main(int argc, char** argv)
{
	doit();
	//doit();
	//doit();
	//doit();
	insertit();

	return 0;
}

