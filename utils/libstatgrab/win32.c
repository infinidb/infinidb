#include "statgrab.h"
#ifdef WIN32
#include <pdh.h>
#include <pdhmsg.h>
#include "win32.h"
#include <stdio.h>
#include "tools.h"

static HQUERY h_query;

static HCOUNTER current_han[SG_WIN32_SIZE];

static char **diskio_names;
static HCOUNTER *diskio_rhan;
static HCOUNTER *diskio_whan;
static int diskio_no;

static int is_started = 0;

/*static char **netio_names;
static HCOUNTER *netio_rhan;
static HCOUNTER *netio_shan;
static int netio_no;*/

int add_counter(const char *fullCounterPath, HCOUNTER *phCounter)
{
	PDH_STATUS pdh_status;

	pdh_status = PdhAddCounter(h_query, fullCounterPath,
			0, phCounter);
	if(pdh_status != ERROR_SUCCESS) {
		//sg_set_error(SG_ERROR_PDHADD, fullCounterPath);
		phCounter = NULL;
		return -1;
	}
	return 0;
}

int read_counter_double(pdh_enum counter, double *result)
{
	PDH_STATUS pdh_status;
	PDH_FMT_COUNTERVALUE *item_buf;
	HCOUNTER hcounter = current_han[counter];

	if(hcounter == NULL)
		return -1;

	item_buf = sg_malloc(sizeof(PDH_FMT_COUNTERVALUE));
	if (item_buf == NULL) {
		return -1;
	}

	pdh_status = PdhGetFormattedCounterValue(hcounter, PDH_FMT_DOUBLE, NULL,
			item_buf);
	if(pdh_status != ERROR_SUCCESS) {
		free(item_buf);
		return -1;
	}
	*result = item_buf->doubleValue;
	free(item_buf);
	return 0;
}

static int read_counter_large_int(HCOUNTER hcounter, long long *result)
{
	PDH_STATUS pdh_status;
	PDH_FMT_COUNTERVALUE *item_buf;

	if(hcounter == NULL)
		return -1;

	item_buf = sg_malloc(sizeof(PDH_FMT_COUNTERVALUE));
	if (item_buf == NULL) {
		return -1;
	}

	pdh_status = PdhGetFormattedCounterValue(hcounter, PDH_FMT_LARGE, NULL,
			item_buf);
	if(pdh_status != ERROR_SUCCESS) {
		free(item_buf);
		/*switch(pdh_status) {
			case PDH_INVALID_ARGUMENT:
				printf("invalid argument\n");
				break;
			case PDH_INVALID_DATA:
				printf("invalid data\n");
				break;
			case PDH_INVALID_HANDLE:
				printf("invalid handle\n");
				break;
		}*/
		return -1;
	}
	*result = item_buf->largeValue;
	free(item_buf);
	return 0;
}

int read_counter_large(pdh_enum counter, long long *result)
{
	return read_counter_large_int(current_han[counter], result);
}

static char *get_instances(const char *object)
{
	PDH_STATUS pdh_status;
	char *instancelistbuf;
	DWORD instancelistsize = 0;
	char *counterlistbuf;
	DWORD counterlistsize = 0;

	/* Get necessary size of buffers */
	pdh_status = PdhEnumObjectItems(NULL, NULL, object, NULL,
			&counterlistsize, NULL, &instancelistsize,
			PERF_DETAIL_WIZARD, 0);
	/* 2k is dodgy and returns ERROR_SUCCESS even though the buffers were
	 * NULL */
	if(pdh_status == PDH_MORE_DATA || pdh_status == ERROR_SUCCESS) {
		instancelistbuf = sg_malloc(instancelistsize * sizeof(TCHAR));
		counterlistbuf = sg_malloc(counterlistsize * sizeof(TCHAR));
		if (instancelistbuf != NULL && counterlistbuf != NULL) {
			pdh_status = PdhEnumObjectItems(NULL, NULL, object,
					counterlistbuf, &counterlistsize,
					instancelistbuf, &instancelistsize,
					PERF_DETAIL_WIZARD, 0);
			if (pdh_status == ERROR_SUCCESS) {
				free(counterlistbuf);
				return instancelistbuf;
			}
		}
		if (counterlistbuf != NULL)
			free(counterlistbuf);
		if(instancelistbuf != NULL)
			free(instancelistbuf);
	}
	return NULL;
}

/* Gets the instance buffer. Removes _Total item. Works out how many items
 * there are. Returns these in a pointer list, rather than single buffer.
 */
static char **get_instance_list(const char *object, int *n)
{
	char *thisinstance = NULL;
	char *instances = NULL;
	char **list;
	char **listtmp = NULL;
	int i;
	*n = 0;

	instances = get_instances(object);
	if (instances == NULL)
		return NULL;

	list = (char **)sg_malloc(sizeof(char *));
	if (list == NULL) {
		return NULL;
	}
	for (thisinstance = instances; *thisinstance != 0;
			thisinstance += strlen(thisinstance) + 1) {
		/* Skip over the _Total item */
		if (strcmp(thisinstance,"_Total") == 0) continue;

		listtmp = (char **)sg_realloc(list, sizeof(char *) * ((*n)+1));
		if (listtmp == NULL) {
			goto out;
		}
		list = listtmp;
		list[*n] = sg_malloc(strlen(thisinstance) +1);
		if(list[*n] == NULL) {
			goto out;
		}
		list[*n] = strcpy(list[*n], thisinstance);
		++*n;
	}
	free (instances);
	return list;

out:
	for (i = 0; i < *n; i++) {
		free(list[i]);
	}
	free(list);
	return NULL;
}

char *get_diskio(const int no, long long *read, long long *write)
{
	int result;
	char *name = NULL;

	if (no >= diskio_no || no < 0)
		return NULL;

	result = read_counter_large_int(diskio_rhan[no], read);
	result = result + read_counter_large_int(diskio_whan[no], write);
	if (result) {
		sg_set_error(SG_ERROR_PDHREAD, "diskio");
		return NULL;
	}
	if (sg_update_string(&name, diskio_names[no]))
		return NULL;
	return name;
}

/* We do not fail on add_counter here, as some counters may not exist on all
 * hosts. The rest will work, and the missing/failed counters will die nicely
 * elsewhere */
static int add_all_monitors()
{
	int i;
	char tmp[512];

	add_counter(PDH_USER, &(current_han[SG_WIN32_PROC_USER]));
	add_counter(PDH_PRIV, &(current_han[SG_WIN32_PROC_PRIV]));
	add_counter(PDH_IDLE, &(current_han[SG_WIN32_PROC_IDLE]));
	add_counter(PDH_INTER, &(current_han[SG_WIN32_PROC_INT]));
	add_counter(PDH_MEM_CACHE, &(current_han[SG_WIN32_MEM_CACHE]));
	add_counter(PDH_UPTIME, &(current_han[SG_WIN32_UPTIME]));
	add_counter(PDH_PAGEIN, &(current_han[SG_WIN32_PAGEIN]));
	add_counter(PDH_PAGEOUT, &(current_han[SG_WIN32_PAGEOUT]));

	diskio_names = get_instance_list("PhysicalDisk", &diskio_no);
	if (diskio_names != NULL) {
		diskio_rhan = (HCOUNTER *)malloc(sizeof(HCOUNTER) * diskio_no);
		if (diskio_rhan == NULL) {
			PdhCloseQuery(h_query);
			return -1;
		}
		diskio_whan = (HCOUNTER *)malloc(sizeof(HCOUNTER) * diskio_no);
		if (diskio_whan == NULL) {
			PdhCloseQuery(h_query);
			free (diskio_rhan);
			return -1;
		}
		for (i = 0; i < diskio_no; i++) {
			snprintf(tmp, sizeof(tmp), PDH_DISKIOREAD, diskio_names[i]);
			add_counter(tmp, &diskio_rhan[i]);

			snprintf(tmp, sizeof(tmp), PDH_DISKIOWRITE, diskio_names[i]);
			add_counter(tmp, &diskio_whan[i]);
		}
	}
	return 0;
}
#endif

/* Call before trying to get search results back, otherwise it'll be dead data
 */
int sg_win32_snapshot()
{
#ifdef WIN32
	PDH_STATUS pdh_status;

	if(!is_started) {
		return -1;
	}

	pdh_status = PdhCollectQueryData(h_query);
	if(pdh_status != ERROR_SUCCESS) {
		sg_set_error(SG_ERROR_PDHCOLLECT, NULL);
		return -1;
	}
#endif
	return 0;
}

/* Must be called before any values can be read. This creates all the 
 * necessary PDH values
 */
int sg_win32_start_capture()
{
#ifdef WIN32
	PDH_STATUS pdh_status;

	if(is_started) {
		return -1;
	}

	pdh_status = PdhOpenQuery(NULL, 0, &h_query);

	if(pdh_status != ERROR_SUCCESS) {
		char *mess = NULL;
		if(pdh_status == PDH_INVALID_ARGUMENT)
			mess = "Invalid argument o.O";
		else if(pdh_status == PDH_MEMORY_ALLOCATION_FAILURE)
			mess = "Memory allocation failure";
		sg_set_error(SG_ERROR_PDHOPEN, mess);
		return -1;
	}
	if (add_all_monitors() == -1) {
		return -1;
	}

	is_started = 1;
#endif
	return 0;
}

/* Must be called before the program exits, or to close all the capture items
 * before opening with a call to start_capture
 */
void sg_win32_end_capture()
{
#ifdef WIN32
	int i;
	PDH_STATUS pdh_status;

	if(!is_started) {
		return;
	}

	pdh_status = PdhCloseQuery(h_query);
	for (i=0; i < SG_WIN32_SIZE; ++i) {
		PdhRemoveCounter(current_han[i]);
		current_han[i] = NULL;
	}
	/*free_io(&diskio_no, diskio_names, diskio_rhan, diskio_whan);
	free_io(&netio_no, netio_names, netio_rhan, netio_shan);*/
	for (i=0; i < diskio_no; ++i) {
		PdhRemoveCounter(diskio_rhan[i]);
		PdhRemoveCounter(diskio_whan[i]);
		free(diskio_names[i]);
	}
	free(diskio_names);
	free(diskio_rhan);
	free(diskio_whan);
	diskio_no = 0;

	is_started = 0;
#endif
}

