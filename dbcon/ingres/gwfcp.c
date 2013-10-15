#include <dlfcn.h>

#include    <compat.h>
#include    <gl.h>
#include    <sl.h>
#include    <me.h>
#include    <pc.h>
#include    <iicommon.h>
#include    <dbdbms.h>
#include    <cs.h>
#include    <lk.h>
#include    <st.h>
#include    <adf.h>
#include    <scf.h>
#include    <dmf.h>
#include    <dmrcb.h>
#include    <dmtcb.h>
#include    <ulm.h>

#include    <gwf.h>
#include    <gwfint.h>

#include    <adudate.h>

typedef struct
{
    PTR	    gwdmf_rhandle;  /* Record access id. This value is returned
			    ** to us by dmt_open and must be provided as
			    ** input to all subsequent record-oriented
			    ** dmf calls we make on this table.
			    */
} GWDMF_RSB;

/*
gwcp_begin
gwcp_open
gwcp_position
gwcp_get
gwcp_close
*/

GLOBALREF   char    *Gwdmfcp_version;
static	    DB_STATUS	(*dmf_cptr)();

//static unsigned int col1;

static void* libptr;

static void* (*gwcp_getqctx)(void);
static int (*gwcp_sendplan)(void*);
static void* (*gwcp_opentbl)(void*, const char*, const char*);
static int (*gwcp_getrows)(void*, void*, char*, unsigned, unsigned);
static int (*gwcp_closetbl)(void*, void*);
static int (*gwcp_relqctx)(void*);

static void* gwcp_qctx;
static void* gwcp_tctx;

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

static void l1(const char* msg)
{
	int fd;
	fd = open("/tmp/gwfcp_debug.log", O_CREAT|O_WRONLY|O_APPEND);
	if (fd >= 0)
	{
		write(fd, msg, strlen(msg));
		write(fd, "\n", 1);
		close(fd);
	}
}

//out must be maxlen+1
static void strfix(char* out, const char* in, unsigned maxlen)
{
	char* p;
	strncpy(out, in, maxlen);
	out[maxlen] = 0;
	p = &out[maxlen - 1];
	while (*p == ' ')
	{
		*p = 0;
		if (--p < &out[0])
			break;
	}
}

static void* loadsym(void* libptr, const char* name)
{
	void* fcnptr;
	const char* dlsymerr;
	(void)dlerror();
	fcnptr = dlsym(libptr, name);
	dlsymerr = dlerror();
	if (dlsymerr != NULL)
	{
		l1("dlsym failed");
		return NULL;
	}
	return fcnptr;
}

DB_STATUS
gwcp_term( gwx_rcb )
GWX_RCB	    *gwx_rcb;
{
	l1("gwcp_term");
    gwx_rcb->xrcb_error.err_code = E_DB_OK;
    return (E_DB_OK);
}

DB_STATUS
gwcp_tabf( gwx_rcb )
GWX_RCB	    *gwx_rcb;
{
	l1("gwcp_tabf");
    gwx_rcb->xrcb_error.err_code = E_DB_OK;
    return (E_DB_OK);
}

DB_STATUS
gwcp_begin( gwx_rcb )
GWX_RCB	    *gwx_rcb;
{
	DB_STATUS dbs = E_DB_OK;
	int dloflags = RTLD_NOW|RTLD_GLOBAL|RTLD_NODELETE|RTLD_NOLOAD;
	const char* libname = "/home/rdempsey/genii/export/lib/libcaling.so";

	l1("gwcp_begin");

	(void)dlerror();
	libptr = dlopen(libname, dloflags);
	if (libptr == NULL)
	{
		dloflags &= ~(RTLD_NOLOAD);
		(void)dlerror();
		libptr = dlopen(libname, dloflags);
		if (libptr == NULL)
		{
			l1("dlopen failed");
		}
		else
		{
			l1("dlopen succeeded");
		}
	}

	if (libptr != NULL)
	{
		gwcp_getqctx = loadsym(libptr, "ing_getqctx");
		gwcp_sendplan = loadsym(libptr, "ing_sendplan");
		gwcp_opentbl = loadsym(libptr, "ing_opentbl");
		gwcp_getrows = loadsym(libptr, "ing_getrows");
		gwcp_closetbl = loadsym(libptr, "ing_closetbl");
		gwcp_relqctx = loadsym(libptr, "ing_relqctx");
	}

	if (!gwcp_getqctx ||
		!gwcp_sendplan ||
		!gwcp_opentbl ||
		!gwcp_getrows ||
		!gwcp_closetbl ||
		!gwcp_relqctx)
	{
		l1("dlsym failed");
		dbs = E_DB_ERROR;
	}

	gwx_rcb->xrcb_error.err_code = dbs;
	return dbs;
}

DB_STATUS
gwcp_open( gwx_rcb )
GWX_RCB	    *gwx_rcb;
{
	GW_RSB	*rsb = (GW_RSB *)gwx_rcb->xrcb_rsb;
	GWDMF_RSB	*gwdmf_rsb;
	DB_STATUS	status;
	char bufxx[1024];
	char tbn[DB_MAXNAME+1];
	char own[DB_MAXNAME+1];
	int cprc;

	strfix(tbn, gwx_rcb->xrcb_table_desc->tbl_name.db_tab_name, DB_MAXNAME);
	strfix(own, gwx_rcb->xrcb_table_desc->tbl_owner.db_own_name, DB_MAXNAME);
	sprintf(bufxx, "gwcp_open:\ntbl_id = %d.%d\ntbl_width = %d\ntbl_name = %s\ntbl_owner = %s",
		gwx_rcb->xrcb_table_desc->tbl_id.db_tab_base,
		gwx_rcb->xrcb_table_desc->tbl_id.db_tab_index,
		gwx_rcb->xrcb_table_desc->tbl_width,
		tbn, own);

	l1(bufxx);

        rsb->gwrsb_rsb_mstream.ulm_psize = sizeof(GWDMF_RSB);
        status = ulm_palloc(&rsb->gwrsb_rsb_mstream);
        if (status != E_DB_OK)
        {
		rsb->gwrsb_rsb_mstream.ulm_error.err_code = gwx_rcb->xrcb_error.err_code = status;
		return status;
        }
	gwdmf_rsb = (GWDMF_RSB *)rsb->gwrsb_rsb_mstream.ulm_pptr;
	rsb->gwrsb_internal_rsb = (PTR)gwdmf_rsb;

	gwcp_qctx = gwcp_getqctx();
	cprc = gwcp_sendplan(NULL);
	gwcp_tctx = gwcp_opentbl(gwcp_qctx, own, tbn);

	gwx_rcb->xrcb_error.err_code = status;
	return status;
}

DB_STATUS
gwcp_close( gwx_rcb )
GWX_RCB	    *gwx_rcb;
{
	l1("gwcp_close");

	gwcp_closetbl(gwcp_qctx, gwcp_tctx);
	gwcp_tctx = NULL;
	gwcp_relqctx(gwcp_qctx);
	gwcp_qctx = NULL;

	gwx_rcb->xrcb_error.err_code = E_DB_OK;
	return (E_DB_OK);
}

DB_STATUS
gwcp_position( gwx_rcb )
GWX_RCB	    *gwx_rcb;
{
	l1("gwcp_position");
	gwx_rcb->xrcb_error.err_code = E_DB_OK;
	return (E_DB_OK);
}

DB_STATUS
gwcp_get( gwx_rcb )
GWX_RCB	    *gwx_rcb;
{
	i4* error = &gwx_rcb->xrcb_error.err_code;
	int cprc;
	DB_STATUS dbs;

	cprc = gwcp_getrows(gwcp_qctx, gwcp_tctx, gwx_rcb->xrcb_var_data1.data_address,
		gwx_rcb->xrcb_var_data1.data_in_size, 1);

	if (cprc == 0)
	{
		gwx_rcb->xrcb_var_data1.data_out_size = gwx_rcb->xrcb_var_data1.data_in_size;
		dbs = E_DB_OK;
		gwx_rcb->xrcb_error.err_code = dbs;
	}
	else if (cprc > 0)
	{
		gwx_rcb->xrcb_var_data1.data_out_size = 0;
		*error = E_GW0641_END_OF_STREAM;
		dbs = E_DB_ERROR;
	}
	else
	{
		dbs = E_DB_ERROR;
		gwx_rcb->xrcb_error.err_code = dbs;
	}

	return dbs;
}

DB_STATUS
gwcp_put( gwx_rcb )
GWX_RCB	    *gwx_rcb;
{
	l1("gwcp_put");
    gwx_rcb->xrcb_error.err_code = E_DB_ERROR;
    return (E_DB_ERROR);
}

DB_STATUS
gwcp_replace( gwx_rcb )
GWX_RCB	    *gwx_rcb;
{
	l1("gwcp_replace");
    gwx_rcb->xrcb_error.err_code = E_DB_ERROR;
    return (E_DB_ERROR);
}

DB_STATUS
gwcp_delete( gwx_rcb )
GWX_RCB	    *gwx_rcb;
{
	l1("gwcp_delete");
    gwx_rcb->xrcb_error.err_code = E_DB_ERROR;
    return (E_DB_ERROR);
}

DB_STATUS
gwcp_commit( gwx_rcb )
GWX_RCB	    *gwx_rcb;
{
	l1("gwcp_commit");
    gwx_rcb->xrcb_error.err_code = E_DB_ERROR;
    return (E_DB_ERROR);
}

DB_STATUS
gwcp_abort( gwx_rcb )
GWX_RCB	    *gwx_rcb;
{
	l1("gwcp_abort");
    gwx_rcb->xrcb_error.err_code = E_DB_OK;
    return (E_DB_OK);
}

DB_STATUS
gwcp_info( gwx_rcb )
GWX_RCB	    *gwx_rcb;
{
	l1("gwcp_info");
    gwx_rcb->xrcb_var_data1.data_address = Gwdmfcp_version;
    gwx_rcb->xrcb_var_data1.data_in_size = STlength(Gwdmfcp_version) + 1;

    gwx_rcb->xrcb_error.err_code = E_DB_OK;
    return (E_DB_OK);
}

DB_STATUS
gwcp_idxf( gwx_rcb )
GWX_RCB	    *gwx_rcb;
{
	l1("gwcp_idxf");
    gwx_rcb->xrcb_error.err_code = E_DB_OK;
    return (E_DB_OK);
}

DB_STATUS
gwcp_init( gwx_rcb )
GWX_RCB	    *gwx_rcb;
{
    if (gwx_rcb->xrcb_gwf_version != GWX_VERSION)
    {
	gwx_rcb->xrcb_error.err_code = E_GW0654_BAD_GW_VERSION;
	return (E_DB_ERROR);
    }

    dmf_cptr = gwx_rcb->xrcb_dmf_cptr;	/* address of dmf_call() */

    (*gwx_rcb->xrcb_exit_table)[GWX_VTERM]	= gwcp_term;
    (*gwx_rcb->xrcb_exit_table)[GWX_VTABF]	= gwcp_tabf;
    (*gwx_rcb->xrcb_exit_table)[GWX_VOPEN]	= gwcp_open;
    (*gwx_rcb->xrcb_exit_table)[GWX_VCLOSE]	= gwcp_close;
    (*gwx_rcb->xrcb_exit_table)[GWX_VPOSITION]	= gwcp_position;
    (*gwx_rcb->xrcb_exit_table)[GWX_VGET]	= gwcp_get;
    (*gwx_rcb->xrcb_exit_table)[GWX_VPUT]	= gwcp_put;
    (*gwx_rcb->xrcb_exit_table)[GWX_VREPLACE]	= gwcp_replace;
    (*gwx_rcb->xrcb_exit_table)[GWX_VDELETE]	= gwcp_delete;
    (*gwx_rcb->xrcb_exit_table)[GWX_VBEGIN]	= gwcp_begin;
    (*gwx_rcb->xrcb_exit_table)[GWX_VCOMMIT]	= gwcp_commit;
    (*gwx_rcb->xrcb_exit_table)[GWX_VABORT]	= gwcp_abort;
    (*gwx_rcb->xrcb_exit_table)[GWX_VINFO]	= gwcp_info;
    (*gwx_rcb->xrcb_exit_table)[GWX_VIDXF]	= gwcp_idxf;

    gwx_rcb->xrcb_exit_cb_size = 0;
    gwx_rcb->xrcb_xrelation_sz = 0;
    gwx_rcb->xrcb_xattribute_sz = 0;
    gwx_rcb->xrcb_xindex_sz = 0;

    gwx_rcb->xrcb_error.err_code = 0;
    return (E_DB_OK);
}
