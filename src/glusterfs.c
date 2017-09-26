#define DONT_POISON_SPRINTF_YET 1
#include "collectd.h"
#undef DONT_POISON_SPRINTF_YET
#include "common.h"
#include "plugin.h"
#include <sys/resource.h>
#include <pthread.h>

#ifdef KERNEL_LINUX
#define GF_LINUX_HOST_OS 1
#endif

#ifdef HAVE_MALLOC_H
#include <malloc.h>
#endif

#ifdef HAVE_MALLOC_STATS
#ifdef DEBUG
#include <mcheck.h>
#endif
#endif

#define HAVE_SPINLOCK 1

#include "glusterfs.h"
#if defined(COLLECT_DEBUG) && COLLECT_DEBUG && defined(__GNUC__) && __GNUC__
#undef sprintf
#pragma GCC poison sprintf
#endif
#include "rpc/rpc-clnt.h"
#include "rpc/protocol-common.h"
#include "logging.h"
#include "rpc/cli1-xdr.h"
#include "event.h"
#include "call-stub.h"
#include "rpc/xdr-generic.h"
#include "rpc/glusterfs3-xdr.h"
#include "globals.h"
#include "stack.h"

#define DEFAULT_EVENT_POOL_SIZE            16384
#define CLI_GLUSTERD_PORT                  24007
#define DATADIR ""
#define GD_OP_VERSION_3_3                   1
#define GD_OP_VERSION_3_4                   2
#define GD_OP_VERSION_3_5                   3
#ifndef STARTING_EVENT_THREADS
#define STARTING_EVENT_THREADS              1
#endif
#ifndef DEFAULT_GLUSTERD_SOCKFILE
#define DEFAULT_GLUSTERD_SOCKFILE           "/run/glusterd.socket"
#endif
#ifndef GF_CLI_INFO_NONE
#define GF_CLI_INFO_NONE                    0
#endif
#ifndef GF_CLI_INFO_CLEAR
#define GF_CLI_INFO_CLEAR                   4
#endif
#ifndef GF_CLI_INFO_ALL
#define GF_CLI_INFO_ALL                     1
#endif
#ifndef GD_OP_VERSION_3_6_0
typedef int gf1_cli_info_op;
#endif

#if GD_OP_VERSION_MAX < GD_OP_VERSION_3_5
int
rpc_transport_unix_options_build (dict_t **options, char *filepath,
                                  int frame_timeout)
{
        dict_t                  *dict = NULL;
        char                    *fpath = NULL;
        int                     ret = -1;

        GF_ASSERT (filepath);
        GF_ASSERT (options);

        dict = dict_new ();
        if (!dict)
                goto out;

        fpath = gf_strdup (filepath);
        if (!fpath) {
                ret = -1;
                goto out;
        }

        ret = dict_set_dynstr (dict, "transport.socket.connect-path", fpath);
        if (ret)
                goto out;

        ret = dict_set_str (dict, "transport.address-family", "unix");
        if (ret)
                goto out;

        ret = dict_set_str (dict, "transport.socket.nodelay", "off");
        if (ret)
                goto out;

        ret = dict_set_str (dict, "transport-type", "socket");
        if (ret)
                goto out;

        ret = dict_set_str (dict, "transport.socket.keepalive", "off");
        if (ret)
                goto out;

        if (frame_timeout > 0) {
                ret = dict_set_int32 (dict, "frame-timeout", frame_timeout);
                if (ret)
                        goto out;
        }

        *options = dict;
out:
        if (ret) {
                GF_FREE (fpath);
                if (dict)
                        dict_unref (dict);
        }
        return ret;
}
#endif

static int kill_self(void);

struct rpc_clnt *global_rpc;


rpc_clnt_prog_t *cli_rpc_prog;
static pthread_t *killme = NULL;

typedef struct profile_info_ {
        uint64_t fop_hits;
        double min_latency;
        double max_latency;
        double avg_latency;
        char   *fop_name;
        double percentage_avg_latency;
} profile_info_t;

struct rpc_clnt_procedure gluster_cli_actors[GLUSTER_CLI_MAXVALUE] = {
        [GLUSTER_CLI_PROFILE_VOLUME]   = {"PROFILE_VOLUME"},
};

struct rpc_clnt_program cli_prog = {
        .progname  = "CollectD Glusterfs Plugin",
        .prognum   = GLUSTER_CLI_PROGRAM,
        .progver   = GLUSTER_CLI_VERSION,
        .numproc   = GLUSTER_CLI_MAXVALUE,
        .proctable = gluster_cli_actors,
};

struct cli_state {
        /* for events dispatching */
        glusterfs_ctx_t      *ctx;

        /* the thread which "executes" the command in non-interactive mode */
        /* also the thread which reads from stdin in non-readline mode */
        pthread_t             input;

        char                 *remote_host;
        int                   remote_port;
        int                   mode;
        int                   await_connected;
        char                 *glusterd_sock;
        xlator_t             *xlator;
        int                   bricknum;
};
struct cli_state state = {.bricknum = 1};

enum mem_types_ {
        mt_xlator_list_t = (gf_common_mt_end + 1),
        mt_xlator_t,
        mt_xlator_cmdline_option_t,
        mt_char,
        cli_mt_call_pool_t,
        mt_cli_local_t,
        mt_cli_get_vol_ctx_t,
        mt_append_str,
        mt_cli_cmd,
        mt_end

};

enum stat_type {
    CUMULATIVE,
    INTERVAL
};

const char* stat_types[] = {"cumulative", "interval"};

int rpc_ret;
int connected;
cdtime_t collectd_interval;
static int cmd_sent;
static int cmd_done;
static pthread_cond_t      cond  = PTHREAD_COND_INITIALIZER;
static pthread_mutex_t     cond_mutex = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t      conn  = PTHREAD_COND_INITIALIZER;
static pthread_mutex_t     conn_mutex = PTHREAD_MUTEX_INITIALIZER;
static pthread_mutex_t     interval_mutex = PTHREAD_MUTEX_INITIALIZER;

// Set up shit for gluster

static void
seconds_from_now (unsigned secs, struct timespec *ts)
{
        struct timeval tv = {0,};

        gettimeofday (&tv, NULL);

        ts->tv_sec = tv.tv_sec + secs;
        ts->tv_nsec = tv.tv_usec * 1000;
}

int32_t
cli_cmd_await_connected (unsigned conn_timo)
{
        int32_t                 ret = 0;
        struct  timespec        ts = {0,};

        if (!conn_timo)
                return 0;

        pthread_mutex_lock (&conn_mutex);
        {
                seconds_from_now (conn_timo, &ts);
                while (!connected && !ret) {
                        ret = pthread_cond_timedwait (&conn, &conn_mutex,
                                                      &ts);
                }
        }
        pthread_mutex_unlock (&conn_mutex);


        return ret;
}

int
cli_cmd_await_response (unsigned time)
{
        struct  timespec        ts = {0,};
        int                     ret = 0;

        rpc_ret = -1;

        seconds_from_now (time, &ts);
        while (!cmd_done && !ret) {
                ret = pthread_cond_timedwait (&cond, &cond_mutex,
                                        &ts);
        }

        if (!cmd_done) {
                if (ret == ETIMEDOUT)
                        ERROR("RPC request timed out");
                else
                        ERROR("RPC command returned with error code:%d", ret);
        }
        cmd_done = 0;

        return rpc_ret;
}

/* This function must be called _only_ after all actions associated with
 * command processing is complete. Otherwise, gluster process may exit before
 * reporting results to stdout/stderr. */
int
cli_cmd_broadcast_response (int32_t status)
{

        pthread_mutex_lock (&cond_mutex);
        {
                if (!cmd_sent)
                        goto out;
                cmd_done = 1;
                rpc_ret = status;
                pthread_cond_broadcast (&cond);
        }


out:
        pthread_mutex_unlock (&cond_mutex);
        return 0;
}

int32_t
cli_cmd_broadcast_connected ()
{
        pthread_mutex_lock (&conn_mutex);
        {
                connected = 1;
                pthread_cond_broadcast (&conn);
        }

        pthread_mutex_unlock (&conn_mutex);
        INFO("Connected to gluster");

        return 0;
}


int
glusterfs_ctx_defaults_init ()
{
        cmd_args_t    *cmd_args = NULL;
        struct rlimit  lim = {0, };
        call_pool_t   *pool = NULL;
        int            ret         = -1;
        /*glusterfs_ctx_t *ctx = THIS->ctx;*/

        ret = xlator_mem_acct_init (THIS, mt_end);
        if (ret != 0) {
            ERROR("ret = xlator_mem_acct_init (THIS, mt_end);");
                return ret;
        }

        THIS->ctx->process_uuid = generate_glusterfs_ctx_id ();
        if (!THIS->ctx->process_uuid)
                return 1;

        THIS->ctx->page_size  = 128 * GF_UNIT_KB;

        THIS->ctx->iobuf_pool = iobuf_pool_new ();
        if (!THIS->ctx->iobuf_pool)
                return 2;

#ifndef GD_OP_VERSION_3_7_0
        THIS->ctx->event_pool = event_pool_new (DEFAULT_EVENT_POOL_SIZE);
#else
        THIS->ctx->event_pool = event_pool_new (DEFAULT_EVENT_POOL_SIZE,
                                          STARTING_EVENT_THREADS);
#endif
        if (!THIS->ctx->event_pool)
                return 3;

        pool = GF_CALLOC(1, sizeof(call_pool_t), 146);
        if (pool == NULL) {
                return 4;
        }

        /*[>pool = THIS->ctx->pool;<]*/
        /*[>memset(&(THIS->ctx->pool), 0, sizeof(call_pool_t));<]*/
        /*THIS->ctx->pool = GF_CALLOC(1, sizeof(call_pool_t), cli_mt_call_pool_t);*/
        /*if (THIS->ctx->pool == NULL) {*/
                /*return 4;*/
        /*}*/

        /*[> frame_mem_pool size 112 * 64 <]*/
        /*[>struct mem_pool * frame_mem_pool = mem_pool_new_fn (sizeof(call_frame_t), 32, "call_frame_t");<]*/
        /*struct mem_pool * frame_mem_pool = mem_pool_new (call_frame_t, 32);*/
        pool->frame_mem_pool = mem_pool_new (call_frame_t, 32);
        if (!pool->frame_mem_pool) {
            ERROR("5");
                return 5;
        }

        /*[> stack_mem_pool size 256 * 128 <]*/
        /*[>THIS->ctx->pool->stack_mem_pool = mem_pool_new_fn (sizeof(call_stack_t), 16, "call_stack_t");<]*/
        /*memset(&(THIS->ctx->pool->stack_mem_pool), 0, sizeof(struct mem_pool));*/
        pool->stack_mem_pool = mem_pool_new (call_stack_t, 16);

        if (!pool->stack_mem_pool) {
            ERROR("6");
                return 6;
        }

        THIS->ctx->stub_mem_pool = mem_pool_new (call_stub_t, 16);
        if (!THIS->ctx->stub_mem_pool)
                return 7;

        THIS->ctx->dict_pool = mem_pool_new (dict_t, 32);
        if (!THIS->ctx->dict_pool)
                return 8;

        THIS->ctx->dict_pair_pool = mem_pool_new (data_pair_t, 512);
        if (!THIS->ctx->dict_pair_pool)
                return 9;

        THIS->ctx->dict_data_pool = mem_pool_new (data_t, 512);
        if (!THIS->ctx->dict_data_pool)
                return 10;

#ifdef GD_OP_VERSION_3_6_0
        THIS->ctx->logbuf_pool = mem_pool_new (log_buf_t, 256);
        if (!THIS->ctx->logbuf_pool)
                return 11;
#endif

        INIT_LIST_HEAD (&pool->all_frames);
        LOCK_INIT (&pool->lock);
        /*[>ctx->pool = (struct call_pool*)pool;<]*/
        /*[>THIS->ctx->pool = (struct call_pool*)pool;<]*/
        /*[>xlator_t **xlator = __glusterfs_this_location();<]*/
        /*[>(*xlator)->ctx->pool = (call_pool_t*)pool->all_frames.next;<]*/
        /*[>glusterfs_this_set((*xlator));<]*/
        THIS->ctx->pool = pool;

        pthread_mutex_init (&(THIS->ctx->lock), NULL);

        /*ret = init_call_pool();*/
        cmd_args = &THIS->ctx->cmd_args;

        INIT_LIST_HEAD (&cmd_args->xlator_options);

        lim.rlim_cur = RLIM_INFINITY;
        lim.rlim_max = RLIM_INFINITY;
        setrlimit (RLIMIT_CORE, &lim);

        return 0;
}


int
cli_submit_request (struct rpc_clnt *rpc, void *req, call_frame_t *frame,
                    rpc_clnt_prog_t *prog,
                    int procnum, struct iobref *iobref,
                    xlator_t *xlator, fop_cbk_fn_t cbkfn, xdrproc_t xdrproc)
{
        int                     ret         = -1;
        int                     count      = 0;
        struct iovec            iov         = {0, };
        struct iobuf            *iobuf = NULL;
        char                    new_iobref = 0;
        ssize_t                 xdr_size   = 0;

        GF_ASSERT (xlator);

        if (req) {
                xdr_size = xdr_sizeof (xdrproc, req);
                iobuf = iobuf_get2 (THIS->ctx->iobuf_pool, xdr_size);
                if (!iobuf) {
                        goto out;
                };

                if (!iobref) {
                        iobref = iobref_new ();
                        if (!iobref) {
                                goto out;
                        }

                        new_iobref = 1;
                }

                iobref_add (iobref, iobuf);

                iov.iov_base = iobuf->ptr;
                iov.iov_len  = iobuf_size (iobuf);


                /* Create the xdr payload */
                ret = xdr_serialize_generic (iov, req, xdrproc);
                if (ret == -1) {
                    ERROR("failed to serialize");
                        goto out;
                }
                iov.iov_len = ret;
                count = 1;
        }

        ret = cli_cmd_await_connected(10);
        if (ret) {
            ERROR("asdf");
        }
        /* Send the msg */
        ret = rpc_clnt_submit (rpc, prog, procnum, cbkfn,
                               &iov, count,
                               NULL, 0, iobref, frame, NULL, 0, NULL, 0, NULL);
        ret = 0;

out:
        if (new_iobref)
                iobref_unref (iobref);
        if (iobuf)
                iobuf_unref (iobuf);
        return ret;
}

int
cli_rpc_notify (struct rpc_clnt *rpc, void *mydata, rpc_clnt_event_t event,
                void *data)
{
        xlator_t                *xlator = NULL;
        int                     ret = 0;

        xlator = mydata;

        switch (event) {
        case RPC_CLNT_CONNECT:
        {

                cli_cmd_broadcast_connected ();
                INFO("got RPC_CLNT_CONNECT");
               break;
        }

        case RPC_CLNT_DISCONNECT:
        {
                INFO("got RPC_CLNT_DISCONNECT");
                connected = 0;
                /*if (!global_state->prompt && global_state->await_connected) {*/
                        /*ret = 1;*/
                        /*cli_out ("Connection failed. Please check if gluster "*/
                                  /*"daemon is operational.");*/
                        /*exit (ret);*/
                /*}*/
                break;
        }

        default:
                INFO("got some other RPC event %d", event);
                ret = 0;
                break;
        }

        return ret;
}

struct rpc_clnt *
cli_rpc_init (struct cli_state* state)
{
        struct rpc_clnt         *rpc = NULL;
        dict_t                  *options = NULL;
        int                     ret = -1;
        int                     port = CLI_GLUSTERD_PORT;
        xlator_t                *xlator = NULL;

        xlator = THIS;
        cli_rpc_prog = &cli_prog;
        options = dict_new ();
        if (!options)
                goto out;

        /* Connect to glusterd using the specified method, giving preference
         * to a unix socket connection.  If nothing is specified, connect to
         * the default glusterd socket.
         */
        if (state->glusterd_sock) {
                NOTICE("Connecting to glusterd using "
                        "sockfile %s", state->glusterd_sock);
                ret = rpc_transport_unix_options_build (&options,
                                                        state->glusterd_sock,
                                                        0);
                if (ret)
                        goto out;
        }
        else if (state->remote_host) {
                NOTICE("Connecting to remote glusterd at "
                        "%s", state->remote_host);
                ret = dict_set_str (options, "remote-host", state->remote_host);
                if (ret)
                        goto out;

                if (state->remote_port)
                        port = state->remote_port;

                ret = dict_set_int32 (options, "remote-port", port);
                if (ret)
                        goto out;

                ret = dict_set_str (options, "transport.address-family",
                                    "inet");
                if (ret)
                        goto out;
        }
        else {
                NOTICE("Connecting to glusterd using "
                        "default socket");
                ret = rpc_transport_unix_options_build
                        (&options, DEFAULT_GLUSTERD_SOCKFILE, 0);
                if (ret)
                        goto out;
        }

#if GD_OP_VERSION_MAX == GD_OP_VERSION_3_4
        rpc = rpc_clnt_new (options, xlator->ctx, xlator->name, 16);
#else
        rpc = rpc_clnt_new (options, xlator, xlator->name, 16);
#endif
        if (!rpc) {
                goto out;
        }
        INFO(xlator->name);

        ret = rpc_clnt_register_notify (rpc, cli_rpc_notify, xlator);
        if (ret) {
                ERROR("failed to register notify");
                goto out;
        }

        ret = rpc_clnt_start (rpc);
        INFO("client started");
out:
        if (ret) {
            ERROR("error");
                if (rpc)
                        rpc_clnt_unref (rpc);
                rpc = NULL;
        }
        return rpc;
}

int process_data2 (dict_t *dict, int count) {

    profile_info_t          cumulative_stats[GF_FOP_MAXVALUE] = {{0}};
    profile_info_t          interval_stats[GF_FOP_MAXVALUE] = {{0}};
    char                    key[256] = {0};
    int                     i = 0;
        int                               interval = 0;
        int                               cumulative = 0;
        double                  total_percentage_latency = 0;
        int ret;
        uint64_t                bytes_read_cumulative = 0;
        uint64_t                bytes_read_interval = 0;
        uint64_t                bytes_written_cumulative = 0;
        uint64_t                bytes_written_interval = 0;

        memset (key, 0, sizeof (key));
        snprintf (key, sizeof (key), "%d-cumulative", state.bricknum);
        ret = dict_get_int32 (dict, key, &cumulative);

        memset (key, 0, sizeof (key));
        snprintf (key, sizeof (key), "%d-interval", state.bricknum);
        ret = dict_get_int32 (dict, key, &interval);

        memset (key, 0, sizeof (key));
        snprintf (key, sizeof (key), "%d-%d-total-read", state.bricknum, cumulative);
        ret = dict_get_uint64 (dict, key, &bytes_read_cumulative);
        memset (key, 0, sizeof (key));
        snprintf (key, sizeof (key), "%d-%d-total-read", state.bricknum, interval);
        ret = dict_get_uint64 (dict, key, &bytes_read_interval);

        memset (key, 0, sizeof (key));
        snprintf (key, sizeof (key), "%d-%d-total-write", state.bricknum, cumulative);
        ret = dict_get_uint64 (dict, key, &bytes_written_cumulative);
        memset (key, 0, sizeof (key));
        snprintf (key, sizeof (key), "%d-%d-total-write", state.bricknum, interval);
        ret = dict_get_uint64 (dict, key, &bytes_written_interval);

        for (i = 0; i < GF_FOP_MAXVALUE; i++) {

                memset (key, 0, sizeof (key));
                snprintf (key, sizeof (key), "%d-%d-%d-hits", state.bricknum,
                          cumulative, i);
                ret = dict_get_uint64 (dict, key, &cumulative_stats[i].fop_hits);
                memset (key, 0, sizeof (key));
                snprintf (key, sizeof (key), "%d-%d-%d-hits", state.bricknum,
                          interval, i);
                ret = dict_get_uint64 (dict, key, &interval_stats[i].fop_hits);

                memset (key, 0, sizeof (key));
                snprintf (key, sizeof (key), "%d-%d-%d-avglatency", state.bricknum,
                          interval, i);
                ret = dict_get_double (dict, key, &interval_stats[i].avg_latency);

                memset (key, 0, sizeof (key));
                snprintf (key, sizeof (key), "%d-%d-%d-minlatency", state.bricknum,
                          interval, i);
                ret = dict_get_double (dict, key, &interval_stats[i].min_latency);

                memset (key, 0, sizeof (key));
                snprintf (key, sizeof (key), "%d-%d-%d-maxlatency", state.bricknum,
                          interval, i);
                ret = dict_get_double (dict, key, &interval_stats[i].max_latency);
                interval_stats[i].fop_name = (char *)gf_fop_list[i];

                total_percentage_latency +=
                       (interval_stats[i].fop_hits * interval_stats[i].avg_latency);
        }
        if (total_percentage_latency) {
                for (i = 0; i < GF_FOP_MAXVALUE; i++) {
                        interval_stats[i].percentage_avg_latency = 100 * (
                     (interval_stats[i].avg_latency* interval_stats[i].fop_hits) /
                                total_percentage_latency);
                }
        }

    value_list_t vl;
    value_t *values = calloc(2, sizeof(value_t));
    pthread_mutex_lock(&interval_mutex);
    cdtime_t asdf = collectd_interval;
    pthread_mutex_unlock(&interval_mutex);

    // initialize memory
    memset(&vl, 0, sizeof(value_list_t));
    // set common stuff
    sstrncpy(vl.plugin, "glusterfs", sizeof(vl.plugin));
    vl.values = values;
    vl.values_len = 2;
    vl.interval = asdf;

    memset(values, 0, sizeof(value_t)*2);
    values[0].counter = bytes_read_cumulative;
    values[1].counter = bytes_written_cumulative;
    sstrncpy(vl.type, "io_bytes_total", sizeof(vl.type));
    plugin_dispatch_values(&vl);

    memset(values, 0, sizeof(value_t)*2);
    values[0].gauge = bytes_read_interval;
    values[1].gauge = bytes_written_interval;
    sstrncpy(vl.type, "io_bytes", sizeof(vl.type));
    plugin_dispatch_values(&vl);

    sstrncpy(vl.type, "fop_hits", sizeof(vl.type));
    for (i = 0; i < GF_FOP_MAXVALUE; i++) {
        if (cumulative_stats[i].fop_hits == 0) {
            continue;
        }
        // fop hits
        memset(values, 0, sizeof(value_t)*2);
        values[0].gauge = interval_stats[i].fop_hits;
        values[1].counter = (unsigned long long)cumulative_stats[i].fop_hits;
        sstrncpy(vl.type_instance, gf_fop_list[i], sizeof(vl.type_instance));
        plugin_dispatch_values (&vl);
    }

    values = realloc(values, sizeof(value_t)*4);
    vl.values = values;
    vl.values_len = 4;

    sstrncpy(vl.type, "fop_latency", sizeof(vl.type));
    for (i = 0; i < GF_FOP_MAXVALUE; i++) {
        if (interval_stats[i].avg_latency == 0.0) {
            continue;
        }

            // fop latency
            memset(values, 0, sizeof(value_t)*4);
            values[0].gauge = interval_stats[i].percentage_avg_latency;
            values[1].gauge = interval_stats[i].avg_latency;
            values[2].gauge = interval_stats[i].min_latency;
            values[3].gauge = interval_stats[i].max_latency;
            sstrncpy(vl.type_instance, gf_fop_list[i], sizeof(vl.type_instance));
            plugin_dispatch_values (&vl);
  }
    free(values);

    INFO("dispatched!");
  return 0;
}

int32_t
profile_volume_cbk (struct rpc_req *req, struct iovec *iov,
                              int count, void *myframe)
{
        gf_cli_rsp                        rsp   = {0,};
        int                               ret   = -1;
        dict_t                            *dict = NULL;
        gf1_cli_stats_op                  op = GF_CLI_STATS_NONE;
        char                              key[256] = {0};
        int32_t                           brick_count = 0;
        char                              *volname = NULL;
        char                              *brick = NULL;
        char                              str[1024] = {0,};
        int                               stats_cleared = 0;
        gf1_cli_info_op                   info_op = GF_CLI_INFO_NONE;

        if (-1 == req->rpc_status) {
                goto out;
        }

        INFO("Received resp to profile");
        ret = xdr_to_generic (*iov, &rsp, (xdrproc_t)xdr_gf_cli_rsp);
        if (ret < 0) {
                ERROR("Failed to decode xdr response");
                goto out;
        }

        dict = dict_new ();

        if (!dict) {
                ret = -1;
                goto out;
        }

        ret = dict_unserialize (rsp.dict.dict_val,
                                rsp.dict.dict_len,
                                &dict);

        if (ret) {
                ERROR("Unable to allocate memory");
                goto out;
        } else {
                dict->extra_stdfree = rsp.dict.dict_val;
        }

        ret = dict_get_str (dict, "volname", &volname);
        if (ret)
                goto out;

        ret = dict_get_int32 (dict, "op", (int32_t*)&op);
        if (ret)
                goto out;

        if (rsp.op_ret && strcmp (rsp.op_errstr, "")) {
                ERROR ("%s", rsp.op_errstr);
        } else {
                switch (op) {
                case GF_CLI_STATS_START:
                        INFO ("Starting volume profile on %s has been %s ",
                                 volname,
                                 (rsp.op_ret) ? "unsuccessful": "successful");
                        break;
                case GF_CLI_STATS_STOP:
                        INFO ("Stopping volume profile on %s has been %s ",
                                 volname,
                                 (rsp.op_ret) ? "unsuccessful": "successful");
                        break;
                case GF_CLI_STATS_INFO:
                        break;
                default:
                        INFO ("volume profile on %s has been %s ",
                                 volname,
                                 (rsp.op_ret) ? "unsuccessful": "successful");
                        break;
                }
        }

        if (rsp.op_ret) {
                ret = rsp.op_ret;
                goto out;
        }

        if (GF_CLI_STATS_INFO != op) {
                ret = 0;
                goto out;
        }

        ret = dict_get_int32 (dict, "info-op", (int32_t*)&info_op);
        if (ret)
                goto out;

        ret = dict_get_int32 (dict, "count", &brick_count);
        if (ret)
                goto out;

        if (!brick_count) {
                ERROR ("All bricks of volume %s are down.", volname);
                goto out;
        }

        /*while (i <= brick_count) {*/
                memset (key, 0, sizeof (key));
                snprintf (key, sizeof (key), "%d-brick", state.bricknum);
                ret = dict_get_str (dict, key, &brick);
                if (ret) {
                        ERROR("Couldn't get brick name using number: %d", state.bricknum);
                        goto out;
                }

                ret = dict_get_str_boolean (dict, "nfs", _gf_false);

                if (ret)
                        snprintf (str, sizeof (str), "NFS Server : %s", brick);
                else
                        snprintf (str, sizeof (str), "Brick: %s", brick);
                memset (str, '-', strlen (str));
                INFO("%s", brick);

                if (GF_CLI_INFO_CLEAR == info_op) {
                        snprintf (key, sizeof (key), "%d-stats-cleared", state.bricknum);
                        ret = dict_get_int32 (dict, key, &stats_cleared);
                        if (ret)
                                goto out;
                        if (stats_cleared) {
                            INFO("Cleared stats.");
                        } else {
                            ERROR("Failed to clear stats.");
                        }
                } else {
                    process_data2(dict, state.bricknum);
                        /*snprintf (key, sizeof (key), "%d-cumulative", state.bricknum);*/
                        /*ret = dict_get_int32 (dict, key, &interval);*/

                        /*if (ret != 0) {*/
                            /*ERROR("Error occurred in parsing return");*/
                            /*goto out;*/
                        /*}*/

                        /*process_data(dict, state.bricknum, interval, CUMULATIVE);*/
                        /*if (ret == 0)*/
                                /*cmd_profile_volume_brick_out (dict, i,*/
                                                              /*interval);*/

                        /*snprintf (key, sizeof (key), "%d-interval", state.bricknum);*/
                        /*ret = dict_get_int32 (dict, key, &interval);*/
                        /*if (ret == 0)*/
                            /*process_data(dict, state.bricknum, interval, INTERVAL);*/
                        /*if (ret == 0)*/
                                /*cmd_profile_volume_brick_out (dict, i,*/
                                                              /*interval);*/
                }
                /*i++;*/
        /*}*/
        ret = rsp.op_ret;

out:
        if (dict)
                dict_unref (dict);
        free (rsp.op_errstr);
        cli_cmd_broadcast_response (ret);
        return ret;
}

void * profiler_thing (void* d) {
    int ret = 0;
        // set up the command
        dict_t *dict = NULL;
        dict = dict_new();
        if (!dict)
            goto out;

        char* volname = "gv0";
        ret = dict_set_str (dict, "volname", volname);
        if (ret)
                goto out;

        ret = dict_set_int32 (dict, "op", (int32_t)GF_CLI_STATS_INFO);
        if (ret)
            goto out;

        ret = dict_set_int32 (dict, "info-op", (int32_t)GF_CLI_INFO_ALL);
        if (ret)
            goto out;

        /*ret = dict_set_int32(dict, "count", state.bricknum);*/
        /*if (ret)*/
            /*goto out;*/

        ret = dict_set_int32 (dict, "peek", _gf_false);
        if (ret)
                goto out;

        rpc_clnt_procedure_t    *proc = NULL;
        call_frame_t            *frame = NULL;

        proc = &cli_rpc_prog->proctable[GLUSTER_CLI_PROFILE_VOLUME];

        frame = create_frame (THIS, THIS->ctx->pool);
        if (!frame)
                goto out;

        GF_ASSERT(THIS->ctx->iobuf_pool);

        gf_cli_req                 req   = {{0,}};
        ret = dict_allocate_and_serialize (dict, &(req.dict).dict_val,
                                           &(req.dict).dict_len);

        pthread_mutex_lock (&cond_mutex);
        int cli_ret = 0;
        cmd_sent = 0;
        cli_ret = cli_submit_request(global_rpc, &req, frame, cli_rpc_prog, GLUSTER_CLI_PROFILE_VOLUME, NULL, THIS, profile_volume_cbk, (xdrproc_t) xdr_gf_cli_req);
        if (!cli_ret) {
            cmd_sent = 1;
            ret = cli_cmd_await_response(600);
        }
        pthread_mutex_unlock (&cond_mutex);

        INFO("Returning %d", ret);

        if (frame) {
            STACK_DESTROY (frame->root);
        }

out:
        INFO("Exiting with: %d", ret);

        return NULL;

}

void* start_loop(void* d)
{
    int ret = -1;
    glusterfs_ctx_t   *ctx = NULL;
    char* log_file = "/var/log/glusterfs/profiler.log";
    xlator_t * xlator = NULL;

    /*if (!state.ctx) {*/
        ctx = glusterfs_ctx_new ();
        if (!ctx) {
                ERROR("glusterfs_ctx_new: %d", ENOMEM);
                return NULL;
        }
/*#ifdef DEBUG*/
        /*ERROR("Setting debug");*/
        /*gf_mem_acct_enable_set (ctx);*/
/*#endif*/

        ret = glusterfs_globals_init (ctx);
        if (ret) {
                ERROR("glusterfs_globals_init: %d", ret);
                return NULL;
        }

        xlator = (*__glusterfs_this_location());
        xlator->ctx = ctx;

        ret = glusterfs_ctx_defaults_init ();
        if (ret) {
            ERROR("ERROR SETTING CTX DEFAULTS: %d", ret);
            return NULL;
        }

        pthread_mutex_init (&cond_mutex, NULL);
        pthread_cond_init (&cond, NULL);
        pthread_mutex_init (&conn_mutex, NULL);
        pthread_cond_init (&conn, NULL);

#if GD_OP_VERSION_MAX < GD_OP_VERSION_3_5
        gf_log_init (ctx, log_file);
#else
        gf_log_init(ctx, log_file, NULL);
#endif
        gf_log_set_loglevel(GF_LOG_TRACE);

        global_rpc = cli_rpc_init (&state);
        if (!global_rpc) {
            ERROR("global_rpc is null");
            return NULL;
        }

        ret = pthread_create ((&(state.input)), NULL, profiler_thing, &state);
        if (ret) {
            ERROR("problem! %d", ret);
            return NULL;
        }

        
        event_dispatch(THIS->ctx->event_pool);
        /*if (killme == NULL) {*/
            /*killme = malloc(sizeof(pthread_t));*/
            /*ret = pthread_create((killme), NULL, start_loop, THIS);*/
        /*}*/
    /*}*/
        return NULL;
}

static const char *config_keys[] =
{
  "Socket",
  "RemoteHost",
  "RemotePort"
};

static int read_stats(void) {
    pthread_t thread;

    pthread_mutex_lock (&interval_mutex);
    {
        collectd_interval = plugin_interval;
    }
    pthread_mutex_unlock (&interval_mutex);
    return pthread_create(&thread, NULL, profiler_thing, NULL);
}
    

static int config_keys_num = STATIC_ARRAY_SIZE (config_keys);

static int set_config (const char *key, const char *value) {
    if (strcmp(key, "Socket") == 0) {
        state.glusterd_sock = strdup(value);
    } else if (strcmp(key, "RemoteHost") == 0) {
        state.remote_host = strdup(value);
    } else if (strcmp(key, "BrickNumber") == 0) {
        state.bricknum = atoi(value);
    }
    return 0;
}

static int kill_self(void) {
    return pthread_cancel(*killme);
}

static int init(void) {
    if (killme == NULL) {
        killme = malloc(sizeof(pthread_t));
        return pthread_create((killme), NULL, start_loop, THIS);
    }
    return 0;
}

void module_register(void) {
  plugin_register_config("glusterfs", set_config, config_keys, config_keys_num);
  plugin_register_init("glusterfs", init);
  plugin_register_read("glusterfs", read_stats);
  plugin_register_shutdown("glusterfs", kill_self);
} /* void module_register */

