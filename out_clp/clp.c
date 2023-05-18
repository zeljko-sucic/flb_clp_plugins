/* -*- Mode: C; tab-width: 4; indent-tabs-mode: nil; c-basic-offset: 4 -*- */

/*  Fluent Bit
 *  ==========
 *  Copyright (C) 2015-2022 The Fluent Bit Authors
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

#include <fluent-bit/flb_output_plugin.h>
#include <fluent-bit/flb_mem.h>
#include <fluent-bit/flb_pack.h>
#include <fluent-bit/flb_utils.h>
#include <fluent-bit/flb_time.h>
#include <fluent-bit/flb_metrics.h>
#include <fluent-bit/flb_log_event_decoder.h>
#include <msgpack.h>
#include <unistd.h>
#include <stdio.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#ifdef FLB_SYSTEM_WINDOWS
#include <Shlobj.h>
#include <Shlwapi.h>
#endif


#ifdef FLB_SYSTEM_WINDOWS
#define NEWLINE "\r\n"
#define S_ISDIR(m)      (((m) & S_IFMT) == S_IFDIR)
#else
#define NEWLINE "\n"
#endif

#define TIME_STR_SIZE 24
#define TIME_STR_FORMAT "%Y-%m-%dT%H:%M:%S"
#define TIME_FOR_LOG_NAME_FORMAT "%Y%m%d%H%M.log"

#define HOSTNAME_SIZE 256

struct flb_clp_conf {
    const char *out_path;
    const char *out_file;
    const char *delimiter;
    const char *hostname;
    struct flb_output_instance *ins;
};

static char *check_delimiter(const char *str)
{
    if (str == NULL) {
        return NULL;
    }

    if (!strcasecmp(str, "\\t") || !strcasecmp(str, "tab")) {
        return "\t";
    }
    else if (!strcasecmp(str, "space")) {
        return " ";
    }
    else if (!strcasecmp(str, "comma")) {
        return ",";
    }

    return NULL;
}



static char* getLocalHostname() {
    static char hostname[HOSTNAME_SIZE];

    if (gethostname(hostname, sizeof(hostname)) != 0) {
        fprintf(stderr, "Failed to get hostname.\n");
        return NULL;
    }

    return hostname;
}
static int cb_clp_init(struct flb_output_instance *ins,
                        struct flb_config *config,
                        void *data)
{
    int ret;
    const char *tmp;
    char *ret_str;
    (void) config;
    (void) data;
    struct flb_clp_conf *ctx;

    ctx = flb_calloc(1, sizeof(struct flb_clp_conf));
    if (!ctx) {
        flb_errno();
        return -1;
    }
    ctx->ins = ins;
    ctx->delimiter = ",";
    ctx->hostname=getLocalHostname();

    ret = flb_output_config_map_set(ins, (void *) ctx);
    if (ret == -1) {
        flb_free(ctx);
        return -1;
    }

    flb_output_set_context(ins, ctx);

    return 0;
}

static int csv_output(FILE *fp,
                      struct flb_time *tm, msgpack_object *obj,
                      struct flb_clp_conf *ctx)
{
    int i;
    int map_size;
    msgpack_object value;
    msgpack_object_kv *kv = NULL;
    bool processedTime=false;
    char *buff = flb_calloc(TIME_STR_SIZE, sizeof(char));

    if (obj->type == MSGPACK_OBJECT_MAP && obj->via.map.size > 0) {
        kv = obj->via.map.ptr;
        map_size = obj->via.map.size;

    //char buff[24];


    for (i = 0; i < map_size; i++) {
            value=(msgpack_object)(kv+i)->val;
            switch(value.type)
            {
                case MSGPACK_OBJECT_POSITIVE_INTEGER:

                    long timeInSec=value.via.u64/1000;
                    long remainderInNano=(value.via.u64-(timeInSec*1000))*1000;
                    strftime(buff, TIME_STR_SIZE, TIME_STR_FORMAT, gmtime(&timeInSec));
                    fprintf(fp, "%s.%06ld%s", buff,remainderInNano, ctx->delimiter);
                    processedTime=true;
                    break;
                case MSGPACK_OBJECT_STR:
                    if (processedTime==false)
                    {
                        strftime(buff, TIME_STR_SIZE, TIME_STR_FORMAT, gmtime(&tm->tm.tv_sec));
                        fprintf(fp, "%s.%06ld%s", buff,tm->tm.tv_nsec, ctx->delimiter);
                        processedTime=true;
                    }

                    msgpack_object_print(fp,value );
                    if (i< map_size-1)
                    {
                        fprintf(fp, "%s", ctx->delimiter);
                    }
                    break;
                default:
                    printf("object type: %d unsupported\n", value.type);
                    break;
            }


        }

        fprintf(fp, NEWLINE);
    }
    flb_free(buff);
    return 0;
}
static void get_output_file(struct flb_clp_conf *ctx,struct flb_event_chunk *event_chunk, char * out_file, struct flb_time *tm)
{

    //struct timespec ts;
    //clock_gettime(CLOCK_REALTIME, &ts);
    time_t roundedTimeInSec=((int)(tm->tm.tv_sec/(1200)))*1200;
    //time_t roundedTimeInSec=((int)(ts.tv_sec/(1200)))*1200;
    char *buff = flb_calloc(TIME_STR_SIZE, sizeof(char));
    strftime(buff, TIME_STR_SIZE, TIME_FOR_LOG_NAME_FORMAT,gmtime(&roundedTimeInSec));
    /* Set the right output file */
    if (ctx->out_path) {
        if (ctx->out_file) {
            snprintf(out_file, PATH_MAX - 1, "%s/%s",
                     ctx->out_path, ctx->out_file);
        }
        else {
            snprintf(out_file, PATH_MAX - 1, "%s/%s_%s",
                     ctx->out_path, event_chunk->tag, buff);
        }
    }
    else {
        if (ctx->out_file) {
            snprintf(out_file, PATH_MAX - 1, "%s", ctx->out_file);
        }
        else {
            snprintf(out_file, PATH_MAX - 1, "%s_%s", event_chunk->tag, buff);
        }
    }

    /* Open output file with default name as the Tag */
    flb_free(buff);
    return;
}

static void cb_clp_flush(struct flb_event_chunk *event_chunk,
                          struct flb_output_flush *out_flush,
                          struct flb_input_instance *ins,
                          void *out_context,
                          struct flb_config *config)
{
    int ret;
    FILE * fp;
    size_t off = 0;
    size_t last_off = 0;
    size_t alloc_size = 0;
    size_t total;

    char *buf;
    struct flb_clp_conf *ctx = out_context;
    struct flb_log_event_decoder log_decoder;
    struct flb_log_event log_event;
    char* out_clp_copy;

    (void) config;





    ret = flb_log_event_decoder_init(&log_decoder,
                                     (char *) event_chunk->data,
                                     event_chunk->size);

    if (ret != FLB_EVENT_DECODER_SUCCESS) {
        flb_plg_error(ctx->ins,
                      "Log event decoder initialization error : %d", ret);
        FLB_OUTPUT_RETURN(FLB_ERROR);
    }

    /*
     * Upon flush, for each array, lookup the time and the first field
     * of the map to use as a data point.
     */
    while ((ret = flb_log_event_decoder_next(
                    &log_decoder,
                    &log_event)) == FLB_EVENT_DECODER_SUCCESS) {
        alloc_size = (off - last_off) + 128; /* JSON is larger than msgpack */
        last_off = off;

        if (fp == NULL)
        {
            char * out_file=flb_calloc(PATH_MAX, sizeof(char));
            flb_plg_debug(ctx->ins, "output file: %s", out_file);
            get_output_file(ctx,event_chunk, out_file, &log_event.timestamp);
            fp = fopen(out_file, "ab+");
            if (fp == NULL) {
                flb_errno();
                flb_plg_error(ctx->ins, "error opening: %s", out_file);
                flb_free(out_file);
                FLB_OUTPUT_RETURN(FLB_ERROR);
            }
            flb_free(out_file);
        }



         csv_output(fp,
                       &log_event.timestamp,
                       log_event.body, ctx);

    }
    if(fp != NULL)
    {
        fclose(fp);
        fp=NULL;
    }

    flb_log_event_decoder_destroy(&log_decoder);

    FLB_OUTPUT_RETURN(FLB_OK);
}

static int cb_clp_exit(void *data, struct flb_config *config)
{
    struct flb_clp_conf *ctx = data;

    if (!ctx) {
        return 0;
    }

    flb_free(ctx);
    return 0;
}

/* Configuration properties map */
static struct flb_config_map config_map[] = {
    {
     FLB_CONFIG_MAP_STR, "path", NULL,
     0, FLB_TRUE, offsetof(struct flb_clp_conf, out_path),
     "Absolute path to store the files. This parameter is optional"
    },

    {
     FLB_CONFIG_MAP_STR, "file", NULL,
     0, FLB_TRUE, offsetof(struct flb_clp_conf, out_file),
     "Name of the target file to write the records. If 'path' is specified, "
     "the value is prefixed"
    },

    {
     FLB_CONFIG_MAP_STR, "delimiter", NULL,
     0, FLB_FALSE, 0,
     "Set a custom delimiter for the records"
    },
    {
     FLB_CONFIG_MAP_STR, "hostname", NULL,
     0, FLB_TRUE, offsetof(struct flb_clp_conf, hostname),
     "Name of the target hostname to name the log file"
    },
    /* EOF */
    {0}
};

struct flb_output_plugin out_clp_plugin = {
    .name         = "clp",
    .description  = "Generate clp log file",
    .cb_init      = cb_clp_init,
    .cb_flush     = cb_clp_flush,
    .cb_exit      = cb_clp_exit,
    .flags        = 0,
    .workers      = 1,
    .event_type   = FLB_OUTPUT_LOGS,
    .config_map   = config_map,
};