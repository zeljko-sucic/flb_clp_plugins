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

#ifndef FLB_IN_FWCLP_CONN_H
#define FLB_IN_FWCLP_CONN_H

#define FLB_IN_FWCLP_CHUNK_SIZE      "1024000" /* 1MB */
#define FLB_IN_FWCLP_CHUNK_MAX_SIZE  "6144000" /* =FLB_IN_FWCLP_CHUNK_SIZE * 6.  6MB */

enum {
    FWCLP_NEW        = 1,  /* it's a new connection                */
    FWCLP_CONNECTED  = 2,  /* MQTT connection per protocol spec OK */
};

struct fwclp_conn_stream {
    char *tag;
    size_t tag_len;
};

/* Respresents a connection */
struct fwclp_conn {
    int status;                      /* Connection status                 */

    /* Buffer */
    char *buf;                       /* Buffer data                       */
    int  buf_len;                    /* Data length                       */
    int  buf_size;                   /* Buffer size                       */
    size_t rest;                     /* Unpacking offset                  */

    struct flb_input_instance *in;   /* Parent plugin instance            */
    struct flb_in_fwclp_config *ctx;    /* Plugin configuration context      */
    struct flb_connection *connection;

    struct mk_list _head;
};

struct fwclp_conn *fwclp_conn_add(struct flb_connection *connection, struct flb_in_fwclp_config *ctx);
int fwclp_conn_del(struct fwclp_conn *conn);
int fwclp_conn_del_all(struct flb_in_fwclp_config *ctx);

#endif
