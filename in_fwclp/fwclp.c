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

#include <fluent-bit/flb_info.h>
#include <fluent-bit/flb_input.h>
#include <fluent-bit/flb_engine.h>
#include <fluent-bit/flb_downstream.h>
#include <fluent-bit/flb_input_plugin.h>
#include <fluent-bit/flb_network.h>
#include <msgpack.h>

#ifdef FLB_HAVE_UNIX_SOCKET
#include <sys/socket.h>
#include <sys/un.h>
#include <sys/stat.h>
#endif

#include "fwclp.h"
#include "fwclp_conn.h"
#include "fwclp_config.h"

#ifdef FLB_HAVE_UNIX_SOCKET
static int remove_existing_socket_file(char *socket_path)
{
    struct stat file_data;
    int         result;

    result = stat(socket_path, &file_data);

    if (result == -1) {
        if (errno == ENOENT) {
            return 0;
        }

        flb_errno();

        return -1;
    }

    if (S_ISSOCK(file_data.st_mode) == 0) {
        return -2;
    }

    result = unlink(socket_path);

    if (result != 0) {
        return -3;
    }

    return 0;
}

static int fwclp_unix_create(struct flb_in_fwclp_config *ctx)
{
    int ret;

    ret = remove_existing_socket_file(ctx->unix_path);

    if (ret != 0) {
        if (ret == -2) {
            flb_plg_error(ctx->ins,
                          "%s exists and it is not a unix socket. Aborting",
                          ctx->unix_path);
        }
        else {
            flb_plg_error(ctx->ins,
                          "could not remove existing unix socket %s. Aborting",
                          ctx->unix_path);
        }

        return -1;
    }

    ctx->downstream = flb_downstream_create(FLB_TRANSPORT_UNIX_STREAM,
                                            ctx->ins->flags,
                                            ctx->unix_path,
                                            0,
                                            ctx->ins->tls,
                                            ctx->ins->config,
                                            &ctx->ins->net_setup);

    if (ctx->downstream == NULL) {
        return -1;
    }

    if (ctx->unix_perm_str) {
        if (chmod(ctx->unix_path, ctx->unix_perm)) {
            flb_errno();

            flb_plg_error(ctx->ins, "cannot set permission on '%s' to %04o",
                          ctx->unix_path, ctx->unix_perm);

            return -1;
        }
    }

    return 0;
}
#endif
static void get_remote_host(struct flb_connection * connection,  struct flb_in_fwclp_config *ctx)
{

    struct sockaddr_storage addr;
    socklen_t addr_len = sizeof(addr);
    int result = getpeername(connection->fd, (struct sockaddr*)&addr, &addr_len);
    if (addr.ss_family == AF_INET) {
        // IPv4 address
        struct sockaddr_in* sockaddr_ipv4 = (struct sockaddr_in*)&addr;
        char ip4[INET_ADDRSTRLEN];
        inet_ntop(AF_INET, &(sockaddr_ipv4->sin_addr), ip4, INET_ADDRSTRLEN);
        flb_plg_debug(ctx->ins, "IPv4 Address: %s\n", ip4);
        memcpy(connection->remote_host, ip4, INET_ADDRSTRLEN);
    } else if (addr.ss_family == AF_INET6) {
        // IPv6 address
        struct sockaddr_in6* sockaddr_ipv6 = (struct sockaddr_in6*)&addr;
        char ip6[INET6_ADDRSTRLEN];
        inet_ntop(AF_INET6, &(sockaddr_ipv6->sin6_addr), ip6, INET6_ADDRSTRLEN);
        flb_plg_debug(ctx->ins, "IPv6 Address: %s\n", ip6);
        memcpy(connection->remote_host, ip6, INET6_ADDRSTRLEN);
    } else {
        // Address family not supported or unknown
        flb_plg_error(ctx->ins,"Unknown address family\n");
    }

}
/*
 * For a server event, the collection event means a new client have arrived, we
 * accept the connection and create a new FW instance which will wait for
 * MessagePack records.
 */
static int in_fwclp_collect(struct flb_input_instance *ins,
                         struct flb_config *config, void *in_context)
{
    struct flb_connection   *connection;
    struct fwclp_conn          *conn;
    struct flb_in_fwclp_config *ctx;

    ctx = in_context;

    connection = flb_downstream_conn_get(ctx->downstream);

    if (connection == NULL) {
        flb_plg_error(ctx->ins, "could not accept new connection");

        return -1;
    }

    if (!config->is_ingestion_active) {
        flb_downstream_conn_release(connection);

        return -1;
    }
    if(strlen(connection->remote_host) == 0)
    {
        get_remote_host(connection, ctx);
    }



    flb_plg_debug(ctx->ins,"remote host:%s\n",connection->remote_host);
    flb_plg_debug(ctx->ins,  "new TCP connection arrived FD=%i\n", connection->fd);
    //flb_plg_trace(ins, "new TCP connection arrived FD=%i", connection->fd);

    conn = fwclp_conn_add(connection, ctx);

    if (!conn) {
        return -1;
    }

    return 0;
}

/* Initialize plugin */
static int in_fwclp_init(struct flb_input_instance *ins,
                      struct flb_config *config, void *data)
{
    unsigned short int       port;
    int                      ret;
    struct flb_in_fwclp_config *ctx;

    (void) data;

    /* Allocate space for the configuration */
    ctx = fwclp_config_init(ins);
    if (!ctx) {
        return -1;
    }

    ctx->coll_fd = -1;
    ctx->ins = ins;
    mk_list_init(&ctx->connections);

    /* Set the context */
    flb_input_set_context(ins, ctx);

    /* Unix Socket mode */
    if (ctx->unix_path) {
#ifndef FLB_HAVE_UNIX_SOCKET
        flb_plg_error(ctx->ins, "unix address is not supported %s:%s. Aborting",
                      ctx->listen, ctx->tcp_port);
        fwclp_config_destroy(ctx);
        return -1;
#else
        ret = fwclp_unix_create(ctx);
        if (ret != 0) {
            flb_plg_error(ctx->ins, "could not listen on unix://%s",
                          ctx->unix_path);
            fwclp_config_destroy(ctx);
            return -1;
        }
        flb_plg_info(ctx->ins, "listening on unix://%s", ctx->unix_path);
#endif
    }
    else {
        port = (unsigned short int) strtoul(ctx->tcp_port, NULL, 10);

        ctx->downstream = flb_downstream_create(FLB_TRANSPORT_TCP,
                                                ctx->ins->flags,
                                                ctx->listen,
                                                port,
                                                ctx->ins->tls,
                                                config,
                                                &ctx->ins->net_setup);

        if (ctx->downstream == NULL) {
            flb_plg_error(ctx->ins,
                          "could not initialize downstream on unix://%s. Aborting",
                          ctx->listen);

            fwclp_config_destroy(ctx);

            return -1;
        }

        if (ctx->downstream != NULL) {
            flb_plg_info(ctx->ins, "listening on %s:%s",
                         ctx->listen, ctx->tcp_port);
        }
        else {
            flb_plg_error(ctx->ins, "could not bind address %s:%s. Aborting",
                          ctx->listen, ctx->tcp_port);

            fwclp_config_destroy(ctx);

            return -1;
        }
    }

    flb_input_downstream_set(ctx->downstream, ctx->ins);

    flb_net_socket_nonblocking(ctx->downstream->server_fd);

    /* Collect upon data available on the standard input */
    ret = flb_input_set_collector_socket(ins,
                                         in_fwclp_collect,
                                         ctx->downstream->server_fd,
                                         config);
    if (ret == -1) {
        flb_plg_error(ctx->ins, "could not set server socket collector");
        fwclp_config_destroy(ctx);
        return -1;
    }

    ctx->coll_fd = ret;

    return 0;
}

static void in_fwclp_pause(void *data, struct flb_config *config)
{
    struct flb_in_fwclp_config *ctx = data;

    /*
     * If the plugin is paused AND the ingestion not longer active,
     * it means we are in a shutdown phase. This plugin can safetly
     * close the socket server collector.
     *
     * This socket stop is a workaround since the server API will be
     * refactored shortly.
     */
    if (config->is_ingestion_active == FLB_FALSE) {
        fwclp_conn_del_all(ctx);
    }
}

static int in_fwclp_exit(void *data, struct flb_config *config)
{
    (void) *config;
    struct flb_in_fwclp_config *ctx = data;

    if (!ctx) {
        return 0;
    }

    fwclp_conn_del_all(ctx);
    fwclp_config_destroy(ctx);
    return 0;
}

/* Configuration properties map */
static struct flb_config_map config_map[] = {
   {
    FLB_CONFIG_MAP_STR, "tag_prefix", NULL,
    0, FLB_TRUE, offsetof(struct flb_in_fwclp_config, tag_prefix),
    "Prefix incoming tag with the defined value."
   },
   {
    FLB_CONFIG_MAP_STR, "unix_path", NULL,
    0, FLB_TRUE, offsetof(struct flb_in_fwclp_config, unix_path),
    "The path to unix socket to receive a Forward message."
   },
   {
    FLB_CONFIG_MAP_STR, "unix_perm", (char *)NULL,
    0, FLB_TRUE, offsetof(struct flb_in_fwclp_config, unix_perm_str),
    "Set the permissions for the UNIX socket"
   },
   {
    FLB_CONFIG_MAP_SIZE, "buffer_chunk_size", FLB_IN_FWCLP_CHUNK_SIZE,
    0, FLB_TRUE, offsetof(struct flb_in_fwclp_config, buffer_chunk_size),
    "The buffer memory size used to receive a Forward message."
   },
   {
    FLB_CONFIG_MAP_SIZE, "buffer_max_size", FLB_IN_FWCLP_CHUNK_MAX_SIZE,
    0, FLB_TRUE, offsetof(struct flb_in_fwclp_config, buffer_max_size),
    "The maximum buffer memory size used to receive a Forward message."
   },
   {0}
};

/* Plugin reference */
struct flb_input_plugin in_fwclp_plugin = {
    .name         = "fwclp",
    .description  = "Fluentd in-forward",
    .cb_init      = in_fwclp_init,
    .cb_pre_run   = NULL,
    .cb_collect   = in_fwclp_collect,
    .cb_flush_buf = NULL,
    .cb_pause     = in_fwclp_pause,
    .cb_exit      = in_fwclp_exit,
    .config_map   = config_map,
    .flags        = FLB_INPUT_NET_SERVER | FLB_IO_OPT_TLS
};
