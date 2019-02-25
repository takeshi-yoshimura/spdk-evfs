/*-
 *   BSD LICENSE
 *
 *   Copyright (c) Intel Corporation.
 *   All rights reserved.
 *
 *   Redistribution and use in source and binary forms, with or without
 *   modification, are permitted provided that the following conditions
 *   are met:
 *
 *     * Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in
 *       the documentation and/or other materials provided with the
 *       distribution.
 *     * Neither the name of Intel Corporation nor the names of its
 *       contributors may be used to endorse or promote products derived
 *       from this software without specific prior written permission.
 *
 *   THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 *   "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 *   LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 *   A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 *   OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 *   SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 *   LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 *   DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 *   THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 *   (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 *   OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

#include "bdev_cxlflash.h"
#include "spdk/rpc.h"
#include "spdk/util.h"
#include "spdk/uuid.h"
#include "spdk/string.h"
#include "spdk_internal/log.h"

struct rpc_construct_cxlflash {
	char * name;
	char * uuid;
	char * devStr;
	int queue_depth;
	int nr_qpairs;
	int timeout_nsec;
};

static void
free_rpc_construct_cxlflash(struct rpc_construct_cxlflash *r)
{
	free(r->name);
	free(r->uuid);
}

static const struct spdk_json_object_decoder rpc_construct_cxlflash_decoders[] = {
	{"name", offsetof(struct rpc_construct_cxlflash, name), spdk_json_decode_string, true},
	{"uuid", offsetof(struct rpc_construct_cxlflash, uuid), spdk_json_decode_string, true},
	{"devStr", offsetof(struct rpc_construct_cxlflash, devStr), spdk_json_decode_string},
	{"queueDepth", offsetof(struct rpc_construct_cxlflash, queue_depth), spdk_json_decode_int32},
	{"nrQpairs", offsetof(struct rpc_construct_cxlflash, nr_qpairs), spdk_json_decode_int32},
	{"timeoutNsec", offsetof(struct rpc_construct_cxlflash, timeout_nsec), spdk_json_decode_int32},
};

static void
spdk_rpc_construct_cxlflash_bdev(struct spdk_jsonrpc_request *request,
			       const struct spdk_json_val *params)
{
	struct rpc_construct_cxlflash req = {NULL};
	struct spdk_json_write_ctx *w;
	struct spdk_uuid *uuid = NULL;
	struct spdk_uuid decoded_uuid;
	struct spdk_bdev *bdev;

	if (spdk_json_decode_object(params, rpc_construct_cxlflash_decoders,
				    SPDK_COUNTOF(rpc_construct_cxlflash_decoders),
				    &req)) {
		SPDK_DEBUGLOG(SPDK_LOG_BDEV_CAPI, "spdk_json_decode_object failed\n");
		goto invalid;
	}

	if (req.uuid) {
		if (spdk_uuid_parse(&decoded_uuid, req.uuid)) {
			goto invalid;
		}
		uuid = &decoded_uuid;
	}

	bdev = create_cxlflash_bdev(req.name, uuid, req.devStr, req.queue_depth, req.nr_qpairs, req.timeout_nsec);
	if (bdev == NULL) {
		goto invalid;
	}

	free_rpc_construct_cxlflash(&req);

	w = spdk_jsonrpc_begin_result(request);
	if (w == NULL) {
		return;
	}

	spdk_json_write_string(w, spdk_bdev_get_name(bdev));
	spdk_jsonrpc_end_result(request, w);
	return;

invalid:
	free_rpc_construct_cxlflash(&req);
	spdk_jsonrpc_send_error_response(request, SPDK_JSONRPC_ERROR_INVALID_PARAMS, "Invalid parameters");
}
SPDK_RPC_REGISTER("construct_cxlflash_bdev", spdk_rpc_construct_cxlflash_bdev, SPDK_RPC_RUNTIME)

struct rpc_delete_cxlflash {
	char *name;
};

static void
free_rpc_delete_cxlflash(struct rpc_delete_cxlflash *r)
{
	free(r->name);
}

static const struct spdk_json_object_decoder rpc_delete_cxlflash_decoders[] = {
	{"name", offsetof(struct rpc_delete_cxlflash, name), spdk_json_decode_string},
};

static void
_spdk_rpc_delete_cxlflash_bdev_cb(void *cb_arg, int bdeverrno)
{
	struct spdk_jsonrpc_request *request = cb_arg;
	struct spdk_json_write_ctx *w;

	w = spdk_jsonrpc_begin_result(request);
	if (w == NULL) {
		return;
	}

	spdk_json_write_bool(w, bdeverrno == 0);
	spdk_jsonrpc_end_result(request, w);
}

static void
spdk_rpc_delete_cxlflash_bdev(struct spdk_jsonrpc_request *request,
			    const struct spdk_json_val *params)
{
	struct rpc_delete_cxlflash req = {NULL};
	struct spdk_bdev *bdev;
	int rc;

	if (spdk_json_decode_object(params, rpc_delete_cxlflash_decoders,
				    SPDK_COUNTOF(rpc_delete_cxlflash_decoders),
				    &req)) {
		SPDK_DEBUGLOG(SPDK_LOG_BDEV_CAPI, "spdk_json_decode_object failed\n");
		rc = -EINVAL;
		goto invalid;
	}

	bdev = spdk_bdev_get_by_name(req.name);
	if (bdev == NULL) {
		SPDK_INFOLOG(SPDK_LOG_BDEV_CAPI, "bdev '%s' does not exist\n", req.name);
		rc = -ENODEV;
		goto invalid;
	}

    delete_bdev_cxlflash(bdev, _spdk_rpc_delete_cxlflash_bdev_cb, request);

	free_rpc_delete_cxlflash(&req);

	return;

invalid:
	free_rpc_delete_cxlflash(&req);
	spdk_jsonrpc_send_error_response(request, SPDK_JSONRPC_ERROR_INVALID_PARAMS, spdk_strerror(-rc));
}
SPDK_RPC_REGISTER("delete_cxlflash_bdev", spdk_rpc_delete_cxlflash_bdev, SPDK_RPC_RUNTIME)
