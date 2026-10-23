#define _GNU_SOURCE
#include "shim.h"

#include <dlfcn.h>
#include <errno.h>
#include <stdlib.h>
#include <string.h>

#include <urma_api.h>

enum {
    AENV_IMPORT_NONE = 0,
    AENV_IMPORT_GET_TP = 1,
    AENV_IMPORT_EX = 2,
    AENV_IMPORT_FALLBACK = 3,
};

static int pointer_error(int fallback)
{
    return errno > 0 ? -errno : fallback;
}

int aenv_urma_create_ctp_jetty(urma_context_t *context, urma_jfc_t *jfc,
                               uint32_t depth, uint32_t token,
                               urma_jetty_t **jetty, urma_jfr_t **jfr)
{
    urma_jfr_cfg_t jfr_cfg = {0};
    urma_jfs_cfg_t jfs_cfg = {0};
    urma_jetty_cfg_t jetty_cfg = {0};
    int status;

    if (context == NULL || jfc == NULL || depth == 0 || jetty == NULL ||
        jfr == NULL) {
        return -EINVAL;
    }
    *jetty = NULL;
    *jfr = NULL;
    jfr_cfg.depth = depth;
    jfr_cfg.trans_mode = URMA_TM_RM;
    jfr_cfg.max_sge = 1;
    jfr_cfg.min_rnr_timer = URMA_TYPICAL_MIN_RNR_TIMER;
    jfr_cfg.jfc = jfc;
    jfr_cfg.token_value.token = token;
    errno = 0;
    *jfr = urma_create_jfr(context, &jfr_cfg);
    if (*jfr == NULL) {
        return pointer_error(-EIO);
    }

    jfs_cfg.depth = depth;
    jfs_cfg.flag.bs.multi_path = 1;
    jfs_cfg.trans_mode = URMA_TM_RM;
    /* CTP is priority 0 in the deployment profile validated by perftest.
     * Keeping this explicit also supports UMDK releases that predate the
     * public priority_info capability table. */
    jfs_cfg.priority = 0;
    jfs_cfg.max_sge = 2;
    jfs_cfg.max_rsge = 1;
    jfs_cfg.rnr_retry = URMA_TYPICAL_RNR_RETRY;
    jfs_cfg.err_timeout = URMA_TYPICAL_ERR_TIMEOUT;
    jfs_cfg.jfc = jfc;

    jetty_cfg.flag.bs.share_jfr = URMA_SHARE_JFR;
    jetty_cfg.jfs_cfg = jfs_cfg;
    jetty_cfg.shared.jfr = *jfr;
    jetty_cfg.shared.jfc = jfc;
    errno = 0;
    *jetty = urma_create_jetty(context, &jetty_cfg);
    if (*jetty == NULL) {
        status = pointer_error(-EIO);
        (void)urma_delete_jfr(*jfr);
        *jfr = NULL;
        return status;
    }
    return 0;
}

int aenv_urma_export_jetty(urma_jetty_t *jetty, uint8_t **data,
                           uint32_t *length)
{
    typedef urma_status_t (*get_rjetty_fn)(urma_jetty_t *, urma_rjetty_t **,
                                            uint32_t *);
    typedef void (*put_rjetty_fn)(urma_rjetty_t *);
    get_rjetty_fn get_rjetty;
    put_rjetty_fn put_rjetty;
    urma_rjetty_t *provider_descriptor = NULL;
    urma_rjetty_t *descriptor;
    urma_status_t status;
    uint32_t provider_length = 0;

    if (jetty == NULL || data == NULL || length == NULL) {
        return -EINVAL;
    }
    *data = NULL;
    *length = 0;

    /* Newer bonding providers append driver data to urma_rjetty_t. Resolve
     * these APIs dynamically so AgentENV also builds with older public UMDK
     * headers. Always copy the result, because the provider owns it. */
    get_rjetty = (get_rjetty_fn)dlsym(RTLD_DEFAULT, "urma_get_rjetty");
    put_rjetty = (put_rjetty_fn)dlsym(RTLD_DEFAULT, "urma_put_rjetty");
    if (get_rjetty != NULL && put_rjetty != NULL) {
        status = get_rjetty(jetty, &provider_descriptor, &provider_length);
        if (status == URMA_SUCCESS && provider_descriptor != NULL &&
            provider_length >= sizeof(*provider_descriptor)) {
            descriptor = malloc(provider_length);
            if (descriptor == NULL) {
                put_rjetty(provider_descriptor);
                return -ENOMEM;
            }
            memcpy(descriptor, provider_descriptor, provider_length);
            put_rjetty(provider_descriptor);
            *length = provider_length;
            *data = (uint8_t *)descriptor;
            return 0;
        }
        if (provider_descriptor != NULL) {
            put_rjetty(provider_descriptor);
        }
    }

    /* Older providers expose no descriptor-export API. Their remote Jetty
     * descriptor has no provider extension, so the fixed public prefix is
     * sufficient and matches the perftest identity exchange. */
    descriptor = calloc(1, sizeof(*descriptor));
    if (descriptor == NULL) {
        return -ENOMEM;
    }
    descriptor->jetty_id = jetty->jetty_id;
    descriptor->trans_mode = URMA_TM_RM;
    descriptor->type = URMA_JETTY;
    descriptor->tp_type = URMA_CTP;
    *length = sizeof(*descriptor);
    *data = (uint8_t *)descriptor;
    return 0;
}

void aenv_urma_free_descriptor(uint8_t *data)
{
    if (data != NULL) {
        free(data);
    }
}

int aenv_urma_import_ctp(urma_context_t *context, const uint8_t *data,
                         uint32_t length, uint32_t token,
                         urma_target_jetty_t **target,
                         aenv_urma_import_diagnostics_t *diagnostics)
{
    urma_rjetty_t *remote;
    urma_get_tp_cfg_t tp_cfg = {0};
    urma_tp_info_t tp_info = {0};
    urma_import_jetty_ex_cfg_t active_cfg = {0};
    urma_token_t token_value = {.token = token};
    urma_status_t status;
    uint32_t tp_count = 1;

    if (context == NULL || data == NULL || length < sizeof(urma_rjetty_t) ||
        target == NULL || diagnostics == NULL) {
        return -EINVAL;
    }
    *target = NULL;
    memset(diagnostics, 0, sizeof(*diagnostics));
    remote = malloc(length);
    if (remote == NULL) {
        return -ENOMEM;
    }
    memcpy(remote, data, length);
    if (remote->trans_mode != URMA_TM_RM || remote->type != URMA_JETTY) {
        free(remote);
        return -EPROTO;
    }
    remote->tp_type = URMA_CTP;

    tp_cfg.flag.bs.ctp = 1;
    tp_cfg.trans_mode = URMA_TM_RM;
    tp_cfg.local_eid = context->eid;
    tp_cfg.peer_eid = remote->jetty_id.eid;
    diagnostics->stage = AENV_IMPORT_GET_TP;
    errno = 0;
    status = urma_get_tp_list(context, &tp_cfg, &tp_count, &tp_info);
    diagnostics->native_status = (int32_t)status;
    diagnostics->system_errno = errno;
    diagnostics->tp_count = tp_count;

    if (status == URMA_SUCCESS && tp_count == 1) {
        active_cfg.tp_handle = tp_info.tp_handle;
        active_cfg.tp_attr.tx_psn = (uint32_t)rand();
        diagnostics->stage = AENV_IMPORT_EX;
        diagnostics->tp_handle = tp_info.tp_handle;
        diagnostics->tx_psn = active_cfg.tp_attr.tx_psn;
        errno = 0;
        *target = urma_import_jetty_ex(context, remote, &token_value,
                                       &active_cfg);
        diagnostics->system_errno = errno;
    } else if (status != URMA_SUCCESS && errno == ENOMEM) {
        diagnostics->stage = AENV_IMPORT_FALLBACK;
        errno = 0;
        *target = urma_import_jetty(context, remote, &token_value);
        diagnostics->system_errno = errno;
    } else {
        free(remote);
        return status == URMA_SUCCESS ? -EPROTO : (int)status;
    }

    free(remote);
    if (*target == NULL) {
        diagnostics->native_status = pointer_error(-EIO);
        return diagnostics->native_status;
    }
    diagnostics->stage = AENV_IMPORT_NONE;
    return 0;
}
