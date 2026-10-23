#ifndef AENV_URMA_SHIM_H
#define AENV_URMA_SHIM_H

#include <stdint.h>

typedef struct urma_context urma_context_t;
typedef struct urma_jetty urma_jetty_t;
typedef struct urma_jfc urma_jfc_t;
typedef struct urma_jfr urma_jfr_t;
typedef struct urma_target_jetty urma_target_jetty_t;

typedef struct aenv_urma_import_diagnostics {
    uint32_t stage;
    int32_t native_status;
    int32_t system_errno;
    uint32_t tp_count;
    uint64_t tp_handle;
    uint32_t tx_psn;
} aenv_urma_import_diagnostics_t;

int aenv_urma_export_jetty(urma_jetty_t *jetty, uint8_t **data,
                           uint32_t *length);
int aenv_urma_create_ctp_jetty(urma_context_t *context, urma_jfc_t *jfc,
                               uint32_t depth, uint32_t token,
                               urma_jetty_t **jetty, urma_jfr_t **jfr);
void aenv_urma_free_descriptor(uint8_t *data);
int aenv_urma_import_ctp(urma_context_t *context, const uint8_t *data,
                         uint32_t length, uint32_t token,
                         urma_target_jetty_t **target,
                         aenv_urma_import_diagnostics_t *diagnostics);

#endif
