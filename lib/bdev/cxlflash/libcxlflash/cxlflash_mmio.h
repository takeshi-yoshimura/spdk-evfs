/* IBM_PROLOG_BEGIN_TAG                                                   */
/* This is an automatically generated prolog.                             */
/*                                                                        */
/* $Source: src/include/cflash_mmio.h $                                   */
/*                                                                        */
/* IBM Data Engine for NoSQL - Power Systems Edition User Library Project */
/*                                                                        */
/* Contributors Listed Below - COPYRIGHT 2014,2015                        */
/* [+] International Business Machines Corp.                              */
/*                                                                        */
/*                                                                        */
/* Licensed under the Apache License, Version 2.0 (the "License");        */
/* you may not use this file except in compliance with the License.       */
/* You may obtain a copy of the License at                                */
/*                                                                        */
/*     http://www.apache.org/licenses/LICENSE-2.0                         */
/*                                                                        */
/* Unless required by applicable law or agreed to in writing, software    */
/* distributed under the License is distributed on an "AS IS" BASIS,      */
/* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or        */
/* implied. See the License for the specific language governing           */
/* permissions and limitations under the License.                         */
/*                                                                        */
/* IBM_PROLOG_END_TAG                                                     */

#ifndef _H_CXLFLASH_MMIO
#define _H_CXLFLASH_MMIO

#include "cxlflash_types.h"
#include "sislite.h"
#include "cxlflash_ioctl.h"

#define CAPI_FLASH_BLOCK_SIZE   (4096)
#define CAPI_SCSI_IO_TIME_OUT  5

#if defined(__PPC64__)
#define CBLK_LWSYNC()        __asm__ __volatile__ ("lwsync")
#define CBLK_SYNC()          __asm__ __volatile__ ("sync")
#define CBLK_EIEIO()         __asm__ __volatile__ ("eieio")
#elif defined(__x86_64__)
#define CBLK_LWSYNC()        do {} while(0)
#define CBLK_SYNC()          do {} while(0)
#define CBLK_EIEIO()         do {} while(0)

#endif

#define CFLASH_BLOCK_RST_CTX_COMPLETE_MASK 0x1LL
#define CFLASH_BLOCK_MAX_WAIT_RST_CTX_RETRIES 2500
#define CFLASH_BLOCK_DELAY_RST_CTX 10000

/*****************************************************************************/
/*                                                                           */
/* SCSI Op codes                                                             */
/*                                                                           */
/*****************************************************************************/

#define SCSI_INQUIRY                0x12
#define SCSI_MODE_SELECT            0x15
#define SCSI_MODE_SELECT_10         0x55
#define SCSI_MODE_SENSE             0x1A
#define SCSI_MODE_SENSE_10          0x5A
#define SCSI_PERSISTENT_RESERVE_IN  0x5E
#define SCSI_PERSISTENT_RESERVE_OUT 0x5F
#define SCSI_READ                   0x08
#define SCSI_READ_16                0x88
#define SCSI_READ_CAPACITY          0x25
#define SCSI_READ_EXTENDED          0x28
#define SCSI_REPORT_LUNS            0xA0
#define SCSI_REQUEST_SENSE          0x03
#define SCSI_SERVICE_ACTION_IN      0x9E
#define SCSI_SERVICE_ACTION_OUT     0x9F
#define SCSI_START_STOP_UNIT        0x1B
#define SCSI_TEST_UNIT_READY        0x00
#define SCSI_WRITE                  0x0A
#define SCSI_WRITE_16               0x8A
#define SCSI_WRITE_AND_VERIFY       0x2E
#define SCSI_WRITE_AND_VERIFY_16    0x8E
#define SCSI_WRITE_EXTENDED         0x2A
#define SCSI_WRITE_SAME             0x41
#define SCSI_WRITE_SAME_16          0x93


/*
 *
 *
 *                        READ(16) Command
 *  +=====-======-======-======-======-======-======-======-======+
 *  |  Bit|   7  |   6  |   5  |   4  |   3  |   2  |   1  |   0  |
 *  |Byte |      |      |      |      |      |      |      |      |
 *  |=====+=======================================================|
 *  | 0   |                Operation Code (88h)                   |
 *  |-----+-------------------------------------------------------|
 *  | 1   |                    | DPO  | FUA  | Reserved    |RelAdr|
 *  |-----+-------------------------------------------------------|
 *  | 2   | (MSB)                                                 |
 *  |-----+---                                                 ---|
 *  | 3   |                                                       |
 *  |-----+---                                                 ---|
 *  | 4   |                                                       |
 *  |-----+---                                                 ---|
 *  | 5   |             Logical Block Address                     |
 *  |-----+---                                                 ---|
 *  | 6   |                                                       |
 *  |-----+---                                                 ---|
 *  | 7   |                                                       |
 *  |-----+---                                                 ---|
 *  | 8   |                                                       |
 *  |-----+---                                                 ---|
 *  | 9   |                                                 (LSB) |
 *  |-----+-------------------------------------------------------|
 *  | 10  | (MSB)                                                 |
 *  |-----+---                                                 ---|
 *  | 11  |                                                       |
 *  |-----+---              Transfer Length                    ---|
 *  | 12  |                                                       |
 *  |-----+---                                                 ---|
 *  | 13  |                                                 (LSB) |
 *  |-----+-------------------------------------------------------|
 *  | 14  |                    Reserved                           |
 *  |-----+-------------------------------------------------------|
 *  | 15  |                    Control                            |
 *  +=============================================================+
 *
 *
 *                        WRITE(16) Command
 *  +=====-======-======-======-======-======-======-======-======+
 *  |  Bit|   7  |   6  |   5  |   4  |   3  |   2  |   1  |   0  |
 *  |Byte |      |      |      |      |      |      |      |      |
 *  |=====+=======================================================|
 *  | 0   |                Operation Code (8Ah)                   |
 *  |-----+-------------------------------------------------------|
 *  | 1   |                    | DPO  | FUA  | Reserved    |RelAdr|
 *  |-----+-------------------------------------------------------|
 *  | 2   | (MSB)                                                 |
 *  |-----+---                                                 ---|
 *  | 3   |                                                       |
 *  |-----+---                                                 ---|
 *  | 4   |                                                       |
 *  |-----+---                                                 ---|
 *  | 5   |             Logical Block Address                     |
 *  |-----+---                                                 ---|
 *  | 6   |                                                       |
 *  |-----+---                                                 ---|
 *  | 7   |                                                       |
 *  |-----+---                                                 ---|
 *  | 8   |                                                       |
 *  |-----+---                                                 ---|
 *  | 9   |                                                 (LSB) |
 *  |-----+-------------------------------------------------------|
 *  | 10  | (MSB)                                                 |
 *  |-----+---                                                 ---|
 *  | 11  |                                                       |
 *  |-----+---              Transfer Length                    ---|
 *  | 12  |                                                       |
 *  |-----+---                                                 ---|
 *  | 13  |                                                 (LSB) |
 *  |-----+-------------------------------------------------------|
 *  | 14  |                    Reserved                           |
 *  |-----+-------------------------------------------------------|
 *  | 15  |                    Control                            |
 *  +=============================================================+
 *
 *
 */

#define SCSI_WRITE_SAME_UNMAP_FLAG 0x08;/* byte1 unmap bit in write same CDB */


/************************************************************************/
/* Request (Auto) Sense Data Block                                      */
/************************************************************************/
/*
 *          Error Codes 70h and 71h Fixed Sense Data Format
 * +=====-======-======-======-======-======-======-======-======+
 * |  Bit|   7  |   6  |   5  |   4  |   3  |   2  |   1  |   0  |
 * |Byte |      |      |      |      |      |      |      |      |
 * |=====+======+================================================|
 * | 0   | Valid|          Error Code (70h or 71h)               |
 * |-----+-------------------------------------------------------|
 * | 1   |                 Segment Number                        |
 * |-----+-------------------------------------------------------|
 * | 2   |Filema|  EOM |  ILI |Reserv|     Sense Key             |
 * |-----+-------------------------------------------------------|
 * | 3   | (MSB)                                                 |
 * |- - -+---              Information                        ---|
 * | 6   |                                                 (LSB) |
 * |-----+-------------------------------------------------------|
 * | 7   |                 Additional Sense Length (n-7)         |
 * |-----+-------------------------------------------------------|
 * | 8   | (MSB)                                                 |
 * |- - -+---              Command-Specific Information       ---|
 * | 11  |                                                 (LSB) |
 * |-----+-------------------------------------------------------|
 * | 12  |                 Additional Sense Code                 |
 * |-----+-------------------------------------------------------|
 * | 13  |                 Additional Sense Code Qualifier       |
 * |-----+-------------------------------------------------------|
 * | 14  |                 Field Replaceable Unit Code           |
 * |-----+-------------------------------------------------------|
 * | 15  |  SKSV|                                                |
 * |- - -+------------     Sense-Key Specific                 ---|
 * | 17  |                                                       |
 * |-----+-------------------------------------------------------|
 * | 18  |                                                       |
 * |- - -+---              Additional Sense Bytes             ---|
 * | n   |                                                       |
 * +=============================================================+
 *
 *  Structure for Fixed Sense Data Format
 */

struct request_sense_data  {
    uint8_t     err_code;        /* error class and code   */
    uint8_t     rsvd0;
    uint8_t     sense_key;
#define CFLSH_NO_SENSE              0x00
#define CFLSH_RECOVERED_ERROR       0x01
#define CFLSH_NOT_READY             0x02
#define CFLSH_MEDIUM_ERROR          0x03
#define CFLSH_HARDWARE_ERROR        0x04
#define CFLSH_ILLEGAL_REQUEST       0x05
#define CFLSH_UNIT_ATTENTION        0x06
#define CFLSH_DATA_PROTECT          0x07
#define CFLSH_BLANK_CHECK           0x08
#define CFLSH_VENDOR_UNIQUE         0x09
#define CFLSH_COPY_ABORTED          0x0A
#define CFLSH_ABORTED_COMMAND       0x0B
#define CFLSH_EQUAL_CMD             0x0C
#define CFLSH_VOLUME_OVERFLOW       0x0D
#define CFLSH_MISCOMPARE            0x0E

    uint8_t     sense_byte0;
    uint8_t     sense_byte1;
    uint8_t     sense_byte2;
    uint8_t     sense_byte3;
    uint8_t     add_sense_length;
    uint8_t     add_sense_byte0;
    uint8_t     add_sense_byte1;
    uint8_t     add_sense_byte2;
    uint8_t     add_sense_byte3;
    uint8_t     add_sense_key;
    uint8_t     add_sense_qualifier;
    uint8_t     fru;
    uint8_t     flag_byte;
    uint8_t     field_ptrM;
    uint8_t     field_ptrL;
};



typedef struct cxlflash_iocmd_s {
    sisl_iocmd_t core;
    int id;
} cxlflash_iocmd_t __attribute__ ((aligned (128)));

struct sisl_host_map * cxlflash_mmio_mmap(struct dk_cxlflash_attach * attached);
void cxlflash_mmio_munmap(struct dk_cxlflash_attach * attached, struct sisl_host_map * reg);
void cxlflash_mmio_init_adapter(struct sisl_host_map * reg, sisl_ioasa_t ** rrq, sisl_ioasa_t ** rrq_end, sisl_ioarcb_t ** sq, sisl_ioarcb_t ** sq_end);
int cxlflash_mmio_is_unmapable(struct sisl_host_map * reg);
int cxlflash_mmio_reset_adapter(struct sisl_host_map * reg, struct dk_cxlflash_attach * attached);
int cxlflash_mmio_setup_cmd(cxlflash_iocmd_t * cmd,
                            struct dk_cxlflash_attach * attached, struct dk_cxlflash_udirect * udirect,
                            void * buf, uint64_t lba, uint32_t nblocks, uint8_t op_code, int usesq);
uint64_t cxlflash_mmio_get_cmd_room(struct sisl_host_map * reg);
sisl_ioarcb_t ** cxlflash_mmio_get_sq_head(struct sisl_host_map * reg);
void cxlflash_mmio_submit_cmd_nosq(struct sisl_host_map * reg, cxlflash_iocmd_t * cmd);
void cxlflash_mmio_submit_cmd_sq(struct sisl_host_map * reg, sisl_ioarcb_t ** sq_cur, cxlflash_iocmd_t * cmd);
int cxlflash_mmio_get_complete_cmd(volatile sisl_ioasa_t ** rrq_cur, uint32_t toggle, int sq, cxlflash_iocmd_t ** ret); // TODO: fix return in param

#endif /* _H_CFLASH_MMIO */
