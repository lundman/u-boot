/*
 *
 * ZFS filesystem ported to u-boot by
 * Jorgen Lundman <lundman at lundman.net>
 *
 *	GRUB  --  GRand Unified Bootloader
 *	Copyright (C) 1999,2000,2001,2002,2003,2004
 *	Free Software Foundation, Inc.
 *	Copyright 2004	Sun Microsystems, Inc.
 *
 * SPDX-License-Identifier:	GPL-2.0+
 */

#include <common.h>
#include <malloc.h>
#include <linux/stat.h>
#include <linux/time.h>
#include <linux/ctype.h>
#include <asm/byteorder.h>
#include <dm/device.h>
#include <dm/uclass.h>
#include <inttypes.h>
#include <zfs_common.h>
#include <div64.h>
#include <linux/math64.h>
#include <mapmem.h>
#include <config.h>

struct blk_desc *zfs_dev_desc;

/*
 * The zfs plug-in routines for U-Boot are:
 *
 * zfs_mount() - locates a valid uberblock of the root pool and reads
 *		in its MOS at the memory address MOS.
 *
 * zfs_open() - locates a plain file object by following the MOS
 *		and places its dnode at the memory address DNODE.
 *
 * zfs_read() - read in the data blocks pointed by the DNODE.
 *
 */

#include <zfs/zfs.h>
#include <zfs/zio.h>
#include <zfs/dnode.h>
#include <zfs/uberblock_impl.h>
#include <zfs/vdev_impl.h>
#include <zfs/zio_checksum.h>
#include <zfs/zap_impl.h>
#include <zfs/zap_leaf.h>
#include <zfs/zfs_znode.h>
#include <zfs/dmu.h>
#include <zfs/dmu_objset.h>
#include <zfs/sa_impl.h>
#include <zfs/dsl_dir.h>
#include <zfs/dsl_dataset.h>

#define	ZPOOL_PROP_BOOTFS		"bootfs"

/*
 * For nvlist manipulation. (from nvpair.h)
 */
#define	NV_ENCODE_NATIVE	0
#define	NV_ENCODE_XDR		1
#define	NV_BIG_ENDIAN			0
#define	NV_LITTLE_ENDIAN	1
#define	DATA_TYPE_UINT64	8
#define	DATA_TYPE_STRING	9
#define	DATA_TYPE_NVLIST	19
#define	DATA_TYPE_NVLIST_ARRAY	20

/*
 * Macros to get fields in a bp or DVA.
 */
#define	P2PHASE(x, align)		((x) & ((align) - 1))
#define ALIGN_UP(addr, align)					\
	((addr + (typeof(addr))align - 1) & ~((typeof(addr))align - 1))
#define ALIGN_DOWN(addr, align)					\
	((addr) & ~((typeof(addr))align - 1))

static inline uint64_t
DVA_OFFSET_TO_PHYS_SECTOR(uint64_t offset)
{
	return ((offset + VDEV_LABEL_START_SIZE) >> SPA_MINBLOCKSHIFT);
}

/*
 * FAT ZAP data structures
 */
#define	ZFS_CRC64_POLY 0xC96C5795D7870F42ULL	/* ECMA-182, reflected form */
static inline uint64_t
ZAP_HASH_IDX(uint64_t hash, uint64_t n)
{
	return (((n) == 0) ? 0 : ((hash) >> (64 - (n))));
}

#define	CHAIN_END	0xffff	/* end of the chunk chain */

/*
 * The amount of space within the chunk available for the array is:
 * chunk size - space for type (1) - space for next pointer (2)
 */
#define	ZAP_LEAF_ARRAY_BYTES (ZAP_LEAF_CHUNKSIZE - 3)

static inline int
ZAP_LEAF_HASH_SHIFT(int bs)
{
	return bs - 5;
}

static inline int
ZAP_LEAF_HASH_NUMENTRIES(int bs)
{
	return 1 << ZAP_LEAF_HASH_SHIFT(bs);
}

static inline size_t
LEAF_HASH(int bs, uint64_t h, zap_leaf_phys_t *l)
{
	return ((ZAP_LEAF_HASH_NUMENTRIES(bs) - 1)
		& ((h) >> (64 - ZAP_LEAF_HASH_SHIFT(bs) -
				l->l_hdr.lh_prefix_len)));
}

/*
 * The amount of space available for chunks is:
 * block size shift - hash entry size (2) * number of hash
 * entries - header space (2*chunksize)
 */
static inline int
ZAP_LEAF_NUMCHUNKS(int bs)
{
	return (((1U << bs) - 2 * ZAP_LEAF_HASH_NUMENTRIES(bs)) /
		ZAP_LEAF_CHUNKSIZE - 2);
}

/*
 * The chunks start immediately after the hash table.  The end of the
 * hash table is at l_hash + HASH_NUMENTRIES, which we simply cast to a
 * chunk_t.
 */
static inline zap_leaf_chunk_t *
ZAP_LEAF_CHUNK(zap_leaf_phys_t *l, int bs, int idx)
{
	return &((zap_leaf_chunk_t *)(l->l_entries
			+ (ZAP_LEAF_HASH_NUMENTRIES(bs) * 2)
			/ sizeof(uint64_t)))[idx];
}

static inline struct zap_leaf_entry *
ZAP_LEAF_ENTRY(zap_leaf_phys_t *l, int bs, int idx)
{
	return &ZAP_LEAF_CHUNK(l, bs, idx)->l_entry;
}

/*
 * Decompression Entry - lzjb
 */
#ifndef	NBBY
#define	NBBY	8
#endif

static int
zle_decompress(void *s, void *d,
	       uint32_t slen, uint32_t dlen);
typedef int zfs_decomp_func_t(void *s_start, void *d_start,
			      uint32_t s_len, uint32_t d_len);
typedef struct decomp_entry {
	char *name;
	zfs_decomp_func_t *decomp_func;
} decomp_entry_t;

typedef struct dnode_end {
	dnode_phys_t dn;
	zfs_endian_t endian;
} dnode_end_t;

struct zfs_device_desc {
	enum { DEVICE_LEAF, DEVICE_MIRROR, DEVICE_RAIDZ } type;
	uint64_t id;
	uint64_t guid;
	unsigned int ashift;
	unsigned int max_children_ashift;

	/* Valid only for non-leafs.  */
	unsigned int n_children;
	struct zfs_device_desc *children;

	/* Valid only for RAIDZ.  */
	unsigned int nparity;

	/* Valid only for leaf devices.  */
	//zfs_device_t dev;
	struct zfs_device_s dev;
	loff_t vdev_phys_sector;
	uberblock_t current_uberblock;
	int original;
};

struct subvolume {
	dnode_end_t mdn;
	uint64_t obj;
	uint64_t case_insensitive;
	size_t nkeys;
};

struct zfs_data {
	/* cache for a file block of the currently zfs_open()-ed file */
	char *file_buf;
	uint64_t file_start;
	uint64_t file_end;

	/* cache for a dnode block */
	dnode_phys_t *dnode_buf;
	dnode_phys_t *dnode_mdn;
	uint64_t dnode_start;
	uint64_t dnode_end;
	zfs_endian_t dnode_endian;

	dnode_end_t mos;
	dnode_end_t dnode;
	struct subvolume subvol;

	struct zfs_device_desc *devices_attached;
	unsigned int n_devices_attached;
	unsigned int n_devices_allocated;
	struct zfs_device_desc *device_original;

	uberblock_t current_uberblock;

	uint64_t guid;

	/* U-Boot */
	int (*userhook)(const char *, const struct zfs_dirhook_info *);
	struct zfs_dirhook_info *dirinfo;
};

/* Context for zfs_dir.  */
struct zfs_dir_ctx {
	int (*hook)(const char *, const struct zfs_dirhook_info *,
		    void *hook_data);
	void *hook_data;
	struct zfs_data *data;
};

/*
 * List of pool features that the u-boot implementation of ZFS supports for
 * read. Note that features that are only required for write do not need
 * to be listed here since u-boot opens pools in read-only mode.
 */
#define MAX_SUPPORTED_FEATURE_STRLEN 50
static const char * const spa_feature_names[] = {
	"org.illumos:lz4_compress",
	"com.delphix:hole_birth",
	"com.delphix:embedded_data",
	"com.delphix:extensible_dataset",
	"org.open-zfs:large_blocks",
	NULL
};

static int
check_feature(const char *name, uint64_t val, struct zfs_dir_ctx *ctx);
static int
check_mos_features(dnode_phys_t *mosmdn_phys, zfs_endian_t endian,
		   struct zfs_data *data);

static int
zlib_decompress(void *s, void *d,
		uint32_t slen, uint32_t dlen)
{
	if (zlib_decompress(s, d, slen, dlen) < 0)
		return ZFS_ERR_BAD_FS;
	return ZFS_ERR_NONE;
}

static int
zle_decompress(void *s, void *d,
	       uint32_t slen, uint32_t dlen)
{
	uint8_t *iptr, *optr;
	size_t clen;

	for (iptr = s, optr = d; iptr < (uint8_t *)s + slen &&
	     optr < (uint8_t *)d + dlen;) {
		if (*iptr & 0x80)
			clen = ((*iptr) & 0x7f) + 0x41;
		else
			clen = ((*iptr) & 0x3f) + 1;
		if ((ssize_t)clen > (uint8_t *)d + dlen - optr)
			clen = (uint8_t *)d + dlen - optr;
		if (*iptr & 0x40 || *iptr & 0x80) {
			memset(optr, 0, clen);
			iptr++;
			optr += clen;
			continue;
		}
		if ((ssize_t)clen > (uint8_t *)s + slen - iptr - 1)
			clen = (uint8_t *)s + slen - iptr - 1;
		memcpy(optr, iptr + 1, clen);
		optr += clen;
		iptr += clen + 1;
	}
	if (optr < (uint8_t *)d + dlen)
		memset(optr, 0, (uint8_t *)d + dlen - optr);
	return ZFS_ERR_NONE;
}

static decomp_entry_t decomp_table[ZIO_COMPRESS_FUNCTIONS] = {
	{"inherit", NULL},		/* ZIO_COMPRESS_INHERIT */
	{"on", lzjb_decompress},	/* ZIO_COMPRESS_ON */
	{"off", NULL},		/* ZIO_COMPRESS_OFF */
	{"lzjb", lzjb_decompress},	/* ZIO_COMPRESS_LZJB */
	{"empty", NULL},		/* ZIO_COMPRESS_EMPTY */
	{"gzip-1", zlib_decompress},  /* ZIO_COMPRESS_GZIP1 */
	{"gzip-2", zlib_decompress},  /* ZIO_COMPRESS_GZIP2 */
	{"gzip-3", zlib_decompress},  /* ZIO_COMPRESS_GZIP3 */
	{"gzip-4", zlib_decompress},  /* ZIO_COMPRESS_GZIP4 */
	{"gzip-5", zlib_decompress},  /* ZIO_COMPRESS_GZIP5 */
	{"gzip-6", zlib_decompress},  /* ZIO_COMPRESS_GZIP6 */
	{"gzip-7", zlib_decompress},  /* ZIO_COMPRESS_GZIP7 */
	{"gzip-8", zlib_decompress},  /* ZIO_COMPRESS_GZIP8 */
	{"gzip-9", zlib_decompress},  /* ZIO_COMPRESS_GZIP9 */
	{"zle", zle_decompress},      /* ZIO_COMPRESS_ZLE   */
	{"lz4", lz4_decompress},      /* ZIO_COMPRESS_LZ4   */
};

static int zio_read_data(blkptr_t *bp, zfs_endian_t endian,
			 void *buf, struct zfs_data *data);

static int
zio_read(blkptr_t *bp, zfs_endian_t endian, void **buf,
	 size_t *size, struct zfs_data *data);

/*
 * Our own version of log2().  Same thing as highbit()-1.
 */
static int
zfs_log2(uint64_t num)
{
	int i = 0;

	while (num > 1) {
		i++;
		num = num >> 1;
	}

	return i;
}

/* Checksum Functions */
static void
zio_checksum_off(const void *buf __attribute__ ((unused)),
		 uint64_t size __attribute__ ((unused)),
		 zfs_endian_t endian __attribute__ ((unused)),
		 zio_cksum_t *zcp)
{
	ZIO_SET_CHECKSUM(zcp, 0, 0, 0, 0);
}

/* Checksum Table and Values */
static zio_checksum_info_t zio_checksum_table[ZIO_CHECKSUM_FUNCTIONS] = {
	{NULL, 0, 0, "inherit"},
	{NULL, 0, 0, "on"},
	{zio_checksum_off, 0, 0, "off"},
	{zio_checksum_SHA256, 1, 1, "label"},
	{zio_checksum_SHA256, 1, 1, "gang_header"},
	{NULL, 0, 0, "zilog"},
	{fletcher_2_endian, 0, 0, "fletcher2"},
	{fletcher_4_endian, 1, 0, "fletcher4"},
	{zio_checksum_SHA256, 1, 0, "SHA256"},
	{NULL, 0, 0, "zilog2"},
	{zio_checksum_SHA256, 1, 0, "SHA256+MAC"},
};

/*
 * zio_checksum_verify: Provides support for checksum verification.
 *
 */
static int
zio_checksum_verify(zio_cksum_t zc, uint32_t checksum,
		    zfs_endian_t endian, char *buf, int size)
{
	zio_eck_t *zec = (zio_eck_t *)(buf + size) - 1;
	zio_checksum_info_t *ci = &zio_checksum_table[checksum];
	zio_cksum_t actual_cksum, expected_cksum;

	if (checksum >= ZIO_CHECKSUM_FUNCTIONS || ci->ci_func == NULL) {
		printf("zfs unknown checksum function %d\n", checksum);
		return ZFS_ERR_NOT_IMPLEMENTED_YET;
	}

	if (ci->ci_eck) {
		expected_cksum = zec->zec_cksum;
		zec->zec_cksum = zc;
		ci->ci_func(buf, size, endian, &actual_cksum);
		zec->zec_cksum = expected_cksum;
		zc = expected_cksum;
	} else {
		ci->ci_func(buf, size, endian, &actual_cksum);
	}

	if (memcmp(&actual_cksum, &zc,
		   checksum != ZIO_CHECKSUM_SHA256_MAC ? 32 : 20) != 0) {
		printf("zfs checksum %s verification failed\n", ci->ci_name);
		return ZFS_ERR_BAD_FS;
	}

	return ZFS_ERR_NONE;
}

/*
 * vdev_uberblock_compare takes two uberblock structures and returns an integer
 * indicating the more recent of the two.
 *	Return Value = 1 if ub2 is more recent
 *	Return Value = -1 if ub1 is more recent
 * The most recent uberblock is determined using its transaction number and
 * timestamp.  The uberblock with the highest transaction number is
 * considered "newer".	If the transaction numbers of the two blocks match, the
 * timestamps are compared to determine the "newer" of the two.
 */
static int
vdev_uberblock_compare(uberblock_t *ub1, uberblock_t *ub2)
{
	zfs_endian_t ub1_endian, ub2_endian;

	if (zfs_to_cpu64(ub1->ub_magic, LITTLE_ENDIAN) == UBERBLOCK_MAGIC)
		ub1_endian = LITTLE_ENDIAN;
	else
		ub1_endian = BIG_ENDIAN;
	if (zfs_to_cpu64(ub2->ub_magic, LITTLE_ENDIAN) == UBERBLOCK_MAGIC)
		ub2_endian = LITTLE_ENDIAN;
	else
		ub2_endian = BIG_ENDIAN;

	if (zfs_to_cpu64(ub1->ub_txg, ub1_endian)
		< zfs_to_cpu64(ub2->ub_txg, ub2_endian))
		return -1;
	if (zfs_to_cpu64(ub1->ub_txg, ub1_endian)
		> zfs_to_cpu64(ub2->ub_txg, ub2_endian))
		return 1;

	if (zfs_to_cpu64(ub1->ub_timestamp, ub1_endian)
		< zfs_to_cpu64(ub2->ub_timestamp, ub2_endian))
		return -1;
	if (zfs_to_cpu64(ub1->ub_timestamp, ub1_endian)
		> zfs_to_cpu64(ub2->ub_timestamp, ub2_endian))
		return 1;

	return 0;
}

/*
 * Three pieces of information are needed to verify an uberblock: the magic
 * number, the version number, and the checksum.
 *
 * Currently Implemented: version number, magic number, checksum
 *
 */
static int
uberblock_verify(uberblock_phys_t *ub, uint64_t offset, size_t s)
{
	uberblock_t *uber = &ub->ubp_uberblock;
	int err;
	zfs_endian_t endian = UNKNOWN_ENDIAN;
	zio_cksum_t zc;

	if (zfs_to_cpu64(uber->ub_magic, LITTLE_ENDIAN) == UBERBLOCK_MAGIC &&
	    SPA_VERSION_IS_SUPPORTED(
			zfs_to_cpu64(uber->ub_version, LITTLE_ENDIAN)))
		endian = LITTLE_ENDIAN;

	if (zfs_to_cpu64(uber->ub_magic, BIG_ENDIAN) == UBERBLOCK_MAGIC &&
	    SPA_VERSION_IS_SUPPORTED(zfs_to_cpu64(uber->ub_version,
						  BIG_ENDIAN)))
		endian = BIG_ENDIAN;

	if (endian == UNKNOWN_ENDIAN) {
		//debug("invalid uberblock magic: 0x%llx\n", offset);
		return ZFS_ERR_BAD_FS;
	}

	memset(&zc, 0, sizeof(zc));

	zc.zc_word[0] = cpu_to_zfs64(offset, endian);
	err = zio_checksum_verify(zc, ZIO_CHECKSUM_LABEL, endian,
				  (char *)ub, s);
	//debug("uberblock checksum: %d\n", err);
	return err;
}

/*
 * Find the best uberblock.
 * Return:
 *	  Success - Pointer to the best uberblock.
 *	  Failure - NULL
 */
static uberblock_phys_t *find_bestub(uberblock_phys_t *ub_array,
				     const struct zfs_device_desc *desc)
{
	uberblock_phys_t *ubbest = NULL, *ubptr;
	int i;
	uint64_t offset;
	int err = ZFS_ERR_NONE;
	int ub_shift;

	ub_shift = desc->ashift;
	if (ub_shift < VDEV_UBERBLOCK_SHIFT)
		ub_shift = VDEV_UBERBLOCK_SHIFT;

	for (i = 0; i < (VDEV_UBERBLOCK_RING >> ub_shift); i++) {
		offset = (desc->vdev_phys_sector << SPA_MINBLOCKSHIFT) +
		    VDEV_PHYS_SIZE
		    + (i << ub_shift);

		ubptr = (uberblock_phys_t *)((void *)ub_array +
		    ((i << ub_shift)));
		err = uberblock_verify(ubptr, offset, 1 << ub_shift);
		if (err) {
			errno = ZFS_ERR_NONE;
			continue;
		}
		if (ubbest == NULL ||
		    vdev_uberblock_compare(&ubptr->ubp_uberblock,
					   &ubbest->ubp_uberblock) > 0)
			ubbest = ubptr;
	}
	if (!ubbest)
		errno = err;

	return ubbest;
}

static inline size_t
get_psize(blkptr_t *bp, zfs_endian_t endian)
{
	return (((zfs_to_cpu64((bp)->blk_prop, endian) >> 16) & 0xffff) + 1)
		<< SPA_MINBLOCKSHIFT;
}

static uint64_t
dva_get_offset(const dva_t *dva, zfs_endian_t endian)
{
	return zfs_to_cpu64((dva)->dva_word[1],
		endian) << SPA_MINBLOCKSHIFT;
}

static int
zfs_fetch_nvlist(struct zfs_device_desc *diskdesc, char **nvlist)
{
	int err;

	*nvlist = 0;

	if (!diskdesc->dev.disk) {
		printf("member drive unknown\n");
		return ZFS_ERR_BAD_FS;
	}

	*nvlist = malloc(VDEV_PHYS_SIZE);

	/* Read in the vdev name-value pair list (112K). */
	err = zfs_devread(&diskdesc->dev, diskdesc->vdev_phys_sector, 0,
			  VDEV_PHYS_SIZE, *nvlist);
	if (err) {
		free(*nvlist);
		*nvlist = 0;
		return err;
	}
	return ZFS_ERR_NONE;
}

static int
fill_vdev_info_real(struct zfs_data *data,
		    const char *nvlist,
		    struct zfs_device_desc *fill,
		    struct zfs_device_desc *insert,
		    int *inserted,
		    unsigned int ashift)
{
	char *type;
	uint64_t par;

	type = zfs_nvlist_lookup_string(nvlist, ZPOOL_CONFIG_TYPE);

	if (!type)
		return errno;

	if (!zfs_nvlist_lookup_uint64(nvlist, "id", &fill->id)) {
		free(type);
		printf("couldn't find vdev id\n");
		return ZFS_ERR_BAD_FS;
	}

	if (!zfs_nvlist_lookup_uint64(nvlist, "guid", &fill->guid)) {
		free(type);
		printf("couldn't find vdev id\n");
		return ZFS_ERR_BAD_FS;
	}

	if (zfs_nvlist_lookup_uint64(nvlist, "ashift", &par)) {
		fill->ashift = par;
	} else if (ashift != 0xffffffff) {
		fill->ashift = ashift;
	} else {
		free(type);
		printf("couldn't find ashift\n");
		return ZFS_ERR_BAD_FS;
	}

	fill->max_children_ashift = 0;

	if (strcmp(type, VDEV_TYPE_DISK) == 0 ||
	    strcmp(type, VDEV_TYPE_FILE) == 0) {
		fill->type = DEVICE_LEAF;

		if (!fill->dev.disk && fill->guid == insert->guid) {
			fill->dev.disk = insert->dev.disk;
			memcpy(&fill->dev.part_info, &insert->dev.part_info,
			       sizeof(fill->dev.part_info));
			fill->vdev_phys_sector = insert->vdev_phys_sector;
			fill->current_uberblock = insert->current_uberblock;
			fill->original = insert->original;
			if (!data->device_original)
				data->device_original = fill;
			insert->ashift = fill->ashift;
			*inserted = 1;
		}

		free(type);

		return ZFS_ERR_NONE;
	}

	if (strcmp(type, VDEV_TYPE_MIRROR) == 0 ||
	    strcmp(type, VDEV_TYPE_RAIDZ) == 0) {
		int nelm, i;

		if (strcmp(type, VDEV_TYPE_MIRROR) == 0) {
			fill->type = DEVICE_MIRROR;
		} else {
			uint64_t par;

			fill->type = DEVICE_RAIDZ;
			if (!zfs_nvlist_lookup_uint64(nvlist,
						      "nparity", &par)) {
				free(type);
				printf("couldn't find raidz parity\n");
				return ZFS_ERR_BAD_FS;
			}
			fill->nparity = par;
		}

		nelm = zfs_nvlist_lookup_nvlist_array_get_nelm(nvlist,
							       ZPOOL_CONFIG_CHILDREN);

		if (nelm <= 0) {
			free(type);
			printf("incorrect mirror VDEV\n");
			return ZFS_ERR_BAD_FS;
		}

		if (!fill->children) {
			fill->n_children = nelm;

			fill->children = calloc(1, fill->n_children
				* sizeof(fill->children[0]));
		}

		for (i = 0; i < nelm; i++) {
			char *child;
			int err;

			child = zfs_nvlist_lookup_nvlist_array(nvlist,
				ZPOOL_CONFIG_CHILDREN, i);

			err = fill_vdev_info_real(data, child,
						  &fill->children[i], insert,
						  inserted, fill->ashift);

			free(child);

			if (err) {
				free(type);
				return err;
			}
			if (fill->children[i].ashift >
			    fill->max_children_ashift)
				fill->max_children_ashift =
					fill->children[i].ashift;
		}
		free(type);
		return ZFS_ERR_NONE;
	}

	printf("vdev %s isn't supported\n", type);
	free(type);
	return ZFS_ERR_NOT_IMPLEMENTED_YET;
}

static int
fill_vdev_info(struct zfs_data *data,
	       char *nvlist, struct zfs_device_desc *diskdesc,
	       int *inserted)
{
	uint64_t id;
	unsigned int i;

	*inserted = 0;

	if (!zfs_nvlist_lookup_uint64(nvlist, "id", &id)) {
		printf("couldn't find vdev id\n");
		return ZFS_ERR_BAD_FS;
	}

	for (i = 0; i < data->n_devices_attached; i++)
		if (data->devices_attached[i].id == id)
			return fill_vdev_info_real(data, nvlist,
						   &data->devices_attached[i],
						   diskdesc, inserted,
						   0xffffffff);

	data->n_devices_attached++;
	if (data->n_devices_attached > data->n_devices_allocated) {
		void *tmp;

		data->n_devices_allocated = 2 * data->n_devices_attached + 1;
		data->devices_attached
			= realloc(tmp = data->devices_attached,
				data->n_devices_allocated
				* sizeof(data->devices_attached[0]));
		if (!data->devices_attached) {
			data->devices_attached = tmp;
			return errno;
		}
	}

	memset(&data->devices_attached[data->n_devices_attached - 1],
	       0, sizeof(data->devices_attached[data->n_devices_attached - 1]));

	return fill_vdev_info_real(data, nvlist,
				   &data->devices_attached[data->n_devices_attached - 1],
				   diskdesc, inserted, 0xffffffff);
}

/*
 * For a given XDR packed nvlist, verify the first 4 bytes and move on.
 *
 * An XDR packed nvlist is encoded as (comments from nvs_xdr_create) :
 *
 *      encoding method/host endian     (4 bytes)
 *      nvl_version                     (4 bytes)
 *      nvl_nvflag                      (4 bytes)
 *	encoded nvpairs:
 *		encoded size of the nvpair      (4 bytes)
 *		decoded size of the nvpair      (4 bytes)
 *		name string size                (4 bytes)
 *		name string data                (sizeof(NV_ALIGN4(string))
 *		data type                       (4 bytes)
 *		# of elements in the nvpair     (4 bytes)
 *		data
 *      2 zero's for the last nvpair
 *		(end of the entire list)	(8 bytes)
 *
 */

/*
 * The nvlist_next_nvpair() function returns a handle to the next nvpair in the
 * list following nvpair. If nvpair is NULL, the first pair is returned. If
 * nvpair is the last pair in the nvlist, NULL is returned.
 */
static const char *
nvlist_next_nvpair(const char *nvl, const char *nvpair)
{
	const char *nvp;
	int encode_size;
	int name_len;

	if (nvl == NULL)
		return NULL;

	if (nvpair == NULL) {
		/* skip over header, nvl_version and nvl_nvflag */
		nvpair = nvl + 4 * 3;
	} else {
		/* skip to the next nvpair */
		encode_size = be32_to_cpu(*(uint32_t *)(nvpair));
		nvpair += encode_size;
		/* If encode_size equals 0 nvlist_next_nvpair would return
		 * the same pair received in input, leading to an infinite loop.
		 * If encode_size is less than 0, this will move the pointer
		 * backwards, *possibly* examinining two times the same nvpair
		 * and potentially getting into an infinite loop.
		 */
		if (encode_size <= 0) {
			debug("zfs nvpair with size <= 0\n");
			return NULL;
		}
	}
	/* 8 bytes of 0 marks the end of the list */
	if (*(uint64_t *)(nvpair) == 0)
		return NULL;
	/*consistency checks*/
	if (nvpair + 4 * 3 >= nvl + VDEV_PHYS_SIZE) {
		debug("zfs nvlist overflow\n");
		return NULL;
	}
	encode_size = be32_to_cpu(*(uint32_t *)(nvpair));

	nvp = nvpair + 4 * 2;
	name_len = be32_to_cpu(*(uint32_t *)(nvp));
	nvp += 4;

	nvp = nvp + ((name_len + 3) & ~3); // align
	if (nvp + 4 >= nvl + VDEV_PHYS_SIZE || encode_size < 0 ||
	    nvp + 4 + encode_size > nvl + VDEV_PHYS_SIZE) {
		debug("zfs nvlist overflow\n");
		return NULL;
	}
	/* end consistency checks */

	return nvpair;
}

/*
 * This function returns 0 on success and 1 on failure. On success, a string
 * containing the name of nvpair is saved in buf.
 */
static int
nvpair_name(const char *nvp, char **buf, size_t *buflen)
{
	/* skip over encode/decode size */
	nvp += 4 * 2;

	*buf = (char *)(nvp + 4);
	*buflen = be32_to_cpu(*(uint32_t *)(nvp));

	return 0;
}

/*
 * This function retrieves the value of the nvpair in the form of enumerated
 * type data_type_t.
 */
static int
nvpair_type(const char *nvp)
{
	int name_len, type;

	/* skip over encode/decode size */
	nvp += 4 * 2;

	/* skip over name_len */
	name_len = be32_to_cpu(*(uint32_t *)(nvp));
	nvp += 4;

	/* skip over name */
	nvp = nvp + ((name_len + 3) & ~3); /* align */

	type = be32_to_cpu(*(uint32_t *)(nvp));

	return type;
}

static int
nvpair_value(const char *nvp, char **val,
	     size_t *size_out, size_t *nelm_out)
{
	int name_len, nelm, encode_size;

	/* skip over encode/decode size */
	encode_size = be32_to_cpu(*(uint32_t *)(nvp));
	nvp += 8;

	/* skip over name_len */
	name_len = be32_to_cpu(*(uint32_t *)(nvp));
	nvp += 4;

	/* skip over name */
	nvp = nvp + ((name_len + 3) & ~3); /* align */

	/* skip over type */
	nvp += 4;
	nelm = be32_to_cpu(*(uint32_t *)(nvp));
	nvp += 4;
	if (nelm < 1) {
		printf("empty nvpair\n");
		return 0;
	}
	*val = (char *)nvp;
	*size_out = encode_size;
	if (nelm_out)
		*nelm_out = nelm;

	return 1;
}

/*
 * Check the disk label information and retrieve needed vdev name-value pairs.
 *
 */
static int
check_pool_label(struct zfs_data *data,
		 struct zfs_device_desc *diskdesc,
		 int *inserted, int original)
{
	uint64_t pool_state, txg = 0;
	char *nvlist, *features;
#if 0
	char *nv;
#endif
	uint64_t poolguid;
	uint64_t version;
	int found;
	int err;
	zfs_endian_t endian;
	vdev_phys_t *phys;
	zio_cksum_t emptycksum;

	*inserted = 0;

	err = zfs_fetch_nvlist(diskdesc, &nvlist);
	if (err)
		return err;

	phys = (vdev_phys_t *)nvlist;
	if (zfs_to_cpu64(phys->vp_zbt.zec_magic, LITTLE_ENDIAN) ==
			 ZEC_MAGIC) {
		endian = LITTLE_ENDIAN;
	} else if (zfs_to_cpu64(phys->vp_zbt.zec_magic, BIG_ENDIAN) ==
			      ZEC_MAGIC) {
		endian = BIG_ENDIAN;
	} else {
		free(nvlist);
		printf("bad vdev_phys_t.vp_zbt.zec_magic number\n");
		return ZFS_ERR_BAD_FS;
	}
	/* Now check the integrity of the vdev_phys_t structure
	 * though checksum.
	 */
	ZIO_SET_CHECKSUM(&emptycksum, diskdesc->vdev_phys_sector << 9, 0, 0, 0);
	err = zio_checksum_verify(emptycksum, ZIO_CHECKSUM_LABEL, endian,
				  nvlist, VDEV_PHYS_SIZE);
	if (err)
		return err;

	debug("zfs check 2 passed\n");

	found = zfs_nvlist_lookup_uint64(nvlist, ZPOOL_CONFIG_POOL_STATE,
					 &pool_state);
	if (!found) {
		free(nvlist);
		printf(ZPOOL_CONFIG_POOL_STATE " not found\n");
		return ZFS_ERR_BAD_FS;
	}
	debug("zfs check 3 passed\n");

	if (pool_state == POOL_STATE_DESTROYED) {
		free(nvlist);
		printf("zpool is marked as destroyed\n");
		return ZFS_ERR_BAD_FS;
	}
	debug("zfs check 4 passed\n");

	found = zfs_nvlist_lookup_uint64(nvlist, ZPOOL_CONFIG_POOL_TXG, &txg);
	if (!found) {
		free(nvlist);
		printf(ZPOOL_CONFIG_POOL_TXG " not found\n");
		return ZFS_ERR_BAD_FS;
	}
	debug("zfs check 6 passed\n");

	/* not an active device */
	if (txg == 0) {
		free(nvlist);
		printf("zpool isn't active\n");
		return ZFS_ERR_BAD_FS;
	}
	debug("zfs check 7 passed\n");

	found = zfs_nvlist_lookup_uint64(nvlist, ZPOOL_CONFIG_VERSION,
					 &version);
	if (!found) {
		free(nvlist);
		printf(ZPOOL_CONFIG_VERSION " not found\n");
		return ZFS_ERR_BAD_FS;
	}
	debug("zfs check 8 passed\n");

	if (!SPA_VERSION_IS_SUPPORTED(version)) {
		free(nvlist);
		printf("too new version %llu > %llu\n",
		       (unsigned long long)version,
		       (unsigned long long)SPA_VERSION_BEFORE_FEATURES);
		return ZFS_ERR_NOT_IMPLEMENTED_YET;
	}
	debug("zfs check 9 passed\n");

	found = zfs_nvlist_lookup_uint64(nvlist, ZPOOL_CONFIG_GUID,
					 &diskdesc->guid);
	if (!found) {
		free(nvlist);
		printf(ZPOOL_CONFIG_GUID " not found\n");
		return ZFS_ERR_BAD_FS;
	}

	found = zfs_nvlist_lookup_uint64(nvlist, ZPOOL_CONFIG_POOL_GUID,
					 &poolguid);
	if (!found) {
		free(nvlist);
		printf(ZPOOL_CONFIG_POOL_GUID " not found\n");
		return ZFS_ERR_BAD_FS;
	}

	debug("zfs check 10 passed\n");

	if (original)
		data->guid = poolguid;

	if (data->guid != poolguid) {
		printf("another zpool\n");
		return ZFS_ERR_BAD_FS;
	}

	char *nv;

	nv = zfs_nvlist_lookup_nvlist(nvlist, ZPOOL_CONFIG_VDEV_TREE);

	if (!nv) {
		free(nvlist);
		printf("couldn't find vdev tree\n");
		return ZFS_ERR_BAD_FS;
	}
	err = fill_vdev_info(data, nv, diskdesc, inserted);
	if (err) {
		free(nv);
		free(nvlist);
		return err;
	}
	free(nv);

	debug("zfs check 11 passed\n");
	features = zfs_nvlist_lookup_nvlist(nvlist,
					    ZPOOL_CONFIG_FEATURES_FOR_READ);
	if (features) {
		const char *nvp = NULL;
		char name[MAX_SUPPORTED_FEATURE_STRLEN + 1];
		char *nameptr;
		size_t namelen;

		while ((nvp = nvlist_next_nvpair(features, nvp)) != NULL) {
			nvpair_name(nvp, &nameptr, &namelen);
			if (namelen > MAX_SUPPORTED_FEATURE_STRLEN)
				namelen = MAX_SUPPORTED_FEATURE_STRLEN;
			memcpy(name, nameptr, namelen);
			name[namelen] = '\0';
			debug("zfs str=%s\n", name);
			if (check_feature(name, 1, NULL) != 0) {
				debug("zfs feature missing in check_pool_label:%s\n",
				      name);
				printf(" check_pool_label missing feature '%s' for read\n",
				       name);
				return ZFS_ERR_NOT_IMPLEMENTED_YET;
			}
		}
	}
	debug("zfs check 12 passed (feature flags)\n");
	free(nvlist);

	return ZFS_ERR_NONE;
}

static uint64_t disk_get_size(zfs_device_t dev)
{
	debug("%s: %lu\n", __func__,
	      dev && dev->part_info.size ? dev->part_info.size :
	      dev->disk->lba);
	if (dev && dev->part_info.size)
		return dev->part_info.size;
	return dev->disk->lba;
}

static int
scan_disk(zfs_device_t dev, struct zfs_data *data,
	  int original, int *inserted)
{
	int label = 0;
	uberblock_phys_t *ub_array, *ubbest = NULL;
	vdev_boot_header_t *bh;
	int err;
	int vdevnum;
	struct zfs_device_desc desc;

	ub_array = malloc(VDEV_UBERBLOCK_RING);
	if (!ub_array)
		return ZFS_ERR_OUT_OF_MEMORY;

	bh = malloc(VDEV_BOOT_HEADER_SIZE);
	if (!bh) {
		free(ub_array);
		return ZFS_ERR_OUT_OF_MEMORY;
	}

	vdevnum = VDEV_LABELS;

	desc.dev.disk = dev->disk;
	desc.dev.part_info = dev->part_info;
	desc.original = original;

#if 0
	/* Don't check back labels on CDROM.  */
	if (disk_get_size(dev->disk) == DISK_SIZE_UNKNOWN)
		vdevnum = VDEV_LABELS / 2;
#endif
	for (label = 0; ubbest == NULL && label < vdevnum; label++) {
		debug("label %d: %lu +\n\t %d +\n\t %llu\n",
		      label,
		      label * (sizeof(vdev_label_t) >> SPA_MINBLOCKSHIFT),
		      ((VDEV_SKIP_SIZE + VDEV_BOOT_HEADER_SIZE) >> SPA_MINBLOCKSHIFT),
		      (label < VDEV_LABELS / 2 ? 0 :
		      ALIGN_DOWN(disk_get_size(dev), sizeof(vdev_label_t))
		      - VDEV_LABELS * (sizeof(vdev_label_t) >> SPA_MINBLOCKSHIFT)));

		desc.vdev_phys_sector
			= label * (sizeof(vdev_label_t) >> SPA_MINBLOCKSHIFT)
			+ ((VDEV_SKIP_SIZE + VDEV_BOOT_HEADER_SIZE) >> SPA_MINBLOCKSHIFT)
			+ (label < VDEV_LABELS / 2 ? 0 :
				ALIGN_DOWN(disk_get_size(dev), sizeof(vdev_label_t))
				- VDEV_LABELS * (sizeof(vdev_label_t) >> SPA_MINBLOCKSHIFT));

		/* Read in the uberblock ring (128K). */
		err = zfs_devread(dev, desc.vdev_phys_sector
				  + (VDEV_PHYS_SIZE >> SPA_MINBLOCKSHIFT),
				  0, VDEV_UBERBLOCK_RING, (char *)ub_array);
		if (err)
			continue;

		debug("zfs label ok %d\n", label);

		err = check_pool_label(data, &desc, inserted, original);

		if (inserted)
			printf("zfs: adding device '%s' dev %u\n",
			       blk_get_if_type_name(dev->disk->if_type),
			       dev->disk->devnum);

		if (err || !*inserted)
			continue;

		ubbest = find_bestub(ub_array, &desc);
		if (!ubbest) {
			debug("zfs No uberblock found\n");
			continue;
		}

		memmove(&desc.current_uberblock,
			&ubbest->ubp_uberblock, sizeof(uberblock_t));
		if (original)
			memmove(&data->current_uberblock,
				&ubbest->ubp_uberblock, sizeof(uberblock_t));

		debug("zfs best uberblock txg: %llu\n",
		      desc.current_uberblock.ub_txg);

#if 0
		if (find_best_root &&
		    vdev_uberblock_compare(&ubbest->ubp_uberblock,
					   &current_uberblock) <= 0)
			continue;
#endif
		free(ub_array);
		free(bh);
		return ZFS_ERR_NONE;
	}

	free(ub_array);
	free(bh);

	printf("couldn't find a valid label\n");
	return ZFS_ERR_BAD_FS;
}

/* Helper for scan_devices.  */
static int
scan_devices_iter(struct blk_desc *desc, void *hook_data)
{
	struct zfs_data *data = hook_data;
	struct zfs_device_s dev;
	int inserted = 0;
	int part;
	int i, j;
	int err;
	char dev_str[10]; // To fit "devnum:partnum".

	dev.disk = desc;

	/* Check if this device is already inserted and skip */
	/* This is an optimisation, and not really required */
	for (i = 0; i < data->n_devices_attached; i++) {
		struct zfs_device_desc *zdesc = &data->devices_attached[i];

		for (j = 0; j < zdesc->n_children; j++) {
			struct zfs_device_desc *child = &zdesc->children[j];

			if ((child->dev.disk != NULL) &&
			    (desc->devnum == child->dev.disk->devnum) &&
			    (desc->if_type == child->dev.disk->if_type)) {
				debug("zfs: already scanned device -- skipping\n");
				return 0;
			}
		}
	}

	/* Start up the device so we can use it */
	for (part = 0; part <= MAX_SEARCH_PARTITIONS; part++) {
		snprintf(dev_str, sizeof(dev_str), "%u:%u", desc->devnum, part);
		part = blk_get_device_part_str(blk_get_if_type_name(desc->if_type),
					       dev_str,
					       &dev.disk, &dev.part_info, 1);
		if (part < 0)
			continue;

		err = scan_disk(&dev, data, 0, &inserted);

		if ((err == 0) || inserted)
			return 0;
	}
	return 0;
}

static int
scan_devices(struct zfs_data *data)
{
	struct udevice *dev = NULL;

	printf("zfs: looking for devices...\n");
	for (
		uclass_first_device(UCLASS_BLK, &dev);
		dev;
		uclass_next_device(&dev)
		) {
		struct blk_desc *desc = dev_get_uclass_platdata(dev);

		debug("***   testing device %d: type 0x%x vendor '%s' product '%s'\n",
		      desc->devnum, desc->if_type, desc->vendor,
		      desc->product);
		scan_devices_iter(desc, data);
	}
	printf("zfs: device scan completed.\n");

	return ZFS_ERR_NONE;
}

/* x**y.  */
static uint8_t powx[255 * 2];
/* Such an s that x**s = y */
static int powx_inv[256];
static const uint8_t poly = 0x1d;

/* perform the operation a ^= b * (x ** (known_idx * recovery_pow) ) */
static inline void
xor_out(uint8_t *a, const uint8_t *b, size_t s,
	unsigned int known_idx, unsigned int recovery_pow)
{
	unsigned int add;

	/* Simple xor.  */
	if (known_idx == 0 || recovery_pow == 0) {
		for (; s--; b++, a++)
			*a = *a ^ *b;
		return;
	}
	add = (known_idx * recovery_pow) % 255;
	for (; s--; b++, a++)
		if (*b)
			*a ^= powx[powx_inv[*b] + add];
}

static inline uint8_t
gf_mul(uint8_t a, uint8_t b)
{
	if (a == 0 || b == 0)
		return 0;
	return powx[powx_inv[a] + powx_inv[b]];
}

#define MAX_NBUFS 4

static int
recovery(uint8_t *bufs[4], size_t s, const int nbufs,
	 const unsigned int *powers,
	 const unsigned int *idx)
{
	printf("zfs recovering %u buffers\n", nbufs);
	/* Now we have */
	/* b_i = sum (r_j* (x ** (powers[i] * idx[j])))*/
	/* Let's invert the matrix in question. */
	switch (nbufs) {
	/* Easy: r_0 = bufs[0] / (x << (powers[i] * idx[j])).  */
	case 1:
	{
		int add;
		uint8_t *a;

		if (powers[0] == 0 || idx[0] == 0)
			return ZFS_ERR_NONE;
		add = 255 - ((powers[0] * idx[0]) % 255);
		for (a = bufs[0]; s--; a++)
			if (*a)
				*a = powx[powx_inv[*a] + add];
		return ZFS_ERR_NONE;
	}
	/* Case 2x2: Let's use the determinant formula.  */
	case 2:
	{
		uint8_t det, det_inv;
		uint8_t matrixinv[2][2];
		unsigned int i;
		/* The determinant is: */
		det = (powx[(powers[0] * idx[0] + powers[1] * idx[1]) % 255]
			^ powx[(powers[0] * idx[1] + powers[1] * idx[0]) % 255]);
		if (det == 0) {
			printf("singular recovery matrix\n");
			return ZFS_ERR_BAD_FS;
		}
		det_inv = powx[255 - powx_inv[det]];
		matrixinv[0][0] = gf_mul(powx[(powers[1] * idx[1]) % 255], det_inv);
		matrixinv[1][1] = gf_mul(powx[(powers[0] * idx[0]) % 255], det_inv);
		matrixinv[0][1] = gf_mul(powx[(powers[0] * idx[1]) % 255], det_inv);
		matrixinv[1][0] = gf_mul(powx[(powers[1] * idx[0]) % 255], det_inv);
		for (i = 0; i < s; i++) {
			uint8_t b0, b1;

			b0 = bufs[0][i];
			b1 = bufs[1][i];

			bufs[0][i] = (gf_mul(b0, matrixinv[0][0])
				^ gf_mul(b1, matrixinv[0][1]));
			bufs[1][i] = (gf_mul(b0, matrixinv[1][0])
				^ gf_mul(b1, matrixinv[1][1]));
		}
		return ZFS_ERR_NONE;
	}
	/* Otherwise use Gauss.  */
	case 3:
	{
		uint8_t matrix1[MAX_NBUFS][MAX_NBUFS], matrix2[MAX_NBUFS][MAX_NBUFS];
		int i, j, k;

		for (i = 0; i < nbufs; i++)
			for (j = 0; j < nbufs; j++)
				matrix1[i][j] = powx[(powers[i] * idx[j]) % 255];
		for (i = 0; i < nbufs; i++)
			for (j = 0; j < nbufs; j++)
				matrix2[i][j] = 0;
		for (i = 0; i < nbufs; i++)
			matrix2[i][i] = 1;

		for (i = 0; i < nbufs; i++) {
			uint8_t mul;

			for (j = i; j < nbufs; j++)
				if (matrix1[i][j])
					break;
			if (j == nbufs) {
				printf("singular recovery matrix\n");
				return ZFS_ERR_BAD_FS;
			}
			if (j != i) {
				int xchng;

				xchng = j;
				for (j = 0; j < nbufs; j++) {
					uint8_t t;

					t = matrix1[xchng][j];
					matrix1[xchng][j] = matrix1[i][j];
					matrix1[i][j] = t;
				}
				for (j = 0; j < nbufs; j++) {
					uint8_t t;

					t = matrix2[xchng][j];
					matrix2[xchng][j] = matrix2[i][j];
					matrix2[i][j] = t;
				}
			}
			mul = powx[255 - powx_inv[matrix1[i][i]]];
			for (j = 0; j < nbufs; j++)
				matrix1[i][j] = gf_mul(matrix1[i][j], mul);
			for (j = 0; j < nbufs; j++)
				matrix2[i][j] = gf_mul(matrix2[i][j], mul);
			for (j = i + 1; j < nbufs; j++) {
				mul = matrix1[j][i];
				for (k = 0; k < nbufs; k++)
					matrix1[j][k] ^= gf_mul(matrix1[i][k], mul);
				for (k = 0; k < nbufs; k++)
					matrix2[j][k] ^= gf_mul(matrix2[i][k], mul);
			}
		}
		for (i = nbufs - 1; i >= 0; i--) {
			for (j = 0; j < i; j++) {
				uint8_t mul;

				mul = matrix1[j][i];
				for (k = 0; k < nbufs; k++)
					matrix1[j][k] ^= gf_mul(matrix1[i][k], mul);
				for (k = 0; k < nbufs; k++)
					matrix2[j][k] ^= gf_mul(matrix2[i][k], mul);
			}
		}

		for (i = 0; i < (int)s; i++) {
			uint8_t b[MAX_NBUFS];

			for (j = 0; j < nbufs; j++)
				b[j] = bufs[j][i];
			for (j = 0; j < nbufs; j++) {
				bufs[j][i] = 0;
				for (k = 0; k < nbufs; k++)
					bufs[j][i] ^= gf_mul(matrix2[j][k], b[k]);
			}
		}
		return ZFS_ERR_NONE;
	}
	default:
		printf("too big matrix\n");
		return ZFS_ERR_BAD_FS;
	}
}

static int
read_device(uint64_t offset, struct zfs_device_desc *desc,
	    size_t len, void *buf)
{
	switch (desc->type) {
	case DEVICE_LEAF:
	{
		uint64_t sector;

		sector = DVA_OFFSET_TO_PHYS_SECTOR(offset);
		if (!desc->dev.disk) {
			printf("zfs: couldn't find a necessary member device of multi-device filesystem\n");
			return ZFS_ERR_BAD_FS;
		}
		/* read in a data block */
		return zfs_devread(&desc->dev, sector, 0, len, buf);
	}
	case DEVICE_MIRROR:
	{
		int err = ZFS_ERR_NONE;
		unsigned int i;

		if (desc->n_children <= 0) {
			printf("zfs: non-positive number of mirror children\n");
			return ZFS_ERR_BAD_FS;
		}
		for (i = 0; i < desc->n_children; i++) {
			err = read_device(offset, &desc->children[i],
					  len, buf);
			if (!err)
				break;
			errno = ZFS_ERR_NONE;
		}
		errno = err;

		return err;
	}
	case DEVICE_RAIDZ:
	{
		unsigned int c = 0;
		uint64_t high;
		uint64_t devn;
		uint64_t m;
		uint32_t s, orig_s;
		void *orig_buf = buf;
		size_t orig_len = len;
		uint8_t *recovery_buf[4];
		size_t recovery_len[4];
		unsigned int recovery_idx[4];
		unsigned int failed_devices = 0;
		int idx, orig_idx;

		if (desc->nparity < 1 || desc->nparity > 3) {
			printf("raidz%d is not supported\n", desc->nparity);
			return ZFS_ERR_NOT_IMPLEMENTED_YET;
		}
		if (desc->n_children <= desc->nparity || desc->n_children < 1) {
			printf("too few devices for given parity\n");
			return ZFS_ERR_BAD_FS;
		}

		orig_s = (((len + (1 << desc->ashift) - 1) >> desc->ashift)
			  + (desc->n_children - desc->nparity) - 1);
		s = orig_s;

		high = div64_u64_rem((offset >> desc->ashift),
				     (uint64_t)desc->n_children, &m);

		if (desc->nparity == 2)
			c = 2;
		if (desc->nparity == 3)
			c = 3;
		if (((len + (1 << desc->ashift) - 1) >> desc->ashift)
		    >= (desc->n_children - desc->nparity))
			idx = (desc->n_children - desc->nparity - 1);
		else
			idx = ((len + (1 << desc->ashift) - 1) >> desc->ashift) - 1;
		orig_idx = idx;
		while (len > 0) {
			size_t csize;
			uint32_t bsize;
			int err;

			bsize = s / (desc->n_children - desc->nparity);

			if (desc->nparity == 1 &&
			    ((offset >> (desc->ashift + 20 -
			    desc->max_children_ashift)) & 1) == c)
				c++;

			high = div64_u64_rem((offset >> desc->ashift) + c,
					     (uint64_t)desc->n_children, &devn);
			csize = bsize << desc->ashift;
			if (csize > len)
				csize = len;

			debug("zfs RAIDZ mapping 0x%" PRIx64
			      "+%u (%lu, %" PRIu32
			      ") -> (0x%" PRIx64 ", 0x%"
			      PRIx64 ")\n",
			      offset >> desc->ashift, c, len, bsize, high,
			      devn);
			err = read_device((high << desc->ashift) |
					  (offset & ((1 << desc->ashift) - 1)),
					  &desc->children[devn],
					  csize, buf);
			if (err && failed_devices < desc->nparity) {
				recovery_buf[failed_devices] = buf;
				recovery_len[failed_devices] = csize;
				recovery_idx[failed_devices] = idx;
				failed_devices++;
				errno = 0;
				err = 0;
			}
			if (err)
				return err;

			c++;
			idx--;
			s--;
			buf = (char *)buf + csize;
			len -= csize;
		}
		if (failed_devices) {
			unsigned int redundancy_pow[4];
			unsigned int cur_redundancy_pow = 0;
			unsigned int n_redundancy = 0;
			unsigned int i, j;
			int err;

			/* Compute mul. x**s has a period of 255.  */
			if (powx[0] == 0) {
				uint8_t cur = 1;

				for (i = 0; i < 255; i++) {
					powx[i] = cur;
					powx[i + 255] = cur;
					powx_inv[cur] = i;
					if (cur & 0x80)
						cur = (cur << 1) ^ poly;
					else
						cur <<= 1;
				}
			}

			/* Read redundancy data.  */
			for (n_redundancy = 0, cur_redundancy_pow = 0;
				 n_redundancy < failed_devices;
				 cur_redundancy_pow++) {
				high = div64_u64_rem((offset >> desc->ashift) +
						     cur_redundancy_pow	+
						     ((desc->nparity == 1) &&
						     ((offset >> (desc->ashift +
						     20	- desc->max_children_ashift))
						     & 1)),
						     (uint64_t)desc->n_children,
						     &devn);
				err = read_device((high << desc->ashift)
					| (offset & ((1 << desc->ashift) - 1)),
					&desc->children[devn],
					recovery_len[n_redundancy],
					recovery_buf[n_redundancy]);
				/* Ignore error if we may still have enough devices.  */
				if (err && n_redundancy + desc->nparity - cur_redundancy_pow - 1
					>= failed_devices)
					continue;
				if (err)
					return err;
				redundancy_pow[n_redundancy] = cur_redundancy_pow;
				n_redundancy++;
			}
			/* Now xor-our the parts we already know.  */
			buf = orig_buf;
			len = orig_len;
			s = orig_s;
			idx = orig_idx;

			while (len > 0) {
				size_t csize;

				csize = ((s / (desc->n_children - desc->nparity))
					<< desc->ashift);
				if (csize > len)
					csize = len;

				for (j = 0; j < failed_devices; j++)
					if (buf == recovery_buf[j])
						break;

				if (j == failed_devices)
					for (j = 0; j < failed_devices; j++)
						xor_out(recovery_buf[j], buf,
							csize < recovery_len[j] ? csize : recovery_len[j],
							idx, redundancy_pow[j]);

				s--;
				buf = (char *)buf + csize;
				len -= csize;
				idx--;
			}
			for (i = 0; i < failed_devices &&
			     recovery_len[i] == recovery_len[0];
			     i++)
				;
			/* Since the chunks have variable length handle the last block
			 * separately.
			 */
			if (i != failed_devices) {
				uint8_t *tmp_recovery_buf[4];

				for (j = 0; j < i; j++)
					tmp_recovery_buf[j] = recovery_buf[j] +
						recovery_len[failed_devices - 1];
				err = recovery(tmp_recovery_buf,
					       recovery_len[0] -
					       recovery_len[failed_devices - 1],
					       i, redundancy_pow,
					       recovery_idx);
				if (err)
					return err;
			}
			err = recovery(recovery_buf,
				       recovery_len[failed_devices - 1],
				       failed_devices, redundancy_pow,
				       recovery_idx);
			if (err)
				return err;
			}
			return ZFS_ERR_NONE;
		}
	}
	printf("unsupported device type\n");
	return ZFS_ERR_BAD_FS;
}

static int
read_dva(const dva_t *dva,
	 zfs_endian_t endian, struct zfs_data *data,
	 void *buf, size_t len)
{
	uint64_t offset;
	unsigned int i;
	int err = 0;
	int try = 0;

	offset = dva_get_offset(dva, endian);

	for (try = 0; try < 2; try++) {
		for (i = 0; i < data->n_devices_attached; i++)
			if (data->devices_attached[i].id == DVA_GET_VDEV(dva)) {
				err = read_device(offset,
						  &data->devices_attached[i],
						  len, buf);
				if (!err)
					return ZFS_ERR_NONE;
				break;
			}
		/* Only scan_devices once */
		if (try == 1)
			break;
		err = scan_devices(data);
		if (err)
			return err;
	}
	if (!err) {
		printf("unknown device %d\n",	(int)DVA_GET_VDEV(dva));
		return ZFS_ERR_BAD_FS;
	}
	return err;
}

/*
 * Read a block of data based on the gang block address dva,
 * and put its data in buf.
 *
 */
static int
zio_read_gang(blkptr_t *bp, zfs_endian_t endian, dva_t *dva, void *buf,
	      struct zfs_data *data)
{
	zio_gbh_phys_t *zio_gb;
	unsigned int i;
	int err;
	zio_cksum_t zc;

	memset(&zc, 0, sizeof(zc));

	zio_gb = malloc(SPA_GANGBLOCKSIZE);
	if (!zio_gb)
		return ZFS_ERR_OUT_OF_MEMORY;

	debug(endian == LITTLE_ENDIAN ? "big-endian gang\n"
		: "little-endian gang\n");

	err = read_dva(dva, endian, data, zio_gb, SPA_GANGBLOCKSIZE);
	if (err) {
		free(zio_gb);
		return err;
	}

	/* XXX */
	/* self checksumming the gang block header */
	ZIO_SET_CHECKSUM(&zc, DVA_GET_VDEV(dva),
			 dva_get_offset(dva, endian), bp->blk_birth, 0);
	err = zio_checksum_verify(zc, ZIO_CHECKSUM_GANG_HEADER, endian,
				  (char *)zio_gb, SPA_GANGBLOCKSIZE);
	if (err) {
		free(zio_gb);
		return err;
	}

	endian = (zfs_to_cpu64(bp->blk_prop, endian) >> 63) & 1;

	for (i = 0; i < SPA_GBH_NBLKPTRS; i++) {
		if (BP_IS_HOLE(&zio_gb->zg_blkptr[i]))
			continue;

		err = zio_read_data(&zio_gb->zg_blkptr[i], endian, buf, data);
		if (err) {
			free(zio_gb);
			return err;
		}
		buf = (char *)buf + get_psize(&zio_gb->zg_blkptr[i], endian);
	}
	free(zio_gb);
	return ZFS_ERR_NONE;
}

/*
 * Read in a block of raw data to buf.
 */
static int
zio_read_data(blkptr_t *bp, zfs_endian_t endian, void *buf,
	      struct zfs_data *data)
{
	int i, psize;
	int err = ZFS_ERR_NONE;

	psize = get_psize(bp, endian);

	/* pick a good dva from the block pointer */
	for (i = 0; i < SPA_DVAS_PER_BP; i++) {

		if (bp->blk_dva[i].dva_word[0] == 0 && bp->blk_dva[i].dva_word[1] == 0)
			continue;

		if ((zfs_to_cpu64(bp->blk_dva[i].dva_word[1], endian) >> 63) & 1) {
			err = zio_read_gang(bp, endian, &bp->blk_dva[i], buf, data);
		} else {
			/* read in a data block */
			err = read_dva(&bp->blk_dva[i], endian, data, buf, psize);
		}
		if (!err)
			return ZFS_ERR_NONE;
	}

	if (!err) {
		printf("couldn't find a valid DVA\n");
		err = ZFS_ERR_BAD_FS;
	}

	return err;
}

/*
 * buf must be at least BPE_GET_PSIZE(bp) bytes long (which will never be
 * more than BPE_PAYLOAD_SIZE bytes).
 */
static int
decode_embedded_bp_compressed(const blkptr_t *bp, void *buf)
{
	size_t psize, i;
	uint8_t *buf8 = buf;
	uint64_t w = 0;
	const uint64_t *bp64 = (const uint64_t *)bp;

	psize = BPE_GET_PSIZE(bp);

	/*
	 * Decode the words of the block pointer into the byte array.
	 * Low bits of first word are the first byte (little endian).
	 */
	for (i = 0; i < psize; i++) {
		if (i % sizeof(w) == 0) {
			/* beginning of a word */
			w = *bp64;
			bp64++;
			if (!BPE_IS_PAYLOADWORD(bp, bp64))
				bp64++;
		}
		buf8[i] = BF64_GET(w, (i % sizeof(w)) * 8, 8);
	}
	return ZFS_ERR_NONE;
}

/*
 * Read in a block of data, verify its checksum, decompress if needed,
 * and put the uncompressed data in buf.
 */
static int
zio_read(blkptr_t *bp, zfs_endian_t endian, void **buf,
	 size_t *size, struct zfs_data *data)
{
	size_t lsize, psize;
	unsigned int comp, encrypted;
	char *compbuf = NULL;
	int err;
	zio_cksum_t zc = bp->blk_cksum;
	uint32_t checksum;

	*buf = NULL;

	checksum = (zfs_to_cpu64((bp)->blk_prop, endian) >> 40) & 0xff;
	comp = (zfs_to_cpu64((bp)->blk_prop, endian) >> 32) & 0x7f;
	encrypted = ((zfs_to_cpu64((bp)->blk_prop, endian) >> 60) & 3);
	if (BP_IS_EMBEDDED(bp)) {
		if (BPE_GET_ETYPE(bp) != BP_EMBEDDED_TYPE_DATA) {
			printf("unsupported embedded BP (type=%llu)\n",
			       BPE_GET_ETYPE(bp));
			return ZFS_ERR_NOT_IMPLEMENTED_YET;
		}
		lsize = BPE_GET_LSIZE(bp);
		psize = BF64_GET_SB(zfs_to_cpu64 ((bp)->blk_prop, endian), 25, 7, 0, 1);
	} else {
		lsize = (BP_IS_HOLE(bp) ? 0 :
			(((zfs_to_cpu64 ((bp)->blk_prop, endian) & 0xffff) + 1)
				<< SPA_MINBLOCKSHIFT));
		psize = get_psize(bp, endian);
	}
	debug("zio_read: %s size %lu/%lu\n",
	      BP_IS_EMBEDDED(bp) ? "embedded" : "",
	      lsize, psize);

	if (size)
		*size = lsize;

	if (comp >= ZIO_COMPRESS_FUNCTIONS) {
		printf("compression algorithm %u not supported\n",
		       (unsigned int)comp);
		return ZFS_ERR_NOT_IMPLEMENTED_YET;
	}

	if (comp != ZIO_COMPRESS_OFF && decomp_table[comp].decomp_func == NULL) {
		printf("compression algorithm %s not supported\n",
		       decomp_table[comp].name);
		return ZFS_ERR_NOT_IMPLEMENTED_YET;
	}

	if (comp != ZIO_COMPRESS_OFF)
		compbuf = malloc(psize);
	else
		compbuf = *buf = malloc(lsize);

	if (!compbuf)
		return ZFS_ERR_OUT_OF_MEMORY;

	if (BP_IS_EMBEDDED(bp)) {
		err = decode_embedded_bp_compressed(bp, compbuf);
	} else {
		err = zio_read_data(bp, endian, compbuf, data);
		/* FIXME is it really necessary? */
		if (comp != ZIO_COMPRESS_OFF)
			memset(compbuf + psize, 0, ALIGN_UP(psize, 16) - psize);
	}

	if (err) {
		free(compbuf);
		*buf = NULL;
		return err;
	}

	if (!BP_IS_EMBEDDED(bp)) {
		err = zio_checksum_verify(zc, checksum, endian,
					  compbuf, psize);
		if (err) {
			printf("zfs incorrect checksum\n");
			free(compbuf);
			*buf = NULL;
			return err;
		}
	}

	if (encrypted) {
		printf("zfs Oracle encrypted datasets not supported\n");
		return ZFS_ERR_NOT_IMPLEMENTED_YET;
	}

	if (comp != ZIO_COMPRESS_OFF) {
		*buf = malloc(lsize);
		if (!*buf) {
			free(compbuf);
			return ZFS_ERR_OUT_OF_MEMORY;
		}

		err = decomp_table[comp].decomp_func(compbuf, *buf, psize, lsize);
		free(compbuf);
		if (err) {
			free(*buf);
			*buf = NULL;
			return err;
		}
	}

	return ZFS_ERR_NONE;
}

/*
 * Get the block from a block id.
 * push the block onto the stack.
 *
 */
static int
dmu_read(dnode_end_t *dn, uint64_t blkid, void **buf,
	 zfs_endian_t *endian_out, struct zfs_data *data)
{
	int level;
	off_t idx;
	blkptr_t *bp_array = dn->dn.dn_blkptr;
	int epbs = dn->dn.dn_indblkshift - SPA_BLKPTRSHIFT;
	blkptr_t *bp;
	void *tmpbuf = 0;
	zfs_endian_t endian;
	int err = ZFS_ERR_NONE;

	bp = malloc(sizeof(blkptr_t));
	if (!bp)
		return ZFS_ERR_OUT_OF_MEMORY;

	endian = dn->endian;
	for (level = dn->dn.dn_nlevels - 1; level >= 0; level--) {
		idx = (blkid >> (epbs * level)) & ((1 << epbs) - 1);
		*bp = bp_array[idx];
		if (bp_array != dn->dn.dn_blkptr) {
			free(bp_array);
			bp_array = 0;
		}

		if (BP_IS_HOLE(bp)) {
			size_t size = zfs_to_cpu16(dn->dn.dn_datablkszsec,
				dn->endian)
				<< SPA_MINBLOCKSHIFT;
			*buf = malloc(size);
			if (*buf) {
				err = ZFS_ERR_OUT_OF_MEMORY;
				break;
			}
			memset(*buf, 0, size);
			endian = (zfs_to_cpu64(bp->blk_prop, endian) >> 63) & 1;
			break;
		}
		if (level == 0) {
			err = zio_read(bp, endian, buf, 0, data);
			endian = (zfs_to_cpu64(bp->blk_prop, endian) >> 63) & 1;
			break;
		}
		err = zio_read(bp, endian, &tmpbuf, 0, data);
		endian = (zfs_to_cpu64(bp->blk_prop, endian) >> 63) & 1;
		if (err)
			break;
		bp_array = tmpbuf;
	}
	if (bp_array != dn->dn.dn_blkptr)
		free(bp_array);
	if (endian_out) {
		/* ondisk endian is 0 for BIG, and 1 for LITTLE */
		if (endian == 0)
			*endian_out = BIG_ENDIAN;
		else
			*endian_out = LITTLE_ENDIAN;
	}
	free(bp);
	return err;
}

/*
 * mzap_lookup: Looks up property described by "name" and returns the value
 * in "value".
 */
static int
mzap_lookup(mzap_phys_t *zapobj, zfs_endian_t endian,
	    uint32_t objsize, const char *name, uint64_t *value,
	    int case_insensitive)
{
	uint32_t i, chunks;
	mzap_ent_phys_t *mzap_ent = zapobj->mz_chunk;

	if (objsize < MZAP_ENT_LEN) {
		printf("file '%s' not found\n", name);
		return ZFS_ERR_FILE_NOT_FOUND;
	}
	chunks = objsize / MZAP_ENT_LEN - 1;
	for (i = 0; i < chunks; i++) {
		if (case_insensitive ? (strcasecmp(mzap_ent[i].mze_name, name) == 0):
			(strcmp(mzap_ent[i].mze_name, name) == 0)) {
			*value = zfs_to_cpu64(mzap_ent[i].mze_value, endian);
			return ZFS_ERR_NONE;
		}
	}

	printf("file '%s' not found\n", name);
	return ZFS_ERR_FILE_NOT_FOUND;
}

static int
mzap_iterate(mzap_phys_t *zapobj, zfs_endian_t endian, int objsize,
	     int (*hook)(const char *name,
			 uint64_t val,
			 struct zfs_dir_ctx *ctx),
	     struct zfs_dir_ctx *ctx)
{
	int i, chunks;
	mzap_ent_phys_t *mzap_ent = zapobj->mz_chunk;

	chunks = objsize / MZAP_ENT_LEN - 1;
	for (i = 0; i < chunks; i++) {
		debug("mzap_iterate: %d '%s'\n", i, mzap_ent[i].mze_name);
	/*
	 * The directory entry has the type (currently unused on Solaris) in the
	 * top 4 bits, and the object number in the low 48 bits.  The "middle"
	 * 12 bits are unused.
	 */
		if (hook(mzap_ent[i].mze_name,
			 ZFS_DIRENT_OBJ(zfs_to_cpu64(mzap_ent[i].mze_value,
						     endian)),
			 ctx))
			return 1;
	}

	return 0;
}

static uint64_t
zap_hash(uint64_t salt, const char *name, int case_insensitive)
{
	static uint64_t table[256];
	const uint8_t *cp;
	uint8_t c;
	uint64_t crc = salt;

	if (table[128] == 0) {
		uint64_t *ct = NULL;
		int i, j;

		for (i = 0; i < 256; i++) {
			for (ct = table + i, *ct = i, j = 8; j > 0; j--)
				*ct = (*ct >> 1) ^ (-(*ct & 1) & ZFS_CRC64_POLY);
		}
	}

	if (case_insensitive)
		for (cp = (const uint8_t *)name; (c = *cp) != '\0'; cp++)
			crc = (crc >> 8) ^ table[(crc ^ toupper(c)) & 0xff];
	else
		for (cp = (const uint8_t *)name; (c = *cp) != '\0'; cp++)
			crc = (crc >> 8) ^ table[(crc ^ c) & 0xff];

	/*
	 * Only use 28 bits, since we need 4 bits in the cookie for the
	 * collision differentiator.  We MUST use the high bits, since
	 * those are the onces that we first pay attention to when
	 * chosing the bucket.
	 */
	crc &= ~((1ULL << (64 - ZAP_HASHBITS)) - 1);

	return crc;
}

/*
 * Only to be used on 8-bit arrays.
 * array_len is actual len in bytes (not encoded le_value_length).
 * buf is null-terminated.
 */
static inline int
name_cmp(const char *s1, const char *s2, size_t n,
	 int case_insensitive)
{
	const char *t1 = (const char *)s1;
	const char *t2 = (const char *)s2;

	if (!case_insensitive)
		return memcmp(t1, t2, n);

	while (n--) {
		if (toupper(*t1) != toupper(*t2))
			return (int)toupper(*t1) - (int)toupper(*t2);
		t1++;
		t2++;
	}

	return 0;
}

/* XXX */
static int
zap_leaf_array_equal(zap_leaf_phys_t *l, zfs_endian_t endian,
		     int blksft, int chunk, size_t array_len, const char *buf,
		     int case_insensitive)
{
	int bseen = 0;

	while (bseen < array_len) {
		struct zap_leaf_array *la = &ZAP_LEAF_CHUNK(l, blksft, chunk)->l_array;
		size_t toread = array_len - bseen;

		if (toread > ZAP_LEAF_ARRAY_BYTES)
			toread = ZAP_LEAF_ARRAY_BYTES;

		if (chunk >= ZAP_LEAF_NUMCHUNKS(blksft))
			return 0;

		if (name_cmp((char *)la->la_array, buf + bseen, toread,
			     case_insensitive) != 0)
			break;
		chunk = zfs_to_cpu16(la->la_next, endian);
		bseen += toread;
	}
	return (bseen == array_len);
}

/* XXX */
static int
zap_leaf_array_get(zap_leaf_phys_t *l, zfs_endian_t endian, int blksft,
		   int chunk, size_t array_len, char *buf)
{
	int bseen = 0;

	while (bseen < array_len) {
		struct zap_leaf_array *la = &ZAP_LEAF_CHUNK(l, blksft, chunk)->l_array;
		size_t toread = array_len - bseen;

		if (toread > ZAP_LEAF_ARRAY_BYTES)
			toread = ZAP_LEAF_ARRAY_BYTES;

		if (chunk >= ZAP_LEAF_NUMCHUNKS(blksft))
			/* Don't use errno because this error is to be ignored.  */
			return ZFS_ERR_BAD_FS;

		memcpy(buf + bseen, la->la_array,  toread);
		chunk = zfs_to_cpu16(la->la_next, endian);
		bseen += toread;
	}
	return ZFS_ERR_NONE;
}

/*
 * Given a zap_leaf_phys_t, walk thru the zap leaf chunks to get the
 * value for the property "name".
 *
 */
/* XXX */
static int
zap_leaf_lookup(zap_leaf_phys_t *l, zfs_endian_t endian,
		int blksft, uint64_t h,
		const char *name, uint64_t *value,
		int case_insensitive)
{
	uint16_t chunk;
	struct zap_leaf_entry *le;

	/* Verify if this is a valid leaf block */
	if (zfs_to_cpu64(l->l_hdr.lh_block_type, endian) != ZBT_LEAF) {
		printf("invalid leaf type\n");
		return ZFS_ERR_BAD_FS;
	}
	if (zfs_to_cpu32(l->l_hdr.lh_magic, endian) != ZAP_LEAF_MAGIC) {
		printf("invalid leaf magic\n");
		return ZFS_ERR_BAD_FS;
	}

	for (chunk = zfs_to_cpu16(l->l_hash[LEAF_HASH(blksft, h, l)], endian);
		 chunk != CHAIN_END; chunk = zfs_to_cpu16(le->le_next, endian)) {

		if (chunk >= ZAP_LEAF_NUMCHUNKS(blksft)) {
			printf("invalid chunk number\n");
			return ZFS_ERR_BAD_FS;
		}

		le = ZAP_LEAF_ENTRY(l, blksft, chunk);

		/* Verify the chunk entry */
		if (le->le_type != ZAP_CHUNK_ENTRY) {
			printf("invalid chunk entry\n");
			return ZFS_ERR_BAD_FS;
		}

		if (zfs_to_cpu64(le->le_hash, endian) != h)
			continue;

		if (zap_leaf_array_equal(l, endian, blksft,
					 zfs_to_cpu16(le->le_name_chunk, endian),
					 zfs_to_cpu16(le->le_name_length, endian),
					 name, case_insensitive)) {
			struct zap_leaf_array *la;

			if (le->le_int_size != 8 ||
			    zfs_to_cpu16 (le->le_value_length, endian) != 1) {
				printf("invalid leaf chunk entry\n");
				return ZFS_ERR_BAD_FS;
			}
			/* get the uint64_t property value */
			la = &ZAP_LEAF_CHUNK(l, blksft,
					     le->le_value_chunk)->l_array;

			*value = be64_to_cpu(la->la_array64);

			return ZFS_ERR_NONE;
		}
	}

	printf("couldn't find '%s'\n", name);
	return ZFS_ERR_FILE_NOT_FOUND;
}

/* Verify if this is a fat zap header block */
static int
zap_verify(zap_phys_t *zap, zfs_endian_t endian)
{
	if (zfs_to_cpu64(zap->zap_magic, endian) != (uint64_t)ZAP_MAGIC) {
		printf("bad ZAP magic\n");
		return ZFS_ERR_BAD_FS;
	}

	if (zap->zap_salt == 0) {
		printf("bad ZAP salt\n");
		return ZFS_ERR_BAD_FS;
	}

	return ZFS_ERR_NONE;
}

/*
 * Fat ZAP lookup
 *
 */
/* XXX */
static int
fzap_lookup(dnode_end_t *zap_dnode, zap_phys_t *zap,
	    char *name, uint64_t *value, struct zfs_data *data,
	    int case_insensitive)
{
	void *l;
	uint64_t hash, idx, blkid;
	int blksft = zfs_log2(zfs_to_cpu16(zap_dnode->dn.dn_datablkszsec,
					   zap_dnode->endian) << DNODE_SHIFT);
	int err;
	zfs_endian_t leafendian;

	err = zap_verify(zap, zap_dnode->endian);
	if (err)
		return err;

	hash = zap_hash(zap->zap_salt, name, case_insensitive);

	/* get block id from index */
	if (zap->zap_ptrtbl.zt_numblks != 0) {
		printf("external pointer tables not supported\n");
		return ZFS_ERR_NOT_IMPLEMENTED_YET;
	}
	idx = ZAP_HASH_IDX(hash, zap->zap_ptrtbl.zt_shift);
	blkid = zfs_to_cpu64(((uint64_t *)zap)[idx + (1 << (blksft - 3 - 1))],
			     zap_dnode->endian);

	/* Get the leaf block */
	if ((1U << blksft) < sizeof(zap_leaf_phys_t)) {
		printf("ZAP leaf is too small\n");
		return ZFS_ERR_BAD_FS;
	}
	err = dmu_read(zap_dnode, blkid, &l, &leafendian, data);
	if (err)
		return err;

	err = zap_leaf_lookup(l, leafendian, blksft, hash, name, value,
			      case_insensitive);
	free(l);
	return err;
}

/* XXX */
static int
fzap_iterate(dnode_end_t *zap_dnode, zap_phys_t *zap,
	     size_t name_elem_length,
	     int (*hook)(const char *name, size_t name_length,
			 const void *val_in, size_t nelem, size_t elemsize,
			 void *data),
	     void *hook_data, struct zfs_data *data)
{
	zap_leaf_phys_t *l;
	void *l_in;
	uint64_t idx, idx2, blkid;
	uint16_t chunk;
	int blksft = zfs_log2(zfs_to_cpu16(zap_dnode->dn.dn_datablkszsec,
			zap_dnode->endian) << DNODE_SHIFT);
	int err;
	zfs_endian_t endian;

	if (zap_verify(zap, zap_dnode->endian))
		return 0;

	/* get block id from index */
	if (zap->zap_ptrtbl.zt_numblks != 0) {
		printf("external pointer tables not supported\n");
		return 0;
	}
	/* Get the leaf block */
	if ((1U << blksft) < sizeof(zap_leaf_phys_t)) {
		printf("ZAP leaf is too small\n");
		return 0;
	}
	for (idx = 0; idx < zap->zap_ptrtbl.zt_shift; idx++) {
		blkid = zfs_to_cpu64(((uint64_t *)zap)[idx + (1 << (blksft - 3 - 1))],
				     zap_dnode->endian);

		for (idx2 = 0; idx2 < idx; idx2++)
			if (blkid == zfs_to_cpu64(((uint64_t *)zap)[idx2 + (1 << (blksft - 3 - 1))],
						  zap_dnode->endian))
				break;

		if (idx2 != idx)
			continue;

		err = dmu_read(zap_dnode, blkid, &l_in, &endian, data);
		l = l_in;
		if (err)
			continue;

		/* Verify if this is a valid leaf block */
		if (zfs_to_cpu64(l->l_hdr.lh_block_type, endian) != ZBT_LEAF) {
			free(l);
			continue;
		}
		if (zfs_to_cpu32(l->l_hdr.lh_magic, endian) != ZAP_LEAF_MAGIC) {
			free(l);
			continue;
		}

		for (chunk = 0; chunk < ZAP_LEAF_NUMCHUNKS(blksft); chunk++) {
			char *buf;
			struct zap_leaf_entry *le;
			void *val;
			size_t val_length;

			le = ZAP_LEAF_ENTRY(l, blksft, chunk);

			/* Verify the chunk entry */
			if (le->le_type != ZAP_CHUNK_ENTRY)
				continue;

			buf = malloc(zfs_to_cpu16(le->le_name_length, endian)
				* name_elem_length + 1);
			if (zap_leaf_array_get(l, endian, blksft,
					       zfs_to_cpu16(le->le_name_chunk,
							    endian),
					       zfs_to_cpu16(le->le_name_length,
							    endian)
					       * name_elem_length, buf)) {
				free(buf);
				continue;
			}
			buf[le->le_name_length * name_elem_length] = 0;

			val_length = ((int)le->le_value_length
				* (int)le->le_int_size);

			val = malloc(zfs_to_cpu16(val_length, endian));
			if (zap_leaf_array_get(l, endian, blksft,
					       zfs_to_cpu16(le->le_value_chunk,
							    endian),
					       val_length, val)) {
				free(buf);
				free(val);
				continue;
			}

			if (hook(buf, le->le_name_length,
				 val, le->le_value_length, le->le_int_size,
				 hook_data)) {
				free(l);
				return 1;
			}
			free(buf);
			free(val);
		}
		free(l);
	}
	return 0;
}

/*
 * Read in the data of a zap object and find the value for a matching
 * property name.
 *
 */
static int
zap_lookup(dnode_end_t *zap_dnode, char *name, uint64_t *val,
	   struct zfs_data *data, int case_insensitive)
{
	uint64_t block_type;
	uint32_t size;
	void *zapbuf;
	int err;
	zfs_endian_t endian;

	/* Read in the first block of the zap object data. */
	size = (uint32_t)zfs_to_cpu16(zap_dnode->dn.dn_datablkszsec,
				      zap_dnode->endian) << SPA_MINBLOCKSHIFT;
	err = dmu_read(zap_dnode, 0, &zapbuf, &endian, data);
	if (err)
		return err;
	block_type = zfs_to_cpu64(*((uint64_t *)zapbuf), endian);

	if (block_type == ZBT_MICRO) {
		err = mzap_lookup(zapbuf, endian, size, name, val,
				  case_insensitive);
		free(zapbuf);
		return err;
	} else if (block_type == ZBT_HEADER) {
		/* this is a fat zap */
		err = fzap_lookup(zap_dnode, zapbuf, name, val, data,
				  case_insensitive);
		free(zapbuf);
		return err;
	}

	printf("unknown ZAP type\n");
	free(zapbuf);
	return ZFS_ERR_BAD_FS;
}

/* Context for zap_iterate_u64.  */
struct zap_iterate_u64_ctx {
	int (*hook)(const char *, uint64_t, struct zfs_dir_ctx *);
	struct zfs_dir_ctx *dir_ctx;
};

/* Helper for zap_iterate_u64.  */
static int
zap_iterate_u64_transform(const char *name,
			  size_t namelen __attribute__ ((unused)),
			  const void *val_in,
			  size_t nelem,
			  size_t elemsize,
			  void *data)
{
	struct zap_iterate_u64_ctx *ctx = data;

	if (elemsize != sizeof(uint64_t) || nelem != 1)
		return 0;
	return ctx->hook(name, be64_to_cpu(*(const uint64_t *)val_in),
		ctx->dir_ctx);
}

static int
zap_iterate_u64(dnode_end_t *zap_dnode,
		int (*hook)(const char *name, uint64_t val,
			    struct zfs_dir_ctx *ctx),
		struct zfs_data *data, struct zfs_dir_ctx *ctx)
{
	uint64_t block_type;
	int size;
	void *zapbuf;
	int err;
	int ret;
	zfs_endian_t endian;

	/* Read in the first block of the zap object data. */
	size = zfs_to_cpu16(zap_dnode->dn.dn_datablkszsec, zap_dnode->endian) << SPA_MINBLOCKSHIFT;
	err = dmu_read(zap_dnode, 0, &zapbuf, &endian, data);
	if (err)
		return 0;
	block_type = zfs_to_cpu64(*((uint64_t *)zapbuf), endian);

	if (block_type == ZBT_MICRO) {
		ret = mzap_iterate(zapbuf, endian, size, hook, ctx);
		free(zapbuf);
		return ret;
	} else if (block_type == ZBT_HEADER) {
		struct zap_iterate_u64_ctx transform_ctx = {
			.hook = hook,
			.dir_ctx = ctx
		};

		/* this is a fat zap */
		ret = fzap_iterate(zap_dnode, zapbuf, 1,
				   zap_iterate_u64_transform, &transform_ctx,
				   data);
		free(zapbuf);
		return ret;
	}
	printf("zfs: unknown ZAP type\n");
	return 0;
}

/*
 * Get the dnode of an object number from the metadnode of an object set.
 *
 * Input
 *	mdn - metadnode to get the object dnode
 *	objnum - object number for the object dnode
 *	buf - data buffer that holds the returning dnode
 */
static int
dnode_get(dnode_end_t *mdn, uint64_t objnum, uint8_t type,
	  dnode_end_t *buf, struct zfs_data *data)
{
	uint64_t blkid, blksz;	/* the block id this object dnode is in */
	int epbs;			/* shift of number of dnodes in a block */
	int idx;			/* index within a block */
	void *dnbuf = 0;
	int err;
	zfs_endian_t endian;

	blksz = zfs_to_cpu16(mdn->dn.dn_datablkszsec,
			     mdn->endian) << SPA_MINBLOCKSHIFT;

	epbs = zfs_log2(blksz) - DNODE_SHIFT;
	blkid = objnum >> epbs;
	idx = objnum & ((1 << epbs) - 1);

	if (data->dnode_buf != NULL && memcmp(data->dnode_mdn, mdn,
					      sizeof(*mdn)) == 0 &&
	    objnum >= data->dnode_start && objnum < data->dnode_end) {
		memmove(&buf->dn, &data->dnode_buf[idx], DNODE_SIZE);
		buf->endian = data->dnode_endian;
		if (type && buf->dn.dn_type != type)  {
			printf("incorrect dnode type: %02X != %02x\n", buf->dn.dn_type, type);
			return ZFS_ERR_BAD_FS;
		}
		return ZFS_ERR_NONE;
	}

	err = dmu_read(mdn, blkid, &dnbuf, &endian, data);
	if (err)
		return err;
	if (dnbuf == NULL)
		return ZFS_ERR_BAD_FS;

	free(data->dnode_buf);
	free(data->dnode_mdn);
	data->dnode_mdn = malloc(sizeof(*mdn));
	if (!data->dnode_mdn) {
		data->dnode_buf = 0;
	} else {
		memcpy(data->dnode_mdn, mdn, sizeof(*mdn));
		data->dnode_buf = dnbuf;
		data->dnode_start = blkid << epbs;
		data->dnode_end = (blkid + 1) << epbs;
		data->dnode_endian = endian;
	}

	memmove(&buf->dn, (dnode_phys_t *)dnbuf + idx, DNODE_SIZE);
	buf->endian = endian;
	if (type && buf->dn.dn_type != type) {
		printf("incorrect dnode type: %02X != %02x\n", buf->dn.dn_type, type);
		return ZFS_ERR_BAD_FS;
	}

	return ZFS_ERR_NONE;
}

/*
 * Get the file dnode for a given file name where mdn is the meta dnode
 * for this ZFS object set. When found, place the file dnode in dn.
 * The 'path' argument will be mangled.
 *
 */
static int
dnode_get_path(struct subvolume *subvol, const char *path_in, dnode_end_t *dn,
	       struct zfs_data *data)
{
	uint64_t objnum, version;
	char *cname, ch;
	int err = ZFS_ERR_NONE;
	char *path, *path_buf;
	struct dnode_chain {
		struct dnode_chain *next;
		dnode_end_t dn;
	};
	struct dnode_chain *dnode_path = 0, *dn_new, *root;

	dn_new = malloc(sizeof(*dn_new));
	if (!dn_new)
		return ZFS_ERR_OUT_OF_MEMORY;
	dn_new->next = 0;
	dnode_path = dn_new;
	root = dn_new;

	err = dnode_get(&subvol->mdn, MASTER_NODE_OBJ, DMU_OT_MASTER_NODE,
			&dnode_path->dn, data);
	if (err) {
		free(dn_new);
		return err;
	}

	err = zap_lookup(&dnode_path->dn, ZPL_VERSION_STR, &version, data, 0);
	if (err) {
		free(dn_new);
		return err;
	}
	if (version > ZPL_VERSION) {
		free(dn_new);
		printf("too new ZPL version\n");
		return ZFS_ERR_NOT_IMPLEMENTED_YET;
	}

	err = zap_lookup(&dnode_path->dn, "casesensitivity",
			 &subvol->case_insensitive,
			 data, 0);
	if (err == ZFS_ERR_FILE_NOT_FOUND)
		subvol->case_insensitive = 0;

	err = zap_lookup(&dnode_path->dn, ZFS_ROOT_OBJ, &objnum, data, 0);
	if (err) {
		free(dn_new);
		return err;
	}

	err = dnode_get(&subvol->mdn, objnum, 0, &dnode_path->dn, data);
	if (err) {
		free(dn_new);
		return err;
	}

	path = strdup(path_in);
	path_buf = path;
	if (!path_buf) {
		free(dn_new);
		return ZFS_ERR_OUT_OF_MEMORY;
	}

	while (1) {
		/* skip leading slashes */
		while (*path == '/')
			path++;
		if (!*path)
			break;
		/* get the next component name */
		cname = path;
		while (*path && *path != '/')
			path++;
		/* Skip dot.  */
		if (cname + 1 == path && cname[0] == '.')
			continue;
		/* Handle double dot.  */
		if (cname + 2 == path && cname[0] == '.' && cname[1] == '.')  {
			if (dn_new->next) {
				dn_new = dnode_path;
				dnode_path = dn_new->next;
				free(dn_new);
			} else {
				printf("can't resolve ..\n");
				err = ZFS_ERR_FILE_NOT_FOUND;
				break;
			}
			continue;
		}

		ch = *path;
		*path = 0;		/* ensure null termination */

		if (dnode_path->dn.dn.dn_type != DMU_OT_DIRECTORY_CONTENTS) {
			free(path_buf);
			printf("not a directory\n");
			return ZFS_ERR_BAD_FILE_TYPE;
		}
		err = zap_lookup(&dnode_path->dn, cname, &objnum, data,
				 subvol->case_insensitive);
		if (err)
			break;

		dn_new = malloc(sizeof(*dn_new));
		if (!dn_new) {
			err = ZFS_ERR_OUT_OF_MEMORY;
			break;
		}
		dn_new->next = dnode_path;
		dnode_path = dn_new;

		objnum = ZFS_DIRENT_OBJ(objnum);
		err = dnode_get(&subvol->mdn, objnum, 0, &dnode_path->dn, data);
		if (err)
			break;

		*path = ch;

		znode_phys_t *zp = (znode_phys_t *)DN_BONUS(&dnode_path->dn.dn);
		if (dnode_path->dn.dn.dn_bonustype == DMU_OT_ZNODE &&
		    ((zfs_to_cpu64(zp->zp_mode, dnode_path->dn.endian)
					>> 12) & 0xf)
			== 0xa) {
			char *sym_value;
			size_t sym_sz;
			int free_symval = 0;
			char *oldpath = path, *oldpathbuf = path_buf;

			sym_value = ((char *)DN_BONUS(&dnode_path->dn.dn) + sizeof(struct znode_phys));

			sym_sz = zfs_to_cpu64(zp->zp_size, dnode_path->dn.endian);

			if (dnode_path->dn.dn.dn_flags & 1) {
				size_t block;
				size_t blksz;

				blksz = (zfs_to_cpu16(dnode_path->dn.dn.dn_datablkszsec,
						      dnode_path->dn.endian)
					<< SPA_MINBLOCKSHIFT);

				if (blksz == 0) {
					printf("0-sized block\n");
					return ZFS_ERR_BAD_FS;
				}
				sym_value = malloc(sym_sz);
				if (!sym_value)
					return ZFS_ERR_OUT_OF_MEMORY;
				for (block = 0; block < (sym_sz + blksz - 1) / blksz; block++) {
					void *t;
					size_t movesize;

					err = dmu_read(&dnode_path->dn, block, &t, 0, data);
					if (err) {
						free(sym_value);
						return err;
					}

					movesize = sym_sz - block * blksz;
					if (movesize > blksz)
						movesize = blksz;

					memcpy(sym_value + block * blksz, t, movesize);
					free(t);
				}
				free_symval = 1;
			}
			path = malloc(sym_sz + strlen(oldpath) + 1);
			path_buf = path;
			if (!path_buf) {
				free(oldpathbuf);
				if (free_symval)
					free(sym_value);
				return ZFS_ERR_OUT_OF_MEMORY;
			}
			memcpy(path, sym_value, sym_sz);
			if (free_symval)
				free(sym_value);
			path[sym_sz] = 0;
			memcpy(path + strlen(path), oldpath,
			       strlen(oldpath) + 1);

			free(oldpathbuf);
			if (path[0] != '/') {
				dn_new = dnode_path;
				dnode_path = dn_new->next;
				free(dn_new);
			} else
			    while (dnode_path != root) {
					dn_new = dnode_path;
					dnode_path = dn_new->next;
					free(dn_new);
				}
		}
		if (dnode_path->dn.dn.dn_bonustype == DMU_OT_SA) {
			void *sahdrp;
			int hdrsize;

			if (dnode_path->dn.dn.dn_bonuslen != 0) {
				sahdrp = DN_BONUS(&dnode_path->dn.dn);
			} else if (dnode_path->dn.dn.dn_flags & DNODE_FLAG_SPILL_BLKPTR) {
				blkptr_t *bp = &dnode_path->dn.dn.dn_spill;

				err = zio_read(bp, dnode_path->dn.endian, &sahdrp, NULL, data);
				if (err)
					return err;
			} else {
				printf("filesystem is corrupt\n");
				return ZFS_ERR_BAD_FS;
			}

			hdrsize = SA_HDR_SIZE(((sa_hdr_phys_t *)sahdrp));

			if (((zfs_to_cpu64(*(uint64_t *)((char *)sahdrp
								+ hdrsize
								+ SA_TYPE_OFFSET),
							dnode_path->dn.endian) >> 12) & 0xf) == 0xa) {
				char *sym_value = (char *)sahdrp + hdrsize + SA_SYMLINK_OFFSET;
				size_t sym_sz =
					zfs_to_cpu64(*(uint64_t *)((char *)sahdrp
							+ hdrsize
							+ SA_SIZE_OFFSET),
						dnode_path->dn.endian);
				char *oldpath = path, *oldpathbuf = path_buf;

				path = malloc(sym_sz + strlen(oldpath) + 1);
				path_buf = path;
				if (!path_buf) {
					free(oldpathbuf);
					return ZFS_ERR_OUT_OF_MEMORY;
				}
				memcpy(path, sym_value, sym_sz);
				path[sym_sz] = 0;
				memcpy(path + strlen(path), oldpath,
				       strlen(oldpath) + 1);

				free(oldpathbuf);
				if (path[0] != '/') {
					dn_new = dnode_path;
					dnode_path = dn_new->next;
					free(dn_new);
				} else {
					while (dnode_path != root) {
						dn_new = dnode_path;
						dnode_path = dn_new->next;
						free(dn_new);
					}
				}
			}
		}
	}

	if (!err)
		memcpy(dn, &dnode_path->dn, sizeof(*dn));

	while (dnode_path) {
		dn_new = dnode_path->next;
		free(dnode_path);
		dnode_path = dn_new;
	}
	free(path_buf);
	return err;
}

/*
 * Given a MOS metadnode, get the metadnode of a given filesystem name (fsname),
 * e.g. pool/rootfs, or a given object number (obj), e.g. the object number
 * of pool/rootfs.
 *
 * If no fsname and no obj are given, return the DSL_DIR metadnode.
 * If fsname is given, return its metadnode and its matching object number.
 * If only obj is given, return the metadnode for this object number.
 *
 */
static int
get_filesystem_dnode(dnode_end_t *mosmdn, char *fsname,
		     dnode_end_t *mdn, struct zfs_data *data)
{
	uint64_t objnum;
	int err;

	err = dnode_get(mosmdn, DMU_POOL_DIRECTORY_OBJECT,
			DMU_OT_OBJECT_DIRECTORY, mdn, data);
	if (err)
		return err;

	err = zap_lookup(mdn, DMU_POOL_ROOT_DATASET, &objnum, data, 0);
	if (err)
		return err;

	err = dnode_get(mosmdn, objnum, 0, mdn, data);
	if (err)
		return err;

	while (*fsname) {
		uint64_t childobj;
		char *cname, ch;

		while (*fsname == '/')
			fsname++;

		if (!*fsname || *fsname == '@')
			break;

		cname = fsname;
		while (*fsname && !isspace(*fsname) && *fsname != '/')
			fsname++;
		ch = *fsname;
		*fsname = 0;

		dsl_dir_phys_t *dp = (dsl_dir_phys_t *)DN_BONUS(&mdn->dn);
		childobj = zfs_to_cpu64(dp->dd_child_dir_zapobj, mdn->endian);
		err = dnode_get(mosmdn, childobj,
				DMU_OT_DSL_DIR_CHILD_MAP, mdn, data);
		if (err)
			return err;

		err = zap_lookup(mdn, cname, &objnum, data, 0);
		if (err)
			return err;

		err = dnode_get(mosmdn, objnum, 0, mdn, data);
		if (err)
			return err;

		*fsname = ch;
	}
	return ZFS_ERR_NONE;
}

static int
make_mdn(dnode_end_t *mdn, struct zfs_data *data)
{
	void *osp;
	blkptr_t *bp;
	size_t ospsize;
	int err;

	dsl_dataset_phys_t *dp = (dsl_dataset_phys_t *)DN_BONUS(&mdn->dn);
	bp = &dp->ds_bp;
	err = zio_read(bp, mdn->endian, &osp, &ospsize, data);
	if (err)
		return err;
	if (ospsize < OBJSET_PHYS_SIZE_V14) {
		free(osp);
		printf("too small osp\n");
		return ZFS_ERR_BAD_FS;
	}

	mdn->endian = (zfs_to_cpu64(bp->blk_prop, mdn->endian) >> 63) & 1;
	memmove((char *)&mdn->dn,
		(char *)&((objset_phys_t *)osp)->os_meta_dnode, DNODE_SIZE);
	free(osp);
	return ZFS_ERR_NONE;
}

/* Context for dnode_get_fullpath.  */
struct dnode_get_fullpath_ctx {
	struct subvolume *subvol;
	uint64_t salt;
	int keyn;
};

static int
dnode_get_fullpath(const char *fullpath, struct subvolume *subvol,
		   dnode_end_t *dn, int *isfs,
		   struct zfs_data *data)
{
	char *fsname, *snapname;
	const char *ptr_at, *filename;
	uint64_t headobj;
	int err;

	ptr_at = strchr(fullpath, '@');
	if (!ptr_at) {
		*isfs = 1;
		filename = 0;
		snapname = 0;
		fsname = strdup(fullpath);
	} else {
		const char *ptr_slash = strchr(ptr_at, '/');

		*isfs = 0;
		fsname = malloc(ptr_at - fullpath + 1);
		if (!fsname)
			return ZFS_ERR_OUT_OF_MEMORY;
		memcpy(fsname, fullpath, ptr_at - fullpath);
		fsname[ptr_at - fullpath] = 0;
		if (ptr_at[1] && ptr_at[1] != '/') {
			snapname = malloc(ptr_slash - ptr_at);
			if (!snapname) {
				free(fsname);
				return ZFS_ERR_OUT_OF_MEMORY;
			}
			memcpy(snapname, ptr_at + 1, ptr_slash - ptr_at - 1);
			snapname[ptr_slash - ptr_at - 1] = 0;
		} else {
			snapname = 0;
		}
		if (ptr_slash)
			filename = ptr_slash;
		else
			filename = "/";
		printf("zfs fsname = '%s' snapname='%s' filename = '%s'\n",
		       fsname, snapname, filename);
	}

	err = get_filesystem_dnode(&data->mos, fsname, dn, data);

	if (err) {
		free(fsname);
		free(snapname);
		return err;
	}

	dsl_dir_phys_t *dp = (dsl_dir_phys_t *)DN_BONUS(&dn->dn);
	headobj = zfs_to_cpu64(dp->dd_head_dataset_obj, dn->endian);

	err = dnode_get(&data->mos, headobj, 0, &subvol->mdn, data);
	if (err) {
		free(fsname);
		free(snapname);
		return err;
	}

	if (snapname) {
		uint64_t snapobj;
		dsl_dataset_phys_t *dp = (dsl_dataset_phys_t *)DN_BONUS(&subvol->mdn.dn);
		snapobj = zfs_to_cpu64(dp->ds_snapnames_zapobj, subvol->mdn.endian);

		err = dnode_get(&data->mos, snapobj,
				DMU_OT_DSL_DS_SNAP_MAP, &subvol->mdn, data);
		if (!err)
			err = zap_lookup(&subvol->mdn, snapname, &headobj, data, 0);
		if (!err)
			err = dnode_get(&data->mos, headobj, DMU_OT_DSL_DATASET,
					&subvol->mdn, data);
		if (err) {
			free(fsname);
			free(snapname);
			return err;
		}
	}

	subvol->obj = headobj;

	make_mdn(&subvol->mdn, data);

	if (*isfs) {
		free(fsname);
		free(snapname);
		return ZFS_ERR_NONE;
	}
	err = dnode_get_path(subvol, filename, dn, data);
	free(fsname);
	free(snapname);
	return err;
}

/*
 * For a given XDR packed nvlist, verify the first 4 bytes and move on.
 *
 * An XDR packed nvlist is encoded as (comments from nvs_xdr_create) :
 *
 *		encoding method/host endian		(4 bytes)
 *		nvl_version						(4 bytes)
 *		nvl_nvflag						(4 bytes)
 *	encoded nvpairs:
 *		encoded size of the nvpair		(4 bytes)
 *		decoded size of the nvpair		(4 bytes)
 *		name string size				(4 bytes)
 *		name string data				(sizeof(NV_ALIGN4(string))
 *		data type						(4 bytes)
 *		# of elements in the nvpair		(4 bytes)
 *		data
 *		2 zero's for the last nvpair
 *		(end of the entire list)	(8 bytes)
 *
 */
static int
nvlist_find_value(const char *nvlist_in, const char *name,
		  int valtype, char **val,
		  size_t *size_out, size_t *nelm_out)
{
	size_t nvp_name_len, name_len = strlen(name);
	int type;
	const char *nvpair = NULL, *nvlist = nvlist_in;
	char *nvp_name;

	/* Verify if the 1st and 2nd byte in the nvlist are valid.
	 * NOTE: independently of what endianness header announces all
	 * subsequent values are big-endian.
	 */
	if (nvlist[0] != NV_ENCODE_XDR ||
	    (nvlist[1] != NV_LITTLE_ENDIAN &&
	     nvlist[1] != NV_BIG_ENDIAN)) {
		printf("zfs incorrect nvlist header\n");
		return ZFS_ERR_BAD_FS;
	}

	/*
	 * Loop thru the nvpair list
	 * The XDR representation of an integer is in big-endian byte order.
	 */
	while ((nvpair = nvlist_next_nvpair(nvlist, nvpair))) {
		nvpair_name(nvpair, &nvp_name, &nvp_name_len);
		type = nvpair_type(nvpair);
		if (type == valtype &&
		    (nvp_name_len == name_len ||
		    (nvp_name_len > name_len && nvp_name[name_len] == '\0')) &&
		    memcmp(nvp_name, name, name_len) == 0) {
			return nvpair_value(nvpair, val, size_out, nelm_out);
		}
	}
	return 0;
}

int
zfs_nvlist_lookup_uint64(const char *nvlist, const char *name, uint64_t *out)
{
	char *nvpair;
	size_t size;
	int found;

	found = nvlist_find_value(nvlist, name, DATA_TYPE_UINT64, &nvpair, &size, 0);
	if (!found)
		return 0;
	if (size < sizeof(uint64_t)) {
		printf("invalid uint64\n");
		return ZFS_ERR_BAD_FS;
	}

	*out = be64_to_cpu(*(uint64_t *)nvpair);
	return 1;
}

char *
zfs_nvlist_lookup_string(const char *nvlist, const char *name)
{
	char *nvpair;
	char *ret;
	size_t slen;
	size_t size;
	int found;

	found = nvlist_find_value(nvlist, name, DATA_TYPE_STRING, &nvpair,
				  &size, 0);
	if (!found)
		return 0;
	if (size < 4) {
		printf("invalid string\n");
		return 0;
	}
	slen = be32_to_cpu(*(uint32_t *)nvpair);
	if (slen > size - 4)
		slen = size - 4;
	ret = malloc(slen + 1);
	if (!ret)
		return 0;
	memcpy(ret, nvpair + 4, slen);
	ret[slen] = 0;
	return ret;
}

char *
zfs_nvlist_lookup_nvlist(const char *nvlist, const char *name)
{
	char *nvpair;
	char *ret;
	size_t size;
	int found;

	found = nvlist_find_value(nvlist, name, DATA_TYPE_NVLIST, &nvpair,
				  &size, 0);
	if (!found)
		return 0;
	ret = calloc(1, size + 3 * sizeof(uint32_t));
	if (!ret)
		return 0;
	memcpy(ret, nvlist, sizeof(uint32_t));

	memcpy(ret + sizeof(uint32_t), nvpair, size);
	return ret;
}

int
zfs_nvlist_lookup_nvlist_array_get_nelm(const char *nvlist, const char *name)
{
	char *nvpair;
	size_t nelm, size;
	int found;

	found = nvlist_find_value(nvlist, name, DATA_TYPE_NVLIST_ARRAY, &nvpair,
				  &size, &nelm);
	if (!found)
		return -1;
	return nelm;
}

static int
get_nvlist_size(const char *beg, const char *limit)
{
	const char *ptr;
	uint32_t encode_size;

	ptr = beg + 8;

	while (ptr < limit && (encode_size = be32_to_cpu(*(uint32_t *)(ptr))))
		ptr += encode_size;	/* goto the next nvpair */
	ptr += 8;
	return (ptr > limit) ? -1 : (ptr - beg);
}

char *
zfs_nvlist_lookup_nvlist_array(const char *nvlist, const char *name,
			       size_t index)
{
	char *nvpair, *nvpairptr;
	int found;
	char *ret;
	size_t size;
	unsigned int i;
	size_t nelm;
	int elemsize = 0;

	found = nvlist_find_value(nvlist, name, DATA_TYPE_NVLIST_ARRAY, &nvpair,
				  &size, &nelm);
	if (!found)
		return 0;
	if (index >= nelm) {
		printf("trying to lookup past nvlist array\n");
		return 0;
	}

	nvpairptr = nvpair;

	for (i = 0; i < index; i++) {
		int r;

		r = get_nvlist_size(nvpairptr, nvpair + size);

		if (r < 0) {
			printf("incorrect nvlist array\n");
			return NULL;
		}
		nvpairptr += r;
	}

	elemsize = get_nvlist_size(nvpairptr, nvpair + size);

	if (elemsize < 0) {
		printf("incorrect nvlist array\n");
		return NULL;
	}

	ret = calloc(1, elemsize + sizeof(uint32_t));
	if (!ret)
		return 0;
	memcpy(ret, nvlist, sizeof(uint32_t));

	memcpy(ret + sizeof(uint32_t), nvpairptr, elemsize);
	return ret;
}

static void
unmount_device(struct zfs_device_desc *desc)
{
	unsigned int i;

	switch (desc->type) {
	case DEVICE_LEAF:
		if (!desc->original && desc->dev.disk) {
			desc->dev.disk = NULL;
			/*device_close(desc->dev)*/;
		}
		return;
	case DEVICE_RAIDZ:
	case DEVICE_MIRROR:
		for (i = 0; i < desc->n_children; i++)
			unmount_device(&desc->children[i]);
		free(desc->children);
		return;
	}
}

void
zfs_unmount(struct zfs_data *data)
{
	unsigned int i;

	for (i = 0; i < data->n_devices_attached; i++)
		unmount_device(&data->devices_attached[i]);
	free(data->devices_attached);
	free(data->dnode_buf);
	free(data->dnode_mdn);
	free(data->file_buf);
	free(data);
}

/*
 * zfs_mount() locates a valid uberblock of the root pool and read in its MOS
 * to the memory address MOS.
 *
 */
struct zfs_data *
zfs_mount(zfs_device_t dev)
{
	struct zfs_data *data = 0;
	int err;
	void *osp = 0;
	size_t ospsize;
	zfs_endian_t ub_endian = UNKNOWN_ENDIAN;
	uberblock_t *ub;
	int inserted;

	if (!dev->disk) {
		printf("not a disk\n");
		return NULL;
	}

	data = calloc(1, sizeof(*data));
	if (!data)
		return NULL;
#if 0
	/* if it's our first time here, zero the best uberblock out */
	if (data->best_drive == 0 && data->best_part == 0 && find_best_root)
		memset(&current_uberblock, 0, sizeof(uberblock_t));
#endif

	data->n_devices_allocated = 16;
	data->devices_attached = malloc(sizeof(data->devices_attached[0])
					* data->n_devices_allocated);
	data->n_devices_attached = 0;
	err = scan_disk(dev, data, 1, &inserted);
	if (err) {
		zfs_unmount(data);
		return NULL;
	}

	ub = &data->current_uberblock;
	ub_endian = (zfs_to_cpu64(ub->ub_magic,
		     LITTLE_ENDIAN) == UBERBLOCK_MAGIC
		     ? LITTLE_ENDIAN : BIG_ENDIAN);

	debug("zfs pool is %s endian\n",
	      ub_endian == BIG_ENDIAN ? "BIG" : "LITTLE");

	err = zio_read(&ub->ub_rootbp, ub_endian,
		       &osp, &ospsize, data);
	if (err) {
		zfs_unmount(data);
		return NULL;
	}

	if (ospsize < OBJSET_PHYS_SIZE_V14) {
		printf("OSP too small\n");
		free(osp);
		zfs_unmount(data);
		return NULL;
	}

	if (ub->ub_version >= SPA_VERSION_FEATURES &&
	    check_mos_features(&((objset_phys_t *)osp)->os_meta_dnode,
			       ub_endian, data) != 0) {
		printf("Unsupported features in pool\n");
		free(osp);
		zfs_unmount(data);
		return NULL;
	}

	/* Got the MOS. Save it at the memory addr MOS. */
	memmove(&data->mos.dn, &((objset_phys_t *)osp)->os_meta_dnode,
		DNODE_SIZE);
	data->mos.endian = (zfs_to_cpu64(ub->ub_rootbp.blk_prop,
					 ub_endian) >> 63) & 1;
	free(osp);

	return data;
}

/*
 * zfs_open() locates a file in the rootpool by following the
 * MOS and places the dnode of the file in the memory address DNODE.
 */
int
zfs_open(struct zfs_file *file, const char *fsfilename)
{
	struct zfs_data *data;
	int err;
	int isfs;

	data = zfs_mount(file->device);
	if (!data)
		return ZFS_ERR_BAD_FS;

	err = dnode_get_fullpath(fsfilename, &data->subvol,
				 &data->dnode, &isfs, data);
	if (err) {
		zfs_unmount(data);
		return err;
	}

	if (isfs) {
		zfs_unmount(data);
		printf("Missing @ or / separator\n");
		return ZFS_ERR_FILE_NOT_FOUND;
	}

	/* We found the dnode for this file. Verify if it is a plain file. */
	if (data->dnode.dn.dn_type != DMU_OT_PLAIN_FILE_CONTENTS) {
		zfs_unmount(data);
		printf("not a regular file\n");
		return ZFS_ERR_BAD_FILE_TYPE;
	}

	/* get the file size and set the file position to 0 */

	/*
	 * For DMU_OT_SA we will need to locate the SIZE attribute
	 * attribute, which could be either in the bonus buffer
	 * or the "spill" block.
	 */
	if (data->dnode.dn.dn_bonustype == DMU_OT_SA) {
		void *sahdrp;
		int hdrsize;

		if (data->dnode.dn.dn_bonuslen != 0) {
			sahdrp = (sa_hdr_phys_t *)DN_BONUS(&data->dnode.dn);
		} else if (data->dnode.dn.dn_flags & DNODE_FLAG_SPILL_BLKPTR) {
			blkptr_t *bp = &data->dnode.dn.dn_spill;

			err = zio_read(bp, data->dnode.endian, &sahdrp, NULL, data);
			if (err)
				return err;
		} else {
			printf("filesystem is corrupt :(\n");
			return ZFS_ERR_BAD_FS;
		}

		hdrsize = SA_HDR_SIZE(((sa_hdr_phys_t *)sahdrp));
		file->size = zfs_to_cpu64 (*(uint64_t *)((char *)sahdrp + hdrsize + SA_SIZE_OFFSET), data->dnode.endian);

	} else if (data->dnode.dn.dn_bonustype == DMU_OT_ZNODE) {
		znode_phys_t *zp = (znode_phys_t *)DN_BONUS(&data->dnode.dn);

		file->size = zfs_to_cpu64(zp->zp_size, data->dnode.endian);
	} else {
		printf("bad bonus type\n");
		return ZFS_ERR_BAD_FS;
	}

	file->data = data;
	file->offset = 0;

	return ZFS_ERR_NONE;
}

ssize_t
zfs_read(zfs_file_t file, char *buf, size_t len)
{
	struct zfs_data *data = (struct zfs_data *)file->data;
	int blksz, movesize;
	size_t length;
	ssize_t red;
	int err;

	/*
	 * If offset is in memory, move it into the buffer provided and return.
	 */
	if (file->offset >= data->file_start &&
	    file->offset + len <= data->file_end) {
		memmove(buf, data->file_buf + file->offset - data->file_start,
			len);
		return len;
	}

	blksz = zfs_to_cpu16(data->dnode.dn.dn_datablkszsec,
			     data->dnode.endian) << SPA_MINBLOCKSHIFT;
	if (blksz == 0) {
		printf("0-sized block\n");
		return -1;
	}

	/*
	 * Entire Dnode is too big to fit into the space available.	 We
	 * will need to read it in chunks.	This could be optimized to
	 * read in as large a chunk as there is space available, but for
	 * now, this only reads in one data block at a time.
	 */
	length = len;
	red = 0;
	while (length) {
		void *t;
		/*
		 * Find requested blkid and the offset within that block.
		 */
		uint64_t blkid = file->offset + red;

		blkid = do_div(blkid, blksz);
		free(data->file_buf);
		data->file_buf = 0;

		err = dmu_read(&data->dnode, blkid, &t,
			       0, data);
		data->file_buf = t;
		if (err) {
			data->file_buf = NULL;
			data->file_start = 0;
			data->file_end = 0;
			return -1;
		}
		data->file_start = blkid * blksz;
		data->file_end = data->file_start + blksz;

		movesize = data->file_end - file->offset - red;
		if (movesize > length)
			movesize = length;

#ifdef CONFIG_SANDBOX
		char *bufx = map_sysmem((phys_addr_t)(uint64_t)buf, movesize);

		memmove(bufx, data->file_buf + file->offset + red
			- data->file_start, movesize);
		unmap_sysmem(bufx);
#else
		memmove(buf, data->file_buf + file->offset + red
			- data->file_start, movesize);
#endif
		buf += movesize;
		length -= movesize;
		red += movesize;
	}

	return len;
}

int
zfs_close(zfs_file_t file)
{
	zfs_unmount((struct zfs_data *)file->data);
	return ZFS_ERR_NONE;
}

int
zfs_getmdnobj(zfs_device_t dev, const char *fsfilename,
	      uint64_t *mdnobj)
{
	struct zfs_data *data;
	int err;
	int isfs;

	data = zfs_mount(dev);
	if (!data)
		return ZFS_ERR_BAD_FS;

	err = dnode_get_fullpath(fsfilename, &data->subvol,
				 &data->dnode, &isfs, data);
	*mdnobj = data->subvol.obj;
	zfs_unmount(data);
	return err;
}

static int
fill_fs_info(struct zfs_dirhook_info *info,
	     dnode_end_t mdn, struct zfs_data *data)
{
	int err;
	dnode_end_t dn;
	uint64_t objnum;
	uint64_t headobj;

	memset(info, 0, sizeof(*info));

	info->dir = 1;

	if (mdn.dn.dn_type == DMU_OT_DSL_DIR) {
		dsl_dir_phys_t *dp = (dsl_dir_phys_t *)DN_BONUS(&mdn.dn);
		headobj = zfs_to_cpu64(dp->dd_head_dataset_obj, mdn.endian);

		err = dnode_get(&data->mos, headobj, 0, &mdn, data);
		if (err) {
			printf("zfs failed here 1\n");
			return err;
		}
	}
	make_mdn(&mdn, data);
	err = dnode_get(&mdn, MASTER_NODE_OBJ, DMU_OT_MASTER_NODE,
			&dn, data);
	if (err) {
		printf("zfs failed here 2\n");
		return err;
	}

	err = zap_lookup(&dn, ZFS_ROOT_OBJ, &objnum, data, 0);
	if (err) {
		printf("zfs failed here 3\n");
		return err;
	}

	err = dnode_get(&mdn, objnum, 0, &dn, data);
	if (err) {
		printf("zfs failed here 4\n");
		return err;
	}

	if (dn.dn.dn_bonustype == DMU_OT_SA) {
		void *sahdrp;
		int hdrsize;

		if (dn.dn.dn_bonuslen != 0) {
			sahdrp = (sa_hdr_phys_t *)DN_BONUS(&dn.dn);

		} else if (dn.dn.dn_flags & DNODE_FLAG_SPILL_BLKPTR) {
			blkptr_t *bp = &dn.dn.dn_spill;

			err = zio_read(bp, dn.endian, &sahdrp, NULL, data);
			if (err)
				return err;

		} else {
			printf("filesystem is corrupt\n");
			return ZFS_ERR_BAD_FS;
		}

		hdrsize = SA_HDR_SIZE(((sa_hdr_phys_t *)sahdrp));
		info->mtimeset = 1;
		info->mtime = zfs_to_cpu64(*(uint64_t *)((char *)sahdrp + hdrsize + SA_MTIME_OFFSET), dn.endian);
	}

	if (dn.dn.dn_bonustype == DMU_OT_ZNODE) {
		info->mtimeset = 1;
		znode_phys_t *zp = (znode_phys_t *)DN_BONUS(&dn.dn);
		info->mtime = zfs_to_cpu64(zp->zp_mtime[0], dn.endian);
	}
	return 0;
}

static int iterate_zap(const char *name, uint64_t val, struct zfs_dir_ctx *ctx)
{
	int err;
	struct zfs_dirhook_info info;
	dnode_end_t dn;

	memset(&info, 0, sizeof(info));

	err = dnode_get(&ctx->data->subvol.mdn, val, 0, &dn, ctx->data);
	if (err) {
		printf("zfs: Failed to read dnode\n");
		return 0;
	}

	if (dn.dn.dn_bonustype == DMU_OT_SA) {
		void *sahdrp;
		int hdrsize;

		if (dn.dn.dn_bonuslen != 0) {
			sahdrp = (sa_hdr_phys_t *)DN_BONUS(&ctx->data->dnode.dn);

		} else if (dn.dn.dn_flags & DNODE_FLAG_SPILL_BLKPTR) {
			blkptr_t *bp = &dn.dn.dn_spill;

			err = zio_read(bp, dn.endian, &sahdrp, NULL, ctx->data);
			if (err) {
				printf("zfs: Failed to read data\n");
				return 0;
			}
		} else {
			printf("filesystem is corrupt\n");
			return 0;
		}

		hdrsize = SA_HDR_SIZE(((sa_hdr_phys_t *)sahdrp));
		info.mtimeset = 1;
		info.mtime = zfs_to_cpu64(*(uint64_t *)((char *)sahdrp + hdrsize + SA_MTIME_OFFSET), dn.endian);
		info.case_insensitive = ctx->data->subvol.case_insensitive;
	}

	if (dn.dn.dn_bonustype == DMU_OT_ZNODE) {
		info.mtimeset = 1;
		znode_phys_t *zp = (znode_phys_t *)DN_BONUS(&dn.dn);
		info.mtime = zfs_to_cpu64(zp->zp_mtime[0],
					  dn.endian);
	}
	info.dir = (dn.dn.dn_type == DMU_OT_DIRECTORY_CONTENTS);
	debug("zfs type=%d, name=%s\n",
	      (int)dn.dn.dn_type, (char *)name);
	if (dn.dn.dn_type != DMU_OT_NONE &&
	    DMU_OT_IS_VALID(dn.dn.dn_type) &&
	    name[0] != 0)
		return ctx->hook(name, &info, ctx->hook_data);
	else
		return 0;
}

static int
iterate_zap_fs(const char *name, uint64_t val, struct zfs_dir_ctx *ctx)
{
	int err;
	struct zfs_dirhook_info info;
	dnode_end_t mdn;

	err = dnode_get(&ctx->data->mos, val, 0, &mdn, ctx->data);
	if (err)
		return 0;
	if (mdn.dn.dn_type != DMU_OT_DSL_DIR)
		return 0;

	err = fill_fs_info(&info, mdn, ctx->data);
	if (err)
		return 0;

	return ctx->hook(name, &info, ctx->hook_data);
}

static int
iterate_zap_snap(const char *name, uint64_t val, struct zfs_dir_ctx *ctx)
{
	int err;
	struct zfs_dirhook_info info;
	char *name2;
	dnode_end_t mdn;

	err = dnode_get(&ctx->data->mos, val, 0, &mdn, ctx->data);
	if (err)
		return 0;

	if (mdn.dn.dn_type != DMU_OT_DSL_DATASET)
		return 0;

	err = fill_fs_info(&info, mdn, ctx->data);
	if (err)
		return 0;

	name2 = malloc(strlen(name) + 2);
	name2[0] = '@';
	memcpy(name2 + 1, name, strlen(name) + 1);
	err = ctx->hook(name2, &info, ctx->hook_data);
	free(name2);
	return err;
}

int zfs_ls(zfs_device_t device, const char *path,
	   int (*hook)(const char *, const struct zfs_dirhook_info *,
		       void *data),
	   void *hook_data)
{
	struct zfs_dir_ctx ctx = {
		.hook = hook,
		.hook_data = hook_data
	};
	struct zfs_data *data;
	int err;
	int isfs;

	data = zfs_mount(device);
	if (!data)
		return ZFS_ERR_BAD_FS;

	err = dnode_get_fullpath(path, &data->subvol, &data->dnode, &isfs, data);
	if (err) {
		zfs_unmount(data);
		return err;
	}
	ctx.data = data;

	if (isfs) {
		uint64_t childobj, headobj;
		uint64_t snapobj;
		dnode_end_t dn;
		struct zfs_dirhook_info info;

		err = fill_fs_info(&info, data->dnode, data);
		if (err) {
			zfs_unmount(data);
			return err;
		}

		if (hook("@", &info, hook_data)) {
			zfs_unmount(data);
			return ZFS_ERR_NONE;
		}

		dsl_dir_phys_t *dp = (dsl_dir_phys_t *)DN_BONUS(&data->dnode.dn);
		childobj = zfs_to_cpu64(dp->dd_child_dir_zapobj, data->dnode.endian);
		headobj = zfs_to_cpu64(dp->dd_head_dataset_obj, data->dnode.endian);
		err = dnode_get(&data->mos, childobj,
				DMU_OT_DSL_DIR_CHILD_MAP, &dn, data);
		if (err) {
			zfs_unmount(data);
			return err;
		}

		zap_iterate_u64(&dn, iterate_zap_fs, data, &ctx);

		err = dnode_get(&data->mos, headobj, DMU_OT_DSL_DATASET, &dn, data);
		if (err) {
			zfs_unmount(data);
			return err;
		}

		dsl_dataset_phys_t *ddp = (dsl_dataset_phys_t *)DN_BONUS(&dn.dn);
		snapobj = zfs_to_cpu64(ddp->ds_snapnames_zapobj, dn.endian);

		err = dnode_get(&data->mos, snapobj,
				DMU_OT_DSL_DS_SNAP_MAP, &dn, data);
		if (err) {
			zfs_unmount(data);
			return err;
		}

		zap_iterate_u64(&dn, iterate_zap_snap, data, &ctx);
	} else {
		if (data->dnode.dn.dn_type != DMU_OT_DIRECTORY_CONTENTS) {
			zfs_unmount(data);
			printf("not a directory\n");
			return ZFS_ERR_BAD_FILE_TYPE;
		}
		zap_iterate_u64(&data->dnode, iterate_zap, data, &ctx);
	}
	zfs_unmount(data);
	return ZFS_ERR_NONE;
}

static int
check_feature(const char *name, uint64_t val,
	      struct zfs_dir_ctx *ctx __attribute__((unused)))
{
	int i;

	if (val == 0)
		return 0;
	if (name[0] == 0)
		return 0;
	for (i = 0; spa_feature_names[i] != NULL; i++)
		if (strcmp(name, spa_feature_names[i]) == 0)
			return 0;
	return 1;
}

/*
 * Checks whether the MOS features that are active are supported by this
 * (U-Boot's) implementation of ZFS.
 *
 * Return:
 *	0: Success.
 *	errnum: Failure.
 */

static int
check_mos_features(dnode_phys_t *mosmdn_phys, zfs_endian_t endian,
		   struct zfs_data *data)
{
	uint64_t objnum;
	int errnum = 0;
	dnode_end_t dn, mosmdn;
	mzap_phys_t *mzp;
	zfs_endian_t endianzap;
	int size;

	memmove(&mosmdn.dn, mosmdn_phys, sizeof(dnode_phys_t));
	mosmdn.endian = endian;
	errnum = dnode_get(&mosmdn, DMU_POOL_DIRECTORY_OBJECT,
			   DMU_OT_OBJECT_DIRECTORY, &dn, data);
	if (errnum != 0)
		return errnum;

	/*
	 * Find the object number for 'features_for_read' and retrieve its
	 * corresponding dnode. Note that we don't check features_for_write
	 * because U-Boot is not opening the pool for write.
	 */
	errnum = zap_lookup(&dn, DMU_POOL_FEATURES_FOR_READ, &objnum, data, 0);
	if (errnum != 0)
		return errnum;

	errnum = dnode_get(&mosmdn, objnum, DMU_OTN_ZAP_METADATA, &dn, data);
	if (errnum != 0)
		return errnum;

	errnum = dmu_read(&dn, 0, (void **)&mzp, &endianzap, data);
	if (errnum != 0)
		return errnum;

	size = zfs_to_cpu16(dn.dn.dn_datablkszsec, dn.endian) << SPA_MINBLOCKSHIFT;
	return mzap_iterate(mzp, endianzap, size, check_feature, NULL);
}
