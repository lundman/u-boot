/*
 *
 *	based on code of fs/reiserfs/dev.c by
 *
 *	(C) Copyright 2003 - 2004
 *	Sysgo AG, <www.elinos.com>, Pavel Bartusek <pba@sysgo.com>
 *
 * SPDX-License-Identifier:	GPL-2.0+
 */

#include <common.h>
#include <config.h>
#include <fs_internal.h>
#include <zfs_common.h>

void zfs_set_blk_dev(zfs_device_t dev, struct blk_desc *rbdd,
		     disk_partition_t *info)
{
	dev->disk = rbdd;
	memcpy(&dev->part_info, info, sizeof(dev->part_info));
}

/* err */
int zfs_devread(zfs_device_t dev, int sector, int byte_offset, int byte_len,
		void *buf)
{
	debug("%s: dev %d sector 0x%x len 0x%x (part start %lu)\n", __func__,
	      dev->disk->devnum, sector, byte_len, dev->part_info.start);
	return !(fs_devread(dev->disk, &dev->part_info,
			sector, byte_offset, byte_len, buf));
}
