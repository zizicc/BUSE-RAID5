 #define _GNU_SOURCE
 #define _LARGEFILE64_SOURCE
 
 #include <argp.h>
 #include <err.h>
 #include <stdio.h>
 #include <stdlib.h>
 #include <stdint.h>
 #include <stdbool.h>
 #include <string.h>
 #include <sys/types.h>
 #include <sys/stat.h>
 #include <fcntl.h>
 #include <unistd.h>
 
 #include "buse.h"
 
 #define MAX_DEVICES 16
 #define UNUSED(x) (void)(x)
 
 
 int dev_fd[MAX_DEVICES];         
 bool dev_missing[MAX_DEVICES];   
 int num_devices = 0;             
 int rebuild_dev = -1;            
 int block_size;                  
 uint64_t raid_device_size;       
 bool verbose = false;           
 
/* RAID5 Logical Mapping Explanation:
   Given total devices n = num_devices.
   Each stripe physically contains n blocks (one per disk), 
   with the parity block at position P = stripe % n.
   The RAID logical device exposes only data blocks, meaning each stripe has n–1 data blocks.
   For logical block number L (L = offset / block_size), compute:
      stripe = L / (n – 1)
      pos    = L % (n – 1)
   Data disk index D = (pos >= P) ? pos + 1 : pos
   Physical offset is stripe * block_size.
 */
 

 struct arguments {
     uint32_t block_size;
     char *raid_device;
     char *devices[MAX_DEVICES];
     int num_devices;
     int verbose;
 };
 
 static struct argp_option options[] = {
     {"verbose", 'v', 0, 0, "Produce verbose output", 0},
     {0},
 };
 
 static error_t parse_opt (int key, char *arg, struct argp_state *state) {
     struct arguments *arguments = state->input;
     switch (key) {
         case 'v':
             arguments->verbose = 1;
             break;
         case ARGP_KEY_ARG:
             if (state->arg_num == 0) {
                 arguments->block_size = strtoul(arg, NULL, 10);
             } else if (state->arg_num == 1) {
                 arguments->raid_device = arg;
             } else {
                 if (arguments->num_devices >= MAX_DEVICES) {
                     errx(EXIT_FAILURE, "At most %d devices", MAX_DEVICES);
                 }
                 arguments->devices[arguments->num_devices++] = arg;
             }
             break;
         case ARGP_KEY_END:
             if (state->arg_num < 3) {
                 // at least：BLOCKSIZE, RAIDDEVICE, DEVICE1（at least 3）
                 argp_usage(state);
             }
             break;
         default:
             return ARGP_ERR_UNKNOWN;
     }
     return 0;
 }
 
 static struct argp argp = {
     .options  = options,
     .parser   = parse_opt,
     .args_doc = "BLOCKSIZE RAIDDEVICE DEVICE1 [DEVICE2 ... DEVICE16]",
     .doc = "BUSE implementation of RAID5 with distributed parity. The logical device contains only data blocks, with each stripe having (n-1) data blocks,"
            "The parity block is rotated across the n disks."
 };
 
/* RAID5 Reconstruction Function:
   Given the number of stripes = raid_device_size / ((n–1) * block_size).
   For each stripe:
     - If the rebuild disk is the parity disk for the current stripe, 
       then new parity = XOR(all other data blocks).
     - If the rebuild disk is a data disk, 
       then new data = parity block XOR (XOR(all other data blocks)).
 */
 static int do_raid5_rebuild() {
     uint64_t num_stripes = raid_device_size / ((num_devices - 1) * block_size);
     unsigned char *temp = malloc(block_size);
     if (!temp) {
         perror("malloc");
         return -1;
     }
     for (uint64_t stripe = 0; stripe < num_stripes; stripe++) {
         uint64_t phys_offset = stripe * block_size;
         int parity_disk = stripe % num_devices;
         if (rebuild_dev == parity_disk) {
             unsigned char parity_block[block_size];
             memset(parity_block, 0, block_size);
             for (int i = 0; i < num_devices; i++) {
                 if (i == parity_disk) continue;
                 if (dev_missing[i]) {
                     fprintf(stderr, "Rebuild error: data device %d is missing, not be able to rebuild\n", i);
                     free(temp);
                     return -1;
                 }
                 if (pread(dev_fd[i], temp, block_size, phys_offset) != block_size) {
                     perror("rebuild read (data)");
                     free(temp);
                     return -1;
                 }
                 for (int j = 0; j < block_size; j++) {
                     parity_block[j] ^= temp[j];
                 }
             }
             if (pwrite(dev_fd[rebuild_dev], parity_block, block_size, phys_offset) != block_size) {
                 perror("rebuild write (parity)");
                 free(temp);
                 return -1;
             }
         } else {

             unsigned char data_block[block_size];
             memset(data_block, 0, block_size);

             if (!dev_missing[parity_disk]) {
                 if (pread(dev_fd[parity_disk], temp, block_size, phys_offset) != block_size) {
                     perror("rebuild read (parity)");
                     free(temp);
                     return -1;
                 }
                 memcpy(data_block, temp, block_size);
             } else {
                 memset(data_block, 0, block_size);
             }

             for (int i = 0; i < num_devices; i++) {
                 if (i == parity_disk || i == rebuild_dev) continue;
                 if (dev_missing[i]) {
                     fprintf(stderr, "Rebuild error: data device %d is missing, cannot rebuild\n", i);
                     free(temp);
                     return -1;
                 }
                 if (pread(dev_fd[i], temp, block_size, phys_offset) != block_size) {
                     perror("rebuild read (data)");
                     free(temp);
                     return -1;
                 }
                 for (int j = 0; j < block_size; j++) {
                     data_block[j] ^= temp[j];
                 }
             }
             if (pwrite(dev_fd[rebuild_dev], data_block, block_size, phys_offset) != block_size) {
                 perror("rebuild write (data)");
                 free(temp);
                 return -1;
             }
         }
     }
     free(temp);
     return 0;
 }
 
/* RAID5 Read Operation:
   For a given logical offset, first compute the logical block number.
   Then, determine the data disk index using:
       stripe = L / (n–1) and pos = L % (n–1),
   where:
       data_disk = (pos >= parity_disk) ? pos + 1 : pos, 
       with parity_disk = stripe % n.
   If the target data disk is available, read directly; 
   otherwise, reconstruct the data using the parity block and other data blocks.
 */
 static int xmp_read(void *buf, u_int32_t len, u_int64_t offset, void *userdata) {
     UNUSED(userdata);
     if (verbose)
         fprintf(stderr, "R - offset: %lu, len: %u\n", offset, len);
     unsigned char *buffer = buf;
     while (len > 0) {
         uint64_t logical_block = offset / block_size;
         uint64_t stripe = logical_block / (num_devices - 1);
         int pos = logical_block % (num_devices - 1);
         int parity_disk = stripe % num_devices;
         int data_disk = (pos >= parity_disk) ? pos + 1 : pos;
         uint64_t phys_offset = stripe * block_size;
         unsigned char temp_block[block_size];
         if (!dev_missing[data_disk]) {
             if (pread(dev_fd[data_disk], temp_block, block_size, phys_offset) != block_size) {
                 perror("pread data");
                 return -1;
             }
         } else {

             if (dev_missing[parity_disk]) {
                 fprintf(stderr, "ERROR: data device %d and parity check device %d are missing, cannot rebuild\n", data_disk, parity_disk);
                 return -1;
             }
             if (pread(dev_fd[parity_disk], temp_block, block_size, phys_offset) != block_size) {
                 perror("pread parity");
                 return -1;
             }
             for (int i = 0; i < num_devices; i++) {
                 if (i == parity_disk || i == data_disk)
                     continue;
                 unsigned char other_block[block_size];
                 if (dev_missing[i]) {
                     fprintf(stderr, "ERROR: multiple devices are missing, cannot rebuild\n");
                     return -1;
                 }
                 if (pread(dev_fd[i], other_block, block_size, phys_offset) != block_size) {
                     perror("pread other data");
                     return -1;
                 }
                 for (int j = 0; j < block_size; j++) {
                     temp_block[j] ^= other_block[j];
                 }
             }
         }
         memcpy(buffer, temp_block, block_size);
         offset += block_size;
         buffer += block_size;
         len -= block_size;
     }
     return 0;
 }
 
/* RAID5 Write Operation:
   Detect full-stripe writes: If the offset is aligned to a stripe boundary 
   and len >= (n–1) * block_size, it is considered a full-stripe write (data portion).
   In this case:
     - For stripe `stripe`, the parity disk index P = stripe % n.
     - The (n–1) logically contiguous data blocks are sequentially written to data disks 
       (mapping: if d >= P, then physical disk = d+1; otherwise, physical disk = d).
     - The parity block is computed as the XOR of all data blocks and written to disk P.

   Otherwise, perform a partial write:
     - For each updated data block, first read the existing parity block and old data.
     - Compute the new parity using: new_parity = old_parity XOR old_data XOR new_data.
     - Write the updated data block and the new parity block.
 */
 static int xmp_write(const void *buf, u_int32_t len, u_int64_t offset, void *userdata) {
     UNUSED(userdata);
     const unsigned char *buffer = buf;
     while (len > 0) {
         if ((offset % ((num_devices - 1) * block_size) == 0) &&
             (len >= (num_devices - 1) * block_size)) {
             uint64_t stripe = offset / ((num_devices - 1) * block_size);
             uint64_t phys_offset = stripe * block_size;
             int parity_disk = stripe % num_devices;
             unsigned char parity_block[block_size];
             memset(parity_block, 0, block_size);
             for (int d = 0; d < num_devices - 1; d++) {
                 int data_disk = (d >= parity_disk) ? d + 1 : d;
                 if (!dev_missing[data_disk]) {
                     if (pwrite(dev_fd[data_disk], buffer + d * block_size, block_size, phys_offset) != block_size) {
                         perror("pwrite full stripe data");
                         return -1;
                     }
                 }
                 for (int j = 0; j < block_size; j++) {
                     parity_block[j] ^= buffer[d * block_size + j];
                 }
             }
             if (!dev_missing[parity_disk]) {
                 if (pwrite(dev_fd[parity_disk], parity_block, block_size, phys_offset) != block_size) {
                     perror("pwrite full stripe parity");
                     return -1;
                 }
             } else {
                 fprintf(stderr, "ERROR: parity device  %d is missing, cannot fully striping write\n", parity_disk);
                 return -1;
             }
             offset += (num_devices - 1) * block_size;
             buffer += (num_devices - 1) * block_size;
             len -= (num_devices - 1) * block_size;
             continue;
         }
         
         uint64_t logical_block = offset / block_size;
         uint64_t stripe = logical_block / (num_devices - 1);
         int pos = logical_block % (num_devices - 1);
         int parity_disk = stripe % num_devices;
         int data_disk = (pos >= parity_disk) ? pos + 1 : pos;
         uint64_t phys_offset = stripe * block_size;
         
         unsigned char old_parity[block_size];
         if (!dev_missing[parity_disk]) {
             if (pread(dev_fd[parity_disk], old_parity, block_size, phys_offset) != block_size) {
                 perror("pread old parity");
                 return -1;
             }
         } else {
             memset(old_parity, 0, block_size);
         }
         
         unsigned char old_data[block_size];
         if (!dev_missing[data_disk]) {
             if (pread(dev_fd[data_disk], old_data, block_size, phys_offset) != block_size) {
                 perror("pread old data");
                 return -1;
             }
         } else {
             memset(old_data, 0, block_size);
         }
         
         unsigned char new_parity[block_size];
         const unsigned char *new_data = buffer;
         for (int j = 0; j < block_size; j++) {
             new_parity[j] = old_parity[j] ^ old_data[j] ^ new_data[j];
         }
         
         if (!dev_missing[data_disk]) {
             if (pwrite(dev_fd[data_disk], new_data, block_size, phys_offset) != block_size) {
                 perror("pwrite new data");
                 return -1;
             }
         }
         if (!dev_missing[parity_disk]) {
             if (pwrite(dev_fd[parity_disk], new_parity, block_size, phys_offset) != block_size) {
                 perror("pwrite new parity");
                 return -1;
             }
         } else {
             fprintf(stderr, "ERROR: parity device %d is missing, write update fails\n", parity_disk);
             return -1;
         }
         
         offset += block_size;
         buffer += block_size;
         len -= block_size;
     }
     return 0;
 }
 
 static int xmp_flush(void *userdata) {
     UNUSED(userdata);
     if (verbose)
         fprintf(stderr, "Received a flush request.\n");
     for (int i = 0; i < num_devices; i++) {
         if (!dev_missing[i] && dev_fd[i] != -1)
             fsync(dev_fd[i]);
     }
     return 0;
 }
 
 static void xmp_disc(void *userdata) {
     UNUSED(userdata);
     if (verbose)
         fprintf(stderr, "Received a disconnect request.\n");
 }
 

 int main(int argc, char *argv[]) {
     struct arguments arguments = { .num_devices = 0, .verbose = 0 };
     argp_parse(&argp, argc, argv, 0, 0, &arguments);
     
     verbose = arguments.verbose;
     block_size = arguments.block_size;
     num_devices = arguments.num_devices;
     if (num_devices < 3) {
         errx(EXIT_FAILURE, "RAID5 requires at least 3 devices");
     }
     
     uint64_t min_blocks = 0;
     for (int i = 0; i < num_devices; i++) {
         char *dev_path = arguments.devices[i];
         if (strcmp(dev_path, "MISSING") == 0) {
             dev_missing[i] = true;
             dev_fd[i] = -1;
             fprintf(stderr, "DEGRADED: device %d is missing!\n", i);
         } else {
             if (dev_path[0] == '+') { 
                 if (rebuild_dev != -1) {
                     fprintf(stderr, "ERROR: can only assign 1 +\n");
                     exit(1);
                 }
                 rebuild_dev = i;
                 dev_path++; 
             }
             dev_missing[i] = false;
             dev_fd[i] = open(dev_path, O_RDWR);
             if (dev_fd[i] < 0) {
                 perror(dev_path);
                 exit(1);
             }
             uint64_t size = lseek(dev_fd[i], 0, SEEK_END);
             fprintf(stderr, "Got device '%s', size %ld bytes.\n", dev_path, size);
             uint64_t blocks = size / block_size;
             if (min_blocks == 0 || blocks < min_blocks) {
                 min_blocks = blocks;
             }
         }
     }
     
     if (min_blocks == 0) {
         fprintf(stderr, "ERROR: no availavle devices, cannot rebuild RAID5\n");
         exit(1);
     }

     raid_device_size = (num_devices - 1) * min_blocks * block_size;
     
     if (rebuild_dev != -1) {
         if (dev_missing[rebuild_dev]) {
             fprintf(stderr, "ERROR: cannot rebuild missing devices, cannot specify missing and + at the same time\n");
             exit(1);
         }
         fprintf(stderr, "Doing RAID5 rebuild on device %d...\n", rebuild_dev);
         if (do_raid5_rebuild() != 0) {
             fprintf(stderr, "Rebuild failed, aborting.\n");
             exit(1);
         }
     }
     
     fprintf(stderr, "RAID device resulting size: %ld bytes.\n", raid_device_size);
     
     struct buse_operations bop = {
         .read  = xmp_read,
         .write = xmp_write,
         .disc  = xmp_disc,
         .flush = xmp_flush,
         .size  = raid_device_size
     };
     
     return buse_main(arguments.raid_device, &bop, NULL);
 }