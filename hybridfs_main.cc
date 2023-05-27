#include <cstring>
#include <gflags/gflags.h>

#include "hybridfs.h"

DEFINE_bool(debug, true, "Debug mode");
DEFINE_string(mount_point, "", "Mount point");
DEFINE_string(ssd_path, "", "SSD path");
DEFINE_string(hdd_path, "", "HDD path");
DEFINE_int64(ssd_upper_limit, 512 * 1024 * 1024, "The upper limit of file size in ssd");
DEFINE_int64(hdd_lower_limit, 256 * 1024 * 1024, "The lower limit of file size in hdd");

static struct fuse_operations hybridfs_operations = {
  .getattr = HybridFS::hfs_getattr,
  .readlink = HybridFS::hfs_readlink,
  .mkdir = HybridFS::hfs_mkdir,
  .unlink = HybridFS::hfs_unlink,
  .rmdir = HybridFS::hfs_rmdir,
  .symlink = HybridFS::hfs_symlink,
  .rename = HybridFS::hfs_rename,
  .link = HybridFS::hfs_link,
  .chmod = HybridFS::hfs_chmod,
  .chown = HybridFS::hfs_chown,
  .truncate = HybridFS::hfs_truncate,
  .open = HybridFS::hfs_open,
  .read = HybridFS::hfs_read,
  .write = HybridFS::hfs_write,
  .flush = HybridFS::hfs_flush,
  .release = HybridFS::hfs_release,
  .fsync = HybridFS::hfs_fsync,
  .setxattr = HybridFS::hfs_setxattr,
  .getxattr = HybridFS::hfs_getxattr,
  .listxattr = HybridFS::hfs_listxattr,
  .removexattr = HybridFS::hfs_removexattr,
  .readdir = HybridFS::hfs_readdir,
  .init = HybridFS::hfs_init,
  .destroy = HybridFS::hfs_destroy,
  .access = HybridFS::hfs_access,
  .create = HybridFS::hfs_create,
  .utimens = HybridFS::hfs_utimens,
  .copy_file_range = HybridFS::hfs_copy_file_range,
  .lseek = HybridFS::hfs_lseek
};

int main(int argc, char *argv[]) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  struct hfs_meta* meta = new hfs_meta{
    FLAGS_mount_point,
    FLAGS_ssd_path,
    FLAGS_hdd_path,
    FLAGS_ssd_upper_limit,
    FLAGS_hdd_lower_limit,
    nullptr
  };

  char mount_point[256];
  memset(mount_point, 0, 256);
  memcpy(mount_point, FLAGS_mount_point.c_str(), 256);
  int fuse_state;
  if(FLAGS_debug) {
    char* r_argv[] = {"hybridfs", "-d", "-f", mount_point};
    fuse_state = fuse_main(4, r_argv, &hybridfs_operations, meta);
  } else {
    char* r_argv[] = {"hybridfs", "-f", mount_point};
    fuse_state = fuse_main(3, r_argv, &hybridfs_operations, meta);
  }
  return fuse_state;
}