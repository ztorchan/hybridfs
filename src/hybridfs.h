#ifndef _HYBRIDFS_HYBRIDFS_H
#define _HYBRIDFS_HYBRIDFS_H

#define FUSE_USE_VERSION 39

#include <cstdint>
#include <cstdlib>
#include <string>
#include <unordered_map>

#include <fuse3/fuse.h>

enum class FileArea{
  NOTFILE,
  SSD,
  HDD,
};

enum class FileType{
  REGULAR,
  DIRECTORY,
  SYMBOLLINK,
};

struct hfs_dentry {
  std::string d_name;
  FileType d_type;
  FileArea d_area;
  struct hfs_dentry* d_parent;
  std::unordered_map<std::string, struct hfs_dentry*>* d_childs;
};

struct hfs_meta {
  std::string fs_path;
  std::string ssd_path;
  std::string hdd_path;
  int64_t ssd_upper_limit;
  int64_t hdd_lower_limit;
  struct hfs_dentry* root_dentry;
};

class HybridFS {
public:
  static int hfs_getattr(const char *, struct stat *, struct fuse_file_info *fi);
  static int hfs_readlink(const char *, char *, size_t);
  static int hfs_mkdir(const char *, mode_t);
  static int hfs_unlink(const char *);
  static int hfs_rmdir(const char *);
  static int hfs_symlink(const char *, const char *);
  static int hfs_rename(const char *, const char *, unsigned int flags);
  static int hfs_link(const char *, const char *);
  static int hfs_chmod(const char *, mode_t, struct fuse_file_info *fi);
  static int hfs_chown(const char *, uid_t, gid_t, struct fuse_file_info *fi);
  static int hfs_truncate(const char *, off_t, struct fuse_file_info *fi);
  static int hfs_open(const char *, struct fuse_file_info *);
  static int hfs_read(const char *, char *, size_t, off_t, struct fuse_file_info *);
  static int hfs_write(const char *, const char *, size_t, off_t, struct fuse_file_info *);
  static int hfs_flush(const char *, struct fuse_file_info *);
  static int hfs_release(const char *, struct fuse_file_info *);
  static int hfs_fsync(const char *, int, struct fuse_file_info *);
  static int hfs_setxattr(const char *, const char *, const char *, size_t, int);
  static int hfs_getxattr(const char *, const char *, char *, size_t);
  static int hfs_listxattr(const char *, char *, size_t);
  static int hfs_removexattr(const char *, const char *);
  static int hfs_readdir(const char *, void *, fuse_fill_dir_t, off_t, struct fuse_file_info *, enum fuse_readdir_flags);
  static void *hfs_init(struct fuse_conn_info *conn, struct fuse_config *cfg);
  static void hfs_destroy(void *private_data);
  static int hfs_access(const char *, int);
  static int hfs_create(const char *, mode_t, struct fuse_file_info *);
  static int hfs_utimens(const char *, const struct timespec tv[2], struct fuse_file_info *fi);
  static ssize_t hfs_copy_file_range(const char *path_in, struct fuse_file_info *fi_in, off_t offset_in, const char *path_out, 
                          struct fuse_file_info *fi_out, off_t offset_out, size_t size, int flags);
  static off_t hfs_lseek(const char *, off_t off, int whence, struct fuse_file_info *);

public:

};

#define HFS_META ((struct hfs_meta*) fuse_get_context()->private_data)

#endif