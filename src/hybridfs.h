#ifndef _HYBRIDFS_HYBRIDFS_H
#define _HYBRIDFS_HYBRIDFS_H

#define FUSE_USE_VERSION 39

#include <cstdint>
#include <cstdlib>

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
  struct hfs_dentry* root_dentry;
};

class HybridFS {
public:
  static int hfs_getattr(const char *, struct stat *, struct fuse_file_info *fi);
  static int hfs_readlink(const char *, char *, size_t);
  static int hfs_mknod(const char *, mode_t, dev_t);
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
  static int hfs_statfs(const char *, struct statvfs *);
  static int hfs_flush(const char *, struct fuse_file_info *);
  static int hfs_release(const char *, struct fuse_file_info *);
  static int hfs_fsync(const char *, int, struct fuse_file_info *);
  static int hfs_setxattr(const char *, const char *, const char *, size_t, int);
  static int hfs_getxattr(const char *, const char *, char *, size_t);
  static int hfs_listxattr(const char *, char *, size_t);
  static int hfs_removexattr(const char *, const char *);
  static int hfs_opendir(const char *, struct fuse_file_info *);
  static int hfs_readdir(const char *, void *, fuse_fill_dir_t, off_t, struct fuse_file_info *, enum fuse_readdir_flags);
  static int hfs_releasedir(const char *, struct fuse_file_info *);
  static int hfs_fsyncdir(const char *, int, struct fuse_file_info *);
  static void *hfs_init(struct fuse_conn_info *conn, struct fuse_config *cfg);
  static void hfs_destroy(void *private_data);
  static int hfs_access(const char *, int);
  static int hfs_create(const char *, mode_t, struct fuse_file_info *);
  static int hfs_lock(const char *, struct fuse_file_info *, int cmd, struct flock *);
  static int hfs_utimens(const char *, const struct timespec tv[2], struct fuse_file_info *fi);
  static int hfs_bmap(const char *, size_t blocksize, uint64_t *idx);
  static int hfs_ioctl(const char *, unsigned int cmd, void *arg, struct fuse_file_info *, unsigned int flags, void *data);
  static int hfs_poll(const char *, struct fuse_file_info *, struct fuse_pollhandle *ph, unsigned *reventsp);
  static int hfs_write_buf(const char *, struct fuse_bufvec *buf, off_t off, struct fuse_file_info *);
  static int hfs_read_buf(const char *, struct fuse_bufvec **bufp, size_t size, off_t off, struct fuse_file_info *);
  static int hfs_flock(const char *, struct fuse_file_info *, int op);
  static int hfs_fallocate(const char *, int, off_t, off_t, struct fuse_file_info *);
  static ssize_t hfs_copy_file_range(const char *path_in, struct fuse_file_info *fi_in, off_t offset_in, const char *path_out, 
                          struct fuse_file_info *fi_out, off_t offset_out, size_t size, int flags);
  static off_t hfs_lseek(const char *, off_t off, int whence, struct fuse_file_info *);

public:

};

#define HFS_META ((struct hfs_meta*) fuse_get_context()->private_data)

#endif