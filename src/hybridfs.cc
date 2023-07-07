#include <unistd.h>
#include <sys/stat.h>
#include <sys/xattr.h>
#include <vector>
#include <filesystem>

#include <spdlog/spdlog.h>

#include "hybridfs.h"

void split_path(const char *path, std::vector<std::string>& d_names) {
  d_names.clear();
  uint32_t head = 0;
  if(path[head] == '/') {
    head++;
  }

  uint32_t len = 1;
  while(path[head] != 0) {
    while(path[head + len] != '/' && path[head + len] != 0) {
      len++;
    }
    d_names.emplace_back(path + head, len);
    
    head += len;
    if(path[head] == 0) {
      break;
    }
    while(path[head] == '/') {
      head++;
    }
  }
}

struct hfs_dentry* find_dentry(const char *path) {
  std::vector<std::string> dnames;
  split_path(path, dnames);
  struct hfs_dentry* target_dentry = HFS_META->root_dentry;
  for(size_t i = 0; i < dnames.size(); i++) {
    if(target_dentry->d_type != FileType::DIRECTORY) {
      return nullptr;
    }
    auto it = target_dentry->d_childs->find(dnames[i]);
    if(it == target_dentry->d_childs->end()) {
      return nullptr;
    }
    target_dentry = it->second;
  }
  return target_dentry;
}

struct hfs_dentry* find_parent_dentry(const char *path) {
  std::vector<std::string> dnames;
  split_path(path, dnames);
  struct hfs_dentry* target_dentry = HFS_META->root_dentry;
  for(size_t i = 0; i < dnames.size() - 1; i++) {
    if(target_dentry->d_type != FileType::DIRECTORY) {
      return nullptr;
    }
    auto it = target_dentry->d_childs->find(dnames[i]);
    if(it == target_dentry->d_childs->end()) {
      return nullptr;
    }
    target_dentry = it->second;
  }
  return target_dentry;
}

int HybridFS::hfs_getattr(const char *path, struct stat *st, struct fuse_file_info *fi) {
  spdlog::info("[getattr] path: {}", path);
  // stat
  struct hfs_dentry* target_dentry = find_dentry(path);
  if(target_dentry == nullptr) {
    spdlog::info("[getattr] failed to find target dentry");
    return -ENOENT;
  }
  std::string real_path;
  if(target_dentry->d_type == FileType::DIRECTORY) {
    real_path = HFS_META->ssd_path + path;
  } else {
    real_path = (target_dentry->d_area == FileArea::SSD ? HFS_META->ssd_path : HFS_META->hdd_path) + path;
  }
  spdlog::info("[getattr] stat from real path {}", real_path.c_str());
  if(stat(real_path.c_str(), st) != 0) {
    return -errno;
  }
  return 0;
}

int HybridFS::hfs_readlink(const char *path, char *buf, size_t len) {
  spdlog::info("[readlink] path: {}", path);
  struct hfs_dentry* target_dentry = find_dentry(path);
  if(target_dentry == nullptr) {
    // target dentry does not exist
    spdlog::info("[readlink] failed to find target dentry");
    return -ENOENT;
  }
  if(target_dentry->d_type != FileType::SYMBOLLINK) {
    // target dentry is not a symbol link
    spdlog::info("[getattr] not a symbollink");
    return -1;
  }
  std::string real_path = (target_dentry->d_area == FileArea::SSD ? HFS_META->ssd_path : HFS_META->hdd_path) + path;
  spdlog::info("[readlink] readlink from real path {}", real_path.c_str());
  if(readlink(real_path.c_str(), buf, len) != 0) {
    return -errno;
  }
  return 0;
}

int HybridFS::hfs_mkdir(const char *path, mode_t mode) {
  spdlog::info("[mkdir] path: {}, mode {}", path, mode);
  std::vector<std::string> dnames;
  split_path(path, dnames);
  struct hfs_dentry* parent_dentry = find_parent_dentry(path);
  if(parent_dentry == nullptr) {
    // parent dentry does not exist
    spdlog::info("[mkdir] failed to find parent dentry");
    return -ENOENT;
  }
  if(parent_dentry->d_type != FileType::DIRECTORY) {
    // target dentry is not a directory
    spdlog::info("[mkdir] parent is not a directory");
    return -ENOENT;
  }
  if(parent_dentry->d_childs->find(dnames[dnames.size() - 1]) != parent_dentry->d_childs->end()) {
    // target dentry exist
    spdlog::info("[mkdir] file exists");
    return -EEXIST;
  }
  // real mkdir 
  int mkdir_state;
  mkdir_state = mkdir((HFS_META->ssd_path + path).c_str(), mode);
  if(mkdir_state == 0) {
    mkdir_state = mkdir((HFS_META->hdd_path + path).c_str(), mode);
  } else {
    spdlog::info("[mkdir] real mkdir {} failed with return value {}", (HFS_META->ssd_path + path).c_str(), errno);
    return -errno;
  }
  if(mkdir_state == 0) {
    // create dentry
    parent_dentry->d_childs->insert(std::make_pair(dnames[dnames.size() - 1], new hfs_dentry{
      dnames[dnames.size() - 1], 
      FileType::DIRECTORY, 
      FileArea::NOTFILE, 
      parent_dentry,
      new std::unordered_map<std::string, struct hfs_dentry*>()
    }));
  } else {
    rmdir((HFS_META->ssd_path + path).c_str());
    return -errno;
  }
  return 0;
}

int HybridFS::hfs_unlink(const char *path) {
  spdlog::info("[unlink] path: {}", path);
  struct hfs_dentry* target_dentry = find_dentry(path);
  if(target_dentry == nullptr) {
    // can not find target dentry
    spdlog::info("[unlink] failed to find target dentry");
    return -ENOENT;
  }
  if(target_dentry->d_type != FileType::REGULAR && target_dentry->d_type != FileType::SYMBOLLINK) {
    // target dentry is not a regular file
    spdlog::info("[unlink] not a regular file");
    return -EISDIR;
  }
  std::string real_path = (target_dentry->d_area == FileArea::SSD ? HFS_META->ssd_path : HFS_META->hdd_path) + path;
  spdlog::info("[unlink] unlink real path: {}", real_path.c_str());
  if(unlink(real_path.c_str()) == 0) {
    // delete target dentry
    target_dentry->d_parent->d_childs->erase(target_dentry->d_name);
    delete target_dentry;
    return 0;
  }
  return -errno;
}

int HybridFS::hfs_rmdir(const char *path) {
  spdlog::info("[rmdir] path: {}", path);
  struct hfs_dentry* target_dentry = find_dentry(path);
  if(target_dentry == nullptr) {
    // can not find target dentry
    spdlog::info("[rmdir] failed to find target dentry");
    return -ENOENT;
  }
  if(target_dentry->d_type != FileType::DIRECTORY) {
    // target dentry is not a directory
    spdlog::info("[rmdir] not a directory");
    return -ENOTDIR;
  }
  if(!target_dentry->d_childs->empty()) {
    // target directory is not empty
    spdlog::info("[rmdir] not a directory");
    return -ENOTEMPTY;
  }
  // stat for recovery
  struct stat st;
  stat((HFS_META->ssd_path + path).c_str(), &st);
  // real remove
  int rmdir_state;
  spdlog::info("[rmdir] remove real path: {}", (HFS_META->ssd_path + path).c_str());
  rmdir_state = rmdir((HFS_META->ssd_path + path).c_str());
  if(rmdir_state == 0) {
    spdlog::info("[rmdir] remove real path: {}", (HFS_META->hdd_path + path).c_str());
    rmdir_state = rmdir((HFS_META->hdd_path + path).c_str());
  } else {
    return -errno;
  }
  if(rmdir_state == 0) {
    // delete target dentry
    spdlog::info("[rmdir] delete dentry");
    target_dentry->d_parent->d_childs->erase(target_dentry->d_name);
    delete target_dentry->d_childs;
    delete target_dentry;
  } else {
    spdlog::info("[rmdir] failed to remove real path, start recovery");
    mkdir((HFS_META->ssd_path + path).c_str(), st.st_mode);
    return -errno;
  }
  return 0;
}

int HybridFS::hfs_symlink(const char *oldpath, const char *newpath) {
  spdlog::info("[symlink] oldpath: {}, newpath: {}", oldpath, newpath);
  std::vector<std::string> dnames;
  split_path(newpath, dnames);
  struct hfs_dentry* parent_dentry = find_parent_dentry(newpath);
  if(parent_dentry == nullptr) {
    // parent dentry does not exist
    spdlog::info("[symlink] failed to find parent dentry");
    return -ENOENT;
  }
  if(parent_dentry->d_type != FileType::DIRECTORY) {
    // parent dentry is not a directory
    spdlog::info("[symlink] parent is not a directory");
    return -ENOENT;
  }
  if(parent_dentry->d_childs->find(dnames[dnames.size() - 1]) != parent_dentry->d_childs->end()) {
    // target dentry exist
    spdlog::info("[symlink] target dentry exists");
    return -EEXIST;
  }
  std::string real_old_path = oldpath;
  std::string real_new_path = HFS_META->ssd_path + newpath;
  spdlog::info("[symlink] real symlink from path {} to path {}", real_new_path.c_str(), real_old_path.c_str());
  if(symlink(real_old_path.c_str(), real_new_path.c_str()) == 0) {
    parent_dentry->d_childs->insert(std::make_pair(dnames[dnames.size() - 1], new hfs_dentry {
      dnames[dnames.size() - 1], 
      FileType::SYMBOLLINK, 
      FileArea::SSD, 
      parent_dentry,
      nullptr
    }));
  } else {
    return -errno;
  }
  return 0;
}

int HybridFS::hfs_rename(const char *oldpath, const char *newpath, unsigned int flags) {
  spdlog::info("[rename] oldpath: {}, newpath: {}", oldpath, newpath);

  if(flags == RENAME_EXCHANGE || flags == RENAME_WHITEOUT) {
    // do not support
    spdlog::info("[rename] not support for RENAME_EXCHANGE and RENAME_WHITEOUT");
    return -EPERM;
  }

  // find and check old file
  struct hfs_dentry* old_dentry = find_dentry(oldpath);
  if(old_dentry == nullptr) {
    // can not find target old dentry
    spdlog::info("[rename] failed to find old target dentry");
    return -ENOENT;
  }
  if(old_dentry->d_type != FileType::REGULAR && old_dentry->d_type != FileType::SYMBOLLINK) {
    // target old dentry is not file
    spdlog::info("[rename] old target dentry is not a file");
    return -1;
  }
  std::string real_old_path = (old_dentry->d_area == FileArea::SSD ? HFS_META->ssd_path : HFS_META->hdd_path) + oldpath;
  std::string real_new_path = (old_dentry->d_area == FileArea::SSD ? HFS_META->ssd_path : HFS_META->hdd_path) + newpath;

  // find new dentry parent
  std::vector<std::string> dnames;
  split_path(newpath, dnames);
  std::string new_dentry_name = dnames[dnames.size() - 1];
  struct hfs_dentry* new_dentry_parent = find_parent_dentry(newpath);
  if(new_dentry_parent == nullptr) {
    // can not find parent dentry
    spdlog::info("[rename] failed to find new parent dentry");
    return -ENOENT;
  }
  if(new_dentry_parent->d_type != FileType::DIRECTORY) {
    // parent dentry is not directory
    spdlog::info("[rename] new parent dentry is not a directory");
    return -ENOENT;
  }

  // for different flags
  if(flags == RENAME_NOREPLACE) {
    // check new path does not exist
    auto it = new_dentry_parent->d_childs->find(new_dentry_name);
    if(it != new_dentry_parent->d_childs->end()) {
      // same path exist
      spdlog::info("[rename] new dentry exists");
      return -EEXIST;
    }
    spdlog::info("[rename] real rename from {} to {}", real_old_path.c_str(), real_new_path.c_str());
    if(rename(real_old_path.c_str(), real_new_path.c_str()) == 0) {
      // rename successful
      old_dentry->d_parent->d_childs->erase(old_dentry->d_name);
      old_dentry->d_name = new_dentry_name;
      old_dentry->d_parent = new_dentry_parent;
      new_dentry_parent->d_childs->insert(std::make_pair(old_dentry->d_name, old_dentry));
    } else {
      return -errno;
    }
  }
  return 0;
}

int HybridFS::hfs_link(const char *oldpath, const char *newpath) {
  spdlog::info("[link] oldpath: {}, newpath: {}", oldpath, newpath);
  struct hfs_dentry* old_dentry = find_dentry(oldpath);
  if(old_dentry == nullptr) {
    // old dentry does not exist
    spdlog::info("[link] failed to find old target dentry");
    return -ENOENT;
  }
  if(old_dentry->d_type == FileType::DIRECTORY) {
    // old dentry is a directory
    spdlog::info("[link] old target dentry is a directory");
    return -EISDIR;
  }
  std::vector<std::string> dnames;
  split_path(newpath, dnames);
  std::string new_dentry_name = dnames[dnames.size() - 1];
  struct hfs_dentry* new_dentry_parent = find_parent_dentry(newpath);
  if(new_dentry_parent == nullptr) {
    // can not find parent
    spdlog::info("[link] failed to find new parent dentry");
    return -ENOENT;
  }
  if(new_dentry_parent->d_type != FileType::DIRECTORY) {
    // parent dentry is not directory
    spdlog::info("[link] new parent dentry is not a directory");
    return -ENOENT;
  }
  if(new_dentry_parent->d_childs->find(new_dentry_name) != new_dentry_parent->d_childs->end()) {
    // new dentry exists
    spdlog::info("[link] new parent dentry exists");
    return -EEXIST;
  }
  // real link
  std::string real_old_path = (old_dentry->d_area == FileArea::SSD ? HFS_META->ssd_path : HFS_META->hdd_path) + oldpath;
  std::string real_new_path = (old_dentry->d_area == FileArea::SSD ? HFS_META->ssd_path : HFS_META->hdd_path) + newpath;
  spdlog::info("[link] real link from {} to {}", real_old_path.c_str(), real_new_path.c_str());
  if(link(real_old_path.c_str(), real_new_path.c_str()) == 0) {
    new_dentry_parent->d_childs->insert(std::make_pair(new_dentry_name, new hfs_dentry{
      new_dentry_name,
      old_dentry->d_type,
      old_dentry->d_area,
      new_dentry_parent,
      nullptr
    }));
  } else {
    return -errno;
  }
  return 0;
}

int HybridFS::hfs_chmod(const char *path, mode_t mode, struct fuse_file_info *fi){
  spdlog::info("[chmod] path: {}, mode: {:#o}", path, mode);
  struct hfs_dentry* target_dentry = find_dentry(path);
  if(target_dentry == nullptr) {
    // can not find such file
    spdlog::info("[chmod] failed to find target dentry");
    return -ENOENT;
  }
  // real chmod
  std::string real_path = (target_dentry->d_area == FileArea::SSD ? HFS_META->ssd_path : HFS_META->hdd_path) + path;
  spdlog::info("[chmod] chmod real path: {}", real_path.c_str());
  if(chmod(real_path.c_str(), mode) != 0) {
    return -errno;
  }
  return 0;
}

int HybridFS::hfs_chown(const char *path, uid_t uid, gid_t gid, struct fuse_file_info *fi) {
  spdlog::info("[chown] path: {}, uid: {}, gid: {}", path, uid, gid);
  struct hfs_dentry* target_dentry = find_dentry(path);
  if(target_dentry == nullptr) {
    // can not find such dentry
    spdlog::info("[chown] failed to find target dentry");
    return -ENOENT;
  }
  std::string real_path;
  if(target_dentry->d_type == FileType::DIRECTORY || target_dentry->d_type == FileType::SYMBOLLINK) {
    real_path = HFS_META->ssd_path + path;
  } else if(target_dentry->d_type == FileType::REGULAR) {
    real_path = (target_dentry->d_area == FileArea::SSD ? HFS_META->ssd_path : HFS_META->hdd_path) + path;
  }
  spdlog::info("[chown] chown real path: {}", real_path.c_str());
  if(chown(real_path.c_str(), uid, gid) != 0) {
    return -errno;
  }
  return 0;
}

int HybridFS::hfs_truncate(const char *path, off_t off, struct fuse_file_info *fi) {
  spdlog::info("[truncate] path: {}, offset: {}", path, off);
  struct hfs_dentry* target_dentry = find_dentry(path);
  if(target_dentry == nullptr) {
    // can not find such file
    spdlog::info("[truncate] failed to find target dentry");
    return -ENOENT;
  }
  if(target_dentry->d_type == FileType::DIRECTORY) {
    // dentry is not file
    spdlog::info("[truncate] target dentry is a directory");
    return -EISDIR;
  }
  std::string real_path = (target_dentry->d_area == FileArea::SSD ? HFS_META->ssd_path : HFS_META->hdd_path) + path;
  spdlog::info("[truncate] truncate real path: {}", real_path.c_str());
  if(truncate(real_path.c_str(), off) != 0) {
    return -errno;
  }
  return 0;
}

int HybridFS::hfs_open(const char *path, struct fuse_file_info *fi) {
  spdlog::info("[open] path: {}, flags: {:#o}", path, fi->flags);
  struct hfs_dentry* target_dentry = find_dentry(path);
  if(target_dentry == nullptr) {
    if((fi->flags & O_CREAT) == 0) {
      // do not create
      spdlog::info("Not such dentry and don't create");
      return -ENOENT;
    }
    // check parent
    struct hfs_dentry* parent_dentry = find_parent_dentry(path);
    if(parent_dentry == nullptr) {
      // parent does not exist
      spdlog::info("parent dentry doesn't exist");
      return -ENOENT;
    }
    // create
    std::string real_path = HFS_META->ssd_path + path;
    spdlog::info("[open] open file from real path {}", real_path.c_str());
    int open_state = open(real_path.c_str(), fi->flags);
    if(open_state != -1){
      fi->fh = open_state;
      std::vector<std::string> dnames;
      split_path(path, dnames);
      std::string new_dentry_name = dnames[dnames.size() - 1];
      parent_dentry->d_childs->insert(std::make_pair(new_dentry_name, new hfs_dentry{
        new_dentry_name,
        FileType::REGULAR,
        FileArea::SSD,
        parent_dentry,
        nullptr
      }));
    } else {
      return -errno;
    }
  } else {
    // file exists
    if((fi->flags & O_EXCL) != 0 && (fi->flags & O_CREAT) != 0) {
      // fail if exist
      spdlog::info("file exist");
      return -EEXIST ;
    }
    std::string real_path;
    if(target_dentry->d_type == FileType::DIRECTORY) {
      real_path = HFS_META->ssd_path + path;
    } else {
      real_path = ((target_dentry->d_area == FileArea::SSD) ? HFS_META->ssd_path : HFS_META->hdd_path) + path;
    }
    spdlog::info("[open] open real path {}", real_path.c_str());
    int open_state = open(real_path.c_str(), fi->flags);
    if(open_state != -1){
      fi->fh = open_state;
    } else {
      return -errno;
    }
  }
  return 0;
}

int HybridFS::hfs_read(const char *path, char *buf, size_t size, off_t off, struct fuse_file_info *fi) {
  spdlog::info("[read] path: {}, offset: {}, size: {}", path, off, size);
  // check file
  struct hfs_dentry* target_dentry = find_dentry(path);
  if(target_dentry == nullptr) {
    // no such file
    spdlog::info("[read] failed to find target dentry");
    return -ENOENT;
  }
  if(target_dentry->d_type == FileType::DIRECTORY){
    // path is a directory
    spdlog::info("[read] target dentry is a directory");
    return -EISDIR;
  }
  // get file fd
  int fd = -1;
  if(fi != nullptr) {
    fd = fi->fh;
  } else {
    std::string real_path = (target_dentry->d_area == FileArea::SSD ? HFS_META->ssd_path : HFS_META->hdd_path) + path;
    spdlog::info("[read] open real path: {}", real_path.c_str());
    fd = open(real_path.c_str(), O_RDONLY);
  }
  if(fd == -1) {
    spdlog::info("[read] failed to open");
    return -errno;
  }
  // read
  spdlog::info("[read] lseek file");
  if(lseek(fd, off, SEEK_SET) == -1) {
    spdlog::info("[read] failed to seek");
    return -errno;
  }
  spdlog::info("[read] real read");
  int read_size = read(fd, buf, size);
  if(read_size == -1) {
    return -errno;
  }
  if(fi == nullptr) {
    spdlog::info("[read] close file");
    close(fd);
  }
  return read_size;
}

int HybridFS::hfs_write(const char *path, const char *buf, size_t size, off_t off, struct fuse_file_info *fi) {
  spdlog::info("[write] path: {}, offset: {}, size: {}", path, off, size);
  // check file
  struct hfs_dentry* target_dentry = find_dentry(path);
  if(target_dentry == nullptr) {
    // no such file
    spdlog::info("[write] failed to find target dentry");
    return -ENOENT;
  }
  if(target_dentry->d_type == FileType::DIRECTORY){
    // path is a directory
    spdlog::info("[write] target dentry is a directory");
    return -EISDIR;
  }
  // get file fd
  int fd = -1;
  std::string real_path = (target_dentry->d_area == FileArea::SSD ? HFS_META->ssd_path : HFS_META->hdd_path) + path;
  if(fi != nullptr) {
    fd = fi->fh;
  } else {
    spdlog::info("[write] open real path: {}", real_path.c_str());
    fd = open(real_path.c_str(), O_WRONLY);
  }
  if(fd == -1) {
    spdlog::info("[write] failed to open");
    return -errno;
  }
  // write
  spdlog::info("[write] lseek file");
  if(lseek(fd, off, SEEK_SET) == -1) {
    spdlog::info("[write] failed to seek");
    return -errno;
  }
  spdlog::info("[read] real write");
  int write_size = write(fd, buf, size);
  if(write_size == -1) {
    return -errno;
  }
  if(fi == nullptr) {
    spdlog::info("[write] close file");
    close(fd);
  }
  // maybe migrate
  struct stat st;
  stat(real_path.c_str(), &st);
  if(target_dentry->d_area == FileArea::SSD && st.st_size >= HFS_META->ssd_upper_limit) {
    // move file to hdd
    char cmd[1024];
    sprintf(cmd, "mv %s %s", (HFS_META->ssd_path + path).c_str(), (HFS_META->hdd_path + path).c_str());
    spdlog::info("[write] migrate {} to {}", (HFS_META->ssd_path + path).c_str(), (HFS_META->hdd_path + path).c_str());
    system(cmd);
    target_dentry->d_area = FileArea::HDD;
  } else if(target_dentry->d_area == FileArea::HDD && st.st_size <= HFS_META->hdd_lower_limit){
    // move file to ssd
    char cmd[1024];
    sprintf(cmd, "mv %s %s", (HFS_META->hdd_path + path).c_str(), (HFS_META->ssd_path + path).c_str());
    spdlog::info("[write] migrate {} to {}", (HFS_META->hdd_path + path).c_str(), (HFS_META->ssd_path + path).c_str());
    system(cmd);
    target_dentry->d_area = FileArea::SSD;
  }
  return write_size;
}

int HybridFS::hfs_flush(const char *path, struct fuse_file_info *fi) {
  spdlog::info("[flush] path: {}", path);
  return 0;
}

int HybridFS::hfs_release(const char *path, struct fuse_file_info *fi) {
  spdlog::info("[release] path: {}", path);
  if(fi != nullptr) {
    spdlog::info("[release] close file handle {}", fi->fh);
    if(close(fi->fh) == -1) {
      return -errno;
    }
  }
  return 0;
}

int HybridFS::hfs_fsync(const char *path, int datasync, struct fuse_file_info *fi) {
  spdlog::info("[fsync] path: {}, datasync: {}", path, datasync);
  if(fi != nullptr) {
    if(datasync) {
      spdlog::info("[fsync] datasync file handle {}", fi->fh);
      if(fdatasync(fi->fh) == -1) {
        return -errno;
      }
    } else {
      spdlog::info("[fsync] fsync file handle {}", fi->fh);
      if(fsync(fi->fh) == -1) {
        return -errno;
      }
    }
  }
  return 0;
}

int HybridFS::hfs_setxattr(const char *path, const char *name, const char *value, size_t size, int flags) {
  spdlog::info("[setxattr] path: {}, name: {}, value: {}", path, name, value);
  struct hfs_dentry* target_dentry = find_dentry(path);
  if(target_dentry == nullptr) {
    // no such file
    spdlog::info("[setxattr] failed to find target dentry");
    return -ENOENT;
  }
  std::string real_path;
  if(target_dentry->d_type == FileType::DIRECTORY) {
    real_path = HFS_META->ssd_path + path;
  } else {
    real_path = (target_dentry->d_area == FileArea::SSD ? HFS_META->ssd_path : HFS_META->hdd_path) + path;
  }
  spdlog::info("[setxattr] setxattr real path: {}", real_path.c_str());
  if(setxattr(real_path.c_str(), name, value, size, flags) == -1) {
    return -errno;
  }
  return 0;
}

int HybridFS::hfs_getxattr(const char *path, const char *name, char *value, size_t size) {
  spdlog::info("[getxattr] path: {}, name: {} ", path, name);
  struct hfs_dentry* target_dentry = find_dentry(path);
  if(target_dentry == nullptr) {
    // no such file
    spdlog::info("[getattr] failed to find target dentry");
    return -ENOENT;
  }
  std::string real_path;
  if(target_dentry->d_type == FileType::DIRECTORY) {
    real_path = HFS_META->ssd_path + path;
  } else {
    real_path = (target_dentry->d_area == FileArea::SSD ? HFS_META->ssd_path : HFS_META->hdd_path) + path;
  }
  spdlog::info("[getxattr] getxattr real path: {}", real_path.c_str());
  if(getxattr(real_path.c_str(), name, value, size) != 0) {
    return -errno;
  }
  return 0;
}

int HybridFS::hfs_listxattr(const char *path, char *list, size_t size) {
  spdlog::info("[listxattr] path: {}", path);
  struct hfs_dentry* target_dentry = find_dentry(path);
  if(target_dentry == nullptr) {
    // no such file
    spdlog::info("[listxattr] failed to find target dentry");
    return -ENOENT;
  }
  std::string real_path;
  if(target_dentry->d_type == FileType::DIRECTORY) {
    real_path = HFS_META->ssd_path + path;
  } else {
    real_path = (target_dentry->d_area == FileArea::SSD ? HFS_META->ssd_path : HFS_META->hdd_path) + path;
  }
  spdlog::info("[listxattr] listxattr real path: {}", real_path.c_str());
  if(listxattr(real_path.c_str(), list, size) == -1) {
    return -errno;
  }
  return 0;
}

int HybridFS::hfs_removexattr(const char *path, const char *name) {
  spdlog::info("[removexattr] path: {}, name: {}", path, name);
  struct hfs_dentry* target_dentry = find_dentry(path);
  if(target_dentry == nullptr) {
    // no such file
    spdlog::info("[removexattr] failed to find target dentry");
    return -ENOENT;
  }
  std::string real_path;
  if(target_dentry->d_type == FileType::DIRECTORY) {
    real_path = HFS_META->ssd_path + path;
  } else {
    real_path = (target_dentry->d_area == FileArea::SSD ? HFS_META->ssd_path : HFS_META->hdd_path) + path;
  }
  spdlog::info("[removexattr] removexattr real path: {}", real_path.c_str());
  if(removexattr(real_path.c_str(), name) == -1) {
    return -errno;
  }
  return 0;
}

int HybridFS::hfs_readdir(const char *path, void *buf, fuse_fill_dir_t filler, off_t off, struct fuse_file_info *fi, enum fuse_readdir_flags flags) {
  spdlog::info("[readdir] path: {}", path);
  struct hfs_dentry* target_dentry = find_dentry(path);
  if(target_dentry == nullptr) {
    // no such dentry
    spdlog::info("[readdir] failed to find target dentry");
    return -ENOENT;
  }
  if(target_dentry->d_type != FileType::DIRECTORY) {
    // not a directory
    spdlog::info("[readdir] target dentry is not a directory");
    return -ENOTDIR;
  }
  filler(buf, ".", NULL, 0, FUSE_FILL_DIR_PLUS);
	filler(buf, "..", NULL, 0, FUSE_FILL_DIR_PLUS);
  for(auto it = target_dentry->d_childs->begin(); it != target_dentry->d_childs->end(); it++) {
    struct hfs_dentry* child = it->second;
    struct stat st;
    std::string real_path;
    if(child->d_type == FileType::DIRECTORY) {
      real_path = HFS_META->ssd_path + path + "/" + child->d_name;
    } else {
      real_path = (child->d_area == FileArea::SSD ? HFS_META->ssd_path : HFS_META->hdd_path) + path + "/" + child->d_name;
    }
    spdlog::info("[readdir] stat real path {}", real_path.c_str());
    if(stat(real_path.c_str(), &st) == 0) {
      filler(buf, child->d_name.c_str(), &st, 0, FUSE_FILL_DIR_PLUS);
    }
  }
  return 0;
}

void *HybridFS::hfs_init(struct fuse_conn_info *conn, struct fuse_config *cfg) {
  spdlog::info("[init] initial data path");
  if(HFS_META->ssd_path.back() == '/') {
    HFS_META->ssd_path.pop_back();
  }
  if(HFS_META->hdd_path.back() == '/') {
    HFS_META->hdd_path.pop_back();
  }
  std::filesystem::remove_all(HFS_META->ssd_path);
  std::filesystem::remove_all(HFS_META->hdd_path);
  std::filesystem::create_directories(HFS_META->ssd_path);
  std::filesystem::create_directories(HFS_META->hdd_path);
  spdlog::info("[init] initial dentry");
  HFS_META->root_dentry = new hfs_dentry {
    "",
    FileType::DIRECTORY,
    FileArea::NOTFILE,
    nullptr,
    new std::unordered_map<std::string, struct hfs_dentry*>()
  };
  return HFS_META;
}

void destroy_dfs(struct hfs_dentry* root) {
  if(root == nullptr) {
    return ;
  }
  for(auto it = root->d_childs->begin(); it != root->d_childs->end(); it++) {
    destroy_dfs(it->second);
    it->second = nullptr;
  }
  delete root->d_childs;
  delete root;
  return ;
}

void HybridFS::hfs_destroy(void *private_data) {
  spdlog::info("[destory]");
  destroy_dfs(HFS_META->root_dentry);
}

int HybridFS::hfs_access(const char *path, int mode) {
  spdlog::info("[access] path: {}, mode: {:#o}", path, mode);
  struct hfs_dentry* target_dentry = find_dentry(path);
  if(target_dentry == nullptr) {
    // no such dentry
    spdlog::info("[access] failed to find target dentry");
    return -ENOENT;
  }
  std::string real_path;
  if(target_dentry->d_type == FileType::DIRECTORY) {
    real_path = HFS_META->ssd_path + path;
  } else {
    real_path = (target_dentry->d_area == FileArea::SSD ? HFS_META->ssd_path : HFS_META->hdd_path) + path;
  }
  spdlog::info("[access] access real path {}", real_path.c_str());
  if(access(real_path.c_str(), mode) != 0) {
    return -errno;
  }
  return 0;
}

int HybridFS::hfs_create(const char *path, mode_t mode, struct fuse_file_info *fi) {
  spdlog::info("[create] path: {}, mode: {:#o}", path, mode);
  struct hfs_dentry* target_dentry = find_dentry(path);
  if(target_dentry == nullptr) {
    // create first
    spdlog::info("[create] need to create");
    struct hfs_dentry* parent_dentry = find_parent_dentry(path);
    if(parent_dentry == nullptr) {
      // parent does not exist
      spdlog::info("[create] failed to find parent dentry");
      return -ENOENT;
    }
    // open file
    std::string real_path = HFS_META->ssd_path + path;
    spdlog::info("[create] creat real path {}", real_path.c_str());
    int open_state = creat(real_path.c_str(), mode);
    if(open_state != -1){
      fi->fh = open_state;
      std::vector<std::string> dnames;
      split_path(path, dnames);
      std::string new_dentry_name = dnames[dnames.size() - 1];
      parent_dentry->d_childs->insert(std::make_pair(new_dentry_name, new hfs_dentry{
        new_dentry_name,
        FileType::REGULAR,
        FileArea::SSD,
        parent_dentry,
        nullptr
      }));
    } else {
      return -errno;
    }
  } else {
    // file exist
    std::string real_path;
    if(target_dentry->d_type == FileType::DIRECTORY) {
      real_path = HFS_META->ssd_path + path;
    } else {
      real_path = ((target_dentry->d_area == FileArea::SSD) ? HFS_META->ssd_path : HFS_META->hdd_path) + path;
    }
    spdlog::info("[create] open real path {}", real_path.c_str());
    int open_state = open(real_path.c_str(), fi->flags);
    if(open_state != -1) {
      fi->fh = open_state;
    } else {
      return -errno;
    }
  }
  return 0;
}

int HybridFS::hfs_utimens(const char *path, const struct timespec tv[2], struct fuse_file_info *fi) {
  spdlog::info("[utimens] path: {}, a_sec: {}, a_nsec: {}, u_sec: {}, u_nsec: {}", path, tv[0].tv_sec, tv[0].tv_nsec, tv[1].tv_sec, tv[1].tv_nsec);
  struct hfs_dentry* target_dentry = find_dentry(path);
  if(target_dentry == nullptr) {
    // does not exist
    spdlog::info("[utimens] failed to find target dentry");
    return -ENOENT;
  }
  std::string real_path;
  if(target_dentry->d_type == FileType::DIRECTORY) {
    real_path = HFS_META->ssd_path + path;
  } else {
    real_path = ((target_dentry->d_area == FileArea::SSD) ? HFS_META->ssd_path : HFS_META->hdd_path) + path;
  }
  spdlog::info("[utimens] utimensat real path {}", real_path.c_str());
  if(utimensat(AT_FDCWD, real_path.c_str(), tv, AT_SYMLINK_NOFOLLOW) == -1) {
    return -errno;
  }
  return 0;
}

ssize_t HybridFS::hfs_copy_file_range(const char *in_path, struct fuse_file_info *fi_in, off_t in_offset, 
                                      const char *out_path, struct fuse_file_info *fi_out, off_t out_offset, 
                                      size_t size, int flags) {
  spdlog::info("[copy_file_range] in_path: {}, in_offset: {}, out_path: {}, out_offset: {}, size: {}, flags: {:#o}", in_path, in_offset, out_path, out_offset, size, flags);
  // check two files
  struct hfs_dentry* in_dentry = find_dentry(in_path);
  struct hfs_dentry* out_dentry = find_dentry(out_path);
  if(in_dentry == nullptr || out_dentry == nullptr) {
    spdlog::info("[copy_file_range] failed to find target dentry");
    return -ENOENT;
  }
  if(in_dentry->d_type == FileType::DIRECTORY || out_dentry->d_type == FileType::DIRECTORY) {
    spdlog::info("[copy_file_range] target dentry is a directory");
    return -EISDIR;
  }
  // copy range
  std::string real_in_path = ((in_dentry->d_area == FileArea::SSD) ? HFS_META->ssd_path : HFS_META->hdd_path) + in_path;
  std::string real_out_path = ((out_dentry->d_area == FileArea::SSD) ? HFS_META->ssd_path : HFS_META->hdd_path) + out_path;
  spdlog::info("[copy_file_range] open real in path {}", real_in_path.c_str());
  int in_fd = open(real_in_path.c_str(), O_RDONLY);
  spdlog::info("[copy_file_range] open real out path {}", real_out_path.c_str());
  int out_fd = open(real_out_path.c_str(), O_WRONLY);
  spdlog::info("[copy_file_range] real copy_file_range");
  ssize_t copy_state = copy_file_range(in_fd, &in_offset, out_fd, &out_offset, size, flags);
  if(copy_state == -1) {
    return -errno;
  }
  close(in_fd);
  close(out_fd);
  // maybe migrate
  struct stat st;
  stat(real_out_path.c_str(), &st);
  if(out_dentry->d_area == FileArea::SSD && st.st_size >= HFS_META->ssd_upper_limit) {
    // move file to hdd
    char cmd[1024];
    sprintf(cmd, "mv %s %s", (HFS_META->ssd_path + out_path).c_str(), (HFS_META->hdd_path + out_path).c_str());
    spdlog::info("[copy_file_range] migrate {} to {}", (HFS_META->ssd_path + out_path).c_str(), (HFS_META->hdd_path + out_path).c_str());
    system(cmd);
    out_dentry->d_area = FileArea::HDD;
  } else if(out_dentry->d_area == FileArea::HDD && st.st_size <= HFS_META->hdd_lower_limit){
    // move file to ssd
    char cmd[1024];
    sprintf(cmd, "mv %s %s", (HFS_META->hdd_path + out_path).c_str(), (HFS_META->ssd_path + out_path).c_str());
    spdlog::info("[copy_file_range] migrate {} to {}", (HFS_META->hdd_path + out_path).c_str(), (HFS_META->ssd_path + out_path).c_str());
    system(cmd);
    out_dentry->d_area = FileArea::SSD;
  }
  // return
  return copy_state;
}

off_t HybridFS::hfs_lseek(const char *path, off_t off, int whence, struct fuse_file_info *fi) {
  spdlog::info("[lseek] path: {}", path);
  if(fi == nullptr) {
    spdlog::info("[lseek] no opened file");
    return -1;
  }
  spdlog::info("[lseek] real lseek");
  off_t seek_state = lseek(fi->fh, off, whence);
  if(seek_state == -1) {
    return -errno;
  }
  return seek_state;
}