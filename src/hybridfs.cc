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
  spdlog::info("[getattr] {}", path);
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
    return errno;
  }
  return 0;
}

int HybridFS::hfs_readlink(const char *path, char *buf, size_t len) {
  spdlog::info("[readlink] {}", path);
  struct hfs_dentry* target_dentry = find_dentry(path);
  if(target_dentry == nullptr) {
    // target dentry does not exist
    return -ENOENT;
  }
  if(target_dentry->d_type != FileType::SYMBOLLINK) {
    // target dentry is not a symbol link
    return -1;
  }
  std::string real_path = (target_dentry->d_area == FileArea::SSD ? HFS_META->ssd_path : HFS_META->hdd_path) + path;
  if(readlink(real_path.c_str(), buf, len) != 0) {
    return errno;
  }
  return 0;
}

int HybridFS::hfs_mkdir(const char *path, mode_t mode) {
  spdlog::info("[mkdir] {} with mode {}", path, mode);
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
    return errno;
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
    return errno;
  }
  return 0;
}

int HybridFS::hfs_unlink(const char *path) {
  spdlog::info("[unlink] {}", path);
  struct hfs_dentry* target_dentry = find_dentry(path);
  if(target_dentry == nullptr) {
    // can not find target dentry
    return -ENOENT;
  }
  if(target_dentry->d_type != FileType::REGULAR && target_dentry->d_type != FileType::SYMBOLLINK) {
    // target dentry is not a regular file
    return -EISDIR;
  }
  std::string real_path = (target_dentry->d_area == FileArea::SSD ? HFS_META->ssd_path : HFS_META->hdd_path) + path;
  if(unlink(real_path.c_str()) == 0) {
    // delete target dentry
    target_dentry->d_parent->d_childs->erase(target_dentry->d_name);
    delete target_dentry;
    return 0;
  }
  return errno;
}

int HybridFS::hfs_rmdir(const char *path) {
  spdlog::info("[rmdir] {}", path);
  struct hfs_dentry* target_dentry = find_dentry(path);
  if(target_dentry == nullptr) {
    // can not find target dentry
    return -ENOENT;
  }
  if(target_dentry->d_type != FileType::DIRECTORY) {
    // target dentry is not a directory
    return -ENOTDIR;
  }
  if(!target_dentry->d_childs->empty()) {
    // target directory is not empty
    return -ENOTEMPTY;
  }
  // stat for recovery
  struct stat st;
  stat((HFS_META->ssd_path + path).c_str(), &st);
  // real remove
  int rmdir_state;
  rmdir_state = rmdir((HFS_META->ssd_path + path).c_str());
  if(rmdir_state == 0) {
    rmdir_state = rmdir((HFS_META->hdd_path + path).c_str());
  } else {
    return errno;
  }
  if(rmdir_state == 0) {
    // delete target dentry
    target_dentry->d_parent->d_childs->erase(target_dentry->d_name);
    delete target_dentry->d_childs;
    delete target_dentry;
  } else {
    mkdir((HFS_META->ssd_path + path).c_str(), st.st_mode);
    return errno;
  }
  return 0;
}

int HybridFS::hfs_symlink(const char *oldpath, const char *newpath) {
  spdlog::info("[symlink] {} to {}", newpath, oldpath);
  std::vector<std::string> dnames;
  split_path(newpath, dnames);
  struct hfs_dentry* parent_dentry = find_parent_dentry(newpath);
  if(parent_dentry == nullptr) {
    // parent dentry does not exist
    return -ENOENT;
  }
  if(parent_dentry->d_type != FileType::DIRECTORY) {
    // parent dentry is not a directory
    return -ENOENT;
  }
  if(parent_dentry->d_childs->find(dnames[dnames.size() - 1]) != parent_dentry->d_childs->end()) {
    // target dentry exist
    return -EEXIST;
  }
  std::string real_old_path = HFS_META->fs_path + oldpath;
  std::string real_new_path = HFS_META->ssd_path + newpath;
  if(symlink(real_old_path.c_str(), real_new_path.c_str()) == 0) {
    parent_dentry->d_childs->insert(std::make_pair(dnames[dnames.size() - 1], new hfs_dentry {
      dnames[dnames.size() - 1], 
      FileType::SYMBOLLINK, 
      FileArea::SSD, 
      parent_dentry,
      nullptr
    }));
  } else {
    return errno;
  }
  return 0;
}

int HybridFS::hfs_rename(const char *oldpath, const char *newpath, unsigned int flags) {
  spdlog::info("[rename] {} to {}", oldpath, newpath);

  if(flags == RENAME_EXCHANGE || flags == RENAME_WHITEOUT) {
    // do not support
    return -EPERM;
  }

  // find and check old file
  struct hfs_dentry* old_dentry = find_dentry(oldpath);
  if(old_dentry == nullptr) {
    // can not find target old dentry
    return -ENOENT;
  }
  if(old_dentry->d_type != FileType::REGULAR && old_dentry->d_type != FileType::SYMBOLLINK) {
    // target old dentry is not file
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
    return -ENOENT;
  }
  if(new_dentry_parent->d_type != FileType::DIRECTORY) {
    // parent dentry is not directory
    return -ENOENT;
  }

  // for different flags
  if(flags == RENAME_NOREPLACE) {
    // check new path does not exist
    auto it = new_dentry_parent->d_childs->find(new_dentry_name);
    if(it != new_dentry_parent->d_childs->end()) {
      // same path exist
      return -EEXIST;
    }
    if(rename(real_old_path.c_str(), real_new_path.c_str()) == 0) {
      // rename successful
      old_dentry->d_parent->d_childs->erase(old_dentry->d_name);
      old_dentry->d_name = new_dentry_name;
      old_dentry->d_parent = new_dentry_parent;
      new_dentry_parent->d_childs->insert(std::make_pair(old_dentry->d_name, old_dentry));
    } else {
      return errno;
    }
  }
  return 0;
}

int HybridFS::hfs_link(const char *oldpath, const char *newpath) {
  spdlog::info("[link] {} to {}", oldpath, newpath);
  struct hfs_dentry* old_dentry = find_dentry(oldpath);
  if(old_dentry == nullptr) {
    // old dentry does not exist
    return -ENOENT;
  }
  if(old_dentry->d_type == FileType::DIRECTORY) {
    // old dentry is a directory
    return -EISDIR;
  }
  std::vector<std::string> dnames;
  split_path(newpath, dnames);
  std::string new_dentry_name = dnames[dnames.size() - 1];
  struct hfs_dentry* new_dentry_parent = find_parent_dentry(newpath);
  if(new_dentry_parent == nullptr) {
    // can not find parent
    return -ENOENT;
  }
  if(new_dentry_parent->d_type != FileType::DIRECTORY) {
    // parent dentry is not directory
    return -ENOENT;
  }
  if(new_dentry_parent->d_childs->find(new_dentry_name) != new_dentry_parent->d_childs->end()) {
    // new dentry exists
    return -EEXIST;
  }
  // real link
  std::string real_old_path = (old_dentry->d_area == FileArea::SSD ? HFS_META->ssd_path : HFS_META->hdd_path) + oldpath;
  std::string real_new_path = (old_dentry->d_area == FileArea::SSD ? HFS_META->ssd_path : HFS_META->hdd_path) + newpath;
  if(link(real_old_path.c_str(), real_new_path.c_str()) == 0) {
    new_dentry_parent->d_childs->insert(std::make_pair(new_dentry_name, new hfs_dentry{
      new_dentry_name,
      old_dentry->d_type,
      old_dentry->d_area,
      new_dentry_parent,
      nullptr
    }));
  } else {
    return errno;
  }
  return 0;
}

int HybridFS::hfs_chmod(const char *path, mode_t mode, struct fuse_file_info *fi){
  spdlog::info("[chmod] {}", path);
  struct hfs_dentry* target_dentry = find_dentry(path);
  if(target_dentry == nullptr) {
    // can not find such file
    return -ENOENT;
  }
  // real chmod
  std::string real_path = (target_dentry->d_area == FileArea::SSD ? HFS_META->ssd_path : HFS_META->hdd_path) + path;
  if(chmod(real_path.c_str(), mode) != 0) {
    return errno;
  }
  return 0;
}

int HybridFS::hfs_chown(const char *path, uid_t uid, gid_t gid, struct fuse_file_info *fi) {
  spdlog::info("[chown] {}", path);
  struct hfs_dentry* target_dentry = find_dentry(path);
  if(target_dentry == nullptr) {
    // can not find such dentry
    return -ENOENT;
  }
  std::string real_path;
  if(target_dentry->d_type == FileType::DIRECTORY || target_dentry->d_type == FileType::SYMBOLLINK) {
    real_path = HFS_META->ssd_path + path;
  } else if(target_dentry->d_type == FileType::REGULAR) {
    real_path = (target_dentry->d_area == FileArea::SSD ? HFS_META->ssd_path : HFS_META->hdd_path) + path;
  }
  if(chown(real_path.c_str(), uid, gid) != 0) {
    return errno;
  }
  return 0;
}

int HybridFS::hfs_truncate(const char *path, off_t off, struct fuse_file_info *fi) {
  spdlog::info("[truncate] {}", path);
  struct hfs_dentry* target_dentry = find_dentry(path);
  if(target_dentry == nullptr) {
    // can not find such file
    return -ENOENT;
  }
  if(target_dentry->d_type == FileType::DIRECTORY) {
    // dentry is not file
    return -EISDIR;
  }
  std::string real_path = (target_dentry->d_area == FileArea::SSD ? HFS_META->ssd_path : HFS_META->hdd_path) + path;
  if(truncate(real_path.c_str(), off) != 0) {
    return errno;
  }
  return 0;
}

int HybridFS::hfs_open(const char *path, struct fuse_file_info *fi) {
  spdlog::info("[open] {}", path);
  struct hfs_dentry* target_dentry = find_dentry(path);
  if(target_dentry == nullptr) {
    if((fi->flags & O_CREAT) != 0) {
      // do not create
      return -ENOENT;
    }
    // check parent
    struct hfs_dentry* parent_dentry = find_parent_dentry(path);
    if(parent_dentry == nullptr) {
      // parent does not exist
      return -ENOENT;
    }
    // create
    std::string real_path = HFS_META->ssd_path + path;
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
      return errno;
    }
  } else {
    // file exists
    if(fi->flags & O_EXCL != 0) {
      // fail if exist
      return -EEXIST ;
    }
    std::string real_path;
    if(target_dentry->d_type == FileType::DIRECTORY) {
      real_path = HFS_META->ssd_path + path;
    } else {
      real_path = (target_dentry->d_area == FileArea::SSD) ? HFS_META->ssd_path : HFS_META->hdd_path + path;
    }
    int open_state = open(real_path.c_str(), fi->flags);
    if(open_state != -1){
      fi->fh = open_state;
    } else {
      return errno;
    }
  }
  return 0;
}

int HybridFS::hfs_read(const char *path, char *buf, size_t size, off_t off, struct fuse_file_info *fi) {
  spdlog::info("[read] {}", path);
  // check file
  struct hfs_dentry* target_dentry = find_dentry(path);
  if(target_dentry == nullptr) {
    // no such file
    return -ENOENT;
  }
  if(target_dentry->d_type == FileType::DIRECTORY){
    // path is a directory
    return -EISDIR;
  }
  // get file fd
  int fd = -1;
  if(fi != nullptr) {
    fd = fi->fh;
  } else {
    std::string real_path = (target_dentry->d_area == FileArea::SSD ? HFS_META->ssd_path : HFS_META->hdd_path) + path;
    fd = open(real_path.c_str(), O_RDONLY);
  }
  if(fd == -1) {
    return errno;
  }
  // read
  if(lseek(fd, off, SEEK_SET) == -1) {
    return errno;
  }
  int read_size = read(fd, buf, size);
  if(read_size == -1) {
    return errno;
  }
  if(fi == nullptr) {
    close(fd);
  }
  return read_size;
}

int HybridFS::hfs_write(const char *path, const char *buf, size_t size, off_t off, struct fuse_file_info *fi) {
  spdlog::info("[write] {}", path);
  // check file
  struct hfs_dentry* target_dentry = find_dentry(path);
  if(target_dentry == nullptr) {
    // no such file
    return -ENOENT;
  }
  if(target_dentry->d_type == FileType::DIRECTORY){
    // path is a directory
    return -EISDIR;
  }
  // get file fd
  int fd = -1;
  std::string real_path = (target_dentry->d_area == FileArea::SSD ? HFS_META->ssd_path : HFS_META->hdd_path) + path;
  if(fi != nullptr) {
    fd = fi->fh;
  } else {
    fd = open(real_path.c_str(), O_WRONLY);
  }
  if(fd == -1) {
    return errno;
  }
  // write
  if(lseek(fd, off, SEEK_SET) == -1) {
    return errno;
  }
  int write_size = write(fd, buf, size);
  if(write_size == -1) {
    return errno;
  }
  if(fi == nullptr) {
    close(fd);
  }
  // maybe migrate
  struct stat st;
  stat(real_path.c_str(), &st);
  if(target_dentry->d_area == FileArea::SSD && st.st_size >= HFS_META->ssd_upper_limit) {
    // move file to hdd
    char cmd[1024];
    sprintf(cmd, "mv %s %s", (HFS_META->ssd_path + path).c_str(), (HFS_META->hdd_path + path).c_str());
    system(cmd);
  } else if(target_dentry->d_area == FileArea::HDD && st.st_size <= HFS_META->hdd_lower_limit){
    // move file to ssd
    char cmd[1024];
    sprintf(cmd, "mv %s %s", (HFS_META->hdd_path + path).c_str(), (HFS_META->ssd_path + path).c_str());
    system(cmd);
  }
  return write_size;
}

int HybridFS::hfs_flush(const char *path, struct fuse_file_info *fi) {
  spdlog::info("[flush] {}", path);
  return 0;
}

int HybridFS::hfs_release(const char *path, struct fuse_file_info *fi) {
  spdlog::info("[release] {}", path);
  if(fi != nullptr) {
    if(close(fi->fh) == -1) {
      return errno;
    }
  }
  return 0;
}

int HybridFS::hfs_fsync(const char *path, int datasync, struct fuse_file_info *fi) {
  spdlog::info("[fsync] {}", path);
  if(fi != nullptr) {
    if(datasync) {
      if(fdatasync(fi->fh) == -1) {
        return errno;
      }
    } else {
      if(fsync(fi->fh) == -1) {
        return errno;
      }
    }
  }
  return 0;
}

int HybridFS::hfs_setxattr(const char *path, const char *name, const char *value, size_t size, int flags) {
  spdlog::info("[setxattr] {}", path);
  struct hfs_dentry* target_dentry = find_dentry(path);
  if(target_dentry == nullptr) {
    // no such file
    return -ENOENT;
  }
  std::string real_path;
  if(target_dentry->d_type == FileType::DIRECTORY) {
    real_path = HFS_META->ssd_path + path;
  } else {
    real_path = (target_dentry->d_area == FileArea::SSD ? HFS_META->ssd_path : HFS_META->hdd_path) + path;
  }
  if(setxattr(real_path.c_str(), name, value, size, flags) == -1) {
    return errno;
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
  if(getxattr(real_path.c_str(), name, value, size) != 0) {
    return errno;
  }
  return 0;
}

int HybridFS::hfs_listxattr(const char *path, char *list, size_t size) {
  spdlog::info("[getxattr] {}", path);
  struct hfs_dentry* target_dentry = find_dentry(path);
  if(target_dentry == nullptr) {
    // no such file
    return -ENOENT;
  }
  std::string real_path;
  if(target_dentry->d_type == FileType::DIRECTORY) {
    real_path = HFS_META->ssd_path + path;
  } else {
    real_path = (target_dentry->d_area == FileArea::SSD ? HFS_META->ssd_path : HFS_META->hdd_path) + path;
  }
  if(listxattr(real_path.c_str(), list, size) == -1) {
    return errno;
  }
  return 0;
}

int HybridFS::hfs_removexattr(const char *path, const char *name) {
  spdlog::info("[removexattr] {}", path);
  struct hfs_dentry* target_dentry = find_dentry(path);
  if(target_dentry == nullptr) {
    // no such file
    return -ENOENT;
  }
  std::string real_path;
  if(target_dentry->d_type == FileType::DIRECTORY) {
    real_path = HFS_META->ssd_path + path;
  } else {
    real_path = (target_dentry->d_area == FileArea::SSD ? HFS_META->ssd_path : HFS_META->hdd_path) + path;
  }
  if(removexattr(real_path.c_str(), name) == -1) {
    return errno;
  }
  return 0;
}

int HybridFS::hfs_readdir(const char *path, void *buf, fuse_fill_dir_t filler, off_t off, struct fuse_file_info *fi, enum fuse_readdir_flags flags) {
  spdlog::info("[readdir] {}", path);
  struct hfs_dentry* target_dentry = find_dentry(path);
  if(target_dentry == nullptr) {
    // no such dentry
    return -ENOENT;
  }
  if(target_dentry->d_type != FileType::DIRECTORY) {
    // not a directory
    return -ENOTDIR;
  }
  for(auto it = target_dentry->d_childs->begin(); it != target_dentry->d_childs->end(); it++) {
    struct hfs_dentry* child = it->second;
    struct stat st;
    std::string real_path;
    if(child->d_type == FileType::DIRECTORY) {
      real_path = HFS_META->ssd_path + path + "/" + child->d_name;
    } else {
      real_path = (child->d_area == FileArea::SSD ? HFS_META->ssd_path : HFS_META->hdd_path) + path + "/" + child->d_name;
    }
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
  spdlog::info("[access] {}", path);
  struct hfs_dentry* target_dentry = find_dentry(path);
  if(target_dentry == nullptr) {
    // no such dentry
    return -ENOENT;
  }
  std::string real_path;
  if(target_dentry->d_type == FileType::DIRECTORY) {
    real_path = HFS_META->ssd_path + path;
  } else {
    real_path = (target_dentry->d_area == FileArea::SSD ? HFS_META->ssd_path : HFS_META->hdd_path) + path;
  }
  return access(real_path.c_str(), mode);
}

int HybridFS::hfs_create(const char *path, mode_t mode, struct fuse_file_info *fi) {
  spdlog::info("[create] {}", path);
  struct hfs_dentry* target_dentry = find_dentry(path);
  if(target_dentry == nullptr) {
    // create first
    struct hfs_dentry* parent_dentry = find_parent_dentry(path);
    if(parent_dentry == nullptr) {
      // parent does not exist
      return -ENOENT;
    }
    // open file
    std::string real_path = HFS_META->ssd_path + path;
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
      return errno;
    }
  } else {
    // file exist
    std::string real_path;
    if(target_dentry->d_type == FileType::DIRECTORY) {
      real_path = HFS_META->ssd_path + path;
    } else {
      real_path = (target_dentry->d_area == FileArea::SSD) ? HFS_META->ssd_path : HFS_META->hdd_path + path;
    }
    int open_state = open(real_path.c_str(), fi->flags);
    if(open_state != -1) {
      fi->fh = open_state;
    } else {
      return errno;
    }
  }
  return 0;
}

int HybridFS::hfs_utimens(const char *path, const struct timespec tv[2], struct fuse_file_info *fi) {
  spdlog::info("[utimens] {}", path);
  struct hfs_dentry* target_dentry = find_dentry(path);
  if(target_dentry == nullptr) {
    // does not exist
    return -ENOENT;
  }
  std::string real_path;
  if(target_dentry->d_type == FileType::DIRECTORY) {
    real_path = HFS_META->ssd_path + path;
  } else {
    real_path = (target_dentry->d_area == FileArea::SSD) ? HFS_META->ssd_path : HFS_META->hdd_path + path;
  }
  if(utimensat(AT_FDCWD, real_path.c_str(), tv, AT_SYMLINK_NOFOLLOW) == -1) {
    return errno;
  }
  return 0;
}

ssize_t HybridFS::hfs_copy_file_range(const char *path_in, struct fuse_file_info *fi_in, off_t offset_in, 
                                      const char *path_out, struct fuse_file_info *fi_out, off_t offset_out, 
                                      size_t size, int flags) {
  spdlog::info("[copy_file_range] from {} to {}", path_in, path_out);
  // check two files
  struct hfs_dentry* in_dentry = find_dentry(path_in);
  struct hfs_dentry* out_dentry = find_dentry(path_out);
  if(in_dentry == nullptr || out_dentry == nullptr) {
    return -ENOENT;
  }
  if(in_dentry->d_type == FileType::DIRECTORY || out_dentry->d_type == FileType::DIRECTORY) {
    return -EISDIR;
  }
  // copy range
  std::string real_in_path = (in_dentry->d_area == FileArea::SSD) ? HFS_META->ssd_path : HFS_META->hdd_path + path_in;
  std::string real_out_path = (out_dentry->d_area == FileArea::SSD) ? HFS_META->ssd_path : HFS_META->hdd_path + path_out;
  int in_fd = open(real_in_path.c_str(), O_RDONLY);
  int out_fd = open(real_out_path.c_str(), O_WRONLY);
  ssize_t copy_state = copy_file_range(in_fd, &offset_in, out_fd, &offset_out, size, flags);
  if(copy_state == -1) {
    return errno;
  }
  close(in_fd);
  close(out_fd);
  // maybe migrate
  struct stat st;
  stat(real_out_path.c_str(), &st);
  if(out_dentry->d_area == FileArea::SSD && st.st_size >= HFS_META->ssd_upper_limit) {
    // move file to hdd
    char cmd[1024];
    sprintf(cmd, "mv %s %s", (HFS_META->ssd_path + path_out).c_str(), (HFS_META->hdd_path + path_out).c_str());
    system(cmd);
  } else if(out_dentry->d_area == FileArea::HDD && st.st_size <= HFS_META->hdd_lower_limit){
    // move file to ssd
    char cmd[1024];
    sprintf(cmd, "mv %s %s", (HFS_META->hdd_path + path_out).c_str(), (HFS_META->ssd_path + path_out).c_str());
    system(cmd);
  }
  // return
  return copy_state;
}

off_t HybridFS::hfs_lseek(const char *path, off_t off, int whence, struct fuse_file_info *fi) {
  if(fi == nullptr) {
    return -1;
  }
  off_t seek_state = lseek(fi->fh, off, whence);
  if(seek_state == -1) {
    return errno;
  }
  return seek_state;
}