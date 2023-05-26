#include <unistd.h>
#include <sys/stat.h>
#include <sys/xattr.h>
#include <vector>

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
  spdlog::debug("[getattr] {%s}\n", path);
  // fstat
  if(fi != nullptr) {
    return fstat(fi->fh, st);
  }
  // stat
  struct hfs_dentry* target_dentry = find_dentry(path);
  if(target_dentry == nullptr) {
    return -1;
  }
  std::string real_path = (target_dentry->d_area == FileArea::SSD ? HFS_META->ssd_path : HFS_META->hdd_path) + path;
  return stat(real_path.c_str(), st);
}

int HybridFS::hfs_readlink(const char *path, char *buf, size_t len) {
  spdlog::debug("[readlink] {%s}\n", path);
  struct hfs_dentry* target_dentry = find_dentry(path);
  if(target_dentry == nullptr) {
    // target dentry does not exist
    return -1;
  }
  if(target_dentry->d_type != FileType::SYMBOLLINK) {
    // target dentry is not a symbol link
    return -1;
  }
  std::string real_path = (target_dentry->d_area == FileArea::SSD ? HFS_META->ssd_path : HFS_META->hdd_path) + path;
  return readlink(real_path.c_str(), buf, len);
}

int HybridFS::hfs_mkdir(const char *path, mode_t mode) {
  spdlog::debug("[mkdir] {%s}\n", path);
  std::vector<std::string> dnames;
  split_path(path, dnames);
  struct hfs_dentry* parent_dentry = find_parent_dentry(path);
  if(parent_dentry == nullptr) {
    // parent dentry does not exist
    return -1;
  }
  if(parent_dentry->d_type != FileType::DIRECTORY) {
    // target dentry is not a directory
    return -1;
  }
  if(parent_dentry->d_childs->find(dnames[dnames.size() - 1]) != parent_dentry->d_childs->end()) {
    // target dentry exist
    return -1;
  }
  // real mkdir 
  int mkdir_state;
  mkdir_state = mkdir((HFS_META->ssd_path + path).c_str(), mode);
  if(mkdir_state == 0) {
    mkdir_state = mkdir((HFS_META->ssd_path + path).c_str(), mode);
  } else {
    return mkdir_state;
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
    return mkdir_state;
  }
  return mkdir_state;
}

int HybridFS::hfs_unlink(const char *path) {
  spdlog::debug("[unlink] {%s}\n", path);
  struct hfs_dentry* target_dentry = find_dentry(path);
  if(target_dentry == nullptr) {
    // can not find target dentry
    return -1;
  }
  if(target_dentry->d_type != FileType::REGULAR && target_dentry->d_type != FileType::SYMBOLLINK) {
    // target dentry is not a regular file
    return -1;
  }
  std::string real_path = (target_dentry->d_area == FileArea::SSD ? HFS_META->ssd_path : HFS_META->hdd_path) + path;
  int unlink_state = unlink(real_path.c_str());
  if(unlink_state == 0) {
    // delete target dentry
    target_dentry->d_parent->d_childs->erase(target_dentry->d_name);
    delete target_dentry;
  }
  return unlink_state;
}

int HybridFS::hfs_rmdir(const char *path) {
  spdlog::debug("[rmdir] {%s}\n", path);
  struct hfs_dentry* target_dentry = find_dentry(path);
  if(target_dentry == nullptr) {
    // can not find target dentry
    return -1;
  }
  if(target_dentry->d_type != FileType::DIRECTORY) {
    // target dentry is not a directory
    return -1;
  }
  if(!target_dentry->d_childs->empty()) {
    // target directory is not empty
    return -1;
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
    return rmdir_state;
  }
  if(rmdir_state == 0) {
    // delete target dentry
    target_dentry->d_parent->d_childs->erase(target_dentry->d_name);
    delete target_dentry->d_childs;
    delete target_dentry;
  } else {
    mkdir((HFS_META->ssd_path + path).c_str(), st.st_mode);
  }
  return rmdir_state;
}

int HybridFS::hfs_symlink(const char *oldpath, const char *newpath) {
  spdlog::debug("[symlink] {%s} to {%s}", newpath, oldpath);
  std::vector<std::string> dnames;
  split_path(newpath, dnames);
  struct hfs_dentry* parent_dentry = find_parent_dentry(newpath);
  if(parent_dentry == nullptr) {
    // parent dentry does not exist
    return -1;
  }
  if(parent_dentry->d_type != FileType::DIRECTORY) {
    // target dentry is not a directory
    return -1;
  }
  if(parent_dentry->d_childs->find(dnames[dnames.size() - 1]) != parent_dentry->d_childs->end()) {
    // target dentry exist
    return -1;
  }
  std::string real_old_path = HFS_META->fs_path + oldpath;
  std::string real_new_path = HFS_META->ssd_path + newpath;
  int symlink_state = symlink(real_old_path.c_str(), real_new_path.c_str());
  if(symlink_state) {
    parent_dentry->d_childs->insert(std::make_pair(dnames[dnames.size() - 1], new hfs_dentry {
      dnames[dnames.size() - 1], 
      FileType::SYMBOLLINK, 
      FileArea::SSD, 
      parent_dentry,
      nullptr
    }));
  }
  return symlink_state;
}

int HybridFS::hfs_rename(const char *oldpath, const char *newpath, unsigned int flags) {
  spdlog::debug("[rename] {%s} to {%s}", oldpath, newpath);

  if(flags == RENAME_EXCHANGE) {
    // do not support
    return -1;
  }

  // find and check old file
  struct hfs_dentry* old_dentry = find_dentry(oldpath);
  if(old_dentry == nullptr) {
    // can not find target old dentry
    return -1;
  }
  if(old_dentry->d_type != FileType::REGULAR && old_dentry->d_type != FileType::SYMBOLLINK) {
    // target old dentry is not file
    return -1;
  }
  std::string real_old_path = (old_dentry->d_area == FileArea::SSD ? HFS_META->ssd_path : HFS_META->hdd_path) + oldpath;
  std::string real_new_path = (old_dentry->d_area == FileArea::SSD ? HFS_META->ssd_path : HFS_META->hdd_path) + newpath;

  // find new dentry parent
  int rename_state;
  std::vector<std::string> dnames;
  split_path(newpath, dnames);
  std::string new_dentry_name = dnames[dnames.size() - 1];
  struct hfs_dentry* new_dentry_parent = find_parent_dentry(newpath);
  if(new_dentry_parent == nullptr) {
    // can not find parent dentry
    return -1;
  }
  if(new_dentry_parent->d_type != FileType::DIRECTORY) {
    // parent dentry is not directory
    return -1;
  }

  // for different flags
  if(flags == RENAME_WHITEOUT) {
    auto it = new_dentry_parent->d_childs->find(new_dentry_name);
    if(it != new_dentry_parent->d_childs->end() && it->second->d_type == FileType::DIRECTORY) {
      // directory with same path exist
      return -1;
    }
    rename_state = rename(real_old_path.c_str(), real_new_path.c_str());
    if(rename_state == 0) {
      // rename successful
      if(it != new_dentry_parent->d_childs->end()) {
        // new dentry exists
        delete it->second;
        new_dentry_parent->d_childs->erase(new_dentry_name);
      }
      old_dentry->d_parent->d_childs->erase(old_dentry->d_name);
      old_dentry->d_name = new_dentry_name;
      old_dentry->d_parent = new_dentry_parent;
      new_dentry_parent->d_childs->insert(std::make_pair(old_dentry->d_name, old_dentry));
    }
  } else if(flags == RENAME_NOREPLACE) {
    // check new path does not exist
    auto it = new_dentry_parent->d_childs->find(new_dentry_name);
    if(it != new_dentry_parent->d_childs->end()) {
      // same path exist
      return -1;
    }
    rename_state = rename(real_old_path.c_str(), real_new_path.c_str());
    if(rename_state == 0) {
      // rename successful
      old_dentry->d_parent->d_childs->erase(old_dentry->d_name);
      old_dentry->d_name = new_dentry_name;
      old_dentry->d_parent = new_dentry_parent;
      new_dentry_parent->d_childs->insert(std::make_pair(old_dentry->d_name, old_dentry));
    }
  }
  return rename_state;
}

int HybridFS::hfs_link(const char *oldpath, const char *newpath) {
  spdlog::debug("[link] {%s} to {%s}", oldpath, newpath);
  struct hfs_dentry* old_dentry = find_dentry(oldpath);
  if(old_dentry == nullptr) {
    // old dentry does not exist
    return -1;
  }
  if(old_dentry->d_type == FileType::DIRECTORY) {
    // old dentry is a directory
    return -1;
  }
  std::vector<std::string> dnames;
  split_path(newpath, dnames);
  std::string new_dentry_name = dnames[dnames.size() - 1];
  struct hfs_dentry* new_dentry_parent = find_parent_dentry(newpath);
  if(new_dentry_parent == nullptr) {
    // can not find parent
    return -1;
  }
  if(new_dentry_parent->d_type != FileType::DIRECTORY) {
    // parent dentry is not directory
    return -1;
  }
  if(new_dentry_parent->d_childs->find(new_dentry_name) != new_dentry_parent->d_childs->end()) {
    // new dentry exists
    return -1;
  }
  // real link
  std::string real_old_path = (old_dentry->d_area == FileArea::SSD ? HFS_META->ssd_path : HFS_META->hdd_path) + oldpath;
  std::string real_new_path = (old_dentry->d_area == FileArea::SSD ? HFS_META->ssd_path : HFS_META->hdd_path) + newpath;
  int link_state = link(real_old_path.c_str(), real_new_path.c_str());
  if(link_state == 0) {
    new_dentry_parent->d_childs->insert(std::make_pair(new_dentry_name, new hfs_dentry{
      new_dentry_name,
      old_dentry->d_type,
      old_dentry->d_area,
      new_dentry_parent,
      nullptr
    }));
  }
  return link_state;
}

int HybridFS::hfs_chmod(const char *path, mode_t mode, struct fuse_file_info *fi){
  spdlog::debug("[chmod] {%s}", path);
  struct hfs_dentry* target_dentry = find_dentry(path);
  if(target_dentry == nullptr) {
    // can not find such file
    return -1;
  }
  // real chmod
  std::string real_path = (target_dentry->d_area == FileArea::SSD ? HFS_META->ssd_path : HFS_META->hdd_path) + path;
  return chmod(real_path.c_str(), mode);
}

int HybridFS::hfs_chown(const char *path, uid_t uid, gid_t gid, struct fuse_file_info *fi) {
  spdlog::debug("[chown] {%s}", path);
  struct hfs_dentry* target_dentry = find_dentry(path);
  if(target_dentry == nullptr) {
    // can not find such dentry
    return -1;
  }
  if(target_dentry->d_type == FileType::DIRECTORY) {
    chown((HFS_META->ssd_path + path).c_str(), uid, gid);
    chown((HFS_META->hdd_path + path).c_str(), uid, gid);
    return 0;
  } else if(target_dentry->d_type == FileType::REGULAR) {
    return chown(((target_dentry->d_area == FileArea::SSD ? HFS_META->ssd_path : HFS_META->hdd_path) + path).c_str(), uid, gid);
  } else {
    return -1;
  }
}

int HybridFS::hfs_truncate(const char *path, off_t off, struct fuse_file_info *fi) {
  spdlog::debug("[truncate] {%s}", path);
  struct hfs_dentry* target_dentry = find_dentry(path);
  if(target_dentry == nullptr) {
    // can not find such file
    return -1;
  }
  if(target_dentry->d_type != FileType::REGULAR) {
    // dentry is not file
    return -1;
  }
  std::string real_path = (target_dentry->d_area == FileArea::SSD ? HFS_META->ssd_path : HFS_META->hdd_path) + path;
  return truncate(real_path.c_str(), off);
}

int HybridFS::hfs_open(const char *path, struct fuse_file_info *fi) {
  spdlog::debug("[open] {%s}", path);
  struct hfs_dentry* parent_dentry = find_dentry(path);
  if(parent_dentry == nullptr) {
    // parent doesn't exist
    return -1;
  }
  if(parent_dentry->d_type != FileType::DIRECTORY) {
    // parent is not directory
    return -1;
  }
  struct hfs_dentry* target_dentry = find_dentry(path);
  if(target_dentry == nullptr) {
    // file doesn't exist
    if(fi->flags | O_CREAT == 0) {
      return -1;
    }
    // create dentry
    std::vector<std::string> dnames;
    split_path(path, dnames);
    std::string new_dentry_name = dnames[dnames.size() - 1];
    target_dentry = new hfs_dentry{
      new_dentry_name,
      FileType::REGULAR,
      FileArea::SSD,
      parent_dentry,
      nullptr
    };
    parent_dentry->d_childs->insert(std::make_pair(new_dentry_name, target_dentry));
  } else {
    // file exists
    if(fi->flags | O_EXCL == 0) {
      return -1;
    }
  }
  // open file
  std::string real_path = (target_dentry->d_area == FileArea::SSD ? HFS_META->ssd_path : HFS_META->hdd_path) + path;
  int open_state = open(real_path.c_str(), fi->flags);
  if(open_state == -1){
    fi->fh = open_state;
  }
  return open_state;
}

int HybridFS::hfs_read(const char *path, char *buf, size_t size, off_t off, struct fuse_file_info *fi) {
  spdlog::debug("[read] {%s}", path);
  // check file
  struct hfs_dentry* target_dentry = find_dentry(path);
  if(target_dentry == nullptr) {
    // no such file
    return -1;
  }
  if(target_dentry->d_type == FileType::DIRECTORY){
    // path is a directory
    return -1;
  }
  // get file fd
  int fd = -1;
  if(fi != nullptr) {
    fd = fi->fh;
  } else {
    std::string real_path = (target_dentry->d_area == FileArea::SSD ? HFS_META->ssd_path : HFS_META->hdd_path) + path;
    fd = open(real_path.c_str(), O_RDONLY);
  }
  // read
  lseek(fd, off, SEEK_SET);
  int read_size = read(fd, buf, size);
  if(fi == nullptr) {
    close(fd);
  }
  return read_size;
}

int HybridFS::hfs_write(const char *path, const char *buf, size_t size, off_t off, struct fuse_file_info *fi) {
  spdlog::debug("[write] {%s}", path);
  // check file
  struct hfs_dentry* target_dentry = find_dentry(path);
  if(target_dentry == nullptr) {
    // no such file
    return -1;
  }
  if(target_dentry->d_type == FileType::DIRECTORY){
    // path is a directory
    return -1;
  }
  // get file fd
  int fd = -1;
  std::string real_path = (target_dentry->d_area == FileArea::SSD ? HFS_META->ssd_path : HFS_META->hdd_path) + path;
  if(fi != nullptr) {
    fd = fi->fh;
  } else {
    fd = open(real_path.c_str(), O_WRONLY);
  }
  // write
  lseek(fd, off, SEEK_SET);
  int write_size = write(fd, buf, size);
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
  spdlog::debug("[flush] {%s}", path);
  return 0;
}

int HybridFS::hfs_release(const char *path, struct fuse_file_info *fi) {
  spdlog::debug("[release] {%s}", path);
  if(fi != nullptr) {
    return close(fi->fh);
  }
  return 0;
}

int HybridFS::hfs_fsync(const char *path, int datasync, struct fuse_file_info *fi) {
  spdlog::debug("[fsync] {%s}", path);
  if(fi != nullptr) {
    if(datasync) {
      return fdatasync(fi->fh);
    } else {
      return fsync(fi->fh);
    }
  }
  return 0;
}

int HybridFS::hfs_setxattr(const char *path, const char *name, const char *value, size_t size, int flags) {
  spdlog::debug("[setxattr] {%s}", path);
  struct hfs_dentry* target_dentry = find_dentry(path);
  if(target_dentry == nullptr) {
    // no such file
    return -1;
  }
  std::string real_path;
  if(target_dentry->d_type == FileType::DIRECTORY) {
    real_path = HFS_META->ssd_path + path;
  } else {
    real_path = (target_dentry->d_area == FileArea::SSD ? HFS_META->ssd_path : HFS_META->hdd_path) + path;
  }
  return setxattr(real_path.c_str(), name, value, size, flags);
}

int HybridFS::hfs_getxattr(const char *path, const char *name, char *value, size_t size) {
  spdlog::debug("[getxattr] {%s}", path);
  struct hfs_dentry* target_dentry = find_dentry(path);
  if(target_dentry == nullptr) {
    // no such file
    return -1;
  }
  std::string real_path;
  if(target_dentry->d_type == FileType::DIRECTORY) {
    real_path = HFS_META->ssd_path + path;
  } else {
    real_path = (target_dentry->d_area == FileArea::SSD ? HFS_META->ssd_path : HFS_META->hdd_path) + path;
  }
  return getxattr(real_path.c_str(), name, value, size);
}

int HybridFS::hfs_listxattr(const char *path, char *list, size_t size) {
  spdlog::debug("[getxattr] {%s}", path);
  struct hfs_dentry* target_dentry = find_dentry(path);
  if(target_dentry == nullptr) {
    // no such file
    return -1;
  }
  std::string real_path;
  if(target_dentry->d_type == FileType::DIRECTORY) {
    real_path = HFS_META->ssd_path + path;
  } else {
    real_path = (target_dentry->d_area == FileArea::SSD ? HFS_META->ssd_path : HFS_META->hdd_path) + path;
  }
  return listxattr(real_path.c_str(), list, size);
}

int HybridFS::hfs_removexattr(const char *path, const char *name) {
  spdlog::debug("[removexattr] {%s}", path);
  struct hfs_dentry* target_dentry = find_dentry(path);
  if(target_dentry == nullptr) {
    // no such file
    return -1;
  }
  std::string real_path;
  if(target_dentry->d_type == FileType::DIRECTORY) {
    real_path = HFS_META->ssd_path + path;
  } else {
    real_path = (target_dentry->d_area == FileArea::SSD ? HFS_META->ssd_path : HFS_META->hdd_path) + path;
  }
  return removexattr(real_path.c_str(), name);
}

int HybridFS::hfs_readdir(const char *path, void *buf, fuse_fill_dir_t filler, off_t off, struct fuse_file_info *fi, enum fuse_readdir_flags flags) {
  spdlog::debug("[readdir] {%s}", path);
  struct hfs_dentry* target_dentry = find_dentry(path);
  if(target_dentry == nullptr) {
    // no such dentry
    return -1;
  }
  if(target_dentry->d_type != FileType::DIRECTORY) {
    // not a directory
    return -1;
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
  spdlog::debug("[init]");
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
  spdlog::debug("[destory]");
  destroy_dfs(HFS_META->root_dentry);
}

int HybridFS::hfs_access(const char *path, int mode) {
  spdlog::debug("[access] {%s}", path);
  struct hfs_dentry* target_dentry = find_dentry(path);
  if(target_dentry == nullptr) {
    // no such dentry
    return -1;
  }
  std::string real_path;
  if(target_dentry->d_type == FileType::DIRECTORY) {
    real_path = HFS_META->ssd_path + path;
  } else {
    real_path = (target_dentry->d_area == FileArea::SSD ? HFS_META->ssd_path : HFS_META->hdd_path) + path;
  }
  return access(real_path.c_str(), mode);
}