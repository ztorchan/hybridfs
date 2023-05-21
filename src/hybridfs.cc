#include <sys/stat.h>
#include <string>
#include <vector>
#include <unordered_map>

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

  int rename_state;
  std::vector<std::string> dnames;
  split_path(newpath, dnames);
  struct hfs_dentry* new_parent_dentry = find_parent_dentry(newpath);
  if(new_parent_dentry == nullptr) {
    // can not find parent dentry
    return -1;
  }
  if(new_parent_dentry->d_type != FileType::DIRECTORY) {
    // parent dentry is not directory
    return -1;
  }

  // for different flags
  if(flags == RENAME_WHITEOUT) {
    rename_state = rename(real_old_path.c_str(), real_new_path.c_str());
    if(rename_state == 0) {
      // rename successful
      auto it = new_parent_dentry->d_childs->find(dnames[dnames.size() - 1]);
      if(it != new_parent_dentry->d_childs->end()) {
        // new dentry exists
        delete it->second;
        new_parent_dentry->d_childs->erase(dnames[dnames.size() - 1]);
      }
      old_dentry->d_parent->d_childs->erase(old_dentry->d_name);
      old_dentry->d_name = dnames[dnames.size() - 1];
      old_dentry->d_parent = new_parent_dentry;
      new_parent_dentry->d_childs->insert(std::make_pair(old_dentry->d_name, old_dentry));
    }
  } else if(flags == RENAME_NOREPLACE) {
    // check new path does not exist
    if(new_parent_dentry->d_childs->find(dnames[dnames.size() - 1]) != new_parent_dentry->d_childs->end()) {
      // target new dentry exists
      return -1;
    }
  }
  // start to rename


}


void *HybridFS::init(struct fuse_conn_info *conn, struct fuse_config *cfg) {

}