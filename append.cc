#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string>

#include <gflags/gflags.h>

DEFINE_uint64(append_size, 0, "");
DEFINE_string(path, "", "");

int main(int argc, char* argv[]) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  FILE* file = fopen(FLAGS_path.c_str(),"a+");
  uint64_t cur_size = 0;
  uint64_t cur_num = 0;
  while(cur_size < FLAGS_append_size) {
    std::string cur_string = std::to_string(cur_num);
    fprintf(file, cur_string.c_str());
    cur_num++;
    cur_size += cur_string.size();
  }
  fclose(file);
  return 0;
}