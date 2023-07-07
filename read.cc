#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string>
#include <cstring>

#include <gflags/gflags.h>

DEFINE_string(path, "", "");
DEFINE_uint64(offset, 0, "");
DEFINE_uint64(size, 0, "");

int main(int argc, char* argv[]) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  char* buf = new char[FLAGS_size];
  memset(buf, 0, FLAGS_size);
  FILE* file = fopen(FLAGS_path.c_str(),"r");
  fseek(file, FLAGS_offset, SEEK_SET);
  fread(buf, 1, FLAGS_size, file);
  printf("%s\n", buf);
  fclose(file);
  return 0;
}