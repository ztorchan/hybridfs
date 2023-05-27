#include <unistd.h>
#include <sys/stat.h>
#include <sys/xattr.h>

void test_mkdir() {
  mkdir("/home/ubuntu/expfs/dir/", 0777);
}

int main() {
  test_mkdir();
  
  return 0;
}