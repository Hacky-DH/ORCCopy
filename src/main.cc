#include <unistd.h>

#include <iostream>
#include <orc/OrcFile.hh>
#include <string>

#include "tool.h"

using namespace orctool;

int main(int argc, char const *argv[]) {
  if (argc < 2) {
    std::cerr << "Usage: orctool <intput path> [output path]" << std::endl;
    return 1;
  }
  ORCTool tool;
  if (!tool.SetInputPath(argv[1])) {
    return 1;
  }
  std::string output_path;
  if (argc > 2) {
    output_path = argv[2];
  } else {
    char *pwd = get_current_dir_name();
    output_path = pwd;
    free(pwd);
  }
  if (!tool.SetOutputPath(output_path)) {
    return 1;
  }
  tool.SetMaxBucketNum(1024);
  if (!tool.Init()) {
    return 1;
  }
  if (!tool.Start()) {
    return 1;
  }
  return 0;
}
