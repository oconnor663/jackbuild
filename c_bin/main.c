#include <stdio.h>

#include "c_lib/lib.h"
#include "rust_lib/lib.h"

int main() {
  printf("C binary!\n");
  printf("%s\n", get_rust_str());
  printf("%s\n", get_c_str());
}
