#include "c_lib.h"
#include "rust_lib.h"

int main() {
  printf("C binary!\n");
  printf("%s\n", get_rust_fn());
  printf("%s\n", get_c_fn());
}
