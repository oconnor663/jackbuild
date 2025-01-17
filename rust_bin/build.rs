fn main() {
    cc::Build::new().file("../c_lib/lib.c").compile("c_lib");
}
