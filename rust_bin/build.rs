fn main() {
    let lib_c_filepath = "../c_lib/lib.c";
    println!("cargo::rerun-if-changed={lib_c_filepath}");
    cc::Build::new().file(lib_c_filepath).compile("c_lib");
}
