use std::ffi::CStr;

fn main() {
    println!("{}", rust_lib::get_rust_str());
    let c_str = unsafe { CStr::from_ptr(c_lib::get_c_str()) };
    println!("{}", c_str.to_string_lossy());
}
