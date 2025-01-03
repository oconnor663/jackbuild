use std::ffi::c_char;

#[no_mangle]
pub extern "C" fn get_rust_str() -> *const c_char {
    c"Rust function!".as_ptr()
}
