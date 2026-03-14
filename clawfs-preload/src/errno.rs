/// Set the thread-local errno and return -1 (the standard libc error convention).
pub fn set_errno(code: i32) -> isize {
    unsafe {
        *libc::__errno_location() = code;
    }
    -1
}

/// Set errno and return a null pointer (for functions that return pointers).
#[allow(dead_code)]
pub fn set_errno_null<T>(code: i32) -> *mut T {
    unsafe {
        *libc::__errno_location() = code;
    }
    std::ptr::null_mut()
}
