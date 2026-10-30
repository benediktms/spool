//! Permission model (spec 15.2): directories 0700; socket, signing key, provider credential,
//! databases and their WAL/SHM files 0600. Used by daemon startup and `doctor`.

use crate::config::Paths;
use std::fs::File;
use std::io;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PermIssue {
    pub path: PathBuf,
    pub expected: u32,
    /// `None` when the owner differs from the current user.
    pub actual: Option<u32>,
    pub detail: String,
}

fn reject_reparse(path: &Path) -> io::Result<std::fs::Metadata> {
    let metadata = std::fs::symlink_metadata(path)?;
    if metadata.file_type().is_symlink() {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            format!("{} is a symlink", path.display()),
        ));
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::MetadataExt;
        if metadata.file_attributes() & 0x400 != 0 {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                format!("{} is a reparse point", path.display()),
            ));
        }
    }
    Ok(metadata)
}

/// Create a private directory, or verify an existing directory before changing it.
///
/// Windows creation supplies a protected DACL before the directory becomes visible; its ACL grants
/// full access only to the current user and LocalSystem, with inheritable entries for its children.
pub(crate) fn ensure_private_dir(path: &Path) -> io::Result<()> {
    match reject_reparse(path) {
        Ok(metadata) => {
            if !metadata.is_dir() {
                return Err(io::Error::new(
                    io::ErrorKind::AlreadyExists,
                    format!("{} is not a directory", path.display()),
                ));
            }
            check_private(path, 0o700)?;
            set_private(path, 0o700)
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => create_private_dir(path),
        Err(error) => Err(error),
    }
}

/// Tighten an existing private path. The caller must not use this to take over foreign state.
pub(crate) fn set_private(path: &Path, mode: u32) -> io::Result<()> {
    let _ = reject_reparse(path)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::{MetadataExt, PermissionsExt};
        if std::fs::metadata(path)?.uid() != rustix::process::geteuid().as_raw() {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "not current-user owned",
            ));
        }
        return std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode));
    }
    #[cfg(windows)]
    {
        let _ = mode;
        windows::set_private(path)
    }
}

/// Verify an existing private path. It never repairs ACLs or follows reparse points.
pub(crate) fn check_private(path: &Path, expected: u32) -> io::Result<()> {
    let metadata = reject_reparse(path)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::{MetadataExt, PermissionsExt};
        let uid = rustix::process::geteuid().as_raw();
        if metadata.uid() != uid {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "not current-user owned",
            ));
        }
        let mode = metadata.permissions().mode() & 0o777;
        if mode & !expected != 0 {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                format!("mode {mode:o}, expected {expected:o}"),
            ));
        }
        Ok(())
    }
    #[cfg(windows)]
    {
        let _ = (metadata, expected);
        windows::check_private(path)
    }
}

/// Whether an already-opened file is owned by the current user. This avoids a stat/open race for
/// source roots and source records.
pub(crate) fn is_current_user(file: &File) -> io::Result<bool> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        return Ok(file.metadata()?.uid() == rustix::process::geteuid().as_raw());
    }
    #[cfg(windows)]
    {
        windows::is_current_user(file)
    }
}

/// Securely open a state file for reading and writing, creating it with the private ACL if absent.
/// Existing paths are checked before opening; callers must use it only beneath a private directory.
pub(crate) fn open_private_file(path: &Path) -> io::Result<File> {
    match reject_reparse(path) {
        Ok(_) => check_private(path, 0o600)?,
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => return Err(error),
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        return std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .mode(0o600)
            .open(path);
    }
    #[cfg(windows)]
    {
        windows::open_private_file(path)
    }
}

/// Whether a Windows kernel object is owned by the current user. Named-pipe clients use this
/// after opening the server endpoint and before sending any framed request.
#[cfg(windows)]
pub(crate) fn is_current_user_handle(handle: *mut std::ffi::c_void) -> io::Result<bool> {
    windows::is_current_user_handle(handle)
}

/// Create a new private file without ever exposing it through an inherited public ACL.
pub(crate) fn create_private_file(path: &Path) -> io::Result<File> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        return std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(path);
    }
    #[cfg(windows)]
    {
        windows::create_private_file(path)
    }
}

/// Atomically replace a private file. Windows uses a replace-existing move because its standard
/// rename cannot replace an existing destination.
pub(crate) fn replace_private(from: &Path, to: &Path) -> io::Result<()> {
    check_private(from, 0o600)?;
    #[cfg(unix)]
    {
        std::fs::rename(from, to)
    }
    #[cfg(windows)]
    {
        windows::replace_private(from, to)
    }
}

#[cfg(unix)]
fn create_private_dir(path: &Path) -> io::Result<()> {
    use std::os::unix::fs::DirBuilderExt;
    std::fs::DirBuilder::new()
        .recursive(true)
        .mode(0o700)
        .create(path)
}

#[cfg(windows)]
fn create_private_dir(path: &Path) -> io::Result<()> {
    windows::create_private_dir(path)
}

fn check_one(path: &Path, expected: u32, out: &mut Vec<PermIssue>) {
    match check_private(path, expected) {
        Ok(()) => {}
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => out.push(PermIssue {
            path: path.into(),
            expected,
            actual: actual_mode(path),
            detail: error.to_string(),
        }),
    }
}

#[cfg(unix)]
fn actual_mode(path: &Path) -> Option<u32> {
    use std::os::unix::fs::PermissionsExt;
    std::fs::metadata(path)
        .ok()
        .map(|metadata| metadata.permissions().mode() & 0o777)
}

#[cfg(windows)]
fn actual_mode(path: &Path) -> Option<u32> {
    path.exists().then_some(0)
}

/// Every existing sensitive path under `paths` (+ extra files such as the provider credential).
pub fn check(paths: &Paths, extra_files: &[PathBuf]) -> Vec<PermIssue> {
    let mut out = Vec::new();
    for directory in [&paths.root, &paths.state_dir, &paths.secrets_dir] {
        check_one(directory, 0o700, &mut out);
    }
    let mut files: Vec<PathBuf> = vec![paths.signing_key.clone(), paths.lock.clone()];
    #[cfg(unix)]
    files.push(paths.socket.clone());
    for database in [&paths.index_db, &paths.jobs_db] {
        for suffix in ["", "-wal", "-shm"] {
            let mut path = database.as_os_str().to_owned();
            path.push(suffix);
            files.push(PathBuf::from(path));
        }
    }
    files.extend(extra_files.iter().cloned());
    for file in files {
        check_one(&file, 0o600, &mut out);
    }
    out
}

/// Tighten modes the daemon itself created (databases, WAL/SHM, lock, socket). Foreign-owned
/// paths are reported, never changed.
pub fn tighten(issues: &[PermIssue]) -> io::Result<()> {
    for issue in issues {
        if issue.actual.is_some() {
            set_private(&issue.path, issue.expected)?;
        }
    }
    Ok(())
}

#[cfg(windows)]
pub(crate) fn current_user_sid() -> io::Result<String> {
    windows::current_user_sid()
}

#[cfg(windows)]
mod windows {
    use super::*;
    use std::ffi::c_void;
    use std::os::windows::ffi::OsStrExt;
    use std::os::windows::io::{AsRawHandle, FromRawHandle};
    use windows_sys::Win32::Foundation::{
        CloseHandle, ERROR_ALREADY_EXISTS, GetLastError, LocalFree,
    };
    use windows_sys::Win32::Security::Authorization::{
        ConvertSidToStringSidW, ConvertStringSecurityDescriptorToSecurityDescriptorW,
        GetNamedSecurityInfoW, GetSecurityInfo, SE_FILE_OBJECT, SE_KERNEL_OBJECT,
        SetNamedSecurityInfoW,
    };
    use windows_sys::Win32::Security::{
        ACCESS_ALLOWED_ACE, ACL, ACL_SIZE_INFORMATION, AclSizeInformation,
        DACL_SECURITY_INFORMATION, EqualSid, GetAce, GetAclInformation, GetTokenInformation,
        OWNER_SECURITY_INFORMATION, PROTECTED_DACL_SECURITY_INFORMATION, PSID, SE_DACL_PROTECTED,
        SECURITY_ATTRIBUTES, TOKEN_QUERY, TOKEN_USER, TokenUser,
    };
    use windows_sys::Win32::Storage::FileSystem::{
        CREATE_NEW, CreateDirectoryW, CreateFileW, FILE_ATTRIBUTE_NORMAL, FILE_GENERIC_READ,
        FILE_GENERIC_WRITE, FILE_SHARE_DELETE, FILE_SHARE_READ, FILE_SHARE_WRITE,
        MOVEFILE_REPLACE_EXISTING, MOVEFILE_WRITE_THROUGH, MoveFileExW, OPEN_ALWAYS,
    };
    use windows_sys::Win32::System::Threading::{GetCurrentProcess, OpenProcessToken};

    const ACCESS_ALLOWED_ACE_TYPE: u8 = 0;
    const SYSTEM_SID: &str = "S-1-5-18";

    fn last_error() -> io::Error {
        io::Error::from_raw_os_error(unsafe { GetLastError() } as i32)
    }

    fn wide(path: &Path) -> Vec<u16> {
        path.as_os_str().encode_wide().chain(Some(0)).collect()
    }

    struct Local(*mut c_void);
    impl Drop for Local {
        fn drop(&mut self) {
            if !self.0.is_null() {
                // SAFETY: Windows allocated this descriptor/string with LocalAlloc.
                unsafe { LocalFree(self.0) };
            }
        }
    }

    fn token_user_sid() -> io::Result<PSID> {
        let mut token = std::ptr::null_mut();
        // SAFETY: output pointer is valid and the pseudo process handle is supplied by Windows.
        if unsafe { OpenProcessToken(GetCurrentProcess(), TOKEN_QUERY, &mut token) } == 0 {
            return Err(last_error());
        }
        let mut length = 0;
        // SAFETY: sizing call intentionally passes no buffer.
        unsafe { GetTokenInformation(token, TokenUser, std::ptr::null_mut(), 0, &mut length) };
        let mut bytes = vec![0u8; length as usize];
        // SAFETY: `bytes` has the exact size returned by the sizing call.
        let ok = unsafe {
            GetTokenInformation(
                token,
                TokenUser,
                bytes.as_mut_ptr().cast(),
                length,
                &mut length,
            )
        };
        // SAFETY: token was returned by OpenProcessToken and is closed exactly once.
        unsafe { CloseHandle(token) };
        if ok == 0 {
            return Err(last_error());
        }
        // The byte buffer has no TOKEN_USER alignment guarantee, so read its pointer fields
        // unaligned while the buffer remains alive.
        let user = unsafe { std::ptr::read_unaligned(bytes.as_ptr().cast::<TOKEN_USER>()) };
        let mut string = std::ptr::null_mut();
        // SAFETY: `user.Sid` belongs to the valid TOKEN_USER buffer.
        if unsafe { ConvertSidToStringSidW(user.User.Sid, &mut string) } == 0 {
            return Err(last_error());
        }
        let guard = Local(string.cast());
        let sid = unsafe { std::slice::from_raw_parts(string, wcslen(string)) };
        let text = String::from_utf16(sid)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "invalid SID"))?;
        drop(guard);
        sid_from_string(&text)
    }

    fn wcslen(mut value: *const u16) -> usize {
        let mut len = 0;
        // SAFETY: Windows returned a NUL-terminated SID string.
        unsafe {
            while *value != 0 {
                len += 1;
                value = value.add(1);
            }
        }
        len
    }

    fn sid_from_string(value: &str) -> io::Result<PSID> {
        let encoded: Vec<u16> = value.encode_utf16().chain(Some(0)).collect();
        let mut sid = std::ptr::null_mut();
        // SAFETY: `encoded` is NUL-terminated and output storage is valid.
        if unsafe {
            windows_sys::Win32::Security::Authorization::ConvertStringSidToSidW(
                encoded.as_ptr(),
                &mut sid,
            )
        } == 0
        {
            return Err(last_error());
        }
        Ok(sid)
    }

    fn descriptor(is_directory: bool) -> io::Result<Local> {
        let sid = current_user_sid()?;
        let inherit = if is_directory { "OICI" } else { "" };
        let sddl = format!("D:P(A;{inherit};FA;;;{sid})(A;{inherit};FA;;;SY)");
        let encoded: Vec<u16> = sddl.encode_utf16().chain(Some(0)).collect();
        let mut security_descriptor = std::ptr::null_mut();
        // SAFETY: `encoded` is a valid NUL-terminated SDDL string and output storage is valid.
        if unsafe {
            ConvertStringSecurityDescriptorToSecurityDescriptorW(
                encoded.as_ptr(),
                1,
                &mut security_descriptor,
                std::ptr::null_mut(),
            )
        } == 0
        {
            return Err(last_error());
        }
        Ok(Local(security_descriptor))
    }

    pub(super) fn create_private_dir(path: &Path) -> io::Result<()> {
        let path = std::path::absolute(path)?;
        let mut missing = Vec::new();
        let mut current = path.as_path();
        loop {
            match super::reject_reparse(current) {
                Ok(metadata) => {
                    if !metadata.is_dir() {
                        return Err(io::Error::new(
                            io::ErrorKind::AlreadyExists,
                            format!("{} is not a directory", current.display()),
                        ));
                    }
                    break;
                }
                Err(error) if error.kind() == io::ErrorKind::NotFound => {
                    missing.push(current.to_path_buf());
                    current = current.parent().ok_or_else(|| {
                        io::Error::new(io::ErrorKind::NotFound, "directory has no existing parent")
                    })?;
                }
                Err(error) => return Err(error),
            }
        }
        for directory in missing.iter().rev() {
            let security = descriptor(true)?;
            let attributes = SECURITY_ATTRIBUTES {
                nLength: std::mem::size_of::<SECURITY_ATTRIBUTES>() as u32,
                lpSecurityDescriptor: security.0,
                bInheritHandle: 0,
            };
            let directory_wide = wide(directory);
            // SAFETY: pointers remain valid for the duration of the call.
            if unsafe { CreateDirectoryW(directory_wide.as_ptr(), &attributes) } == 0 {
                let error = last_error();
                if error.raw_os_error() != Some(ERROR_ALREADY_EXISTS as i32) {
                    return Err(error);
                }
                super::check_private(directory, 0o700)?;
            }
        }
        Ok(())
    }

    pub(super) fn open_private_file(path: &Path) -> io::Result<File> {
        let security = descriptor(false)?;
        let attributes = SECURITY_ATTRIBUTES {
            nLength: std::mem::size_of::<SECURITY_ATTRIBUTES>() as u32,
            lpSecurityDescriptor: security.0,
            bInheritHandle: 0,
        };
        let path = wide(path);
        // SAFETY: all pointers are valid; CreateFileW transfers ownership of a successful handle.
        let handle = unsafe {
            CreateFileW(
                path.as_ptr(),
                FILE_GENERIC_READ | FILE_GENERIC_WRITE,
                FILE_SHARE_DELETE | FILE_SHARE_READ | FILE_SHARE_WRITE,
                &attributes,
                OPEN_ALWAYS,
                FILE_ATTRIBUTE_NORMAL,
                std::ptr::null_mut(),
            )
        };
        if handle == windows_sys::Win32::Foundation::INVALID_HANDLE_VALUE {
            return Err(last_error());
        }
        // SAFETY: successful CreateFileW returns an owned file handle.
        Ok(unsafe { File::from_raw_handle(handle) })
    }

    pub(super) fn create_private_file(path: &Path) -> io::Result<File> {
        let security = descriptor(false)?;
        let attributes = SECURITY_ATTRIBUTES {
            nLength: std::mem::size_of::<SECURITY_ATTRIBUTES>() as u32,
            lpSecurityDescriptor: security.0,
            bInheritHandle: 0,
        };
        let path = wide(path);
        // SAFETY: all pointers are valid; CreateFileW transfers ownership of a successful handle.
        let handle = unsafe {
            CreateFileW(
                path.as_ptr(),
                FILE_GENERIC_READ | FILE_GENERIC_WRITE,
                FILE_SHARE_DELETE | FILE_SHARE_READ | FILE_SHARE_WRITE,
                &attributes,
                CREATE_NEW,
                FILE_ATTRIBUTE_NORMAL,
                std::ptr::null_mut(),
            )
        };
        if handle == windows_sys::Win32::Foundation::INVALID_HANDLE_VALUE {
            return Err(last_error());
        }
        // SAFETY: successful CreateFileW returns an owned file handle.
        Ok(unsafe { File::from_raw_handle(handle) })
    }

    pub(super) fn replace_private(from: &Path, to: &Path) -> io::Result<()> {
        let from = wide(from);
        let to = wide(to);
        // SAFETY: both paths are NUL-terminated and valid for this call.
        if unsafe {
            MoveFileExW(
                from.as_ptr(),
                to.as_ptr(),
                MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH,
            )
        } == 0
        {
            Err(last_error())
        } else {
            Ok(())
        }
    }

    pub(super) fn current_user_sid() -> io::Result<String> {
        let sid = token_user_sid()?;
        let mut string = std::ptr::null_mut();
        // SAFETY: `sid` was returned by ConvertStringSidToSidW and is valid.
        let ok = unsafe { ConvertSidToStringSidW(sid, &mut string) };
        let _sid = Local(sid);
        if ok == 0 {
            return Err(last_error());
        }
        let text = unsafe { std::slice::from_raw_parts(string, wcslen(string)) };
        let result = String::from_utf16(text)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "invalid SID"));
        drop(Local(string.cast()));
        result
    }

    pub(super) fn is_current_user(file: &File) -> io::Result<bool> {
        // SAFETY: `file` keeps this file handle valid for the duration of the query.
        unsafe { is_current_user_object(file.as_raw_handle().cast(), SE_FILE_OBJECT) }
    }

    pub(super) fn is_current_user_handle(handle: *mut c_void) -> io::Result<bool> {
        // SAFETY: callers retain the live named-pipe handle for this query.
        unsafe { is_current_user_object(handle, SE_KERNEL_OBJECT) }
    }

    unsafe fn is_current_user_object(
        handle: *mut c_void,
        object_type: windows_sys::Win32::Security::Authorization::SE_OBJECT_TYPE,
    ) -> io::Result<bool> {
        let current = token_user_sid()?;
        let _current = Local(current);
        let mut owner = std::ptr::null_mut();
        let mut descriptor = std::ptr::null_mut();
        // SAFETY: caller guarantees `handle` is live; output pointers are valid.
        let status = unsafe {
            GetSecurityInfo(
                handle,
                object_type,
                OWNER_SECURITY_INFORMATION,
                &mut owner,
                std::ptr::null_mut(),
                std::ptr::null_mut(),
                std::ptr::null_mut(),
                &mut descriptor,
            )
        };
        let _descriptor = Local(descriptor);
        if status != 0 {
            return Err(io::Error::from_raw_os_error(status as i32));
        }
        // SAFETY: both SIDs are valid for the duration of this call.
        Ok(unsafe { EqualSid(owner, current) } != 0)
    }

    fn path_is_current_user(path: &Path) -> io::Result<bool> {
        let current = token_user_sid()?;
        let _current = Local(current);
        let path = wide(path);
        let mut owner = std::ptr::null_mut();
        let mut descriptor = std::ptr::null_mut();
        // SAFETY: path is NUL-terminated and output pointers are valid.
        let status = unsafe {
            GetNamedSecurityInfoW(
                path.as_ptr(),
                SE_FILE_OBJECT,
                OWNER_SECURITY_INFORMATION,
                &mut owner,
                std::ptr::null_mut(),
                std::ptr::null_mut(),
                std::ptr::null_mut(),
                &mut descriptor,
            )
        };
        let _descriptor = Local(descriptor);
        if status != 0 {
            return Err(io::Error::from_raw_os_error(status as i32));
        }
        // SAFETY: both SIDs are valid for the duration of this call.
        Ok(unsafe { EqualSid(owner, current) } != 0)
    }

    pub(super) fn set_private(path: &Path) -> io::Result<()> {
        if !path_is_current_user(path)? {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "not current-user owned",
            ));
        }
        let security = descriptor(std::fs::metadata(path)?.is_dir())?;
        let mut present = 0;
        let mut dacl: *mut ACL = std::ptr::null_mut();
        let mut defaulted = 0;
        // SAFETY: descriptor is valid and output pointers are valid.
        if unsafe {
            windows_sys::Win32::Security::GetSecurityDescriptorDacl(
                security.0,
                &mut present,
                &mut dacl,
                &mut defaulted,
            )
        } == 0
        {
            return Err(last_error());
        }
        let path = wide(path);
        // SAFETY: path and ACL remain valid for the call.
        let status = unsafe {
            SetNamedSecurityInfoW(
                path.as_ptr(),
                SE_FILE_OBJECT,
                DACL_SECURITY_INFORMATION | PROTECTED_DACL_SECURITY_INFORMATION,
                std::ptr::null_mut(),
                std::ptr::null_mut(),
                dacl,
                std::ptr::null(),
            )
        };
        if status == 0 {
            Ok(())
        } else {
            Err(io::Error::from_raw_os_error(status as i32))
        }
    }

    pub(super) fn check_private(path: &Path) -> io::Result<()> {
        let current = token_user_sid()?;
        let _current = Local(current);
        let system = sid_from_string(SYSTEM_SID)?;
        let _system = Local(system);
        let path_wide = wide(path);
        let mut owner = std::ptr::null_mut();
        let mut dacl: *mut ACL = std::ptr::null_mut();
        let mut descriptor = std::ptr::null_mut();
        // SAFETY: path is NUL terminated and output pointers are valid.
        let status = unsafe {
            GetNamedSecurityInfoW(
                path_wide.as_ptr(),
                SE_FILE_OBJECT,
                OWNER_SECURITY_INFORMATION | DACL_SECURITY_INFORMATION,
                &mut owner,
                std::ptr::null_mut(),
                &mut dacl,
                std::ptr::null_mut(),
                &mut descriptor,
            )
        };
        let _descriptor = Local(descriptor);
        if status != 0 {
            return Err(io::Error::from_raw_os_error(status as i32));
        }
        // SAFETY: owner and current SID pointers remain valid.
        if unsafe { EqualSid(owner, current) } == 0 {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "not current-user owned",
            ));
        }
        if dacl.is_null() {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "missing DACL",
            ));
        }
        let mut control = 0;
        let mut revision = 0;
        // SAFETY: descriptor is valid until `_descriptor` drops.
        if unsafe {
            windows_sys::Win32::Security::GetSecurityDescriptorControl(
                descriptor,
                &mut control,
                &mut revision,
            )
        } == 0
        {
            return Err(last_error());
        }
        if std::fs::metadata(path)?.is_dir() && control & SE_DACL_PROTECTED == 0 {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "directory DACL inherits from parent",
            ));
        }
        let mut size = ACL_SIZE_INFORMATION::default();
        // SAFETY: `dacl` is valid and `size` has the requested layout.
        if unsafe {
            GetAclInformation(
                dacl,
                (&mut size as *mut ACL_SIZE_INFORMATION).cast(),
                std::mem::size_of::<ACL_SIZE_INFORMATION>() as u32,
                AclSizeInformation,
            )
        } == 0
        {
            return Err(last_error());
        }
        let mut saw_current = false;
        let mut saw_system = false;
        for index in 0..size.AceCount {
            let mut ace = std::ptr::null_mut();
            // SAFETY: `index` is within the ACL count and `ace` output is valid.
            if unsafe { GetAce(dacl, index, &mut ace) } == 0 {
                return Err(last_error());
            }
            let ace = ace.cast::<ACCESS_ALLOWED_ACE>();
            // SAFETY: every ACE begins with ACCESS_ALLOWED_ACE's header layout; only allowed ACEs
            // use the following SID layout, and other types are rejected before reading it.
            if unsafe { (*ace).Header.AceType } != ACCESS_ALLOWED_ACE_TYPE {
                return Err(io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    "DACL contains a non-allow ACE",
                ));
            }
            let sid = unsafe { (&(*ace).SidStart as *const u32).cast_mut().cast() };
            // SAFETY: `sid` points to the valid SID immediately following ACCESS_ALLOWED_ACE.
            if unsafe { EqualSid(sid, current) } != 0 {
                saw_current = true;
            } else if unsafe { EqualSid(sid, system) } != 0 {
                saw_system = true;
            } else {
                return Err(io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    "DACL grants another principal access",
                ));
            }
        }
        if saw_current && saw_system {
            Ok(())
        } else {
            Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "DACL lacks required private entries",
            ))
        }
    }
}
