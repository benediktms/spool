use super::{OpenedSource, ScanError, ScanOutcome, SourceError, SourceFileInfo};
use crate::perms;
use std::{
    ffi::{OsStr, OsString, c_void},
    fs::{self, File},
    io, mem,
    os::windows::{
        ffi::{OsStrExt, OsStringExt},
        io::{AsRawHandle, FromRawHandle},
    },
    path::{Path, PathBuf},
    sync::atomic::{AtomicBool, Ordering},
};
use windows_sys::Win32::{
    Foundation::HANDLE,
    Storage::FileSystem::{
        BY_HANDLE_FILE_INFORMATION, CreateFileW, GetFileInformationByHandle, GetFileType,
    },
};

const GENERIC_READ: u32 = 0x8000_0000;
const NATIVE_FILE_GENERIC_READ: u32 = 0x0012_0089;
const FILE_SHARE_ALL: u32 = 7;
const OPEN_EXISTING: u32 = 3;
const FILE_FLAG_BACKUP_SEMANTICS: u32 = 0x0200_0000;
const FILE_FLAG_OPEN_REPARSE_POINT: u32 = 0x0020_0000;
const FILE_ATTRIBUTE_DIRECTORY: u32 = 0x10;
const FILE_ATTRIBUTE_REPARSE_POINT: u32 = 0x400;
const FILE_TYPE_DISK: u32 = 1;
const FILE_OPEN: u32 = 1;
const FILE_DIRECTORY_FILE: u32 = 1;
const FILE_NON_DIRECTORY_FILE: u32 = 0x40;
const FILE_SYNCHRONOUS_IO_NONALERT: u32 = 0x20;
const FILE_OPEN_REPARSE_POINT: u32 = 0x0020_0000;
const OBJ_CASE_INSENSITIVE: u32 = 0x40;
const FILE_ID_BOTH_DIRECTORY_INFORMATION: u32 = 37;
const STATUS_SUCCESS: i32 = 0;
const STATUS_BUFFER_OVERFLOW: i32 = -2_147_483_643;
const STATUS_NO_MORE_FILES: i32 = -2_147_483_642;
const WINDOWS_TO_UNIX_EPOCH_100NS: i128 = 116_444_736_000_000_000;

#[repr(C)]
struct UnicodeString {
    _length: u16,
    _maximum_length: u16,
    _buffer: *mut u16,
}

#[repr(C)]
struct ObjectAttributes {
    _length: u32,
    _root_directory: HANDLE,
    _object_name: *mut UnicodeString,
    _attributes: u32,
    _security_descriptor: *mut c_void,
    _security_quality_of_service: *mut c_void,
}

#[repr(C)]
struct IoStatusBlock {
    _status: i32,
    information: usize,
}

#[repr(C)]
struct FileIdBothDirectoryInfo {
    next_entry_offset: u32,
    _file_index: u32,
    _creation_time: i64,
    _last_access_time: i64,
    _last_write_time: i64,
    _change_time: i64,
    _end_of_file: i64,
    _allocation_size: i64,
    file_attributes: u32,
    file_name_length: u32,
    _ea_size: u32,
    _short_name_length: u8,
    _short_name: [u16; 12],
    _file_id: i64,
}

#[link(name = "ntdll")]
unsafe extern "system" {
    fn NtCreateFile(
        file_handle: *mut HANDLE,
        desired_access: u32,
        object_attributes: *const ObjectAttributes,
        io_status_block: *mut IoStatusBlock,
        allocation_size: *mut i64,
        file_attributes: u32,
        share_access: u32,
        create_disposition: u32,
        create_options: u32,
        ea_buffer: *mut c_void,
        ea_length: u32,
    ) -> i32;
    fn NtQueryDirectoryFile(
        file_handle: HANDLE,
        event: HANDLE,
        apc_routine: *mut c_void,
        apc_context: *mut c_void,
        io_status_block: *mut IoStatusBlock,
        file_information: *mut c_void,
        length: u32,
        file_information_class: u32,
        return_single_entry: u8,
        file_name: *mut UnicodeString,
        restart_scan: u8,
    ) -> i32;
    fn RtlNtStatusToDosError(status: i32) -> u32;
}

struct Root {
    path: PathBuf,
    file: File,
}

pub struct SourceRoots {
    roots: Vec<Root>,
}

struct FileDetails {
    attributes: u32,
    len: u64,
    mtime_ms: i64,
    dev: u64,
    ino: u64,
}

impl SourceRoots {
    pub fn new(paths: &[PathBuf]) -> io::Result<Self> {
        let mut roots = Vec::with_capacity(paths.len());
        for path in paths {
            // Resolve the configured root once, then hold and validate that resolved directory.
            // Descendants are never reopened through this pathname.
            let canonical = fs::canonicalize(path)?;
            let file = open_root(&canonical)?;
            roots.push(Root {
                path: canonical,
                file,
            });
        }
        roots.sort_by(|a, b| a.path.cmp(&b.path));
        let mut filtered = Vec::with_capacity(roots.len());
        for root in roots {
            if !filtered
                .iter()
                .any(|parent: &Root| root.path.starts_with(&parent.path))
            {
                filtered.push(root);
            }
        }
        Ok(Self { roots: filtered })
    }

    pub fn scan(&self, cancel: &AtomicBool, max_entries: usize) -> ScanOutcome {
        self.scan_from(cancel, max_entries, None)
    }

    /// Yields only regular *.jsonl files: every supported transcript format is JSONL; anything else under a root is a session artifact, not a session.
    pub fn scan_from(
        &self,
        cancel: &AtomicBool,
        max_entries: usize,
        after: Option<&Path>,
    ) -> ScanOutcome {
        let mut out = ScanOutcome::default();
        let page_limit = max_entries.max(1);
        for root in &self.roots {
            if cancel.load(Ordering::Relaxed) {
                break;
            }
            if let Err(kind) =
                self.walk_dir(&root.file, &root.path, after, page_limit, cancel, &mut out)
            {
                out.errors.push(ScanError {
                    path: root.path.clone(),
                    kind,
                });
            }
            if out.truncated {
                break;
            }
        }
        if out.truncated {
            out.next_after = out.files.last().map(|file| file.path.clone());
        } else {
            out.complete = !cancel.load(Ordering::Relaxed) && out.errors.is_empty();
        }
        out
    }

    fn walk_dir(
        &self,
        dir_file: &File,
        dir: &Path,
        after: Option<&Path>,
        limit: usize,
        cancel: &AtomicBool,
        out: &mut ScanOutcome,
    ) -> Result<(), io::ErrorKind> {
        if cancel.load(Ordering::Relaxed) {
            return Ok(());
        }
        let mut entries = read_dir(dir_file).map_err(|error| error.kind())?;
        entries.sort_unstable_by(|a, b| a.0.cmp(&b.0));
        for (name, attributes) in entries {
            if cancel.load(Ordering::Relaxed) {
                return Ok(());
            }
            if name == OsStr::new(".") || name == OsStr::new("..") {
                continue;
            }
            let path = dir.join(&name);
            if attributes & FILE_ATTRIBUTE_REPARSE_POINT != 0 {
                continue;
            }
            if attributes & FILE_ATTRIBUTE_DIRECTORY != 0 {
                if !after.is_none_or(|cursor| path.as_path() > cursor || cursor.starts_with(&path))
                {
                    continue;
                }
                let child = match open_relative(dir_file, &name, true) {
                    Ok(file) => file,
                    Err(error) => {
                        out.errors.push(scan_error(path, error));
                        continue;
                    }
                };
                self.walk_dir(&child, &path, after, limit, cancel, out)?;
                if out.truncated || cancel.load(Ordering::Relaxed) {
                    return Ok(());
                }
            } else if path.extension() == Some(OsStr::new("jsonl"))
                && after.is_none_or(|cursor| path.as_path() > cursor)
            {
                if out.files.len() == limit {
                    out.truncated = true;
                    return Ok(());
                }
                match open_relative(dir_file, &name, false).and_then(|file| {
                    source_info(&file, path.clone()).map(|info| OpenedSource { file, info })
                }) {
                    Ok(opened) => out.files.push(opened.info),
                    Err(error) => out.errors.push(scan_error(path, error)),
                }
            }
        }
        Ok(())
    }

    pub fn open(&self, path: &Path) -> Result<OpenedSource, SourceError> {
        for root in &self.roots {
            let Ok(relative) = path.strip_prefix(&root.path) else {
                continue;
            };
            if relative
                .components()
                .any(|component| !matches!(component, std::path::Component::Normal(_)))
            {
                return Err(SourceError::OutsideRoot);
            }
            let mut parts = relative.components().peekable();
            let Some(first) = parts.next() else {
                return Err(SourceError::UnsafeFile);
            };
            let mut current = open_relative(&root.file, first.as_os_str(), parts.peek().is_some())?;
            while let Some(component) = parts.next() {
                current = open_relative(&current, component.as_os_str(), parts.peek().is_some())?;
            }
            return Ok(OpenedSource {
                info: source_info(&current, path.to_owned())?,
                file: current,
            });
        }
        Err(SourceError::OutsideRoot)
    }
}

fn open_root(path: &Path) -> io::Result<File> {
    let mut wide: Vec<u16> = path.as_os_str().encode_wide().collect();
    if wide.contains(&0) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "source root contains NUL",
        ));
    }
    wide.push(0);
    // SAFETY: `wide` is NUL-terminated and remains live for the call; the remaining arguments
    // are either valid constants or null optional pointers.
    let handle = unsafe {
        CreateFileW(
            wide.as_ptr(),
            GENERIC_READ,
            FILE_SHARE_ALL,
            std::ptr::null(),
            OPEN_EXISTING,
            FILE_FLAG_BACKUP_SEMANTICS | FILE_FLAG_OPEN_REPARSE_POINT,
            std::ptr::null_mut(),
        )
    };
    if handle as isize == -1 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: `CreateFileW` returned a valid owned handle, transferred to `File` exactly once.
    let file = unsafe { File::from_raw_handle(handle) };
    let details = file_details(&file)?;
    if details.attributes & (FILE_ATTRIBUTE_DIRECTORY | FILE_ATTRIBUTE_REPARSE_POINT)
        != FILE_ATTRIBUTE_DIRECTORY
        || file_type(&file)? != FILE_TYPE_DISK
        || !perms::is_current_user(&file)?
    {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "source root must be a current-user non-reparse directory",
        ));
    }
    Ok(file)
}

fn open_relative(parent: &File, name: &OsStr, directory: bool) -> Result<File, SourceError> {
    let mut wide: Vec<u16> = name.encode_wide().collect();
    if wide.is_empty() || wide.contains(&0) {
        return Err(SourceError::Io(io::Error::new(
            io::ErrorKind::InvalidInput,
            "invalid source path component",
        )));
    }
    let byte_len = wide
        .len()
        .checked_mul(mem::size_of::<u16>())
        .and_then(|len| u16::try_from(len).ok())
        .ok_or_else(|| {
            SourceError::Io(io::Error::new(
                io::ErrorKind::InvalidInput,
                "source name too long",
            ))
        })?;
    let mut unicode = UnicodeString {
        _length: byte_len,
        _maximum_length: byte_len,
        _buffer: wide.as_mut_ptr(),
    };
    let attributes = ObjectAttributes {
        _length: mem::size_of::<ObjectAttributes>() as u32,
        _root_directory: parent.as_raw_handle(),
        _object_name: &mut unicode,
        _attributes: OBJ_CASE_INSENSITIVE,
        _security_descriptor: std::ptr::null_mut(),
        _security_quality_of_service: std::ptr::null_mut(),
    };
    let mut handle = std::ptr::null_mut();
    let mut status = IoStatusBlock {
        _status: 0,
        information: 0,
    };
    let options = FILE_SYNCHRONOUS_IO_NONALERT
        | FILE_OPEN_REPARSE_POINT
        | if directory {
            FILE_DIRECTORY_FILE
        } else {
            FILE_NON_DIRECTORY_FILE
        };
    // SAFETY: `parent` remains live; `unicode` borrows `wide`, and all output and attribute
    // storage remain valid for this synchronous call. A successful result transfers one handle.
    let result = unsafe {
        NtCreateFile(
            &mut handle,
            NATIVE_FILE_GENERIC_READ,
            &attributes,
            &mut status,
            std::ptr::null_mut(),
            0,
            FILE_SHARE_ALL,
            FILE_OPEN,
            options,
            std::ptr::null_mut(),
            0,
        )
    };
    if result < 0 {
        return Err(SourceError::Io(nt_error(result)));
    }
    // SAFETY: `NtCreateFile` returned one valid owned handle.
    let file = unsafe { File::from_raw_handle(handle) };
    let details = file_details(&file)?;
    if details.attributes & FILE_ATTRIBUTE_REPARSE_POINT != 0
        || file_type(&file)? != FILE_TYPE_DISK
        || !perms::is_current_user(&file)?
        || (directory != (details.attributes & FILE_ATTRIBUTE_DIRECTORY != 0))
    {
        return Err(SourceError::UnsafeFile);
    }
    Ok(file)
}

fn source_info(file: &File, path: PathBuf) -> Result<SourceFileInfo, SourceError> {
    let details = file_details(file)?;
    if details.attributes & (FILE_ATTRIBUTE_DIRECTORY | FILE_ATTRIBUTE_REPARSE_POINT) != 0
        || file_type(file)? != FILE_TYPE_DISK
        || !perms::is_current_user(file)?
    {
        return Err(SourceError::UnsafeFile);
    }
    Ok(SourceFileInfo {
        path,
        len: details.len,
        mtime_ms: details.mtime_ms,
        dev: details.dev,
        ino: details.ino,
    })
}

fn scan_error(path: PathBuf, error: SourceError) -> ScanError {
    ScanError {
        path,
        kind: match error {
            SourceError::Io(error) => error.kind(),
            SourceError::OutsideRoot | SourceError::UnsafeFile => io::ErrorKind::PermissionDenied,
        },
    }
}

fn file_details(file: &File) -> io::Result<FileDetails> {
    let mut info = mem::MaybeUninit::<BY_HANDLE_FILE_INFORMATION>::zeroed();
    // SAFETY: `file` owns a live handle and `info` is valid writable storage for the API result.
    if unsafe { GetFileInformationByHandle(file.as_raw_handle(), info.as_mut_ptr()) } == 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: a successful `GetFileInformationByHandle` fully initialized `info`.
    let info = unsafe { info.assume_init() };
    let ticks = (u64::from(info.ftLastWriteTime.dwHighDateTime) << 32)
        | u64::from(info.ftLastWriteTime.dwLowDateTime);
    let unix_100ns = i128::from(ticks) - WINDOWS_TO_UNIX_EPOCH_100NS;
    let milliseconds = if unix_100ns >= 0 {
        unix_100ns / 10_000
    } else {
        (unix_100ns - 9_999) / 10_000
    };
    Ok(FileDetails {
        attributes: info.dwFileAttributes,
        len: (u64::from(info.nFileSizeHigh) << 32) | u64::from(info.nFileSizeLow),
        mtime_ms: milliseconds.clamp(i128::from(i64::MIN), i128::from(i64::MAX)) as i64,
        dev: u64::from(info.dwVolumeSerialNumber),
        ino: (u64::from(info.nFileIndexHigh) << 32) | u64::from(info.nFileIndexLow),
    })
}

fn file_type(file: &File) -> io::Result<u32> {
    // SAFETY: `file` owns a live handle accepted by `GetFileType`.
    let kind = unsafe { GetFileType(file.as_raw_handle()) };
    if kind == 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(kind)
}

fn read_dir(dir: &File) -> io::Result<Vec<(OsString, u32)>> {
    let mut entries = Vec::new();
    let mut buffer = [0u64; 8192];
    let mut restart = 1;
    loop {
        let mut status = IoStatusBlock {
            _status: 0,
            information: 0,
        };
        // SAFETY: `dir` is a held directory handle; the aligned buffer is writable for its full
        // size, and all optional asynchronous arguments are null for this synchronous query.
        let result = unsafe {
            NtQueryDirectoryFile(
                dir.as_raw_handle(),
                std::ptr::null_mut(),
                std::ptr::null_mut(),
                std::ptr::null_mut(),
                &mut status,
                buffer.as_mut_ptr().cast(),
                mem::size_of_val(&buffer) as u32,
                FILE_ID_BOTH_DIRECTORY_INFORMATION,
                0,
                std::ptr::null_mut(),
                restart,
            )
        };
        restart = 0;
        if result == STATUS_NO_MORE_FILES {
            return Ok(entries);
        }
        if result != STATUS_SUCCESS && result != STATUS_BUFFER_OVERFLOW {
            return Err(nt_error(result));
        }
        let used = status.information;
        if used == 0 || used > mem::size_of_val(&buffer) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid directory result",
            ));
        }
        let mut offset = 0usize;
        while offset < used {
            let remaining = used - offset;
            let alignment = mem::align_of::<FileIdBothDirectoryInfo>();
            if remaining < mem::size_of::<FileIdBothDirectoryInfo>()
                || !offset.is_multiple_of(alignment)
            {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "short directory entry",
                ));
            }
            // SAFETY: `buffer` is aligned to eight bytes; bounds above guarantee the fixed header
            // is present. Windows supplies entries in this documented native layout.
            let entry = unsafe {
                &*(buffer
                    .as_ptr()
                    .cast::<u8>()
                    .add(offset)
                    .cast::<FileIdBothDirectoryInfo>())
            };
            let next = usize::try_from(entry.next_entry_offset).map_err(|_| {
                io::Error::new(io::ErrorKind::InvalidData, "invalid directory offset")
            })?;
            let entry_len = if next == 0 { remaining } else { next };
            let name_offset = mem::size_of::<FileIdBothDirectoryInfo>();
            let name_len = usize::try_from(entry.file_name_length).map_err(|_| {
                io::Error::new(io::ErrorKind::InvalidData, "invalid directory name length")
            })?;
            if entry_len < name_offset
                || entry_len > remaining
                || (next != 0 && next % alignment != 0)
                || name_len % mem::size_of::<u16>() != 0
                || name_len > entry_len - name_offset
            {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "invalid directory entry",
                ));
            }
            // SAFETY: the entry bounds above cover the UTF-16 file-name bytes supplied by Windows.
            let name = unsafe {
                let file_name = buffer
                    .as_ptr()
                    .cast::<u8>()
                    .add(offset + name_offset)
                    .cast();
                OsString::from_wide(std::slice::from_raw_parts(
                    file_name,
                    name_len / mem::size_of::<u16>(),
                ))
            };
            entries.push((name, entry.file_attributes));
            if next == 0 {
                break;
            }
            offset = offset.checked_add(next).ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidData, "directory offset overflow")
            })?;
        }
    }
}

fn nt_error(status: i32) -> io::Error {
    // SAFETY: `RtlNtStatusToDosError` accepts every NTSTATUS value and has no borrowed pointers.
    let code = unsafe { RtlNtStatusToDosError(status) };
    io::Error::from_raw_os_error(i32::try_from(code).unwrap_or(i32::MAX))
}
