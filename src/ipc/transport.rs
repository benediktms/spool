//! Platform-local IPC transport. The wire framing lives above this module.

use std::io;
use std::path::Path;

#[cfg(unix)]
pub type Listener = tokio::net::UnixListener;
#[cfg(unix)]
pub type Stream = tokio::net::UnixStream;

#[cfg(unix)]
pub fn bind(socket: &Path) -> io::Result<Listener> {
    tokio::net::UnixListener::bind(socket)
}

#[cfg(unix)]
pub async fn accept(listener: &mut Listener) -> io::Result<Stream> {
    listener.accept().await.map(|(stream, _)| stream)
}

#[cfg(unix)]
pub async fn connect(socket: &Path) -> io::Result<Stream> {
    tokio::net::UnixStream::connect(socket).await
}

#[cfg(unix)]
pub fn endpoint(socket: &Path) -> io::Result<String> {
    Ok(std::path::absolute(socket)?.display().to_string())
}

#[cfg(windows)]
mod windows {
    use super::*;
    use crate::config::Paths;
    use crate::perms;
    use std::ffi::c_void;
    use std::mem;
    use std::os::windows::io::AsRawHandle;

    use std::time::Duration;
    use tokio::net::windows::named_pipe::{ClientOptions, NamedPipeServer, ServerOptions};
    use windows_sys::Win32::Foundation::{
        CloseHandle, ERROR_ALREADY_EXISTS, GetLastError, HANDLE, LocalFree, WAIT_OBJECT_0,
        WAIT_TIMEOUT,
    };
    use windows_sys::Win32::Security::Authorization::{
        ConvertStringSecurityDescriptorToSecurityDescriptorW, SDDL_REVISION_1,
    };
    use windows_sys::Win32::Security::{PSECURITY_DESCRIPTOR, SECURITY_ATTRIBUTES};
    use windows_sys::Win32::System::Threading::{
        CreateEventW, EVENT_MODIFY_STATE, OpenEventW, SetEvent, WaitForSingleObject,
    };

    pub type Stream = NamedPipeServer;

    pub struct Listener {
        name: String,
        sddl: String,
        server: NamedPipeServer,
    }

    pub fn endpoint(socket: &Path) -> io::Result<String> {
        let absolute = std::path::absolute(socket)?;
        let digest = blake3::hash(absolute.to_string_lossy().as_bytes()).to_hex();
        Ok(format!(r"\\.\pipe\spool-{}", &digest[..32]))
    }

    fn event_name(socket: &Path) -> io::Result<String> {
        let absolute = std::path::absolute(socket)?;
        let digest = blake3::hash(absolute.to_string_lossy().as_bytes()).to_hex();
        Ok(format!(
            r"Global\spool-{}-{}-shutdown",
            perms::current_user_sid()?,
            &digest[..32]
        ))
    }

    fn sddl() -> io::Result<String> {
        Ok(format!(
            "D:P(A;;GA;;;{})(A;;GA;;;SY)",
            perms::current_user_sid()?
        ))
    }

    struct SecurityDescriptor(PSECURITY_DESCRIPTOR);

    impl SecurityDescriptor {
        fn from_sddl(sddl: &str) -> io::Result<Self> {
            let wide: Vec<u16> = sddl.encode_utf16().chain(Some(0)).collect();
            let mut descriptor = std::ptr::null_mut();
            // SAFETY: `wide` is NUL-terminated for the call and Windows allocates the returned
            // descriptor, which this type releases with LocalFree.
            if unsafe {
                ConvertStringSecurityDescriptorToSecurityDescriptorW(
                    wide.as_ptr(),
                    SDDL_REVISION_1,
                    &mut descriptor,
                    std::ptr::null_mut(),
                )
            } == 0
            {
                return Err(io::Error::last_os_error());
            }
            Ok(Self(descriptor))
        }
    }

    impl Drop for SecurityDescriptor {
        fn drop(&mut self) {
            // SAFETY: ConvertStringSecurityDescriptorToSecurityDescriptorW allocated this pointer.
            unsafe { LocalFree(self.0.cast()) };
        }
    }

    fn attributes(sddl: &str) -> io::Result<(SecurityDescriptor, SECURITY_ATTRIBUTES)> {
        let descriptor = SecurityDescriptor::from_sddl(sddl)?;
        let attrs = SECURITY_ATTRIBUTES {
            nLength: mem::size_of::<SECURITY_ATTRIBUTES>() as u32,
            lpSecurityDescriptor: descriptor.0.cast(),
            bInheritHandle: 0,
        };
        Ok((descriptor, attrs))
    }

    impl Listener {
        fn create(&self, first: bool) -> io::Result<NamedPipeServer> {
            let (_descriptor, mut attrs) = attributes(&self.sddl)?;
            let mut options = ServerOptions::new();
            options
                .first_pipe_instance(first)
                .reject_remote_clients(true);
            // SAFETY: `attrs` and its descriptor remain valid for the synchronous creation call;
            // Windows copies the security descriptor into the named-pipe object.
            unsafe {
                options.create_with_security_attributes_raw(
                    &self.name,
                    (&mut attrs as *mut SECURITY_ATTRIBUTES).cast::<c_void>(),
                )
            }
        }
    }

    pub fn bind(socket: &Path) -> io::Result<Listener> {
        let name = endpoint(socket)?;
        let sddl = sddl()?;
        let mut options = ServerOptions::new();
        options
            .first_pipe_instance(true)
            .reject_remote_clients(true);
        let (_descriptor, mut attrs) = attributes(&sddl)?;
        // SAFETY: `attrs` and its descriptor remain valid for this synchronous creation call;
        // Windows copies the security descriptor into the named-pipe object.
        let server = unsafe {
            options.create_with_security_attributes_raw(
                &name,
                (&mut attrs as *mut SECURITY_ATTRIBUTES).cast::<c_void>(),
            )
        }?;
        Ok(Listener { name, sddl, server })
    }
    pub async fn accept(listener: &mut Listener) -> io::Result<Stream> {
        listener.server.connect().await?;
        let next = listener.create(false)?;
        Ok(mem::replace(&mut listener.server, next))
    }

    pub async fn connect(
        socket: &Path,
    ) -> io::Result<tokio::net::windows::named_pipe::NamedPipeClient> {
        let name = endpoint(socket)?;
        let mut last = None;
        for _ in 0..50 {
            match ClientOptions::new().open(&name) {
                Ok(client) => {
                    if !perms::is_current_user_handle(client.as_raw_handle().cast())? {
                        return Err(io::Error::new(
                            io::ErrorKind::PermissionDenied,
                            "named pipe server is not owned by the current user",
                        ));
                    }
                    return Ok(client);
                }
                Err(error) if error.raw_os_error() == Some(231) => last = Some(error),
                Err(error) => return Err(error),
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        Err(last.unwrap_or_else(|| io::Error::other("named pipe remained busy")))
    }

    struct Event(HANDLE);

    impl Event {
        fn is_signaled(&self) -> io::Result<bool> {
            // SAFETY: Event owns this valid handle for the duration of the wait.
            match unsafe { WaitForSingleObject(self.0, 0) } {
                WAIT_OBJECT_0 => Ok(true),
                WAIT_TIMEOUT => Ok(false),
                _ => Err(io::Error::last_os_error()),
            }
        }
    }

    impl Drop for Event {
        fn drop(&mut self) {
            // SAFETY: Event owns this valid handle until it is dropped.
            unsafe { CloseHandle(self.0) };
        }
    }

    fn create_shutdown_event(paths: &Paths) -> io::Result<Event> {
        let name = event_name(&paths.socket)?;
        let sddl = sddl()?;
        let (_descriptor, attrs) = attributes(&sddl)?;
        let wide: Vec<u16> = name.encode_utf16().chain(Some(0)).collect();
        // SAFETY: attributes and name remain valid for this synchronous call; Event takes the
        // returned owned handle on success.
        let event = unsafe { CreateEventW(&attrs, 1, 0, wide.as_ptr()) };
        if event.is_null() {
            return Err(io::Error::last_os_error());
        }
        // A surviving event means another daemon already owns this endpoint; never join it.
        // SAFETY: `event` is a valid owned handle returned by CreateEventW.
        if unsafe { GetLastError() } == ERROR_ALREADY_EXISTS {
            // SAFETY: `event` is a valid owned handle returned by CreateEventW.
            unsafe { CloseHandle(event) };
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                "a daemon shutdown event already exists",
            ));
        }
        Ok(Event(event))
    }

    pub fn shutdown_signal(paths: &Paths) -> io::Result<impl Future<Output = io::Result<()>>> {
        let event = create_shutdown_event(paths)?;
        Ok(async move {
            loop {
                if event.is_signaled()? {
                    return Ok(());
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        })
    }

    pub fn request_shutdown(paths: &Paths) -> io::Result<()> {
        let name = event_name(&paths.socket)?;
        let wide: Vec<u16> = name.encode_utf16().chain(Some(0)).collect();
        // SAFETY: `wide` remains valid for the synchronous OpenEventW call.
        let event = unsafe { OpenEventW(EVENT_MODIFY_STATE, 0, wide.as_ptr()) };
        if event.is_null() {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: event is a valid handle returned by OpenEventW and is closed below.
        let set = unsafe { SetEvent(event) };
        let error = (set == 0).then(io::Error::last_os_error);
        // SAFETY: event is a valid owned handle returned by OpenEventW.
        unsafe { CloseHandle(event) };
        if let Some(error) = error {
            return Err(error);
        }
        Ok(())
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[tokio::test]
        async fn shutdown_future_retains_its_event_handle() {
            let temp = tempfile::tempdir().unwrap();
            let paths = Paths::from_root(temp.path().join("spool state")).unwrap();
            let signal = shutdown_signal(&paths).unwrap();
            request_shutdown(&paths).unwrap();
            assert!(
                tokio::time::timeout(Duration::from_secs(1), signal)
                    .await
                    .unwrap()
                    .is_ok()
            );
        }
    }
}

#[cfg(windows)]
pub use windows::*;
