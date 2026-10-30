#![cfg_attr(windows, windows_subsystem = "windows")]

use std::path::PathBuf;
use std::process::{Command, Stdio};

fn main() {
    let mut args = std::env::args_os();
    let _program = args.next();
    let (Some(binary), Some(home), None) = (args.next(), args.next(), args.next()) else {
        fail("usage: spool-service <absolute-spool-path> <absolute-spool-home>");
    };
    let binary = PathBuf::from(binary);
    let home = PathBuf::from(home);
    if !binary.is_absolute() || !home.is_absolute() {
        fail("spool-service paths must be absolute");
    }

    let mut command = Command::new(&binary);
    command
        .args(["daemon", "run"])
        .env("SPOOL_HOME", &home)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    #[cfg(windows)]
    {
        use std::os::windows::process::CommandExt;
        command.creation_flags(windows_sys::Win32::System::Threading::CREATE_NO_WINDOW);
    }

    match command.status() {
        Ok(status) => std::process::exit(status.code().unwrap_or(1)),
        Err(error) => fail(&format!("starting {}: {error}", binary.display())),
    }
}

fn fail(message: &str) -> ! {
    eprintln!("spool-service: {message}");
    std::process::exit(1);
}
