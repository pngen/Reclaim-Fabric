//! Reclaim Fabric binary entry point.

use std::process::ExitCode;

fn main() -> ExitCode {
    let json_errors = std::env::args_os().any(|arg| arg == "--json");
    match reclaim_fabric::cli::run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            if json_errors {
                eprintln!(
                    "{}",
                    serde_json::json!({
                        "ok": false,
                        "error": {
                            "class": e.class(),
                            "message": e.to_string(),
                        }
                    })
                );
            } else {
                eprintln!("error: {e}");
            }
            ExitCode::FAILURE
        }
    }
}
