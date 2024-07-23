use std::{fs::File, io};

use crate::ExplorerError;
use crate::datatypes::ExS3Entry;
use rustler::types::LocalPid;
use rustler::Env;

#[rustler::nif(schedule = "DirtyIo")]
pub fn fifo_file_to_cloud(
    env: Env,
    filename: &str,
    _ex_s3_entry: ExS3Entry,
    writer_task_pid: LocalPid,
) -> Result<(), ExplorerError> {
    let mut fifo_file = File::open(filename)?;
    let mut target = File::open("/tmp/my_target.parquet")?;

    while LocalPid::is_alive(writer_task_pid, env) {
        match io::copy(&mut fifo_file, &mut target) {
           Ok(_) => continue,
           Err(_) => break,
        };
    }

    Ok(())
}
