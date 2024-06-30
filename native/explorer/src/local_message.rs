use std::sync::Mutex;

use rustler::env::SavedTerm;
use rustler::{Env, LocalPid, OwnedEnv, ResourceArc, Term};

pub struct LocalMessage {
    pid: LocalPid,
    env: Mutex<OwnedEnv>,
    term: Mutex<SavedTerm>,
}

unsafe impl Sync for LocalMessage {}

impl LocalMessage {
    pub fn new(pid: LocalPid, term: Term<'_>) -> Self {
        let env = OwnedEnv::new();
        let term = env.save(term);
        Self {
            pid,
            env: Mutex::new(env),
            term: Mutex::new(term),
        }
    }
}

impl Drop for LocalMessage {
    fn drop(&mut self) {
        // Since these mutexes are never locked by anything but this drop, both `try_lock`s should
        // always succeed.
        if let Ok(mut env) = self.env.try_lock() {
            if let Ok(term) = self.term.try_lock() {
                // If the send fails, the other process is gone.
                env.run(|env| {
                    let term = term.load(env);
                    let _ = env.send(&self.pid, term);
                });
                let _ = env.send_and_clear(&self.pid, |env: Env| term.load(env));
            }
        }
    }
}

#[rustler::nif]
pub fn message_on_gc(pid: LocalPid, term: Term<'_>) -> ResourceArc<LocalMessage> {
    ResourceArc::new(LocalMessage::new(pid, term))
}

#[rustler::nif]
pub fn is_message_on_gc(term: Term<'_>) -> bool {
    term.decode::<ResourceArc<LocalMessage>>().is_ok()
}
