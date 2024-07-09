use std::sync::Mutex;

use rustler::env::SavedTerm;
use rustler::{Env, LocalPid, OwnedEnv, Resource, ResourceArc, Term};

pub struct LocalMessage {
    pid: LocalPid,
    env_and_term: Mutex<(OwnedEnv, SavedTerm)>,
}

impl LocalMessage {
    pub fn new(pid: LocalPid, term: Term<'_>) -> Self {
        let env = OwnedEnv::new();
        let term = env.save(term);
        Self {
            pid,
            env_and_term: Mutex::new((env, term)),
        }
    }
}

#[rustler::resource_impl]
impl Resource for LocalMessage {
    fn destructor(self, env: Env) {
        // Since this mutex is never locked by anything but this destructor, the `try_lock` should
        // always succeed.
        if let Ok(guard) = self.env_and_term.try_lock() {
            let owned_env = &guard.0;
            let term = &guard.1;

            // If the send fails, the other process is gone.
            owned_env.run(|owned_env| {
                let term = term.load(owned_env);
                let _ = env.send(&self.pid, term);
            });
        }
    }
}

#[rustler::nif]
pub fn message_on_gc(pid: LocalPid, term: Term<'_>) -> ResourceArc<LocalMessage> {
    LocalMessage::new(pid, term).into()
}

#[rustler::nif]
pub fn is_message_on_gc(term: Term<'_>) -> bool {
    term.decode::<ResourceArc<LocalMessage>>().is_ok()
}
