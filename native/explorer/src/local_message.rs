use rustler::env::SavedTerm;
use rustler::{Env, LocalPid, OwnedEnv, Resource, ResourceArc, Term};

pub struct LocalMessage {
    pid: LocalPid,
    env: OwnedEnv,
    term: SavedTerm,
}

impl LocalMessage {
    pub fn new(pid: LocalPid, term: Term<'_>) -> Self {
        let env = OwnedEnv::new();
        let term = env.save(term);
        Self { pid, env, term }
    }
}

// This is only safe because we only use the object for its destructor
unsafe impl Sync for LocalMessage {}

#[rustler::resource_impl]
impl Resource for LocalMessage {
    fn destructor(self, env: Env) {
        let owned_env = self.env;

        // If the send fails, the other process is gone.
        owned_env.run(|owned_env| {
            let term = self.term.load(owned_env);
            let _ = env.send(&self.pid, term);
        });
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
