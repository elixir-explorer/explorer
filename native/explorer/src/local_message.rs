
use rustler::{Term, LocalPid, NifStruct, ResourceArc};
use rustler_sys::enif_send;
use std::ops::Deref;

#[derive(Clone)]
pub struct LocalMessage<'a> {
    pub pid: LocalPid,
    pub term: Term<'a>,
}

impl<'a> Drop for LocalMessage<'a> {
    fn drop(&mut self) {
        let env = self.term.get_env();
        unsafe {
            enif_send(env.as_c_arg(), self.pid.as_c_arg(), env.as_c_arg(), self.term.as_c_arg());
        }
    }
}

pub struct ExLocalMessageRef<'a>(pub LocalMessage<'a>);

impl<'a> ExLocalMessageRef<'a> {
    pub fn new(m: LocalMessage<'a>) -> Self {
        Self(m)
    }
}

#[derive(NifStruct)]
#[module = "Explorer.Remote.LocalGC"]
pub struct ExLocalMessage<'a> {
    pub resource: ResourceArc<ExLocalMessageRef<'a>>,
}

impl<'a> ExLocalMessage<'a> {
    pub fn new(m: LocalMessage<'a>) -> Self {
        Self {
            resource: ResourceArc::new(ExLocalMessageRef::new(m)),
        }
    }

    // Returns a clone of the DataFrame inside the ResourceArc container.
    pub fn clone_inner(&self) -> LocalMessage<'a> {
        self.resource.0.clone()
    }
}

impl<'a> Deref for ExLocalMessage<'a> {
    type Target = LocalMessage<'a>;

    fn deref(&self) -> &Self::Target {
        &self.resource.0
    }
}

#[rustler::nif]
pub fn message_on_gc<'a>(pid: LocalPid, term: Term<'a>) -> ExLocalMessage<'a> {
    ExLocalMessage::new(LocalMessage{
        pid,
        term
    })
}
