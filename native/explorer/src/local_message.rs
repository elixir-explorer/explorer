use rustler::wrapper::{NIF_ENV, NIF_TERM};
use rustler::{Env, Term};
use std::ffi::c_void;

extern "C" {
    pub fn local_message_open_resource(env: NIF_ENV) -> c_void;
    fn local_message_on_gc(env: NIF_ENV, arg1: NIF_TERM, arg2: NIF_TERM) -> NIF_TERM;
    fn is_local_message_when_gc(env: NIF_ENV, arg1: NIF_TERM) -> NIF_TERM;
}

#[rustler::nif]
pub fn message_on_gc<'a>(env: Env<'a>, pid: Term<'a>, term: Term<'a>) -> Term<'a> {
    unsafe {
        Term::new(
            env,
            local_message_on_gc(env.as_c_arg(), pid.as_c_arg(), term.as_c_arg()),
        )
    }
}

#[rustler::nif]
pub fn is_message_when_gc<'a>(env: Env<'a>, term: Term<'a>) -> Term<'a> {
    unsafe {
        Term::new(
            env,
            is_local_message_when_gc(env.as_c_arg(), term.as_c_arg()),
        )
    }
}
