#include <stdint.h>
#include <stdio.h>

#ifdef __cplusplus
extern "C" {
#endif

/// ----- minimal erlang nif api header -----
typedef uint64_t ERL_NIF_TERM;
typedef void ErlNifEnv;
typedef enum {
  ERL_NIF_RT_CREATE = 1,
  ERL_NIF_RT_TAKEOVER = 2
} ErlNifResourceFlags;
typedef struct {
  ERL_NIF_TERM pid; /* internal, may change */
} ErlNifPid;

static void *message_resource;
struct message {
  ErlNifEnv *env;
  ErlNifPid pid;
  ERL_NIF_TERM value;
};
extern int enif_is_pid(ErlNifEnv *env, ERL_NIF_TERM term);
extern int enif_make_badarg(ErlNifEnv *env);
extern void *enif_open_resource_type(ErlNifEnv *env, const char *module_str,
                                     const char *name_str,
                                     void (*dtor)(ErlNifEnv *, void *),
                                     ErlNifResourceFlags flags,
                                     ErlNifResourceFlags *result);
extern ERL_NIF_TERM enif_make_atom(ErlNifEnv *env, const char *name);
extern int enif_get_local_pid(ErlNifEnv *env, ERL_NIF_TERM term,
                              ErlNifPid *pid);
ERL_NIF_TERM enif_make_tuple_from_array(ErlNifEnv *env,
                                        const ERL_NIF_TERM arr[], unsigned cnt);
extern ERL_NIF_TERM enif_make_copy(ErlNifEnv *env, ERL_NIF_TERM term);
extern void *enif_alloc_resource(void *type, size_t size);
extern ErlNifEnv *enif_alloc_env();
extern void enif_free_env(ErlNifEnv *env);
extern ERL_NIF_TERM enif_make_resource(ErlNifEnv *env, void *resource);
extern void enif_release_resource(void *resource);
int enif_send(ErlNifEnv *caller_env, ErlNifPid *to_pid, ErlNifEnv *msg_env,
              ERL_NIF_TERM msg);

/// ----- minimal erlang nif api header -----

static ERL_NIF_TERM kAtomError;

void destruct_local_message(ErlNifEnv *env, void *obj) {
  struct message *m = (struct message *)obj;
  enif_send(env, &m->pid, m->env, m->value);
  enif_free_env(m->env);
}

ERL_NIF_TERM is_message_when_gc(ErlNifEnv *env, const ERL_NIF_TERM term) {
  struct message * m = NULL;
  if (enif_get_resource(env, term, message_resource, (void **)&m)) {
    return enif_make_atom(env, "true");  
  }
  return enif_make_atom(env, "false");
}

ERL_NIF_TERM local_message_on_gc(ErlNifEnv *env, const ERL_NIF_TERM pid_term,
                                 const ERL_NIF_TERM term) {
  if (!enif_is_pid(env, pid_term)) {
    return enif_make_badarg(env);
  }
  ErlNifPid pid;
  if (!enif_get_local_pid(env, pid_term, &pid)) {
    return kAtomError;
  }

  struct message *m =
      enif_alloc_resource(message_resource, sizeof(struct message));
  if (!m) {
    return kAtomError;
  }

  m->env = enif_alloc_env();
  if (!m->env) {
    return kAtomError;
  }
  m->pid = pid;
  m->value = enif_make_copy(m->env, term);

  ERL_NIF_TERM res_term = enif_make_resource(env, m);
  enif_release_resource(m);
  return res_term;
}

void local_message_open_resource(ErlNifEnv *env) {
  void *rt =
      enif_open_resource_type(env, "Elixir.Explorer.Remote.LocalGC", "message",
                              destruct_local_message, ERL_NIF_RT_CREATE, 0);
  if (!rt)
    return;
  message_resource = rt;

  kAtomError = enif_make_atom(env, "error");
}

#ifdef __cplusplus
}
#endif
