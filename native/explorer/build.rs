fn main() {
    cc::Build::new()
        .file("src/local_message.c")
        .compile("local_message");
    println!("cargo:rerun-if-changed=src/local_message.c");
}
