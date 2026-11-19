//! Track the SQL inputs embedded by the migrator.

fn main() {
    println!("cargo:rerun-if-changed=migrations");
}
