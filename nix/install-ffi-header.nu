# Install the generated header using Cargo's reported build-script output.
def main [build_log: path, package_id: string, output: path] {
    let directories = (
        open --raw $build_log
        | lines
        | each { from json }
        | where reason? == 'build-script-executed'
        | where package_id? == $package_id
        | get out_dir
        | uniq
    )
    let count = ($directories | length)
    if $count != 1 {
        error make {msg: $'Expected one FFI build-script output directory; found ($count)'}
    }

    let header = ($directories | get 0 | path join 'event_sorcery.h')
    let destination = ($output | path join 'include')
    mkdir $destination
    cp $header ($destination | path join 'event_sorcery.h')
}
