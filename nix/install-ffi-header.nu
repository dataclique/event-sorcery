# Install the generated header using Cargo's reported build-script output.
def main [build_log: path, package_id: string, output: path] {
    let directories = (
        open --raw $build_log
        | lines
        | each { |line|
            let message = (try { $line | from json } catch { null })
            match $message {
                {reason: 'build-script-executed', package_id: $identity, out_dir: $directory}
                    if $identity == $package_id => $directory
                _ => null
            }
        }
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
