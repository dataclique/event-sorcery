use std/assert

def main [installer: path, work: path] {
    let package = 'path+file:///workspace/crates/event-sorcery-ffi#0.5.0'
    let header = "#ifndef EVENT_SORCERY_H\n#define EVENT_SORCERY_H\n#endif\n"

    for case in ([
        'target/release/build/event-sorcery-ffi-abcdef/out'
        'target/release/build/event-sorcery-ffi/abcdef/out'
    ] | enumerate) {
        let root = ($work | path join $'case-($case.index)' | path expand)
        let generated = ($root | path join $case.item)
        mkdir $generated
        $header | save --force ($generated | path join 'event_sorcery.h')
        let log = ($root | path join 'cargo.jsonl')
        [
            {reason: 'compiler-artifact', package_id: $package}
            {reason: 'build-script-executed', package_id: 'other-package', out_dir: '/unused'}
            {reason: 'build-script-executed', package_id: $package, out_dir: $generated}
            {reason: 'build-script-executed', package_id: $package, out_dir: $generated}
        ]
        | each { to json --raw }
        | prepend ['procedural macro output' '123' '"scalar"' '[]' 'null']
        | str join (char newline)
        | save --force $log
        let output = ($root | path join 'installed')

        let result = (^nu $installer $log $package $output | complete)
        assert equal $result.exit_code 0 $result.stderr
        assert equal (open --raw ($output | path join 'include/event_sorcery.h')) $header

        let absent = (^nu $installer $log 'missing-package' ($root | path join 'absent') | complete)
        assert equal $absent.exit_code 1
        assert equal ($root | path join 'absent/include/event_sorcery.h' | path exists) false

        let ambiguous = ($root | path join 'ambiguous.jsonl')
        [
            {reason: 'build-script-executed', package_id: $package, out_dir: $generated}
            {reason: 'build-script-executed', package_id: $package, out_dir: '/different'}
        ] | each { to json --raw } | str join (char newline) | save --force $ambiguous
        let result = (^nu $installer $ambiguous $package ($root | path join 'ambiguous') | complete)
        assert equal $result.exit_code 1
        assert equal ($root | path join 'ambiguous/include/event_sorcery.h' | path exists) false
    }
}
