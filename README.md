## Coverage reports
Install tools:
```shell
rustup component add llvm-tools-preview
cargo install grcov
```

Then run the build with coverage turned on (source mode):
```shell
RUSTFLAGS="-C instrument-coverage" LLVM_PROFILE_FILE='coverage-%p-%m.profraw' cargo test
```

Generate HTML Report:
```shell
grcov . -s . --binary-path ./target/debug/ -t html --branch --ignore-not-existing -o coverage
```

To see the report open the `index.html` file inside the new coverage directory.
