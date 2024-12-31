PROFILE_DIR=prof
COVERAGE_DIR=coverage

LIB_SRC=src/lib.rs

.PHONY: build
build:
	cargo build

.PHONY: test
test:
	cargo test

.PHONY: clean
clean:
	cargo clean
	rm -rf $(PROFILE_DIR) $(COVERAGE_DIR)

prof: $(LIB_SRC)
	RUSTFLAGS="-C instrument-coverage" LLVM_PROFILE_FILE='$(PROFILE_DIR)/coverage-%p-%m.profraw' cargo test

coverage: prof
	grcov $(PROFILE_DIR) -s . --binary-path ./target/debug/ -t html --branch --ignore-not-existing -o $(COVERAGE_DIR) --llvm --keep-only src/lib.rs
