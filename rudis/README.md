# A Toy Redis Implement in Rust

## Get started
**start the server**
```bash
RUST_LOG=debug cargo run --bin redust-server
````

**Try cli**

```bash
RUST_LOG=debug cargo run --bin redust-cli set hello world
RUST_LOG=debug cargo run --bin redust-cli get hello
```

