# Implementation Guide

## Development Environment

### Language Configuration
- **Language**: Zig v0.16 (latest release candidate)
- **Build System**: Zig build system (`build.zig`)
- **Documentation**: https://ziglang.org/documentation/0.14.1/ (use as reference, adapt for v0.16 changes)
- **Project Structure**: Multi-module with shared types

### Build Environment
- **Nix flake**: Reproducible development environment with Zig toolchain
- **Required**: Zig compiler v0.16.x
- **Development shell**: `nix develop` or `direnv allow`

### Dependency Management
- **Minimal external dependencies**: Prefer Zig standard library
- **build.zig.zon**: Package manifest for any external dependencies
- **Vendoring**: Acceptable for small, stable dependencies

## Coding Standards

### Code Style

#### Zig Naming Conventions
- `camelCase` for functions and methods
- `PascalCase` for types, structs, enums, unions
- `snake_case` for local variables, parameters, and struct fields
- `SCREAMING_SNAKE_CASE` for compile-time constants
- Prefix unused variables with `_`

#### Zig Idioms
- Explicit is better than implicit — no hidden control flow or allocations
- Prefer `comptime` for compile-time known values and zero-cost abstractions
- Use slices (`[]T`) over pointers-to-arrays when size is runtime-known
- Prefer tagged unions over boolean flags for state machines
- Use `std.meta` and `@typeInfo` for generic programming when appropriate
- Use sentinel-terminated slices (`[:0]const u8`) for C interop

#### Memory Management
- **All allocations through `std.mem.Allocator` parameter** — never use a global allocator
- Document which functions allocate and caller's responsibility to free
- Use `defer` for deterministic cleanup
- Use `errdefer` for cleanup on error paths
- Prefer arena allocators for request-scoped or batch operations
- Use `std.testing.allocator` in tests to detect memory leaks
- Consider fixed-buffer allocators for bounded operations

#### Error Handling
- Use error unions (`!T`) for all fallible operations
- Define specific error sets per module — avoid `anyerror`
- Use `try` for propagation, `catch` for recovery
- Use `errdefer` for cleanup on error paths
- Return errors to callers — don't silently swallow failures
- Provide error context through structured error types or logging

#### Code Organization
- Keep public API surface minimal — default to private, use `pub` judiciously
- Group related types and functions in the same file
- One concept per file — avoid monolithic source files
- Export module API through a root file that re-exports public symbols
- Co-locate tests with the code they test (inline `test` blocks)

### File I/O Patterns
- Use `std.fs` for all file operations
- Prefer `pread`/`pwrite` for concurrent access to shared files
- Always handle partial reads/writes
- Use `std.posix.fsync` or `std.posix.fdatasync` for durability
- Memory-map files with `std.posix.mmap` for read-heavy workloads
- Close file descriptors in `defer` blocks immediately after opening

### Networking Patterns
- Use `std.net` for TCP and `std.net.Address` for Unix domain sockets
- Non-blocking I/O with poll-based event loop
- Handle `EAGAIN`/`EWOULDBLOCK` gracefully
- Implement connection timeouts and keep-alive
- Buffer management: use fixed-size ring buffers for I/O

### Serialization
- Start with JSON (`std.json`) for human-readable data
- Design protocol messages with binary encoding in mind for future migration
- Use packed structs or manual byte-level encoding for wire format
- Always validate untrusted input — never trust message sizes or offsets
- Include version fields in wire formats for forward compatibility

## Documentation Standards

- Document all public functions with doc comments (`///`)
- Include parameter descriptions and return value behavior
- Document allocation behavior: does this function allocate? Who frees?
- Document error conditions for each error in the error set
- Provide usage examples for non-trivial APIs
- Use `//` for implementation notes, `///` for API documentation

## Development Practices

### Testing Strategy

#### Built-in Testing Framework
- Use Zig's built-in `test` blocks for unit tests
- Place tests in the same file as the code they test
- Use `std.testing.allocator` to automatically detect memory leaks
- Test both success and error paths
- Use `std.testing.expectEqual`, `std.testing.expect`, etc.

#### Testing Commands
```bash
# Run all tests
zig build test

# Run tests with specific filter
zig build test -- --test-filter "segment"

# Run tests in debug mode (default)
zig build test -Doptimize=Debug

# Run tests in release mode (catch different class of bugs)
zig build test -Doptimize=ReleaseSafe
```

#### Test Requirements
- All public functions must have corresponding tests
- Test error conditions and edge cases (empty input, boundary values, overflow)
- Verify no memory leaks with `std.testing.allocator`
- Use descriptive test names that describe the behavior being tested
- Test concurrent access patterns for shared data structures
- Integration tests for multi-component interactions (store + publisher + subscriber)

### Performance Guidelines

#### Optimization Strategy
- Profile first, optimize second — measure before changing
- Sequential I/O over random I/O
- Batch operations to amortize syscall overhead
- Use `comptime` for zero-cost abstractions
- Minimize allocations in hot paths — prefer stack or arena allocation
- Use `@prefetch` for predictable access patterns (carefully, measure impact)

#### Buffer Management
- Reuse buffers across operations
- Pre-calculate required buffer sizes to avoid reallocation
- Provide both buffer-based (non-allocating) and allocating API variants
- Use ring buffers for streaming I/O
- Document buffer size requirements in API documentation

### Build Configuration

#### build.zig Structure
- Define main executable and library targets
- Include comprehensive test step covering all modules
- Provide debug and release build modes
- Support cross-compilation for target platforms
- Keep build script simple and well-documented

### Commit Practices
- Small, focused commits with clear messages
- Separate refactoring commits from feature commits
- Test passes before every commit
- Reference Air document when implementing a specification

### Zig v0.16 API Notes (Learnings)
- **Io parameter**: All file, directory, and network operations require an `std.Io` parameter. In tests use `std.testing.io`. In `main`, accept `std.process.Init` and use `init.io`.
- **ArrayList is unmanaged**: `std.ArrayList(T)` is now the unmanaged variant. Use `.empty` to initialize, pass allocator per-call: `list.append(allocator, item)`, `list.deinit(allocator)`, `list.toOwnedSlice(allocator)`.
- **Dir operations**: `Dir.openDir`, `Dir.createFile`, `Dir.openFile`, `Dir.iterate` all require `io` parameter. For iteration, the dir must be opened with `.iterate = true`.
- **File I/O**: Use `file.writePositionalAll(io, bytes, offset)` and `file.readPositionalAll(io, buf, offset)`. No `writeAll`/`preadAll` without io.
- **Networking**: `std.net` is gone. Use `std.Io.net.IpAddress`, `.listen(io, opts)`, `.connect(io, opts)`. Server returns `net.Stream` with `.socket.handle` for the posix fd.
- **No `std.posix.write`**: Use `std.os.linux.write` directly with manual errno handling, or use Io.net Stream's writer.
- **No `std.posix.socket`**: Use `std.Io.net.IpAddress.listen()` / `.connect()` instead.
- **JSON**: `std.json.Stringify.valueAlloc(allocator, value, .{})` replaces the old `stringify` with writer pattern.
- **Time**: `std.time.milliTimestamp()` is gone. Use `std.posix.clock_gettime(.REALTIME)` directly.
- **Enum from int**: `std.meta.intToEnum` is gone. Use `@enumFromInt(value)` builtin.
- **tmpDir in tests**: Pass `.{ .iterate = true }` if you need to iterate the directory.
- **Main signature**: Use `pub fn main(init: std.process.Init) !void` to get allocator, io, and args.
- **Args iteration**: Use `std.process.Args.Iterator.init(init.minimal.args)` then `.next()`.

### Event Memory Ownership
- `Log.read()` and `Log.readBatch()` return events with key+value in a single contiguous allocation
- Always use `store.freeEvent(allocator, event)` to free — never free key and value separately
- This reduces allocator pressure from 2-3 allocs per read to 1

### Debugging
- Use `std.debug.print` for quick debugging (remove before commit)
- Use `std.log` for structured logging that stays in production code
- Enable safety checks in debug mode — they catch real bugs
- Use `@breakpoint()` for debugger integration
- Compile with `-Doptimize=Debug` for full safety checks and stack traces
