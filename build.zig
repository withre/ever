const std = @import("std");

// Forward `zig build run -- <args>` to the run step.
//
// The two toolchains we support disagree on the API: 0.16.0 has `b.args` +
// `Run.addArgs`, while 0.17-dev removed `b.args` in favour of
// `Run.addPassthruArgs`. Neither call site compiles under the other version,
// so this is feature-detected at comptime -- only the branch valid for the
// active toolchain is analyzed.
//
// Why not just call addPassthruArgs directly: it fails to compile on 0.16.0,
// which is still our declared `minimum_zig_version` and deployment baseline.
// Picking one call site would quietly drop a supported toolchain.
fn passthruArgs(b: *std.Build, run: *std.Build.Step.Run) void {
    if (@hasDecl(std.Build.Step.Run, "addPassthruArgs")) {
        run.addPassthruArgs();
    } else if (@hasField(std.Build, "args")) {
        if (b.args) |args| run.addArgs(args);
    } else {
        // Fail loudly rather than silently dropping the user's args.
        @compileError("no passthrough-args API found on this Zig version");
    }
}

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    const zck = b.dependency("zig_cli_kit", .{
        .target = target,
        .optimize = optimize,
    });

    // Library module
    const ever_module = b.addModule("ever", .{
        .root_source_file = b.path("src/root.zig"),
    });

    // Main executable
    const exe = b.addExecutable(.{
        .name = "ever",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/main.zig"),
            .target = target,
            .optimize = optimize,
            .link_libc = true,
        }),
    });
    exe.root_module.addImport("ever", ever_module);
    exe.root_module.addImport("zig-cli-kit", zck.module("zig-cli-kit"));
    b.installArtifact(exe);

    // Run step
    const run_cmd = b.addRunArtifact(exe);
    run_cmd.step.dependOn(b.getInstallStep());
    passthruArgs(b, run_cmd);

    const run_step = b.step("run", "Run the Ever CLI");
    run_step.dependOn(&run_cmd.step);

    // Unit tests for library
    const lib_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/root.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    const run_lib_tests = b.addRunArtifact(lib_tests);

    // Unit tests for main
    const main_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/main.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    main_tests.root_module.addImport("ever", ever_module);
    main_tests.root_module.addImport("zig-cli-kit", zck.module("zig-cli-kit"));
    const run_main_tests = b.addRunArtifact(main_tests);

    // Integration tests — exercise TopicManager / HookTable through their
    // public APIs without standing up TCP. Lives in src/tests/integration.zig.
    const integration_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/tests/integration.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    integration_tests.root_module.addImport("ever", ever_module);
    const run_integration_tests = b.addRunArtifact(integration_tests);

    const test_step = b.step("test", "Run all tests");
    test_step.dependOn(&run_lib_tests.step);
    test_step.dependOn(&run_main_tests.step);
    test_step.dependOn(&run_integration_tests.step);

    // Benchmark executable (always ReleaseFast for realistic numbers)
    const bench_exe = b.addExecutable(.{
        .name = "ever-bench",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/bench.zig"),
            .target = target,
            .optimize = .ReleaseFast,
        }),
    });
    bench_exe.root_module.addImport("ever", ever_module);

    const bench_run = b.addRunArtifact(bench_exe);
    passthruArgs(b, bench_run);

    const bench_step = b.step("bench", "Run benchmarks (ReleaseFast)");
    bench_step.dependOn(&bench_run.step);
}
