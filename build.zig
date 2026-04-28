const std = @import("std");

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
    if (b.args) |args| {
        run_cmd.addArgs(args);
    }

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
    if (b.args) |args| {
        bench_run.addArgs(args);
    }

    const bench_step = b.step("bench", "Run benchmarks (ReleaseFast)");
    bench_step.dependOn(&bench_run.step);
}
