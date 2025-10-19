const std = @import("std");
const DurableStorage = @import("storage.zig").DurableStorage;

// Make sure this matches std.Options in 0.14.x
pub const std_options: std.Options = .{
    .log_level = .info,
};

const Cli = struct {
    /// Owned string (allocated via provided allocator). Caller must free.
    data_dir: []const u8,
};

fn printUsage() !void {
    const w = std.io.getStdOut().writer();
    try w.print(
        \\PocketKV — tiny durable KV store
        \\
        \\Usage:
        \\  pocketkv [--data-dir PATH] [--help|-h]
        \\
        \\Options:
        \\  --data-dir PATH   Directory to store pocketkv.log  (default: data)
        \\  -h, --help        Show this help and exit
        \\
        \\Notes:
        \\  - Today (Day 4) we only initialize storage.
        \\  - Day 5 will start the HTTP server.
        \\
    , .{});
}

/// Parse CLI. Returns a Cli whose fields are **owned** strings (allocated
/// with `alloc`). The caller must free them (e.g., `gpa.free(cli.data_dir)`).
fn parseCli(alloc: std.mem.Allocator) !Cli {
    // Own a copy so it survives after args are freed.
    var data_dir = try alloc.dupe(u8, "data");

    const args = try std.process.argsAlloc(alloc);
    defer std.process.argsFree(alloc, args);

    var i: usize = 1; // skip argv[0]
    while (i < args.len) : (i += 1) {
        const a = args[i];

        if (std.mem.eql(u8, a, "--help") or std.mem.eql(u8, a, "-h")) {
            try printUsage();
            std.process.exit(0);
        } else if (std.mem.eql(u8, a, "--data-dir")) {
            i += 1;
            if (i >= args.len) return error.InvalidCli;
            // replace the owned copy safely
            const new_dir = try alloc.dupe(u8, args[i]);
            alloc.free(data_dir);
            data_dir = new_dir;
        } else {
            return error.InvalidCli;
        }
    }

    return .{ .data_dir = data_dir };
}

pub fn main() !void {
    // Allocator setup
    var gpa_state = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa_state.deinit();
    const gpa = gpa_state.allocator();

    // CLI
    const cli = parseCli(gpa) catch |e| {
        if (e == error.InvalidCli) {
            try printUsage();
            std.process.exit(2);
        }
        return e;
    };
    defer gpa.free(cli.data_dir); // free owned string on exit

    std.log.info("Initializing storage at '{s}' ...", .{cli.data_dir});

    // Initialize durable storage (replays the log). It will create the dir.
    var ds = try DurableStorage.init(gpa, cli.data_dir);
    defer {
        std.log.info("Shutting down storage ...", .{});
        ds.deinit();
        std.log.info("Bye!", .{});
    }

    std.log.info("Storage ready. (Day 5 will start the HTTP server)", .{});
    std.log.info("Press Enter to exit.", .{});

    // Keep process alive until user presses Enter
    const stdin = std.io.getStdIn();
    var reader = stdin.reader();
    while (true) {
        const b = reader.readByte() catch break;
        if (b == '\n') break;
    }
}
