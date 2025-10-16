const std = @import("std");

pub fn main() !void {
    try std.io.getStdOut().writer().print("PocketKV bootstrap OK. Use `zig build run` and `zig build test`.\n", .{});
}
