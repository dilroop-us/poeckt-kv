const std = @import("std");
const Errors = @import("types.zig").Errors;

/// In-memory key-value store with explicit ownership.
/// Keys are hashed by contents via StringHashMap; we own key/value buffers.
pub const Storage = struct {
    alloc: std.mem.Allocator,
    map: std.StringHashMap([]u8),

    pub fn init(alloc: std.mem.Allocator) Storage {
        return .{
            .alloc = alloc,
            .map = std.StringHashMap([]u8).init(alloc),
        };
    }

    pub fn deinit(self: *Storage) void {
        var it = self.map.iterator();
        while (it.next()) |entry| {
            const k_const = entry.key_ptr.*;
            const v = entry.value_ptr.*; // []u8
            // We allocated both; free them.
            self.alloc.free(v);
            self.alloc.free(@constCast(k_const));
        }
        self.map.deinit();
    }

    /// Insert or overwrite.
    pub fn put(self: *Storage, key: []const u8, value: []const u8) !void {
        const gop = try self.map.getOrPut(key);

        if (gop.found_existing) {
            // keep existing key buffer; replace value buffer
            self.alloc.free(gop.value_ptr.*);
            gop.value_ptr.* = try self.alloc.dupe(u8, value);
        } else {
            // duplicate both key and value into owned memory
            gop.key_ptr.* = try self.alloc.dupe(u8, key);
            gop.value_ptr.* = try self.alloc.dupe(u8, value);
        }
    }

    /// Borrowed view; caller must not free/mutate.
    pub fn get(self: *Storage, key: []const u8) ![]const u8 {
        if (self.map.get(key)) |v| return v;
        return Errors.NotFound;
    }

    /// Delete and free both buffers.
    pub fn del(self: *Storage, key: []const u8) !void {
        if (self.map.fetchRemove(key)) |kv| {
            self.alloc.free(kv.value);
            self.alloc.free(@constCast(kv.key));
            return;
        }
        return Errors.NotFound;
    }
};

// Unit tests

test "put/get/del basic happy path" {
    var gpa_state = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa_state.deinit();
    const gpa = gpa_state.allocator();

    var store = Storage.init(gpa);
    defer store.deinit();

    try store.put("a", "1");
    try store.put("b", "2");
    try store.put("c", "3");

    try std.testing.expectEqualSlices(u8, "1", try store.get("a"));
    try std.testing.expectEqualSlices(u8, "2", try store.get("b"));
    try std.testing.expectEqualSlices(u8, "3", try store.get("c"));

    try store.del("b");
    try std.testing.expectError(Errors.NotFound, store.get("b"));
}

test "overwrite value reuses key and frees old value" {
    var gpa_state = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa_state.deinit();
    const gpa = gpa_state.allocator();

    var store = Storage.init(gpa);
    defer store.deinit();

    try store.put("k", "v1");
    try std.testing.expectEqualSlices(u8, "v1", try store.get("k"));

    try store.put("k", "v2");
    try std.testing.expectEqualSlices(u8, "v2", try store.get("k"));
}

test "delete missing key yields NotFound" {
    var gpa_state = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa_state.deinit();
    const gpa = gpa_state.allocator();

    var store = Storage.init(gpa);
    defer store.deinit();

    try std.testing.expectError(Errors.NotFound, store.del("nope"));
}

test "large-ish values and non-utf keys are fine" {
    var gpa_state = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa_state.deinit();
    const gpa = gpa_state.allocator();

    var store = Storage.init(gpa);
    defer store.deinit();

    const key = "\x00\x01\x02\xff";
    var buf: [1024]u8 = undefined;
    for (&buf, 0..) |*b, i| b.* = @as(u8, @intCast(i & 0xff));

    try store.put(key, &buf);
    const got = try store.get(key);
    try std.testing.expectEqual(@as(usize, buf.len), got.len);
    try std.testing.expectEqual(got[0], 0);
    try std.testing.expectEqual(got[255], 255);
}
