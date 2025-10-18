const std = @import("std");
const Errors = @import("types.zig").Errors;

// Day 2: In-memory Storage (StringHashMap) + tests

/// In-memory key-value store with explicit ownership.
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
            self.alloc.free(v);
            self.alloc.free(@constCast(k_const));
        }
        self.map.deinit();
    }

    /// Insert or overwrite.
    pub fn put(self: *Storage, key: []const u8, value: []const u8) !void {
        const gop = try self.map.getOrPut(key);

        if (gop.found_existing) {
            self.alloc.free(gop.value_ptr.*);
            gop.value_ptr.* = try self.alloc.dupe(u8, value);
        } else {
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

test "D2: put/get/del basic happy path" {
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

test "D2: overwrite value reuses key and frees old value" {
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

test "D2: delete missing key yields NotFound" {
    var gpa_state = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa_state.deinit();
    const gpa = gpa_state.allocator();

    var store = Storage.init(gpa);
    defer store.deinit();

    try std.testing.expectError(Errors.NotFound, store.del("nope"));
}

test "D2: large-ish values and non-utf keys are fine" {
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

// Day 3: Append-Only Log (AOF) + Replay + tests

const OpTag = enum(u8) {
    put = 0,
    del = 1,
};

/// DurableStorage wraps the in-memory Storage with an append-only log (AOF).
pub const DurableStorage = struct {
    alloc: std.mem.Allocator,
    store: Storage, // in-memory KV from Day 2
    log_path: []const u8, // owned
    file: std.fs.File, // open R/W handle

    /// Initialize durable storage at `data_dir`. Creates the dir/file if missing.
    /// Replays the log to rebuild the in-memory store.
    pub fn init(alloc: std.mem.Allocator, data_dir: []const u8) !DurableStorage {
        try std.fs.cwd().makePath(data_dir);

        const path = try std.fs.path.join(alloc, &.{ data_dir, "pocketkv.log" });
        errdefer alloc.free(path);

        const file = std.fs.cwd().openFile(path, .{ .mode = .read_write }) catch |e| switch (e) {
            error.FileNotFound => try std.fs.cwd().createFile(path, .{ .read = true }),
            else => return e,
        };

        var self = DurableStorage{
            .alloc = alloc,
            .store = Storage.init(alloc),
            .log_path = path,
            .file = file,
        };

        self.replay() catch |e| {
            self.file.close();
            self.store.deinit();
            self.alloc.free(self.log_path);
            return e;
        };
        {}

        return self;
    }

    pub fn deinit(self: *DurableStorage) void {
        self.file.close();
        self.alloc.free(self.log_path);
        self.store.deinit();
    }

    /// PUT (durable): append record, then update memory.
    pub fn put(self: *DurableStorage, key: []const u8, value: []const u8) !void {
        try self.appendRecord(.put, key, value);
        try self.store.put(key, value);
    }

    /// GET served from memory.
    pub fn get(self: *DurableStorage, key: []const u8) ![]const u8 {
        return self.store.get(key);
    }

    /// DEL (durable): append record, then update memory.
    pub fn del(self: *DurableStorage, key: []const u8) !void {
        try self.appendRecord(.del, key, &.{});
        try self.store.del(key);
    }

    /// Append a record at end of file and flush.
    fn appendRecord(self: *DurableStorage, tag: OpTag, key: []const u8, value: []const u8) !void {
        // Seek to end (portable way for 0.14).
        const end = try self.file.getEndPos();
        try self.file.seekTo(end);

        var bw = std.io.bufferedWriter(self.file.writer());
        const w = bw.writer();

        try w.writeByte(@intFromEnum(tag));
        try w.writeInt(u32, @as(u32, @intCast(key.len)), .little);
        try w.writeInt(u32, @as(u32, @intCast(value.len)), .little);
        try w.writeAll(key);
        try w.writeAll(value);

        try bw.flush();
    }

    /// Replay whole log into the in-memory map. Truncation → CorruptLog.
    fn replay(self: *DurableStorage) !void {
        try self.file.seekTo(0);

        var br = std.io.bufferedReader(self.file.reader());
        const r = br.reader();

        while (true) {
            const tag_byte = r.readByte() catch |e| switch (e) {
                error.EndOfStream => break,
                else => return e,
            };

            const klen = r.readInt(u32, .little) catch return Errors.CorruptLog;
            const vlen = r.readInt(u32, .little) catch return Errors.CorruptLog;

            if (klen > (1 << 24) or vlen > (1 << 30)) return Errors.CorruptLog;

            const k = try self.alloc.alloc(u8, klen);
            defer self.alloc.free(k);
            try r.readNoEof(k);

            const tag: OpTag = @enumFromInt(tag_byte);

            if (tag == .put) {
                const v = try self.alloc.alloc(u8, vlen);
                defer self.alloc.free(v);
                try r.readNoEof(v);
                try self.replayPut(k, v);
            } else if (tag == .del) {
                if (vlen != 0) try r.skipBytes(vlen, .{});
                try self.replayDel(k);
            } else {
                return Errors.CorruptLog;
            }
        }
    }

    /// Apply a PUT during replay (no logging).
    fn replayPut(self: *DurableStorage, key: []const u8, value: []const u8) !void {
        const gop = try self.store.map.getOrPut(key);
        if (gop.found_existing) {
            self.store.alloc.free(gop.value_ptr.*);
            gop.value_ptr.* = try self.store.alloc.dupe(u8, value);
        } else {
            gop.key_ptr.* = try self.store.alloc.dupe(u8, key);
            gop.value_ptr.* = try self.store.alloc.dupe(u8, value);
        }
    }

    /// Apply a DEL during replay (no logging).
    fn replayDel(self: *DurableStorage, key: []const u8) !void {
        if (self.store.map.fetchRemove(key)) |kv| {
            self.store.alloc.free(kv.value);
            self.store.alloc.free(@constCast(kv.key));
        }
    }
};

// Day 3 Unit Tests

test "D3: AOF persists across restart (put/get)" {
    var gpa_state = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa_state.deinit();
    const gpa = gpa_state.allocator();

    const dir = "data_test_d3_basic";
    std.fs.cwd().deleteTree(dir) catch {};

    {
        var ds = try DurableStorage.init(gpa, dir);
        defer ds.deinit();

        try ds.put("a", "1");
        try ds.put("b", "2");
        try ds.put("a", "1x"); // overwrite

        try std.testing.expectEqualSlices(u8, "1x", try ds.get("a"));
        try std.testing.expectEqualSlices(u8, "2", try ds.get("b"));
    }
    {
        var ds2 = try DurableStorage.init(gpa, dir);
        defer ds2.deinit();

        try std.testing.expectEqualSlices(u8, "1x", try ds2.get("a"));
        try std.testing.expectEqualSlices(u8, "2", try ds2.get("b"));
    }

    try std.fs.cwd().deleteTree(dir);
}

test "D3: delete persists across restart" {
    var gpa_state = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa_state.deinit();
    const gpa = gpa_state.allocator();

    const dir = "data_test_d3_del";
    std.fs.cwd().deleteTree(dir) catch {};

    {
        var ds = try DurableStorage.init(gpa, dir);
        defer ds.deinit();
        try ds.put("x", "42");
        try ds.del("x");
    }
    {
        var ds2 = try DurableStorage.init(gpa, dir);
        defer ds2.deinit();
        try std.testing.expectError(Errors.NotFound, ds2.get("x"));
    }

    try std.fs.cwd().deleteTree(dir);
}

test "D3: truncated/corrupt log is detected" {
    var gpa_state = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa_state.deinit();
    const gpa = gpa_state.allocator();

    const dir = "data_test_d3_corrupt";
    std.fs.cwd().deleteTree(dir) catch {};

    {
        var ds = try DurableStorage.init(gpa, dir);
        defer ds.deinit();
        try ds.put("k", "v");
    }

    {
        const path = try std.fs.path.join(gpa, &.{ dir, "pocketkv.log" });
        defer gpa.free(path);
        var f = try std.fs.cwd().openFile(path, .{ .mode = .read_write });
        defer f.close();

        const end = try f.getEndPos();
        if (end >= 2) try f.setEndPos(end - 2);
    }

    {
        try std.testing.expectError(Errors.CorruptLog, DurableStorage.init(gpa, dir));
    }

    try std.fs.cwd().deleteTree(dir);
}
