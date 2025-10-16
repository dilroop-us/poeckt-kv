const std = @import("std");
const E = @import("types.zig").Errors;

// Day 1: just proving tests run and allocator wiring is OK.
test "Day 1: storage test scaffold runs (Zig 0.14)" {
    var gpa_state = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa_state.deinit();
    const gpa = gpa_state.allocator();

    const buf = try gpa.alloc(u8, 4);
    defer gpa.free(buf);

    // 0.14: use copyForwards instead of copy
    std.mem.copyForwards(u8, buf, "ok!!");

    try std.testing.expectEqual(@as(usize, 4), buf.len);
    try std.testing.expectEqualSlices(u8, "ok!!", buf);
}
