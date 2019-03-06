/*
 * (C) YANDEX LLC, 2014-2018
 *
 * The Source Code called "YoctoDB" available at
 * https://github.com/yandex/yoctodb is subject to the terms of the
 * Mozilla Public License, v. 2.0 (hereinafter referred to as the "License").
 *
 * A copy of the License is also available at http://mozilla.org/MPL/2.0/.
 */

package com.yandex.yoctodb.util.common;

public final class TrieNodeMetadata {
    public static final int PREFIX_FLAG       = 0b0001; // there is a prefix that must be consumed
    public static final int VALUE_FLAG        = 0b0010; // there is a value in this node

    public static final int EDGES_NONE        = 0b0000; // there is no edges
    public static final int EDGES_SINGLE      = 0b0100; // there is exactly one edge
    public static final int EDGES_BITSET      = 0b1000; // there is multiple scattered edges
    public static final int EDGES_CONDENSED   = 0b1100; // there is multiple continuous edges

    private static final int EDGES_MASK       = 0b1100;

    static {
        new TrieNodeMetadata(); // for coverage
    }

    public static int edgeType(final int metadata) {
        return metadata & EDGES_MASK;
    }

    public static boolean hasPrefix(final int metadata) {
        return (metadata & PREFIX_FLAG) != 0;
    }

    public static boolean hasValue(final int metadata) {
        return (metadata & VALUE_FLAG) != 0;
    }
}
