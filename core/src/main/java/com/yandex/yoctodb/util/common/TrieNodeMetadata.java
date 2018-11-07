package com.yandex.yoctodb.util.common;

public class TrieNodeMetadata {
    public static final int INFIX_FLAG = 0b0001;
    public static final int VALUE_FLAG = 0b0010;

    public static final int EDGES_NONE = 0b0000;
    public static final int EDGES_SINGLE = 0b0100;
    public static final int EDGES_BITSET = 0b1000;
    public static final int EDGES_CONDENSED = 0b1100;

    private static final int EDGES_MASK = 0b1100;

    public static int edgeType(final int metadata) {
        return metadata & EDGES_MASK;
    }

    public static boolean hasInfix(final int metadata) {
        return (metadata & INFIX_FLAG) != 0;
    }

    public static boolean hasValue(final int metadata) {
        return (metadata & VALUE_FLAG) != 0;
    }
}
