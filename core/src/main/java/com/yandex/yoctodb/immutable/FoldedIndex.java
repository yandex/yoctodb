package com.yandex.yoctodb.immutable;

import com.yandex.yoctodb.util.buf.Buffer;
import org.jetbrains.annotations.NotNull;

public interface FoldedIndex extends Index {
    /**
     * Get folded value by document ID
     *
     * @param document document ID
     * @return folded value
     */
    @NotNull
    Buffer getFoldedValue(int document);
}
