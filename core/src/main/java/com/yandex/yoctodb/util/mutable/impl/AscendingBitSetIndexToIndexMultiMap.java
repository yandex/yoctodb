/*
 * (C) YANDEX LLC, 2014-2018
 *
 * The Source Code called "YoctoDB" available at
 * https://github.com/yandex/yoctodb is subject to the terms of the
 * Mozilla Public License, v. 2.0 (hereinafter referred to as the "License").
 *
 * A copy of the License is also available at http://mozilla.org/MPL/2.0/.
 */

package com.yandex.yoctodb.util.mutable.impl;

import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import com.yandex.yoctodb.util.OutputStreamWritable;
import com.yandex.yoctodb.util.mutable.ArrayBitSet;
import com.yandex.yoctodb.util.mutable.IndexToIndexMultiMap;
import com.yandex.yoctodb.v1.V1DatabaseFormat;
import net.jcip.annotations.Immutable;
import net.jcip.annotations.NotThreadSafe;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Collection;

/**
 * {@link IndexToIndexMultiMap} implementation based on grouped {@link LongArrayBitSet}s
 * For more information see {@link com.yandex.yoctodb.util.immutable.impl.AscendingBitSetIndexToIndexMultiMap}
 *
 * @author Andrey Korzinev (ya-goodfella@yandex.com)
 */
@Immutable
@NotThreadSafe
public class AscendingBitSetIndexToIndexMultiMap implements IndexToIndexMultiMap, OutputStreamWritable {
    private final int documentsCount;
    @NotNull
    private final Collection<? extends Collection<Integer>> map;

    public AscendingBitSetIndexToIndexMultiMap(
            @NotNull final Collection<? extends Collection<Integer>> map,
            final int documentsCount) {
        if (documentsCount < 0)
            throw new IllegalArgumentException("Negative document count");

        this.map = map;
        this.documentsCount = documentsCount;
    }

    @Override
    public long getSizeInBytes() {
        /*
         * This index contains:
         * + 4 bytes for index type
         * + 4 bytes for keys count
         * + 4 bytes for bitset size (in terms of {@code long[]} size)
         * + bitsets (as long[]) for each associated key and one extra bitset for storing
         *   all non-null values
         */
        return Integer.BYTES + // Type
               Integer.BYTES + // Keys count
               Integer.BYTES + // Bit set size in longs
               (map.size() + 1) * Long.BYTES * LongArrayBitSet.arraySize(documentsCount); // Docs
    }

    @Override
    public void writeTo(
            @NotNull final OutputStream os) throws IOException {
        // Type
        os.write(Ints.toByteArray(V1DatabaseFormat.MultiMapType.ASCENDING_BIT_SET_BASED.getCode()));

        // Keys count
        os.write(Ints.toByteArray(map.size()));

        // Count longs in bit-set
        os.write(Ints.toByteArray(LongArrayBitSet.arraySize(documentsCount)));

        // Sets
        final ArrayBitSet docs = LongArrayBitSet.zero(documentsCount);
        for (Collection<Integer> ids : map) {
            for (long currentWord : docs.toArray()) {
                os.write(Longs.toByteArray(currentWord));
            }
            for (int docId : ids) {
                assert 0 <= docId && docId < documentsCount;
                docs.set(docId);
            }
        }

        // Last one bit-set for all values associated with keys
        for (long currentWord : docs.toArray()) {
            os.write(Longs.toByteArray(currentWord));
        }
    }

    @Override
    public String toString() {
        return "AscendingBitSetIndexToIndexMultiMap{" +
                "values=" + map.size() +
                ", documentsCount=" + documentsCount +
                '}';
    }
}
