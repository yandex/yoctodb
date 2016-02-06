/*
 * (C) YANDEX LLC, 2014-2016
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
import com.yandex.yoctodb.util.mutable.ArrayBitSet;
import com.yandex.yoctodb.util.mutable.IndexToIndexMultiMap;
import com.yandex.yoctodb.v1.V1DatabaseFormat;
import net.jcip.annotations.NotThreadSafe;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Collection;

/**
 * {@link IndexToIndexMultiMap} implementation based on {@link LongArrayBitSet}s
 *
 * @author svyatoslav
 * @author incubos
 */
@NotThreadSafe
public final class BitSetIndexToIndexMultiMap implements IndexToIndexMultiMap {
    private final int documentsCount;
    @NotNull
    private final Collection<? extends Collection<Integer>> map;

    public BitSetIndexToIndexMultiMap(
            @NotNull
            final Collection<? extends Collection<Integer>> map,
            final int documentsCount) {
        if (documentsCount < 0)
            throw new IllegalArgumentException("Negative document count");

        this.map = map;
        this.documentsCount = documentsCount;
    }

    @Override
    public long getSizeInBytes() {
        return 4L + // Type
               4L + // Keys count
               4L + // Bit set size in longs
               8L * map.size() * LongArrayBitSet.arraySize(documentsCount);
    }

    @Override
    public void writeTo(
            @NotNull
            final OutputStream os) throws IOException {
        // Type
        os.write(Ints.toByteArray(V1DatabaseFormat.MultiMapType.LONG_ARRAY_BIT_SET_BASED.getCode()));

        // Keys count
        os.write(Ints.toByteArray(map.size()));

        // Count longs in bit-set
        os.write(Ints.toByteArray(LongArrayBitSet.arraySize(documentsCount)));

        // Sets
        final ArrayBitSet docs = LongArrayBitSet.zero(documentsCount);
        for (Collection<Integer> ids : map) {
            docs.clear();
            for (int docId : ids) {
                assert 0 <= docId && docId < documentsCount;
                docs.set(docId);
            }
            for (long currentWord : docs.toArray()) {
                os.write(Longs.toByteArray(currentWord));
            }
        }
    }

    @Override
    public String toString() {
        return "BitSetIndexToIndexMultiMap{" +
               "values=" + map.size() +
               ", documentsCount=" + documentsCount +
               '}';
    }
}
