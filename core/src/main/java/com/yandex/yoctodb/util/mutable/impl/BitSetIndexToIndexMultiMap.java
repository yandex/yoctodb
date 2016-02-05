/*
 * (C) YANDEX LLC, 2014-2015
 *
 * The Source Code called "YoctoDB" available at
 * https://github.com/yandex/yoctodb is subject to the terms of the
 * Mozilla Public License, v. 2.0 (hereinafter referred to as the "License").
 *
 * A copy of the License is also available at http://mozilla.org/MPL/2.0/.
 */

package com.yandex.yoctodb.util.mutable.impl;

import com.google.common.collect.Multimap;
import com.google.common.collect.TreeMultimap;
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
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * @author svyatoslav
 */
@NotThreadSafe
public final class BitSetIndexToIndexMultiMap implements IndexToIndexMultiMap {
    private final int documentsCount;
    private Multimap<Integer, Integer> rawMap = TreeMultimap.create();

    private SortedMap<Integer, ArrayBitSet> map = null;

    public BitSetIndexToIndexMultiMap(final int documentsCount) {
        if (documentsCount < 0)
            throw new IllegalArgumentException("Negative document count");

        this.documentsCount = documentsCount;
    }

    @Override
    public void put(final int key, final int value) {
        if (map != null)
            throw new IllegalStateException("The collection is frozen");

        if (key < 0)
            throw new IllegalArgumentException("Negative key");
        if (value < 0)
            throw new IllegalArgumentException("Negative value");
        if (value >= documentsCount)
            throw new IllegalArgumentException("Value out of bounds");

        rawMap.put(key, value);
    }

    @Override
    public long getSizeInBytes() {
        if (map == null) {
            build();
        }

        return 4L + // Type
               4L + // Keys count
               4L + // Bit set size in longs
               8L * map.size() * LongArrayBitSet.arraySize(documentsCount);
    }

    @Override
    public void writeTo(
            @NotNull
            final OutputStream os) throws IOException {
        if (map == null) {
            build();
        }

        // Type
        os.write(Ints.toByteArray(V1DatabaseFormat.MultiMapType.LONG_ARRAY_BIT_SET_BASED.getCode()));

        // Keys count
        os.write(Ints.toByteArray(map.size()));

        // Count longs in bit-set
        os.write(Ints.toByteArray(LongArrayBitSet.arraySize(documentsCount)));

        // Sets
        for (ArrayBitSet value : map.values()) {
            for (long currentWord : value.toArray()) {
                os.write(Longs.toByteArray(currentWord));
            }
        }
    }

    private void build() {
        map = new TreeMap<Integer, ArrayBitSet>();
        int index = 0;
        for (Map.Entry<Integer, Collection<Integer>> entry :
                rawMap.asMap().entrySet()) {
            if (entry.getKey() != index) {
                throw new IllegalStateException("Indexes are not continuous");
            }

            final ArrayBitSet docs = LongArrayBitSet.zero(documentsCount);
            for (int docId : entry.getValue()) {
                docs.set(docId);
            }

            map.put(index, docs);

            index++;
        }

        // Releasing resources
        rawMap = null;
    }

    @Override
    public String toString() {
        return "BitSetIndexToIndexMultiMap{" +
               "values=" +
               (map == null ? rawMap.keySet().size() : map.keySet().size()) +
               ", documentsCount=" + documentsCount +
               '}';
    }
}
