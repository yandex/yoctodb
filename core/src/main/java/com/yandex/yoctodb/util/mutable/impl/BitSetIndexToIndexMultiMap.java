/*
 * (C) YANDEX LLC, 2014-2015
 *
 * The Source Code called "YoctoDB" available at
 * https://bitbucket.org/yandex/yoctodb is subject to the terms of the
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
import java.util.TreeMap;

/**
 * @author svyatoslav
 */
@NotThreadSafe
public final class BitSetIndexToIndexMultiMap implements IndexToIndexMultiMap {
    private final Map<Integer, ArrayBitSet> map = new TreeMap<Integer, ArrayBitSet>();
    private final Multimap<Integer, Integer> rawMap = TreeMultimap.create();

    private final int documentsCount;
    private final int bitSetSizeInLongs;
    private boolean mapFilled = false;

    public BitSetIndexToIndexMultiMap(final int documentsCount) {
        this.documentsCount = documentsCount;
        this.bitSetSizeInLongs = getBitSetSizeInLongs();
    }

    @Override
    public void add(final int key, final int value) {
        if (key < 0)
            throw new IllegalArgumentException("Negative key");
        if (value < 0)
            throw new IllegalArgumentException("Negative value");

        rawMap.put(key, value);
    }

    @Override
    public long getSizeInBytes() {
        if (!mapFilled) {
            fillMap();
        }

        return 4L + //size four bytes
               4L + //type
               4L +
               8L * map.size() * bitSetSizeInLongs;
    }

    private LongArrayBitSet fillArray(
            @NotNull
            final Collection<Integer> docIds,
            final int size) {
        LongArrayBitSet arrayBitSet = (LongArrayBitSet) LongArrayBitSet.zero(
                size);
        for (int docId : docIds) {
            arrayBitSet.set(docId);
        }
        return arrayBitSet;
    }

    private int getBitSetSizeInLongs() {
        return LongArrayBitSet.arraySize(documentsCount);
    }

    @Override
    public void writeTo(
            @NotNull
            final OutputStream os) throws IOException {
        if (!mapFilled) {
            fillMap();
        }
        //type
        os.write(Ints.toByteArray(V1DatabaseFormat.MultiMapType.LONG_ARRAY_BIT_SET_BASED.getCode()));

        // Keys count
        os.write(Ints.toByteArray(map.size()));

        //count longs in bit-set
        os.write(Ints.toByteArray(bitSetSizeInLongs));

        // Sets
        for (ArrayBitSet value : map.values()) {
            for (long currentWord : value.toArray()) {
                os.write(Longs.toByteArray(currentWord));
            }
        }
    }

    private void fillMap() {
        mapFilled = true;
        for (Map.Entry<Integer, Collection<Integer>> entry :
                rawMap.asMap().entrySet()) {
            final int key = entry.getKey();
            final LongArrayBitSet bitSet =
                    fillArray(
                            entry.getValue(),
                            documentsCount);
            map.put(key, bitSet);
        }
    }

    @Override
    public String toString() {
        return "BitSetMultiMap{" +
               "map=" + map +
               '}';
    }


}
