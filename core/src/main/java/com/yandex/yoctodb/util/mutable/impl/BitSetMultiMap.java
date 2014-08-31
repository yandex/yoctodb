/*
 * (C) YANDEX LLC, 2014
 *
 * The Source Code called "YoctoDB" available at
 * https://bitbucket.org/yandex/yoctodb is subject to the terms of the
 * Mozilla Public License, v. 2.0 (hereinafter - MPL).
 *
 * A copy of the MPL is available at http://mozilla.org/MPL/2.0/.
 */

package com.yandex.yoctodb.util.mutable.impl;

import com.google.common.collect.Multimap;
import com.google.common.collect.TreeMultimap;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import net.jcip.annotations.NotThreadSafe;
import org.jetbrains.annotations.NotNull;
import com.yandex.yoctodb.util.mutable.ArrayBitSet;
import com.yandex.yoctodb.util.mutable.IndexToIndexMultiMap;
import com.yandex.yoctodb.v1.V1DatabaseFormat;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Collection;
import java.util.Map;
import java.util.TreeMap;

/**
 * @author svyatoslav
 */
@NotThreadSafe
public final class BitSetMultiMap implements IndexToIndexMultiMap {
    private final Map<Integer, ArrayBitSet> map = new TreeMap<Integer, ArrayBitSet>();
    private final Multimap<Integer, Integer> rawMap = TreeMultimap.create();

    private final int documentsCount;
    private final int bitSetSizeInLongs;
    private boolean mapFilled = false;

    public BitSetMultiMap(int documentsCount) {
        this.documentsCount = documentsCount;
        this.bitSetSizeInLongs = getBitSetSizeInLongs();
    }

    @Override
    public void add(int key, int value) {
        assert key >= 0;
        assert value >= 0;

        rawMap.put(key, value);
    }


    @Override
    public int getSizeInBytes() {
        if (!mapFilled){
            fillMap();
        }
        return 4 + //size four bytes
                4 + //type
                4 +
                (8) * map.size() * bitSetSizeInLongs;
    }

    private LongArrayBitSet fillArray(Collection<Integer> docIds, int size) {
        LongArrayBitSet arrayBitSet = (LongArrayBitSet) LongArrayBitSet.zero(
                size);
        for (int docId : docIds) {
            arrayBitSet.set(docId);
        }
        return arrayBitSet;
    }

    private int getBitSetSizeInLongs() {
        return ((LongArrayBitSet) LongArrayBitSet.one(documentsCount)).toArray().length;
    }

    @Override
    public void writeTo(
            @NotNull
            final
            OutputStream os) throws IOException {

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
            long[] words = value.toArray();
            for (long currentWord : words) {
                os.write(Longs.toByteArray(currentWord));
            }
        }
    }

    private void fillMap() {
        mapFilled = true;
        for (Map.Entry<Integer, Collection<Integer>> entry :
                rawMap.asMap().entrySet()) {
            final int key = entry.getKey();
            LongArrayBitSet bitSet = fillArray(
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
