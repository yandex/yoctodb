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
import com.yandex.yoctodb.util.mutable.IndexToIndexMultiMap;
import com.yandex.yoctodb.v1.V1DatabaseFormat;
import net.jcip.annotations.NotThreadSafe;
import org.jetbrains.annotations.NotNull;
import org.roaringbitmap.buffer.MutableRoaringBitmap;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collection;

/**
 * {@link IndexToIndexMultiMap} implementation based on
 * {@link MutableRoaringBitmap}s
 *
 * Format:
 *
 * <pre>
 * {@code
 * type (int)
 * sets
 *   set1
 *     size (int)
 *     bitset
 *     ...
 *   set2
 *     size (int)
 *     bitset
 *     ...
 * }
 * </pre>
 *
 * @author incubos
 */
@NotThreadSafe
public final class RoaringBitSetIndexToIndexMultiMap implements IndexToIndexMultiMap {
    @NotNull
    private final Collection<byte[]> map;
    private final long sizeInBytes;

    public RoaringBitSetIndexToIndexMultiMap(
            @NotNull
            final Collection<? extends Collection<Integer>> map) {
        try {
            this.map = new ArrayList<>(map.size());

            // Packing
            long bitSetsSize = 0L;
            for (Collection<Integer> indexes : map) {
                assert !indexes.isEmpty();

                final MutableRoaringBitmap set = new MutableRoaringBitmap();
                for (int i : indexes) {
                    assert i >= 0;

                    set.add(i);
                }
                set.runOptimize();

                final ByteArrayOutputStream bos = new ByteArrayOutputStream();
                final DataOutputStream dos = new DataOutputStream(bos);
                set.serialize(dos);
                dos.close();

                final byte[] data = bos.toByteArray();
                bitSetsSize += data.length;
                this.map.add(data);
            }

            this.sizeInBytes =
                    4L + // type
                    4L * this.map.size() + // size of each bitset
                    bitSetsSize; // set elements
        } catch (Exception e) { // To be able to test error handling
            throw new RuntimeException(e);
        }
    }

    @Override
    public long getSizeInBytes() {
        return sizeInBytes;
    }

    @Override
    public void writeTo(
            @NotNull
            final OutputStream os) throws IOException {
        // Type
        os.write(Ints.toByteArray(V1DatabaseFormat.MultiMapType.ROARING_BIT_SET_BASED.getCode()));

        // Sets
        for (byte[] data : map) {
            os.write(Ints.toByteArray(data.length));
            os.write(data);
        }
    }

    @Override
    public String toString() {
        return "RoaringBitSetIndexToIndexMultiMap{" +
               "values=" + map.size() +
               '}';
    }
}
