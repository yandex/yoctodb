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
 * @author Andrey Korzinev (goodfella@yandex-team.ru)
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
        return 4L + // Type
                4L + // Keys count
                4L + // Bit set size in longs
                8L * (map.size() + 1) * LongArrayBitSet.arraySize(documentsCount);
    }

    @Override
    public void writeTo(
            @NotNull final OutputStream os) throws IOException {
        // Type
        os.write(Ints.toByteArray(V1DatabaseFormat.MultiMapType.LONG_ARRAY_BIT_SET_BASED.getCode()));

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
        return "BitSetIndexToIndexMultiMap{" +
                "values=" + map.size() +
                ", documentsCount=" + documentsCount +
                '}';
    }
}
