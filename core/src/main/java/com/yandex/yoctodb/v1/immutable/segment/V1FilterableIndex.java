/*
 * (C) YANDEX LLC, 2014-2016
 *
 * The Source Code called "YoctoDB" available at
 * https://github.com/yandex/yoctodb is subject to the terms of the
 * Mozilla Public License, v. 2.0 (hereinafter referred to as the "License").
 *
 * A copy of the License is also available at http://mozilla.org/MPL/2.0/.
 */

package com.yandex.yoctodb.v1.immutable.segment;

import com.yandex.yoctodb.immutable.FilterableIndex;
import com.yandex.yoctodb.util.buf.Buffer;
import com.yandex.yoctodb.util.immutable.ByteArraySortedSet;
import com.yandex.yoctodb.util.immutable.IndexToIndexMultiMap;
import com.yandex.yoctodb.util.immutable.impl.FixedLengthByteArraySortedSet;
import com.yandex.yoctodb.util.immutable.impl.IndexToIndexMultiMapReader;
import com.yandex.yoctodb.util.immutable.impl.TrieByteArraySortedSet;
import com.yandex.yoctodb.util.immutable.impl.VariableLengthByteArraySortedSet;
import com.yandex.yoctodb.util.mutable.BitSet;
import com.yandex.yoctodb.v1.V1DatabaseFormat;
import net.jcip.annotations.Immutable;
import org.jetbrains.annotations.NotNull;

/**
 * Immutable {@link FilterableIndex} implementation
 *
 * @author incubos
 */
@Immutable
public final class V1FilterableIndex implements FilterableIndex, Segment {
    @NotNull
    private final String fieldName;
    @NotNull
    private final ByteArraySortedSet values;
    @NotNull
    private final IndexToIndexMultiMap valueToDocuments;

    V1FilterableIndex(
            @NotNull
            final String fieldName,
            @NotNull
            final ByteArraySortedSet values,
            @NotNull
            final IndexToIndexMultiMap valueToDocuments) {
        // May be constructed only from SegmentReader
        this.fieldName = fieldName;
        this.values = values;
        this.valueToDocuments = valueToDocuments;
    }

    @NotNull
    @Override
    public String getFieldName() {
        return fieldName;
    }

    @Override
    public boolean eq(
            @NotNull
            final BitSet dest,
            @NotNull
            final Buffer value) {
        final int valueIndex = values.indexOf(value);
        return valueIndex != -1 && valueToDocuments.get(dest, valueIndex);
    }

    @Override
    public boolean in(
            @NotNull
            final BitSet dest,
            @NotNull
            final Buffer... value) {
        boolean result = false;
        for (Buffer currentValue : value) {
            final int valueIndex = values.indexOf(currentValue);
            result |=
                    valueIndex != -1 && valueToDocuments.get(dest, valueIndex);
        }
        return result;
    }

    @Override
    public boolean lessThan(
            @NotNull
            final BitSet dest,
            @NotNull
            final Buffer value,
            final boolean orEquals) {
        final int greatestValueIndex = values.indexOfLessThan(
                value,
                orEquals,
                0);
        return greatestValueIndex != -1 &&
                valueToDocuments.getTo(dest, greatestValueIndex + 1);
    }

    @Override
    public boolean greaterThan(
            @NotNull
            final BitSet dest,
            @NotNull
            final Buffer value,
            final boolean orEquals) {
        final int leastValueIndex = values.indexOfGreaterThan(
                value,
                orEquals,
                values.size() - 1);
        return leastValueIndex != -1 &&
                valueToDocuments.getFrom(dest, leastValueIndex);
    }

    @Override
    public boolean between(
            @NotNull
            final BitSet dest,
            @NotNull
            final Buffer from,
            final boolean fromInclusive,
            @NotNull
            final Buffer to,
            final boolean toInclusive) {
        final int fromValueIndex =
                values.indexOfGreaterThan(
                        from,
                        fromInclusive,
                        values.size() - 1);

        if (fromValueIndex == -1) {
            return false;
        }

        final int toValueIndex =
                values.indexOfLessThan(
                        to,
                        toInclusive,
                        fromValueIndex);

        return toValueIndex != -1 &&
                valueToDocuments.getBetween(
                        dest,
                        fromValueIndex,
                        toValueIndex + 1);
    }

    static void registerReader() {
        SegmentRegistry.register(
                V1DatabaseFormat.SegmentType.FIXED_LENGTH_FILTER.getCode(),
                new SegmentReader() {
                    @NotNull
                    @Override
                    public Segment read(
                            @NotNull
                            final Buffer buffer) {
                        final String fieldName = Segments.extractString(buffer);

                        final ByteArraySortedSet values =
                                FixedLengthByteArraySortedSet.from(
                                        Segments.extract(buffer));

                        final IndexToIndexMultiMap valueToDocuments =
                                IndexToIndexMultiMapReader.from(
                                        Segments.extract(buffer));

                        return new V1FilterableIndex(
                                fieldName,
                                values,
                                valueToDocuments);
                    }

                }
        );

        SegmentRegistry.register(
                V1DatabaseFormat.SegmentType.VARIABLE_LENGTH_FILTER.getCode(),
                new SegmentReader() {
                    @NotNull
                    @Override
                    public Segment read(
                            @NotNull
                            final Buffer buffer) {
                        final String fieldName = Segments.extractString(buffer);

                        final ByteArraySortedSet values =
                                VariableLengthByteArraySortedSet.from(
                                        Segments.extract(buffer));

                        final IndexToIndexMultiMap valueToDocuments =
                                IndexToIndexMultiMapReader.from(
                                        Segments.extract(buffer));

                        return new V1FilterableIndex(
                                fieldName,
                                values,
                                valueToDocuments);
                    }

                }
        );

        SegmentRegistry.register(
                V1DatabaseFormat.SegmentType.TRIE_FILTER.getCode(),
                new SegmentReader() {
                    @NotNull
                    @Override
                    public Segment read(
                            @NotNull
                            final Buffer buffer) {
                        final String fieldName = Segments.extractString(buffer);

                        final ByteArraySortedSet values =
                                TrieByteArraySortedSet.from(
                                        Segments.extract(buffer));

                        final IndexToIndexMultiMap valueToDocuments =
                                IndexToIndexMultiMapReader.from(
                                        Segments.extract(buffer));

                        return new V1FilterableIndex(
                                fieldName,
                                values,
                                valueToDocuments);
                    }

                }
        );
    }
}
