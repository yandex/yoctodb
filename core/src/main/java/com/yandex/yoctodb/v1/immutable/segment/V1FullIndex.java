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
import com.yandex.yoctodb.immutable.SortableIndex;
import com.yandex.yoctodb.util.buf.Buffer;
import com.yandex.yoctodb.util.immutable.ByteArraySortedSet;
import com.yandex.yoctodb.util.immutable.IndexToIndexMap;
import com.yandex.yoctodb.util.immutable.IndexToIndexMultiMap;
import com.yandex.yoctodb.util.immutable.IntToIntArray;
import com.yandex.yoctodb.util.immutable.impl.FixedLengthByteArraySortedSet;
import com.yandex.yoctodb.util.immutable.impl.IndexToIndexMultiMapReader;
import com.yandex.yoctodb.util.immutable.impl.IntIndexToIndexMap;
import com.yandex.yoctodb.util.immutable.impl.VariableLengthByteArraySortedSet;
import com.yandex.yoctodb.util.mutable.BitSet;
import com.yandex.yoctodb.v1.V1DatabaseFormat;
import net.jcip.annotations.Immutable;
import org.jetbrains.annotations.NotNull;

import java.util.Iterator;

/**
 * Immutable implementation of {@link FilterableIndex} and {@link
 * SortableIndex}
 *
 * @author incubos
 */
@Immutable
public final class V1FullIndex
        implements FilterableIndex, SortableIndex, Segment {
    @NotNull
    private final String fieldName;
    @NotNull
    private final V1FilterableIndex filterableDelegate;
    @NotNull
    private final V1SortableIndex sortableDelegate;

    private V1FullIndex(
            @NotNull
            final String fieldName,
            @NotNull
            final ByteArraySortedSet values,
            @NotNull
            final IndexToIndexMultiMap valueToDocuments,
            @NotNull
            final IndexToIndexMap documentToValue) {
        // May be constructed only from SegmentReader
        this.fieldName = fieldName;
        this.filterableDelegate =
                new V1FilterableIndex(
                        fieldName,
                        values,
                        valueToDocuments);
        this.sortableDelegate =
                new V1SortableIndex(
                        fieldName,
                        values,
                        valueToDocuments,
                        documentToValue);
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
        return filterableDelegate.eq(dest, value);
    }

    @Override
    public boolean in(
            @NotNull
            final BitSet dest,
            @NotNull
            final Buffer... value) {
        return filterableDelegate.in(dest, value);
    }

    @Override
    public boolean lessThan(
            @NotNull
            final BitSet dest,
            @NotNull
            final Buffer value,
            final boolean orEquals) {
        return filterableDelegate.lessThan(dest, value, orEquals);
    }

    @Override
    public boolean greaterThan(
            @NotNull
            final BitSet dest,
            @NotNull
            final Buffer value,
            final boolean orEquals) {
        return filterableDelegate.greaterThan(dest, value, orEquals);
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
        return filterableDelegate.between(
                dest,
                from,
                fromInclusive,
                to,
                toInclusive);
    }

    @Override
    public int getSortValueIndex(final int document) {
        return sortableDelegate.getSortValueIndex(document);
    }

    @NotNull
    @Override
    public Buffer getSortValue(final int index) {
        return sortableDelegate.getSortValue(index);
    }

    @NotNull
    @Override
    public Buffer getStoredValue(final int document) {
        return sortableDelegate.getStoredValue(document);
    }

    @Override
    public long getLongValue(final int document) {
        return sortableDelegate.getLongValue(document);
    }

    @Override
    public int getIntValue(final int document) {
        return sortableDelegate.getIntValue(document);
    }

    @Override
    public short getShortValue(final int document) {
        return sortableDelegate.getShortValue(document);
    }

    @Override
    public char getCharValue(final int document) {
        return sortableDelegate.getCharValue(document);
    }

    @Override
    public byte getByteValue(final int document) {
        return sortableDelegate.getByteValue(document);
    }

    @NotNull
    @Override
    public Iterator<IntToIntArray> ascending(
            @NotNull
            final BitSet docs) {
        return sortableDelegate.ascending(docs);
    }

    @NotNull
    @Override
    public Iterator<IntToIntArray> descending(
            @NotNull
            final BitSet docs) {
        return sortableDelegate.descending(docs);
    }

    static void registerReader() {
        SegmentRegistry.register(
                V1DatabaseFormat.SegmentType.FIXED_LENGTH_FULL_INDEX.getCode(),
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

                        final IndexToIndexMap documentToValues =
                                IntIndexToIndexMap.from(
                                        Segments.extract(buffer));

                        return new V1FullIndex(
                                fieldName,
                                values,
                                valueToDocuments,
                                documentToValues);
                    }


                });

        SegmentRegistry.register(
                V1DatabaseFormat.SegmentType
                        .VARIABLE_LENGTH_FULL_INDEX
                        .getCode(),
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

                        final IndexToIndexMap documentToValues =
                                IntIndexToIndexMap.from(
                                        Segments.extract(buffer));

                        return new V1FullIndex(
                                fieldName,
                                values,
                                valueToDocuments,
                                documentToValues);
                    }
                });
    }
}
