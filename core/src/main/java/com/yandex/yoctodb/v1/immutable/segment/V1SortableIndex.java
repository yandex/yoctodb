/*
 * (C) YANDEX LLC, 2014-2015
 *
 * The Source Code called "YoctoDB" available at
 * https://bitbucket.org/yandex/yoctodb is subject to the terms of the
 * Mozilla Public License, v. 2.0 (hereinafter referred to as the "License").
 *
 * A copy of the License is also available at http://mozilla.org/MPL/2.0/.
 */

package com.yandex.yoctodb.v1.immutable.segment;

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

import java.io.IOException;
import java.util.Iterator;

/**
 * Immutable implementation of {@link SortableIndex}
 *
 * @author incubos
 */
@Immutable
public final class V1SortableIndex
        implements SortableIndex, Segment {
    @NotNull
    private final String fieldName;
    @NotNull
    private final ByteArraySortedSet values;
    @NotNull
    private final IndexToIndexMultiMap valueToDocuments;
    @NotNull
    private final IndexToIndexMap documentToValue;

    V1SortableIndex(
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
        this.values = values;
        this.valueToDocuments = valueToDocuments;
        this.documentToValue = documentToValue;
    }

    @NotNull
    @Override
    public String getFieldName() {
        return fieldName;
    }

    @Override
    public int getSortValueIndex(final int document) {
        return documentToValue.get(document);
    }

    @NotNull
    @Override
    public Buffer getSortValue(final int index) {
        return values.get(index);
    }

    @NotNull
    @Override
    public Iterator<IntToIntArray> ascending(
            @NotNull
            final BitSet docs) {
        return valueToDocuments.ascending(docs);
    }

    @NotNull
    @Override
    public Iterator<IntToIntArray> descending(
            @NotNull
            final BitSet docs) {
        return valueToDocuments.descending(docs);
    }

    static void registerReader() {
        SegmentRegistry.register(
                V1DatabaseFormat.SegmentType
                        .FIXED_LENGTH_SORTABLE_INDEX.getCode(),
                new SegmentReader() {
                    @NotNull
                    @Override
                    public Segment read(
                            @NotNull
                            final Buffer buffer) throws IOException {
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

                        return new V1SortableIndex(
                                fieldName,
                                values,
                                valueToDocuments,
                                documentToValues);
                    }


                });

        SegmentRegistry.register(
                V1DatabaseFormat.SegmentType
                        .VARIABLE_LENGTH_SORTABLE_INDEX.getCode(),
                new SegmentReader() {
                    @NotNull
                    @Override
                    public Segment read(
                            @NotNull
                            final Buffer buffer) throws IOException {
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

                        return new V1SortableIndex(
                                fieldName,
                                values,
                                valueToDocuments,
                                documentToValues);
                    }
                });
    }
}
