/*
 * Copyright (c) 2014 Yandex
 */

package ru.yandex.yoctodb.v1.immutable.segment;

import net.jcip.annotations.Immutable;
import org.jetbrains.annotations.NotNull;
import ru.yandex.yoctodb.util.immutable.IntToIntArray;
import ru.yandex.yoctodb.immutable.SortableIndex;
import ru.yandex.yoctodb.util.immutable.ByteArrayIndexedList;
import ru.yandex.yoctodb.util.immutable.impl.FixedLengthByteArrayIndexedList;
import ru.yandex.yoctodb.util.immutable.impl.VariableLengthByteArrayIndexedList;
import ru.yandex.yoctodb.util.mutable.BitSet;
import ru.yandex.yoctodb.v1.V1DatabaseFormat;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;

/**
 * Immutable {@link SortableIndex} implementation
 *
 * @author incubos
 */
@Deprecated
@Immutable
public final class V1SortableIndex implements SortableIndex, Segment {
    @NotNull
    private final String fieldName;
    @NotNull
    private final ByteArrayIndexedList documentToValue;

    private V1SortableIndex(
            @NotNull
            final String fieldName,
            @NotNull
            final ByteArrayIndexedList documentToValue) {
        assert !fieldName.isEmpty();

        // May be constructed only from SegmentReader
        this.fieldName = fieldName;
        this.documentToValue = documentToValue;
    }

    @NotNull
    @Override
    public String getFieldName() {
        return fieldName;
    }

    public ByteBuffer get(final int document) {
        return documentToValue.get(document);
    }

    @Override
    public int getSortValueIndex(final int document) {
        throw new UnsupportedOperationException();
    }

    @NotNull
    @Override
    public ByteBuffer getSortValue(final int index) {
        throw new UnsupportedOperationException();
    }

    @NotNull
    @Override
    public Iterator<IntToIntArray> ascending(
            @NotNull
            final BitSet docs) {
        throw new UnsupportedOperationException();
    }

    @NotNull
    @Override
    public Iterator<IntToIntArray> descending(
            @NotNull
            final BitSet docs) {
        throw new UnsupportedOperationException();
    }

    static void registerReader() {
        SegmentRegistry.register(
                V1DatabaseFormat.SegmentType.FIXED_LENGTH_SORT.getCode(),
                new SegmentReader() {
                    @NotNull
                    @Override
                    public Segment read(
                            @NotNull
                            final ByteBuffer buffer) throws IOException {

                        final byte[] digest = Segments.calculateDigest(buffer, V1DatabaseFormat.MESSAGE_DIGEST_ALGORITHM);

                        final String fieldName = Segments.extractString(buffer);

                        final ByteArrayIndexedList documentToValue =
                                FixedLengthByteArrayIndexedList.from(
                                        Segments.extract(buffer));

                        final ByteBuffer digestActual = Segments.extract(buffer);
                        if (!digestActual.equals(ByteBuffer.wrap(digest))) {
                            throw new CorruptSegmentException("checksum error");
                        }


                        return new V1SortableIndex(fieldName, documentToValue);
                    }
                });

        SegmentRegistry.register(
                V1DatabaseFormat.SegmentType.VARIABLE_LENGTH_SORT.getCode(),
                new SegmentReader() {
                    @NotNull
                    @Override
                    public Segment read(
                            @NotNull
                            final ByteBuffer buffer) throws IOException {

                        final byte[] digest = Segments.calculateDigest(buffer, V1DatabaseFormat.MESSAGE_DIGEST_ALGORITHM);

                        final String fieldName = Segments.extractString(buffer);

                        final ByteArrayIndexedList documentToValue =
                                VariableLengthByteArrayIndexedList.from(
                                        Segments.extract(buffer));

                        final ByteBuffer digestActual = Segments.extract(buffer);
                        if (!digestActual.equals(ByteBuffer.wrap(digest))) {
                            throw new CorruptSegmentException("checksum error");
                        }


                        return new V1SortableIndex(fieldName, documentToValue);
                    }
                });
    }
}
