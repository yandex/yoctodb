package com.yandex.yoctodb.v1.immutable.segment;

import com.yandex.yoctodb.immutable.StoredIndex;
import com.yandex.yoctodb.util.buf.Buffer;
import com.yandex.yoctodb.util.immutable.ByteArrayIndexedList;
import com.yandex.yoctodb.util.immutable.impl.FoldedByteArrayIndexedList;
import com.yandex.yoctodb.util.immutable.impl.VariableLengthByteArrayIndexedList;
import com.yandex.yoctodb.v1.V1DatabaseFormat;
import org.jetbrains.annotations.NotNull;

/**
 * Immutable {@link StoredIndex} implementation
 *
 * @author incubos
 */
public class V1StoredIndex implements StoredIndex, Segment {
    @NotNull
    private final String fieldName;
    @NotNull
    private final ByteArrayIndexedList values;

    V1StoredIndex(
            @NotNull final String fieldName,
            @NotNull final ByteArrayIndexedList values) {
        // May be constructed only from SegmentReader
        this.fieldName = fieldName;
        this.values = values;
    }

    @NotNull
    @Override
    public String getFieldName() {
        return fieldName;
    }

    @NotNull
    @Override
    public Buffer getStoredValue(final int document) {
        return values.get(document);
    }

    @Override
    public long getLongValue(final int document) {
        return values.getLongUnsafe(document);
    }

    @Override
    public int getIntValue(final int document) {
        return values.getIntUnsafe(document);
    }

    @Override
    public short getShortValue(final int document) {
        return values.getShortUnsafe(document);
    }

    @Override
    public char getCharValue(final int document) {
        return values.getCharUnsafe(document);
    }

    @Override
    public byte getByteValue(final int document) {
        return values.getByteUnsafe(document);
    }

    static void registerReader() {
        SegmentRegistry.register(
                V1DatabaseFormat.SegmentType
                        .VARIABLE_LENGTH_FOLDED_INDEX.getCode(),
                new SegmentReader() {
                    @NotNull
                    @Override
                    public Segment read(
                            @NotNull final Buffer buffer) {
                        final String fieldName = Segments.extractString(buffer);

                        final ByteArrayIndexedList values =
                                FoldedByteArrayIndexedList.from(
                                        Segments.extract(buffer));

                        return new V1StoredIndex(
                                fieldName,
                                values);
                    }
                });

        SegmentRegistry.register(
                V1DatabaseFormat.SegmentType
                        .VARIABLE_LENGTH_STORED_INDEX.getCode(),
                new SegmentReader() {
                    @NotNull
                    @Override
                    public Segment read(
                            @NotNull final Buffer buffer) {
                        final String fieldName = Segments.extractString(buffer);

                        final ByteArrayIndexedList values =
                                VariableLengthByteArrayIndexedList.from(
                                        Segments.extract(buffer));

                        return new V1StoredIndex(
                                fieldName,
                                values);
                    }


                });
    }
}
