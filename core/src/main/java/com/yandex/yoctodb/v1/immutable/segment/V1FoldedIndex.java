package com.yandex.yoctodb.v1.immutable.segment;

import com.yandex.yoctodb.immutable.FoldedIndex;
import com.yandex.yoctodb.util.buf.Buffer;
import com.yandex.yoctodb.util.immutable.ByteArrayIndexedList;
import com.yandex.yoctodb.util.immutable.impl.FoldedByteArrayIndex;
import com.yandex.yoctodb.v1.V1DatabaseFormat;
import org.jetbrains.annotations.NotNull;

final public class V1FoldedIndex implements FoldedIndex, Segment {
    @NotNull
    private final String fieldName;
    @NotNull
    private final ByteArrayIndexedList values;

    V1FoldedIndex(
            @NotNull
            final String fieldName,
            @NotNull
            final ByteArrayIndexedList values) {
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
    public Buffer getFoldedValue(final int document) {
        return values.get(document);
    }

    static void registerReader() {
        SegmentRegistry.register(
                V1DatabaseFormat.SegmentType
                        .VARIABLE_LENGTH_FOLDED_INDEX.getCode(),
                new SegmentReader() {
                    @NotNull
                    @Override
                    public Segment read(
                            @NotNull
                            final Buffer buffer) {
                        final String fieldName = Segments.extractString(buffer);

                        final ByteArrayIndexedList values =
                                FoldedByteArrayIndex.from(
                                        Segments.extract(buffer));

                        return new V1FoldedIndex(
                                fieldName,
                                values);
                    }
                });
    }
}

