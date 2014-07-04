/*
 * Copyright (c) 2014 Yandex
 */

package ru.yandex.yoctodb.v1.immutable.segment;

import net.jcip.annotations.Immutable;
import org.jetbrains.annotations.NotNull;
import ru.yandex.yoctodb.immutable.FilterableIndex;
import ru.yandex.yoctodb.util.immutable.IndexToIndexMultiMap;
import ru.yandex.yoctodb.util.immutable.TrieBasedByteArraySet;
import ru.yandex.yoctodb.util.immutable.impl.IndexToIndexMultiMapReader;
import ru.yandex.yoctodb.util.immutable.impl.SimpleTrieBasedByteArraySet;
import ru.yandex.yoctodb.util.mutable.BitSet;
import ru.yandex.yoctodb.v1.V1DatabaseFormat;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * @author svyatoslav
 */
@Immutable
public class V1TrieBasedFilterableIndex implements FilterableIndex, Segment {
    @NotNull
    private final String fieldName;
    @NotNull
    private final TrieBasedByteArraySet values;
    @NotNull
    private final IndexToIndexMultiMap valueToDocuments;

    public V1TrieBasedFilterableIndex(@NotNull String fieldName,
                                      @NotNull TrieBasedByteArraySet values,
                                      @NotNull IndexToIndexMultiMap valueToDocuments) {
        this.fieldName = fieldName;
        this.values = values;
        this.valueToDocuments = valueToDocuments;
    }

    @Override
    public boolean eq(@NotNull BitSet dest, @NotNull ByteBuffer value) {
        final int valueIndex = values.indexOf(value);
        return valueIndex != -1 && valueToDocuments.get(dest, valueIndex);
    }

    @Override
    public boolean in(
            @NotNull
            final BitSet dest,
            @NotNull
            final ByteBuffer... value) {
        boolean result = false;
        for (ByteBuffer currentValue : value) {
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
            final ByteBuffer value,
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
            final ByteBuffer value,
            final boolean orEquals) {
        final int greatestValueIndex = values.indexOfGreaterThan(
                value,
                orEquals,
                values.size() - 1);
        return greatestValueIndex != -1 &&
                valueToDocuments.getFrom(dest, greatestValueIndex);
    }

    @Override
    public boolean between(
            @NotNull
            final BitSet dest,
            @NotNull
            final ByteBuffer from,
            final boolean fromInclusive,
            @NotNull
            final ByteBuffer to,
            final boolean toInclusive) {
        final int rawFromValueIndex =
                values.indexOfGreaterThan(
                        from,
                        fromInclusive,
                        values.size() - 1);
        final int rawToValueIndex =
                values.indexOfLessThan(
                        to,
                        toInclusive,
                        rawFromValueIndex == -1 ? 0 : rawFromValueIndex);

        if (rawFromValueIndex == -1 && rawToValueIndex == -1) {
            return false;
        } else {
            final int fromIndexInclusive =
                    rawFromValueIndex == -1 ? 0 : rawFromValueIndex;
            final int toIndexExclusive =
                    rawToValueIndex == -1 ?
                            values.size() :
                            rawToValueIndex + 1;
            return valueToDocuments.getBetween(
                    dest,
                    fromIndexInclusive,
                    toIndexExclusive);
        }
    }

    static void registerReader() {
        SegmentRegistry.register(
                V1DatabaseFormat.SegmentType.TRIE_BASED_FILTER.getCode(),
                new SegmentReader() {
                    @NotNull
                    @Override
                    public Segment read(
                            @NotNull
                            final ByteBuffer buffer) throws IOException {

                        final byte[] digest = Segments.calculateDigest(buffer, V1DatabaseFormat.MESSAGE_DIGEST_ALGORITHM);

                        final String fieldName = Segments.extractString(buffer);

                        final TrieBasedByteArraySet values =
                                SimpleTrieBasedByteArraySet.from(
                                        Segments.extract(buffer));

                        final IndexToIndexMultiMap valueToDocuments =
                                IndexToIndexMultiMapReader.from(
                                        Segments.extract(buffer));

                        final ByteBuffer digestActual = Segments.extract(buffer);
                        if (!digestActual.equals(ByteBuffer.wrap(digest))) {
                            throw new CorruptSegmentException("checksum error");
                        }

                        return new V1TrieBasedFilterableIndex(
                                fieldName,
                                values,
                                valueToDocuments);
                    }


                }
        );
    }

    @NotNull
    @Override
    public String getFieldName() {
        return fieldName;
    }
}
