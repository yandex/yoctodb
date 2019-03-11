package com.yandex.yoctodb.v1.mutable.segment;

import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import com.yandex.yoctodb.util.OutputStreamWritable;
import com.yandex.yoctodb.util.UnsignedByteArray;
import com.yandex.yoctodb.util.UnsignedByteArrays;
import com.yandex.yoctodb.util.mutable.impl.FoldedByteArrayIndexedList;
import com.yandex.yoctodb.util.mutable.impl.VariableLengthByteArrayIndexedList;
import com.yandex.yoctodb.v1.V1DatabaseFormat;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Index supporting extracting stored field value by document ID
 *
 * @author incubos
 */
public class V1StoredIndex
        extends Freezable
        implements IndexSegment {
    @NotNull
    private final byte[] fieldName;
    private Map<UnsignedByteArray, List<Integer>> valueDocId = new LinkedHashMap<>();
    private int databaseDocumentsCount = -1;
    private boolean uniqueValues = true;
    private int segmentTypeCode;

    public V1StoredIndex(
            @NotNull final String fieldName) {
        this.fieldName = fieldName.getBytes();
    }

    @NotNull
    @Override
    public IndexSegment addDocument(
            final int documentId,
            @NotNull final Collection<UnsignedByteArray> values) {
        if (documentId < 0)
            throw new IllegalArgumentException("Negative document ID");
        if (values.size() != 1)
            throw new IllegalArgumentException("A single value expected");

        checkNotFrozen();

        final UnsignedByteArray value = values.iterator().next();

        List<Integer> indexes =
                this.valueDocId.computeIfAbsent(value,
                v -> new LinkedList<>());

        if (!indexes.isEmpty()) {
            uniqueValues = false;
        }
        indexes.add(documentId);

        return this;
    }

    @Override
    public void setDatabaseDocumentsCount(final int documentsCount) {
        assert documentsCount > 0;

        this.databaseDocumentsCount = documentsCount;
    }

    @NotNull
    @Override
    public OutputStreamWritable buildWritable() {
        checkNotFrozen();

        freeze();

        assert databaseDocumentsCount > 0;

        // Building the index
        final OutputStreamWritable valueIndex =
                getWritable();
        // Free memory
        valueDocId = null;

        return new OutputStreamWritable() {
            @Override
            public long getSizeInBytes() {
                return Integer.BYTES + // Field name
                       fieldName.length +
                       Long.BYTES + // Values
                       valueIndex.getSizeInBytes();
            }
            @Override
            public void writeTo(
                    @NotNull final OutputStream os) throws IOException {
                os.write(Longs.toByteArray(getSizeInBytes()));

                // Payload segment type
                os.write(Ints.toByteArray(segmentTypeCode));

                // Field name
                os.write(Ints.toByteArray(fieldName.length));
                os.write(fieldName);

                // Values
                os.write(Longs.toByteArray(valueIndex.getSizeInBytes()));
                valueIndex.writeTo(os);
            }
        };
    }

    private OutputStreamWritable getWritable() {
        if (uniqueValues) {
            final Collection<UnsignedByteArray> padded =
                    new ArrayList<>(databaseDocumentsCount);
            int expectedDocument = 0;
            final UnsignedByteArray empty = UnsignedByteArrays.from(new byte[]{});
            for (Map.Entry<UnsignedByteArray, List<Integer>> e : valueDocId.entrySet()) {
                while (expectedDocument < e.getValue().iterator().next()) {
                    padded.add(empty);
                    expectedDocument++;
                }
                padded.add(e.getKey());
                expectedDocument++;
            }
            while (expectedDocument < databaseDocumentsCount) {
                padded.add(empty);
                expectedDocument++;
            }
            segmentTypeCode = V1DatabaseFormat
                    .SegmentType
                    .VARIABLE_LENGTH_STORED_INDEX
                    .getCode();
            return new VariableLengthByteArrayIndexedList(padded);
        } else {
            segmentTypeCode = V1DatabaseFormat
                    .SegmentType
                    .VARIABLE_LENGTH_FOLDED_INDEX
                    .getCode();
            return new FoldedByteArrayIndexedList(valueDocId, databaseDocumentsCount);
        }
    }
}
