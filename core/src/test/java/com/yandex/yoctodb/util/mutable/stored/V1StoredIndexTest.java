package com.yandex.yoctodb.util.mutable.stored;

import com.yandex.yoctodb.util.OutputStreamWritable;
import com.yandex.yoctodb.util.UnsignedByteArray;
import com.yandex.yoctodb.util.UnsignedByteArrays;
import com.yandex.yoctodb.util.mutable.impl.VariableLengthByteArrayIndexedList;
import com.yandex.yoctodb.v1.mutable.segment.V1StoredIndex;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * {@link com.yandex.yoctodb.util.immutable.ByteArrayIndexedList} with fixed size
 * elements
 *
 * @author irenkamalova
 */
public class V1StoredIndexTest {

    @Test
    public void buildV1StoredIndex() {
        final String fieldName = "testFiledName";
        V1StoredIndex index = new V1StoredIndex(fieldName);
        final Map<Integer, UnsignedByteArray> data = initData();
        List<UnsignedByteArray> elements = new ArrayList<>(data.size());
        for(Map.Entry<Integer, UnsignedByteArray> entry : data.entrySet()) {
            index.addDocument(entry.getKey(), Collections.singletonList(entry.getValue()));
            elements.add(entry.getValue());
        }
        VariableLengthByteArrayIndexedList indexedList =
                new VariableLengthByteArrayIndexedList(elements);

        index.setDatabaseDocumentsCount(data.size());
        OutputStreamWritable outputStreamWritable = index.buildWritable();

        assert (outputStreamWritable.getSizeInBytes() == getSizeInBytes(fieldName, indexedList));
    }

    private Map<Integer, UnsignedByteArray> initData() {
        Map<Integer, UnsignedByteArray> data = new HashMap<>();
        data.put(0, UnsignedByteArrays.from("NEW"));
        data.put(1, UnsignedByteArrays.from("USED"));
        return data;
    }

    private long getSizeInBytes(String fieldName, VariableLengthByteArrayIndexedList valueIndex) {
        return 4L + // Field name
                (long) fieldName.length() +
                8 + // Values
                valueIndex.getSizeInBytes();
    }
}
