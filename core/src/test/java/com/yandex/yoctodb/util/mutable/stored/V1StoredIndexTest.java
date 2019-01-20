package com.yandex.yoctodb.util.mutable.stored;

import com.yandex.yoctodb.util.OutputStreamWritable;
import com.yandex.yoctodb.util.UnsignedByteArray;
import com.yandex.yoctodb.util.UnsignedByteArrays;
import com.yandex.yoctodb.util.mutable.impl.VariableLengthByteArrayIndexedList;
import com.yandex.yoctodb.v1.mutable.segment.V1StoredIndex;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class V1StoredIndexTest {

    @Test
    public void buildV1StoredIndex() throws IOException {
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
        System.out.println(indexedList.getSizeInBytes());

        index.setDatabaseDocumentsCount(data.size());
        OutputStreamWritable outputStreamWritable = index.buildWritable();
        System.out.println(outputStreamWritable.getSizeInBytes());

        System.out.println(getSizeInBytes(fieldName, indexedList));

        assert (outputStreamWritable.getSizeInBytes() == getSizeInBytes(fieldName, indexedList));

    }

    private Map<Integer, UnsignedByteArray> initData() {
        Map<Integer, UnsignedByteArray> data = new HashMap<>();
        data.put(0, UnsignedByteArrays.from("NEW"));
        data.put(1, UnsignedByteArrays.from("USED"));
        data.put(2, UnsignedByteArrays.from("NEW"));
        data.put(3, UnsignedByteArrays.from("USED"));
        data.put(4, UnsignedByteArrays.from("NEW"));
        data.put(5, UnsignedByteArrays.from("USED"));
        return data;
    }

    private long getSizeInBytes(String fieldName, VariableLengthByteArrayIndexedList valueIndex) {
        return 4L + // Field name
                (long) fieldName.length() +
                8 + // Values
                valueIndex.getSizeInBytes();
    }
}
