package com.yandex.yoctodb.util.mutable.stored;

import com.yandex.yoctodb.util.UnsignedByteArray;
import com.yandex.yoctodb.util.UnsignedByteArrays;
import com.yandex.yoctodb.util.mutable.impl.VariableLengthByteArrayIndexedList;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;

public class VariableLengthByteArrayIndexedListTest {

    @Test
    public void buildV1StoredIndex() {
        final Collection<UnsignedByteArray> elements = initElements();

        VariableLengthByteArrayIndexedList indexedList =
                new VariableLengthByteArrayIndexedList(elements);

        System.out.println(indexedList.getSizeInBytes());
        System.out.println(getSizeInBytes(elements));

        assert indexedList.getSizeInBytes() == getSizeInBytes(elements);
    }

    private Collection<UnsignedByteArray> initElements() {
        Collection<UnsignedByteArray> elements = new ArrayList<>();
        elements.add(UnsignedByteArrays.from("NEW"));
        elements.add(UnsignedByteArrays.from("USED"));
        return elements;
    }

    public long getSizeInBytes(Collection<UnsignedByteArray> elements) {
        long elemSum = 0;
        for (UnsignedByteArray elem : elements) {
            elemSum = elemSum + elem.length();
        }
        return 4L + // Element count
                8L * (elements.size() + 1L) + // Element offsets
                elemSum; // Element array size
    }

}
