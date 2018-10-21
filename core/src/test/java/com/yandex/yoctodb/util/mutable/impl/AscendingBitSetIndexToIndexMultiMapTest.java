package com.yandex.yoctodb.util.mutable.impl;

import org.junit.Test;

import static java.util.Collections.*;
import static org.junit.Assert.*;

public class AscendingBitSetIndexToIndexMultiMapTest {
    @Test(expected = IllegalArgumentException.class)
    public void negativeDocumentCount() {
        new AscendingBitSetIndexToIndexMultiMap(singletonList(singletonList(1)), -1);
    }

    @Test
    public void tostring() {
        assertNotNull(
                new AscendingBitSetIndexToIndexMultiMap(
                        singletonList(singletonList(1)),
                        0).toString());
    }
}
