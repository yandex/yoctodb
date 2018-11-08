/*
 * (C) YANDEX LLC, 2014-2018
 *
 * The Source Code called "YoctoDB" available at
 * https://github.com/yandex/yoctodb is subject to the terms of the
 * Mozilla Public License, v. 2.0 (hereinafter referred to as the "License").
 *
 * A copy of the License is also available at http://mozilla.org/MPL/2.0/.
 */

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
