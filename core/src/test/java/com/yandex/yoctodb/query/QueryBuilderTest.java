/*
 * (C) YANDEX LLC, 2014-2015
 *
 * The Source Code called "YoctoDB" available at
 * https://bitbucket.org/yandex/yoctodb is subject to the terms of the
 * Mozilla Public License, v. 2.0 (hereinafter referred to as the "License").
 *
 * A copy of the License is also available at http://mozilla.org/MPL/2.0/.
 */

package com.yandex.yoctodb.query;

import com.yandex.yoctodb.util.UnsignedByteArrays;
import org.junit.Test;

/**
 * Unit tests for {@link Query}
 *
 * @author incubos
 */
public class QueryBuilderTest {

    @Test
    public void pointRange() {
        QueryBuilder.in(
                "test",
                UnsignedByteArrays.from(1),
                true,
                UnsignedByteArrays.from(1),
                true);
    }

    @Test(expected = IllegalArgumentException.class)
    public void emptyLeftRange() {
        QueryBuilder.in(
                "test",
                UnsignedByteArrays.from(1),
                true,
                UnsignedByteArrays.from(1),
                false);
    }

    @Test(expected = IllegalArgumentException.class)
    public void emptyRightRange() {
        QueryBuilder.in(
                "test",
                UnsignedByteArrays.from(1),
                true,
                UnsignedByteArrays.from(1),
                false);
    }

    @Test(expected = IllegalArgumentException.class)
    public void reversedRange() {
        QueryBuilder.in(
                "test",
                UnsignedByteArrays.from(2),
                true,
                UnsignedByteArrays.from(1),
                false);
    }
}
