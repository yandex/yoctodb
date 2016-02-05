/*
 * (C) YANDEX LLC, 2014-2016
 *
 * The Source Code called "YoctoDB" available at
 * https://github.com/yandex/yoctodb is subject to the terms of the
 * Mozilla Public License, v. 2.0 (hereinafter referred to as the "License").
 *
 * A copy of the License is also available at http://mozilla.org/MPL/2.0/.
 */

package com.yandex.yoctodb.v1.mutable.segment;

import org.junit.Test;

/**
 * Unit tests for {@link V1PayloadSegment}
 *
 * @author incubos
 */
public class V1PayloadSegmentTest {
    @Test
    public void addDocument() {
        new V1PayloadSegment().addDocument(0, new byte[]{0});
    }

    @Test(expected = IllegalArgumentException.class)
    public void wrongDocument() {
        new V1PayloadSegment().addDocument(-1, new byte[]{0});
    }

    @Test(expected = IllegalArgumentException.class)
    public void inconsequentDocuments() {
        new V1PayloadSegment()
                .addDocument(0, new byte[]{0})
                .addDocument(2, new byte[]{0});
    }
}
