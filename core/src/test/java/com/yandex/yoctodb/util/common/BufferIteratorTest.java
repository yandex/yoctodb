/*
 * (C) YANDEX LLC, 2014-2018
 *
 * The Source Code called "YoctoDB" available at
 * https://github.com/yandex/yoctodb is subject to the terms of the
 * Mozilla Public License, v. 2.0 (hereinafter referred to as the "License").
 *
 * A copy of the License is also available at http://mozilla.org/MPL/2.0/.
 */

package com.yandex.yoctodb.util.common;

import com.yandex.yoctodb.util.buf.Buffer;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.NoSuchElementException;

public class BufferIteratorTest {
    @Test(expected = NoSuchElementException.class)
    public void invalidNext() {
        new BufferIterator(Buffer.from(ByteBuffer.allocate(0))).next();
    }
}
