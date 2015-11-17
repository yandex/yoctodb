/*
 * (C) YANDEX LLC, 2014-2015
 *
 * The Source Code called "YoctoDB" available at
 * https://bitbucket.org/yandex/yoctodb is subject to the terms of the
 * Mozilla Public License, v. 2.0 (hereinafter referred to as the "License").
 *
 * A copy of the License is also available at http://mozilla.org/MPL/2.0/.
 */

package com.yandex.yoctodb.v1.immutable.segment;

import com.yandex.yoctodb.util.buf.Buffer;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import java.io.IOException;
import java.util.NoSuchElementException;

import static junit.framework.Assert.assertEquals;

/**
 * Unit tests for {@link SegmentRegistry}
 *
 * @author incubos
 */
public class SegmentRegistryTest {
    private final Segment PROBE =
            new Segment() {
            };

    private final SegmentReader DUMMY =
            new SegmentReader() {
                @NotNull
                @Override
                public Segment read(
                        @NotNull
                        final Buffer buffer) throws IOException {
                    return PROBE;
                }
            };

    @Test(expected = NoSuchElementException.class)
    public void notExisting() throws IOException {
        final int ID = -1;
        SegmentRegistry.read(ID, Buffer.from(new byte[]{}));
    }

    @Test
    public void register() {
        final int ID = -1;
        SegmentRegistry.register(ID, DUMMY);
    }

    @Test
    public void registerAndReturn() throws IOException {
        final int ID = -2;
        SegmentRegistry.register(ID, DUMMY);
        assertEquals(
                PROBE,
                SegmentRegistry.read(ID, Buffer.from(new byte[]{})));
    }

    @Test(expected = IllegalArgumentException.class)
    public void registerExisting() {
        final int ID = -3;
        SegmentRegistry.register(ID, DUMMY);
        SegmentRegistry.register(ID, DUMMY);
    }
}
