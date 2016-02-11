/*
 * (C) YANDEX LLC, 2014-2016
 *
 * The Source Code called "YoctoDB" available at
 * https://github.com/yandex/yoctodb is subject to the terms of the
 * Mozilla Public License, v. 2.0 (hereinafter referred to as the "License").
 *
 * A copy of the License is also available at http://mozilla.org/MPL/2.0/.
 */

package com.yandex.yoctodb.v1.mutable;

import com.yandex.yoctodb.mutable.DocumentBuilder;
import org.junit.Test;

/**
 * Unit tests for {@link V1DocumentBuilder}
 *
 * @author incubos
 */
public class V1DocumentBuilderTest {
    @Test(expected = IllegalStateException.class)
    public void payloadOverwrite() {
        new V1DocumentBuilder()
                .withPayload(new byte[]{})
                .withPayload(new byte[]{});
    }

    @Test(expected = IllegalArgumentException.class)
    public void differentIndexOptions() {
        new V1DocumentBuilder()
                .withField("k", "v1", DocumentBuilder.IndexOption.FILTERABLE)
                .withField("k", "v2", DocumentBuilder.IndexOption.SORTABLE);
    }

    @Test(expected = IllegalArgumentException.class)
    public void differentLength() {
        new V1DocumentBuilder()
                .withField("k", "0", DocumentBuilder.IndexOption.FULL)
                .withField("k", 0, DocumentBuilder.IndexOption.FULL);
    }
}
