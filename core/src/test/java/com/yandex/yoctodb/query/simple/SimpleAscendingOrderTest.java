/*
 * (C) YANDEX LLC, 2014-2015
 *
 * The Source Code called "YoctoDB" available at
 * https://bitbucket.org/yandex/yoctodb is subject to the terms of the
 * Mozilla Public License, v. 2.0 (hereinafter referred to as the "License").
 *
 * A copy of the License is also available at http://mozilla.org/MPL/2.0/.
 */

package com.yandex.yoctodb.query.simple;

import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for {@link com.yandex.yoctodb.query.simple.SimpleAscendingOrder}
 *
 * @author incubos
 */
public class SimpleAscendingOrderTest {
    @Test
    public void nonDefaultToString() {
        Assert.assertTrue(
                new SimpleAscendingOrder("myField").toString()
                        .contains("myField"));
    }
}
