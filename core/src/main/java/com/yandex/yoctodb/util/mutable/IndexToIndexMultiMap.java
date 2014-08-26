/*
 * (C) YANDEX LLC, 2014
 *
 * The Source Code called "YoctoDB" available at
 * https://bitbucket.org/yandex/yoctodb is subject to the terms of the
 * Mozilla Public License, v. 2.0 (hereinafter - MPL).
 *
 * A copy of the MPL is available at http://mozilla.org/MPL/2.0/.
 */

package com.yandex.yoctodb.util.mutable;

import net.jcip.annotations.NotThreadSafe;
import com.yandex.yoctodb.util.OutputStreamWritable;

/**
 * Integer to Integer multi map
 *
 * @author incubos
 */
@NotThreadSafe
public interface IndexToIndexMultiMap extends OutputStreamWritable {
    void add(int key, int value);
}
