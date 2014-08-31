/*
 * (C) YANDEX LLC, 2014
 *
 * The Source Code called "YoctoDB" available at
 * https://bitbucket.org/yandex/yoctodb is subject to the terms of the
 * Mozilla Public License, v. 2.0 (hereinafter - MPL).
 *
 * A copy of the MPL is available at http://mozilla.org/MPL/2.0/.
 */

package com.yandex.yoctodb.v1.immutable.segment;

import java.io.IOException;

/**
 * This exception is thrown when detects
 * an inconsistency in the segment.
 *
 * @author svyatoslav
 */
public class CorruptSegmentException extends IOException {
    public CorruptSegmentException(String message) {
        super(message);
    }
}
