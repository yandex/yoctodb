/*
 * Copyright © 2014 Yandex
 * This work is free. You can redistribute it and/or modify it under the
 * terms of the Do What The Fuck You Want To Public License, Version 2,
 * as published by Sam Hocevar. See the COPYING file or
 * http://www.wtfpl.net/ for more details.
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
