/*
 * Copyright (c) 2014 Yandex
 */

package ru.yandex.yoctodb.v1.immutable.segment;

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
