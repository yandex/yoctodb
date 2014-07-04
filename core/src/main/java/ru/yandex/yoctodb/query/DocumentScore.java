/*
 * Copyright © 2014 Yandex
 * This work is free. You can redistribute it and/or modify it under the
 * terms of the Do What The Fuck You Want To Public License, Version 2,
 * as published by Sam Hocevar. See the COPYING file or
 * http://www.wtfpl.net/ for more details.
 */

package ru.yandex.yoctodb.query;

import net.jcip.annotations.Immutable;

/**
 * Comparable document score
 *
 * @author incubos
 */
@Immutable
public interface DocumentScore<T extends DocumentScore<T>>
        extends Comparable<T> {
}
