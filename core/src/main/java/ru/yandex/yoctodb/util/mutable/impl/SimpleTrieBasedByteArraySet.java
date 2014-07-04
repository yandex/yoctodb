/*
 * Copyright © 2014 Yandex
 * This work is free. You can redistribute it and/or modify it under the
 * terms of the Do What The Fuck You Want To Public License, Version 2,
 * as published by Sam Hocevar. See the COPYING file or
 * http://www.wtfpl.net/ for more details.
 */

package ru.yandex.yoctodb.util.mutable.impl;

import com.google.common.primitives.Ints;
import net.jcip.annotations.NotThreadSafe;
import org.jetbrains.annotations.NotNull;
import ru.yandex.yoctodb.util.UnsignedByteArray;
import ru.yandex.yoctodb.util.UnsignedByteArrays;

import java.io.IOException;
import java.io.OutputStream;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;

/**
 * @author svyatoslav
 */
@NotThreadSafe
public class SimpleTrieBasedByteArraySet extends AbstractTrieBasedByteArraySet {
    @Override
    public int indexOf(@NotNull
                           UnsignedByteArray e) {
        if (!frozen) {
            build();
        }

        final Integer result = sortedElements.get(e);

        assert result != null;

        return result;
    }

    @Override
    public int getSizeInBytes() {
        return trieSizeInBytes + 4;
    }

    @Override
    public void writeTo(@NotNull OutputStream os) throws IOException {
        if (!frozen) {
            build();
        }


        assert !sortedElements.isEmpty();

        // Element count
        os.write(Ints.toByteArray(sortedElements.size()));
        final Queue<TrieNode> queue = new LinkedList<TrieNode>();
        queue.add(root);
        while (queue.size() > 0) {
            final TrieNode node = queue.poll();
            if (node.getPosition() >= 0) {
                os.write(UnsignedByteArrays.BOOLEAN_TRUE_IN_BYTE); //node is terminal
                os.write(Ints.toByteArray(node.getPosition())); //position of key in sorted set
            } else {
                os.write(UnsignedByteArrays.BOOLEAN_FALSE_IN_BYTE); //node is not terminal
            }
            os.write(node.isCompressed() ? UnsignedByteArrays.BOOLEAN_TRUE_IN_BYTE : UnsignedByteArrays.BOOLEAN_FALSE_IN_BYTE);//compression
            //write jmp's
            if (node.isCompressed()) {
                //if node is compressed, write list of children with jmp's
                os.write(Ints.toByteArray(node.getChildren().size())); //count of children
                for (final Map.Entry<Byte, TrieNode> entry : node.getChildren().entrySet()) {
                    os.write(entry.getKey()); //jmp key
                    os.write(Ints.toByteArray(entry.getValue().getOffset())); //jmp destination
                }
            } else {
                //if node isn't compressed, write all possible jmp's
                os.write(node.getMinByteJmpValue());
                os.write(node.getMaxByteJmpValue());
                byte b = node.getMinByteJmpValue();
                for (int k = node.getMinByteJmpValue(); k <= node.getMaxByteJmpValue(); k++) {
                    final TrieNode child = node.getChildren().get(b);
                    if (child != null) {
                        //jmp to child offset
                        os.write(Ints.toByteArray(child.getOffset()));
                    } else {
                        os.write(Ints.toByteArray(-1));
                    }
                    b++;
                }
            }
            for (final TrieNode child : node.getChildren().values()) {
                queue.add(child);
            }
        }
    }
}
