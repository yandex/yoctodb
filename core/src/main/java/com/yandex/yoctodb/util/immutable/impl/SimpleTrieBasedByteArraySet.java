/*
 * (C) YANDEX LLC, 2014
 *
 * The Source Code called "YoctoDB" available at
 * https://bitbucket.org/yandex/yoctodb is subject to the terms of the
 * Mozilla Public License, v. 2.0 (hereinafter - MPL).
 *
 * A copy of the MPL is available at http://mozilla.org/MPL/2.0/.
 */

package com.yandex.yoctodb.util.immutable.impl;

import net.jcip.annotations.Immutable;
import org.jetbrains.annotations.NotNull;
import com.yandex.yoctodb.util.UnsignedByteArrays;
import com.yandex.yoctodb.util.immutable.TrieBasedByteArraySet;

import java.nio.ByteBuffer;

/**
 * @author svyatoslav
 */
@Immutable
public class SimpleTrieBasedByteArraySet implements TrieBasedByteArraySet {
    private final int size;
    private final ByteBuffer trie;

    public static TrieBasedByteArraySet from(
            @NotNull
            final ByteBuffer buffer) {
        final int elementsCount = buffer.getInt();
        assert elementsCount > 0;
        return new SimpleTrieBasedByteArraySet(elementsCount, buffer.slice());
    }

    public SimpleTrieBasedByteArraySet(int size, ByteBuffer trie) {
        this.size = size;
        this.trie = trie;
    }

    @Override
    public int size() {
        return size;
    }

    @Override
    public int indexOf(
            @NotNull
            final ByteBuffer e) {
        int position = 0;
        for (byte currentByte : e.array()) {
            //node children is compressed (stored ordered list of child nodes)
            final byte terminalTypeByte = trie.get(position++);
            if (terminalTypeByte == UnsignedByteArrays.BOOLEAN_TRUE_IN_BYTE) {
                //skiping position of subjects key id
                position += 4;
            }
            final byte isCompressedByte = trie.get(position++);

            if (isCompressedByte == UnsignedByteArrays.BOOLEAN_TRUE_IN_BYTE) {
                final int countOfChildren = trie.getInt(position);
                position += 4;
                if (countOfChildren == 0) {
                    return -1;
                }
                int left = 0;
                int right = countOfChildren - 1;
                final int offset = position;
                boolean founded = false;
                while (left <= right) {
                    final int middle = (right + left) >>> 1;
                    position = offset + 5 * middle;
                    final byte jmpKey = trie.get(position++);
                    if (jmpKey == currentByte) {
                        position = trie.getInt(position);
                        founded = true;
                        break;
                    } else if (jmpKey < currentByte) {
                        left = middle + 1;
                    } else {
                        right = middle - 1;
                    }
                }
                if (!founded) {
                    return -1;
                }
            } else {
                final byte minJmpKey = trie.get(position++);
                final byte maxJmpKey = trie.get(position++);

                assert maxJmpKey >= minJmpKey;

                if (currentByte > maxJmpKey
                        || currentByte < minJmpKey) {
                    return -1;
                }
                position += (currentByte - minJmpKey) * 4;
                final int jmpDest = trie.getInt(position);
                if (jmpDest > 0) {
                    position = jmpDest;
                } else {
                    return -1;
                }
            }
        }
        final byte terminalTypeByte = trie.get(position++);
        if (terminalTypeByte == UnsignedByteArrays.BOOLEAN_TRUE_IN_BYTE) {
            return trie.getInt(position);
        }
        return -1;
    }

    @Override
    public int indexOfGreaterThan(
            @NotNull
            final ByteBuffer e,
            final boolean orEquals,
            final int upToIndexInclusive) {
        assert 0 <= upToIndexInclusive && upToIndexInclusive < size();

        int position = 0;
        for (byte currentByte : e.array()) {
            //node children is compressed (stored ordered list of child nodes)
            final byte terminalTypeByte = trie.get(position++);
            if (terminalTypeByte == UnsignedByteArrays.BOOLEAN_TRUE_IN_BYTE) {
                //skiping position of subjects key id
                position += 4;
            }
            final byte isCompressedByte = trie.get(position++);

            if (isCompressedByte == UnsignedByteArrays.BOOLEAN_TRUE_IN_BYTE) {
                final int countOfChildren = trie.getInt(position);
                position += 4;
                if (countOfChildren == 0) {
                    return -1;
                }
                int left = 0;
                int right = countOfChildren - 1;
                final int offset = position;
                boolean founded = false;
                while (left <= right) {
                    final int middle = (right + left) >>> 1;
                    position = offset + 5 * middle;
                    final byte jmpKey = trie.get(position++);
                    if (jmpKey == currentByte) {
                        position = trie.getInt(position);
                        founded = true;
                        break;
                    } else if (jmpKey < currentByte) {
                        left = middle + 1;
                    } else {
                        right = middle - 1;
                    }
                }
                if (!founded) {
                    return -1;
                }
            } else {
                final byte minJmpKey = trie.get(position++);
                final byte maxJmpKey = trie.get(position++);

                assert minJmpKey > maxJmpKey;

                if (currentByte > maxJmpKey
                        || currentByte < minJmpKey) {
                    return -1;
                }
                position += (currentByte - minJmpKey) * 4;
                final int jmpDest = trie.getInt(position);
                if (jmpDest > 0) {
                    position = jmpDest;
                } else {
                    return -1;
                }
            }
        }
        final byte terminalTypeByte = trie.get(position++);
        if (terminalTypeByte == UnsignedByteArrays.BOOLEAN_TRUE_IN_BYTE) {
            return trie.getInt(position);
        }
        return -1;
    }

    @Override
    public int indexOfLessThan(
            @NotNull
            final ByteBuffer e,
            final boolean orEquals,
            final int fromIndexInclusive) {
        assert 0 <= fromIndexInclusive && fromIndexInclusive < size();

        int position = 0;
        for (byte currentByte : e.array()) {
            //node children is compressed (stored ordered list of child nodes)
            final byte terminalTypeByte = trie.get(position++);
            if (terminalTypeByte == UnsignedByteArrays.BOOLEAN_TRUE_IN_BYTE) {
                //skiping position of subjects key id
                position += 4;
            }
            final byte isCompressedByte = trie.get(position++);

            if (isCompressedByte == UnsignedByteArrays.BOOLEAN_TRUE_IN_BYTE) {
                final int countOfChildren = trie.getInt(position);
                position += 4;
                if (countOfChildren == 0) {
                    return -1;
                }
                int left = 0;
                int right = countOfChildren - 1;
                final int offset = position;
                boolean founded = false;
                while (left <= right) {
                    final int middle = (right + left) >>> 1;
                    position = offset + 5 * middle;
                    final byte jmpKey = trie.get(position++);
                    if (jmpKey == currentByte) {
                        position = trie.getInt(position);
                        founded = true;
                        break;
                    } else if (jmpKey < currentByte) {
                        left = middle + 1;
                    } else {
                        right = middle - 1;
                    }
                }
                if (!founded) {
                    return -1;
                }
            } else {
                final byte minJmpKey = trie.get(position++);
                final byte maxJmpKey = trie.get(position++);

                assert minJmpKey > maxJmpKey;

                if (currentByte > maxJmpKey
                        || currentByte < minJmpKey) {
                    return -1;
                }
                position += (currentByte - minJmpKey) * 4;
                final int jmpDest = trie.getInt(position);
                if (jmpDest > 0) {
                    position = jmpDest;
                } else {
                    return -1;
                }
            }
        }
        final byte terminalTypeByte = trie.get(position++);
        if (terminalTypeByte == UnsignedByteArrays.BOOLEAN_TRUE_IN_BYTE) {
            return trie.getInt(position);
        }
        return -1;
    }

    @Override
    public String toString() {
        return "SimpleTrieBasedByteArraySet{" +
                "size=" + size +
                '}';
    }
}
