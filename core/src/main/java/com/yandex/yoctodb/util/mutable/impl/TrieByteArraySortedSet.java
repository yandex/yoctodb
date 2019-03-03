/*
 * (C) YANDEX LLC, 2014-2018
 *
 * The Source Code called "YoctoDB" available at
 * https://github.com/yandex/yoctodb is subject to the terms of the
 * Mozilla Public License, v. 2.0 (hereinafter referred to as the "License").
 *
 * A copy of the License is also available at http://mozilla.org/MPL/2.0/.
 */

package com.yandex.yoctodb.util.mutable.impl;

import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import com.yandex.yoctodb.util.OutputStreamWritable;
import com.yandex.yoctodb.util.UnsignedByteArray;
import com.yandex.yoctodb.util.common.BufferIterator;
import com.yandex.yoctodb.util.common.TrieNodeMetadata;
import com.yandex.yoctodb.util.mutable.ArrayBitSet;
import com.yandex.yoctodb.util.mutable.ByteArraySortedSet;
import com.yandex.yoctodb.v1.mutable.segment.Freezable;
import net.jcip.annotations.NotThreadSafe;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.io.OutputStream;
import java.util.*;

/**
 * Trie-based implementation for keys storage.
 * Provides an average complexity O(|key|) for all operations with
 * worst-case performance proportional to the depth of compressed trie.
 *
 * @author Andrey Korzinev (ya-goodfella@yandex.com)
 */
@NotThreadSafe
public class TrieByteArraySortedSet implements ByteArraySortedSet {
    private static class Trie extends Freezable implements OutputStreamWritable {
        private final List<Byte> prefix = new LinkedList<>();
        private SortedMap<Integer, Trie> edges = new TreeMap<>();
        private Integer value = null;
        private long offset;

        void append(UnsignedByteArray key, int value) {
            checkNotFrozen();
            append(key.iterator(), value);
        }

        private void compress() {
            while (edges.size() == 1 && value == null) {
                int key = edges.firstKey();
                prefix.add((byte) key);
                Trie child = edges.get(key);
                prefix.addAll(child.prefix);
                edges = child.edges;
                value = child.value;
            }

            for (Trie edge : edges.values()) {
                edge.compress();
            }
        }

        void markDone() {
            freeze();
            compress();

            final Queue<Trie> queue = new LinkedList<>();
            queue.add(this);
            long offset = 0;
            while (!queue.isEmpty()) {
                final Trie node = queue.poll();
                node.offset = offset;
                offset += node.getNodeSizeInBytes();
                queue.addAll(node.edges.values());
            }
        }

        int valueOf(@NotNull BufferIterator bytes) {
            Trie node = this;
            while (node != null) {
                if (bytes.compareToPrefix(BufferIterator.wrapCopy(node.prefix)) < 0) {
                    throw new NoSuchElementException();
                }

                if (bytes.hasNext()) {
                    node = node.edges.get(bytes.next());
                } else {
                    if (node.value == null) {
                        throw new NoSuchElementException();
                    }

                    return node.value;
                }
            }

            throw new NoSuchElementException();
        }

        private void append(Iterator<Byte> bytes, int value) {
            if (bytes.hasNext()) {
                final int key = Byte.toUnsignedInt(bytes.next());
                Trie node = edges.get(key);
                if (node == null) {
                    node = new Trie();
                    edges.put(key, node);
                }

                node.append(bytes, value);
            } else {
                assert this.value == null;

                this.value = value;
            }
        }

        private long getNodeSizeInBytes() {
            long size = Byte.BYTES; // metadata
            if (prefix.size() > 0) {
                size += Integer.BYTES + // size of prefix
                        Byte.BYTES * prefix.size(); // prefix elements
            }

            if (value != null) {
                size += Integer.BYTES; // value
            }

            if (edges.size() == 1) {
                size += Byte.BYTES + // edge key
                        Long.BYTES; // offset to next edge
            } else if (isCondensed()) {
                size += Byte.BYTES + // min element
                        Byte.BYTES + // max element
                        Long.BYTES * edges.size(); // offsets
            } else if (!edges.isEmpty()) {
                size += Byte.BYTES + // min element
                        Byte.BYTES + // max element
                        Long.BYTES * bitSetSizeInLongs() + // backing bitset
                        Long.BYTES * edges.size(); // elements
            }

            return size;
        }

        private int getMin() {
            return edges.firstKey();
        }

        private int getMax() {
            return edges.lastKey();
        }

        private int bitSetSizeInLongs() {
            return LongArrayBitSet.arraySize(getMax() - getMin() + 1);
        }

        private boolean isCondensed() {
            return !edges.isEmpty() && ((getMax() - getMin()) == (edges.size() - 1));
        }

        @Override
        public long getSizeInBytes() {
            return getNodeSizeInBytes() +
                    edges.values()
                            .stream()
                            .mapToLong(Trie::getSizeInBytes)
                            .sum();
        }

        private void writeMetadata(@NotNull OutputStream os) throws IOException {
            int result = 0;

            if (!prefix.isEmpty()) {
                result |= TrieNodeMetadata.PREFIX_FLAG; // prefix exists
            }

            if (value != null) {
                result |= TrieNodeMetadata.VALUE_FLAG; // value exists
            }

            if (edges.size() == 1) {
                result |= TrieNodeMetadata.EDGES_SINGLE; // only one edge
            } else if (isCondensed()) {
                result |= TrieNodeMetadata.EDGES_CONDENSED; // many condensed edges
            } else if (!edges.isEmpty()) {
                result |= TrieNodeMetadata.EDGES_BITSET; // many thin edges
            }

            os.write(result);
        }

        private void writeNodeTo(@NotNull OutputStream os) throws IOException {
            writeMetadata(os);

            if (prefix.size() > 0) {
                os.write(Ints.toByteArray(prefix.size())); // prefix size
                // prefix bytes
                for (byte b : prefix) {
                    os.write(b);
                }
            }

            if (value != null) {
                os.write(Ints.toByteArray(value)); // value
            }

            if (edges.size() == 1) {
                int key = getMin();
                os.write(key); // edge key
                os.write(Longs.toByteArray(edges.get(key).offset)); // offset to next edge
            } else if (isCondensed()) {
                os.write(getMin()); // min element
                os.write(getMax()); // max element
                // offsets
                for (Trie n : edges.values()) {
                    os.write(Longs.toByteArray(n.offset));
                }
            } else if (!edges.isEmpty()) {
                os.write(getMin()); // min element
                os.write(getMax()); // max element
                // backing bitset
                for (long bsChunk : edgesToBitSet().toArray()) {
                    os.write(Longs.toByteArray(bsChunk));
                }
                // offsets
                for (Trie n : edges.values()) {
                    os.write(Longs.toByteArray(n.offset));
                }
            }
        }

        private ArrayBitSet edgesToBitSet() {
            int min = getMin();
            ArrayBitSet bs = LongArrayBitSet.zero(getMax() - min + 1);
            for (Integer key : edges.keySet()) {
                bs.set(key - min);
            }

            return bs;
        }

        @Override
        public void writeTo(@NotNull OutputStream os) throws IOException {
            final Queue<Trie> queue = new LinkedList<>();
            queue.add(this);
            while (!queue.isEmpty()) {
                final Trie node = queue.poll();
                node.writeNodeTo(os);
                queue.addAll(node.edges.values());
            }
        }
    }

    private final Trie root = new Trie();
    private final int keysCount;

    public TrieByteArraySortedSet(SortedSet<UnsignedByteArray> keys) {
        this.keysCount = keys.size();
        int i = 0;
        for (UnsignedByteArray key : keys) {
            this.root.append(key, i++);
        }

        this.root.markDone();
    }

    @Override
    public int indexOf(@NotNull UnsignedByteArray e) throws NoSuchElementException {
        return root.valueOf(new BufferIterator(e.toByteBuffer()));
    }

    @Override
    public long getSizeInBytes() {
        return Integer.BYTES +
                this.root.getSizeInBytes();
    }

    @Override
    public void writeTo(@NotNull OutputStream os) throws IOException {
        os.write(Ints.toByteArray(this.keysCount));
        this.root.writeTo(os);
    }
}
