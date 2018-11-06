package com.yandex.yoctodb.util.mutable.impl;

import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import com.yandex.yoctodb.util.OutputStreamWritable;
import com.yandex.yoctodb.util.UnsignedByteArray;
import com.yandex.yoctodb.util.UnsignedByteArrays;
import com.yandex.yoctodb.util.mutable.ArrayBitSet;
import com.yandex.yoctodb.util.mutable.ByteArraySortedSet;
import net.jcip.annotations.NotThreadSafe;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.io.OutputStream;
import java.util.*;
import java.util.stream.Collectors;

import static com.yandex.yoctodb.util.common.BufferIterator.compareMutableIterators;

@NotThreadSafe
public class TrieByteArraySortedSet implements ByteArraySortedSet {
    private static class Trie implements OutputStreamWritable {
        private final List<Byte> infix = new LinkedList<>();
        private SortedMap<Integer, Trie> edges = new TreeMap<>();
        private Integer value = null;
        private long offset;

        void append(UnsignedByteArray key, int value) {
            append(key.iterator(), value);
        }

        void compress() {
            while (edges.size() == 1 && value == null) {
                int key = edges.firstKey();
                infix.add((byte) key);
                Trie child = edges.get(key);
                infix.addAll(child.infix);
                edges = child.edges;
                value = child.value;
            }

            for (Trie edge : edges.values()) {
                edge.compress();
            }
        }

        void processOffsets() {
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

        int valueOf(@NotNull Iterator<Byte> bytes) {
            Trie node = this;
            while (node != null) {
                if (compareMutableIterators(bytes, node.infix.iterator()) < 0) {
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
            if (infix.size() > 0) {
                size += Integer.BYTES + // size of infix
                        Byte.BYTES * infix.size(); // infix elements
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
            return !edges.isEmpty() && getMax() - getMin() == edges.size() - 1;
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

            if (!infix.isEmpty()) {
                result |= 0b0001; // infix exists
            }

            if (value != null) {
                result |= 0b0010; // value exists
            }

            if (edges.size() == 1) {
                result |= 0b0100; // only one edge
            } else if (isCondensed()) {
                result |= 0b1100; // many condensed edges
            } else if (!edges.isEmpty()) {
                result |= 0b1000; // many thin edges
            }

            os.write(result);
        }

        private void writeNodeTo(@NotNull OutputStream os) throws IOException {
            writeMetadata(os);

            if (infix.size() > 0) {
                os.write(Ints.toByteArray(infix.size())); // infix size
                for (byte b : infix) {
                    os.write(b);
                } // infix bytes
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
                for (Trie n : edges.values()) {
                    os.write(Longs.toByteArray(n.offset));
                } // offsets
            } else if (!edges.isEmpty()) {
                os.write(getMin()); // min element
                os.write(getMax()); // max element
                for (long bsChunk : edgesToBitSet().toArray()) {
                    os.write(Longs.toByteArray(bsChunk));
                } // backing bitset
                for (Trie n : edges.values()) {
                    os.write(Longs.toByteArray(n.offset));
                } // offsets
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

        @Override
        public String toString() {
            return String.format("<value=%d, infix=%s, edges=(%s)>",
                        value,
                        infix.stream()
                                .map(b -> Byte.toString(b))
                                .collect(Collectors.joining(", ")),
                        edges.entrySet().stream()
                                .map(e -> e.getKey().toString() + "=" + e.getValue().toString())
                                .collect(Collectors.joining(", "))
                    );
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

        this.root.compress();
        this.root.processOffsets();
    }

    @Override
    public int indexOf(@NotNull UnsignedByteArray e) throws NoSuchElementException {
        return root.valueOf(e.iterator());
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
