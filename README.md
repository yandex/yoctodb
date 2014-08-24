# YoctoDB

YoctoDB is a database engine with the following features:

 * Purely in Java 6 (only `guava` and annotation dependencies)
 * Read-only (concurrent by design)
 * Optionally partitioned (a composite database is a set of partition databases)
 * Space-efficient (uses dictionaries for indexed values after building)
 * Flat document-oriented (a document is an opaque byte sequence)
 * Indexed fields for filtering/sorting (a field is an opaque comparable byte sequence)
 * Embedded (no network API or query languages, just some DSL)
 * `mmap`'ed (`ByteBuffer`'ed)

See [Project Wiki][1] and [Getting Started Guide][2].

Contact us through [YoctoDB User Group][3].

[1]: https://bitbucket.org/yandex/yoctodb/wiki/Home
[2]: https://bitbucket.org/yandex/yoctodb/wiki/GettingStarted
[3]: https://groups.google.com/forum/#!forum/yoctodb

