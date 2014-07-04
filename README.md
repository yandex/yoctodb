# YoctoDB

YoctoDB is a database engine with the following features (unordered):

 * Purely in Java (Java 7 actually)
 * Extremely simple
 * Flat document-oriented (a document is an opaque byte sequence)
 * Indexed fields (a field is an opaque comparable byte sequence)
 * Embedded (no network API or query languages, just some DSLs)
 * Read-only (concurrent by design)
 * Partitioned (a database is a set of partitions)
 * `mmap`'ed documents (to leverage disk cache)
 * Space-efficient (uses dictionaries for indexed values)

See [Project Wiki][1] and [Getting Started Guide][2].

[1]: https://bitbucket.org/yandex/yoctodb/wiki/Home
[2]: https://bitbucket.org/yandex/yoctodb/wiki/GettingStarted

