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
