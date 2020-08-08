# ara

The `ara` crate provides traits for async random access I/O.

The `ReadAt` trait provides both an `u64` length, and an async `read_at`
method, thanks to `async-trait`. It is intended to be implemented for
anything from in-memory buffers, to local files, to remote resources
acessed over protocols like HTTP, FTP, etc.

`ReadAtWrapper` can be used to turn a type that implements `GetReaderAt`
into a type that implements `ReadAt`.

`BufReaderAt` implements buffering on top of a `ReadAt` type. Whereas linear
reads only need a single buffer, a `ReadAt` can be read at any offset, and
thus a `BufReaderAt` caches "pages", with LRU (least-recently used) eviction.

Note that `ara` does not implement any of the traits for files, or HTTP
resources. It exists purely to define an interface between resources, and
consumers of those resources (like format readers, zip extractors, etc.).

## License

This project is licensed under either of

 * Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or http://www.apache.org/licenses/LICENSE-2.0)
 * MIT license ([LICENSE-MIT](LICENSE-MIT) or http://opensource.org/licenses/MIT)

at your option.
