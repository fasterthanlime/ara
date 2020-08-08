use crate::ReadAt;
use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use futures::lock::Mutex;
use lru_time_cache::LruCache;
use std::{
    io,
    sync::atomic::{AtomicUsize, Ordering},
};

/// Page-based buffering over a `ReadAt` type.
pub struct BufReaderAt<R>
where
    R: ReadAt,
{
    inner: R,
    layout: PageLayout,
    cache: Mutex<LruCache<u64, Bytes>>,
    stats: InternalStats,
}

#[derive(Default)]
struct InternalStats {
    hits: AtomicUsize,
    miss: AtomicUsize,
}

#[derive(Debug, Clone)]
pub struct Stats {
    pub hits: usize,
    pub miss: usize,
}

pub struct BufReaderAtOpts {
    /// Length of a single cached page
    page_len: u64,
    /// Capacity of page cache
    max_cached_pages: usize,
}

impl Default for BufReaderAtOpts {
    fn default() -> Self {
        Self {
            // 256 KiB
            page_len: 256 * 1024,
            // 32 * 256 KiB = MiB
            max_cached_pages: 32,
        }
    }
}

impl<R> BufReaderAt<R>
where
    R: ReadAt,
{
    /// Builds a new instance with default options
    pub fn new(inner: R) -> Self {
        Self::with_opts(inner, Default::default())
    }

    pub fn with_opts(inner: R, opts: BufReaderAtOpts) -> Self {
        Self {
            cache: Mutex::new(LruCache::with_capacity(opts.max_cached_pages)),
            layout: PageLayout {
                resource_len: inner.len(),
                page_len: opts.page_len,
            },
            inner,
            stats: Default::default(),
        }
    }

    pub fn stats(&self) -> Stats {
        Stats {
            hits: self.stats.hits.load(Ordering::SeqCst),
            miss: self.stats.miss.load(Ordering::SeqCst),
        }
    }
}

#[async_trait(?Send)]
impl<R> ReadAt for BufReaderAt<R>
where
    R: ReadAt,
{
    async fn read_at(&self, offset: u64, buf: &mut [u8]) -> std::io::Result<usize> {
        let page_info = self.layout.page_at(offset).map_err(make_io_error)?;
        tracing::trace!(
            ">> read(len {} @ {}, page = {:?})",
            buf.len(),
            offset,
            page_info
        );
        let read_len = std::cmp::min(buf.len(), page_info.remaining() as usize);

        let mut cache = self.cache.lock().await;
        if let Some(page_bytes) = cache.get(&page_info.number) {
            self.stats.hits.fetch_add(1, Ordering::SeqCst);
            for i in 0..read_len {
                buf[i] = page_bytes[page_info.offset_in_page as usize + i];
            }
        } else {
            self.stats.miss.fetch_add(1, Ordering::SeqCst);
            let mut page_bytes = BytesMut::with_capacity(page_info.len as _);
            unsafe {
                page_bytes.set_len(page_info.len as _);
            }
            tracing::trace!(
                ">> fetching page {} ({} bytes)",
                page_info.number,
                page_info.len
            );
            self.inner
                .read_at_exact(page_info.page_start(), page_bytes.as_mut())
                .await?;

            for i in 0..read_len {
                buf[i] = page_bytes[page_info.offset_in_page as usize + i];
            }

            cache.insert(page_info.number, page_bytes.into());
            tracing::trace!("  (cached pages: {})", cache.len());
        }

        Ok(read_len)
    }

    fn len(&self) -> u64 {
        self.layout.resource_len
    }
}

fn make_io_error<E: std::error::Error + Send + Sync + 'static>(e: E) -> io::Error {
    io::Error::new(io::ErrorKind::Other, e)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct PageLayout {
    resource_len: u64,
    page_len: u64,
}

#[derive(Debug, thiserror::Error)]
enum PageError {
    #[error("out of bounds: requested offset {requested} > resource length {resource_len}")]
    OutOfBounds { requested: u64, resource_len: u64 },
}

impl PageLayout {
    /// Returns information for the page at a given offset, or an error
    /// if out of bounds.
    fn page_at(self, offset: u64) -> Result<PageInfo, PageError> {
        if offset > self.resource_len {
            return Err(PageError::OutOfBounds {
                requested: offset,
                resource_len: self.resource_len,
            });
        }

        let number = offset / self.page_len;
        let offset_in_page = offset - number * self.page_len;

        let end = (number + 1) * self.page_len;
        let len = if end > self.resource_len {
            let page_start = number * self.page_len;
            self.resource_len - page_start
        } else {
            self.page_len
        };

        Ok(PageInfo {
            number,
            offset_in_page,
            len,
            layout: self,
        })
    }
}

/// Page-aware position information
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct PageInfo {
    /// Number of the page. For 1024-byte pages, page 0
    /// is bytes 0..1024, page 1 is bytes 1024..2048, etc.
    number: u64,

    /// Offset within page itself. For 1024-byte pages,
    /// page 1 with offset 10 is byte 1034.
    offset_in_page: u64,

    /// Actual length of this page, may be less than `max_page_len`
    /// if this is the last page and the length of the resource
    /// is not a multiple of `max_page_len`.
    len: u64,

    /// How the resource is divided into pages
    layout: PageLayout,
}

impl PageInfo {
    /// Returns the number of bytes that remain in this page.
    /// For example, page 0 with offset 1014 has 10 bytes remaining
    /// (for 1024-byte pages).
    fn remaining(self) -> u64 {
        self.len - self.offset_in_page
    }

    /// Returns the offset at which this page starts in the resouce
    /// Page 2 starts at offset 2048 (for 1024-byte pages).
    fn page_start(self) -> u64 {
        self.number * self.layout.page_len
    }
}

#[cfg(test)]
mod layout_tests {
    use super::*;
    use pretty_assertions::assert_eq;

    #[test]
    fn test_page_layout() {
        let layout = PageLayout {
            page_len: 100,
            resource_len: 328,
        };

        assert!(layout.page_at(0).is_ok());
        assert!(layout.page_at(128).is_ok());
        assert!(layout.page_at(328).is_ok());

        assert!(layout.page_at(329).is_err());
        assert!(layout.page_at(350).is_err());

        assert_eq!(
            layout.page_at(0).unwrap(),
            PageInfo {
                number: 0,
                offset_in_page: 0,
                len: 100,
                layout,
            }
        );

        assert_eq!(
            layout.page_at(99).unwrap(),
            PageInfo {
                number: 0,
                offset_in_page: 99,
                len: 100,
                layout,
            }
        );

        assert_eq!(
            layout.page_at(100).unwrap(),
            PageInfo {
                number: 1,
                offset_in_page: 0,
                len: 100,
                layout,
            }
        );

        assert_eq!(
            layout.page_at(150).unwrap(),
            PageInfo {
                number: 1,
                offset_in_page: 50,
                len: 100,
                layout,
            }
        );

        assert_eq!(
            layout.page_at(199).unwrap(),
            PageInfo {
                number: 1,
                offset_in_page: 99,
                len: 100,
                layout,
            }
        );

        assert_eq!(
            layout.page_at(300).unwrap(),
            PageInfo {
                number: 3,
                offset_in_page: 0,
                len: 28,
                layout,
            }
        );

        assert_eq!(
            layout.page_at(328).unwrap(),
            PageInfo {
                number: 3,
                offset_in_page: 28,
                len: 28,
                layout,
            }
        );
    }
}

#[cfg(test)]
mod buf_reader_at_tests {
    use super::{make_io_error, BufReaderAt, BufReaderAtOpts};
    use crate::ReadAt;
    use async_trait::async_trait;
    use color_eyre::eyre;
    use oorandom::Rand32;
    use pretty_assertions::assert_eq;
    use std::io;

    fn install_tracing() {
        use tracing_error::ErrorLayer;
        use tracing_subscriber::prelude::*;
        use tracing_subscriber::{fmt, EnvFilter};

        let fmt_layer = fmt::layer();
        let filter_layer = EnvFilter::try_from_default_env()
            .or_else(|_| EnvFilter::try_new("info"))
            .unwrap();

        tracing_subscriber::registry()
            .with(filter_layer)
            .with(fmt_layer)
            .with(ErrorLayer::default())
            .init();
    }

    #[tokio::test(threaded_scheduler)]
    async fn test_buf_reader_at() {
        std::env::set_var("RUST_LOG", "ara=info");
        install_tracing();
        color_eyre::install().unwrap();
        test_buf_reader_at_inner().await.unwrap();
    }

    #[tracing::instrument]
    async fn test_buf_reader_at_inner() -> Result<(), eyre::Error> {
        let mut rand = Rand32::new(0xDEFACE);
        let v = get_random_data(&mut rand, 32768);

        let mem_read = MemReader { data: &v[..] };
        let buf_read = BufReaderAt::with_opts(
            &mem_read,
            BufReaderAtOpts {
                max_cached_pages: 8,
                page_len: 2048,
            },
        );

        let max_read_len: u32 = 1024;
        let mut buf_expect: Vec<u8> = Vec::with_capacity(max_read_len as _);
        let mut buf_actual: Vec<u8> = Vec::with_capacity(max_read_len as _);

        let num_reads = 200;
        for _ in 0..num_reads {
            let offset = rand.rand_range(0..v.len() as u32 - max_read_len) as u64;
            let read_len = rand.rand_range(0..max_read_len) as usize;

            unsafe { buf_expect.set_len(read_len) };
            mem_read
                .read_at_exact(offset, &mut buf_expect[..read_len])
                .await
                .unwrap();

            unsafe { buf_actual.set_len(read_len) };
            buf_read
                .read_at_exact(offset, &mut buf_actual[..read_len])
                .await
                .unwrap();

            assert_eq!(buf_expect, buf_actual);
        }

        let stats = buf_read.stats();
        tracing::info!(
            "performed {} reads, {} hits, {} misses",
            num_reads,
            stats.hits,
            stats.miss,
        );

        Ok(())
    }

    fn get_random_data(rand: &mut Rand32, len: usize) -> Vec<u8> {
        let mut v = Vec::with_capacity(len);
        for _ in 0..len {
            v.push(rand.rand_range(0..256) as u8);
        }
        v
    }

    #[derive(Debug, thiserror::Error)]
    enum MemReaderError {
        #[error("positional read out of bounds")]
        OutOfBounds,
    }

    struct MemReader<'a> {
        data: &'a [u8],
    }

    #[async_trait(?Send)]
    impl<'a> ReadAt for MemReader<'a> {
        async fn read_at(&self, offset: u64, buf: &mut [u8]) -> io::Result<usize> {
            let offset = offset as usize;
            if offset > self.data.len() {
                return Err(make_io_error(MemReaderError::OutOfBounds));
            }

            let range = offset..std::cmp::min(offset + buf.len(), self.data.len());
            let read_len = range.end - range.start;

            let dst = &mut buf[..read_len];
            let src = &self.data[offset..offset + read_len];
            dst.copy_from_slice(src);

            Ok(read_len)
        }

        fn len(&self) -> u64 {
            self.data.len() as u64
        }
    }
}
