use async_trait::async_trait;
use std::{io, sync::Arc};

pub mod buf_reader_at;
pub mod range_reader;
pub mod read_at_wrapper;

/// Provides length and asynchronous random access to a resource
#[async_trait(?Send)]
pub trait ReadAt {
    /// Read bytes from resource, starting at `offset`, into `buf`
    async fn read_at(&self, offset: u64, buf: &mut [u8]) -> io::Result<usize>;

    /// Reads exactly `buf`, starting at `offset`
    async fn read_at_exact(&self, mut offset: u64, mut buf: &mut [u8]) -> io::Result<()> {
        while !buf.is_empty() {
            let n = self.read_at(offset, buf).await?;
            offset += n as u64;
            buf = &mut buf[n..];
        }
        Ok(())
    }

    /// Returns the length of the resource, in bytes
    fn len(&self) -> u64;

    /// Returns true if that resource is empty (has a length of 0)
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[async_trait(?Send)]
impl<'a, T> ReadAt for &'a T
where
    T: ReadAt,
{
    async fn read_at(&self, offset: u64, buf: &mut [u8]) -> io::Result<usize> {
        Ok((*self).read_at(offset, buf).await?)
    }

    fn len(&self) -> u64 {
        (*self).len()
    }
}

#[async_trait(?Send)]
impl<'a, T> ReadAt for Arc<T>
where
    T: ReadAt,
{
    async fn read_at(&self, offset: u64, buf: &mut [u8]) -> io::Result<usize> {
        Ok(self.as_ref().read_at(offset, buf).await?)
    }

    fn len(&self) -> u64 {
        self.as_ref().len()
    }
}

/// Type that can be converted into an `AsyncReadAt`.
pub trait AsAsyncReadAt {
    type Out: ReadAt;

    fn as_async_read_at(self: &Arc<Self>) -> Self::Out;
}
