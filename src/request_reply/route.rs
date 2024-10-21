use std::{
    pin::Pin,
    sync::Arc,
    task::{ready, Context, Poll},
};

use async_zmq::{Error, Message, MultipartIter, RecvError, Result, SendError, Sink, Stream};
use pin_project::pin_project;

pub type RouteMessage = (ZmqRoute, Vec<Vec<u8>>);

#[derive(Clone, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct ZmqRoute(Arc<[u8]>);

const FRAGMENT_SIZE: usize = std::mem::size_of::<usize>();

#[test]
fn test_layout_0() {
    let key = ZmqRoute::from_parts(&[]);
    assert!(key.is_empty());
    assert_eq!(key.len(), 0);
    let parts = key.into_parts().collect::<Vec<_>>();
    assert!(parts.is_empty());
}

#[test]
fn test_layout_1() {
    let key = ZmqRoute::from_parts(&[Message::from("abc")]);
    assert!(!key.is_empty());
    assert_eq!(key.len(), 1);
    let parts = key.into_parts().collect::<Vec<_>>();
    assert_eq!(parts, [Message::from("abc")]);
}

#[test]
fn test_layout_2() {
    let key = ZmqRoute::from_parts(&[Message::from("abc"), Message::from("def")]);
    assert!(!key.is_empty());
    assert_eq!(key.len(), 2);
    let parts = key.into_parts().collect::<Vec<_>>();
    assert_eq!(parts, [Message::from("abc"), Message::from("def")]);
}

#[test]
fn test_layout_3() {
    let key = ZmqRoute::from_parts(&[
        Message::from("abc"),
        Message::from("def"),
        Message::from("ghi"),
    ]);
    assert!(!key.is_empty());
    assert_eq!(key.len(), 3);
    let parts = key.into_parts().collect::<Vec<_>>();
    assert_eq!(
        parts,
        [
            Message::from("abc"),
            Message::from("def"),
            Message::from("ghi"),
        ]
    );
}

impl ZmqRoute {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    fn from_parts(parts: &[Message]) -> Self {
        let size = parts.len() * FRAGMENT_SIZE + parts.iter().map(|part| part.len()).sum::<usize>();
        let mut data = vec![0u8; size];
        let mut total = parts.len();
        {
            let mut data = &mut *data;
            if total > 0 {
                let temp;
                (temp, data) = data.split_at_mut(FRAGMENT_SIZE);
                temp.copy_from_slice(&total.to_le_bytes());
            }
            for message in parts {
                total -= 1;
                let len = message.len();
                if total > 0 {
                    let temp;
                    (temp, data) = data.split_at_mut(FRAGMENT_SIZE);
                    temp.copy_from_slice(&len.to_le_bytes());
                }
                {
                    let temp;
                    (temp, data) = data.split_at_mut(len);
                    temp.copy_from_slice(message);
                }
            }
            assert!(data.is_empty());
        }
        Self(data.into())
    }

    fn into_parts(self) -> KeyIter {
        KeyIter::new(self.0)
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        if self.0.is_empty() {
            0
        } else {
            let (data, _) = self.0.split_first_chunk().expect("out of bounds");
            usize::from_le_bytes(*data)
        }
    }
}

struct KeyIter {
    key: Arc<[u8]>,
    at: usize,
    total: usize,
}

impl KeyIter {
    fn new(key: Arc<[u8]>) -> Self {
        let mut this = Self {
            key,
            at: 0,
            total: 0,
        };
        this.prepare();
        this
    }

    fn prepare(&mut self) {
        if !self.key.is_empty() {
            self.total = self.read_usize();
        }
    }

    fn read_array<const N: usize>(&mut self) -> [u8; N] {
        let chunk = self.key[self.at..].first_chunk().expect("out of bounds");
        self.at += N;
        *chunk
    }

    fn read_slice(&mut self, len: usize) -> &[u8] {
        let chunk = &self.key[self.at..self.at + len];
        self.at += len;
        chunk
    }

    fn read_usize(&mut self) -> usize {
        usize::from_le_bytes(self.read_array())
    }

    fn read_len(&mut self) -> usize {
        self.total -= 1;
        if self.total > 0 {
            self.read_usize()
        } else {
            self.key.len() - self.at
        }
    }

    fn read_message(&mut self) -> Message {
        let len = self.read_len();
        let slice = self.read_slice(len);
        Message::from(slice)
    }
}

impl Iterator for KeyIter {
    type Item = Message;

    fn next(&mut self) -> Option<Self::Item> {
        if self.at == self.key.len() {
            None
        } else {
            Some(self.read_message())
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.total, Some(self.total))
    }
}

impl ExactSizeIterator for KeyIter {
    fn len(&self) -> usize {
        self.total
    }
}

pub(super) struct RouteIter {
    key: Option<KeyIter>,
    data: std::vec::IntoIter<Vec<u8>>,
}

impl Iterator for RouteIter {
    type Item = Message;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(key) = self.key.as_mut() {
            if let Some(msg) = key.next() {
                return Some(msg);
            }
            self.key = None;
            return Some(Message::new());
        }
        self.data.next().map(Message::from)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let len = self.len();
        (len, Some(len))
    }
}

impl ExactSizeIterator for RouteIter {
    fn len(&self) -> usize {
        self.key
            .as_ref()
            .map(|iter| 1 + iter.total)
            .unwrap_or_default()
            + self.data.len()
    }
}

#[pin_project]
pub(super) struct RouteStream<S: ?Sized>(#[pin] pub(super) S);

impl<S: ?Sized + Stream<Item = std::result::Result<Vec<Message>, RecvError>>> Stream
    for RouteStream<S>
{
    type Item = Result<RouteMessage>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut stream = self.project().0;
        while let Some(message) = ready!(stream.as_mut().poll_next(cx)?) {
            if let Some(sep) = message.iter().position(|m| m.is_empty()) {
                let (key, data) = message.split_at(sep);
                let (sep, data) = data
                    .split_first()
                    .expect("sep position was acquired by iteration but is out of bounds");
                assert!(sep.is_empty());
                return Poll::Ready(Some(Ok((
                    ZmqRoute::from_parts(key),
                    data.iter().map(|m| (*m).to_owned()).collect(),
                ))));
            }
        }
        Poll::Ready(None)
    }
}

impl<S: ?Sized + Sink<MultipartIter<RouteIter, Message>, Error = SendError>> Sink<RouteMessage>
    for RouteStream<S>
{
    type Error = Error;

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<()>> {
        self.project().0.poll_ready(cx).map_err(Into::into)
    }

    fn start_send(self: Pin<&mut Self>, (key, data): RouteMessage) -> Result<()> {
        self.project()
            .0
            .start_send(MultipartIter(RouteIter {
                key: Some(key.into_parts()),
                data: data.into_iter(),
            }))
            .map_err(Into::into)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<()>> {
        self.project().0.poll_flush(cx).map_err(Into::into)
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<()>> {
        self.project().0.poll_close(cx).map_err(Into::into)
    }
}
