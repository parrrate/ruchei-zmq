use std::{
    iter::{once, Chain, Once},
    pin::Pin,
    task::{Context, Poll},
};

use async_zmq::{zmq::Socket, Error, Message, MultipartIter, Result, Sink};
use pin_project::pin_project;

use super::topic::TopicMessage;

type PubIter = Chain<Once<Vec<u8>>, std::vec::IntoIter<Vec<u8>>>;

#[pin_project]
pub struct Publish(#[pin] async_zmq::Publish<PubIter, Vec<u8>>);

impl std::fmt::Debug for Publish {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("Publish").finish()
    }
}

pub trait IntoPub {
    fn into_pub<I: Unpin + Iterator<Item: Into<Message>>>(
        self,
    ) -> async_zmq::Result<async_zmq::Publish<I, I::Item>>;
}

impl IntoPub for Socket {
    fn into_pub<I: Unpin + Iterator<Item: Into<Message>>>(
        self,
    ) -> async_zmq::Result<async_zmq::Publish<I, I::Item>> {
        Ok(self.into())
    }
}

impl Publish {
    pub fn new(publish: impl IntoPub) -> async_zmq::Result<Self> {
        Ok(Self(publish.into_pub()?))
    }
}

impl Sink<TopicMessage> for Publish {
    type Error = Error;

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<()>> {
        self.project().0.poll_ready(cx).map_err(Into::into)
    }

    fn start_send(self: Pin<&mut Self>, (topic, data): TopicMessage) -> Result<()> {
        self.project()
            .0
            .start_send(MultipartIter(once(topic.into()).chain(data)))
            .map_err(Into::into)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<()>> {
        self.project().0.poll_flush(cx).map_err(Into::into)
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<()>> {
        self.project().0.poll_close(cx).map_err(Into::into)
    }
}
