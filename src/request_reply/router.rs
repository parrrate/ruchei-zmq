use std::{
    pin::Pin,
    task::{Context, Poll},
};

use async_zmq::{zmq::Socket, Error, Message, Result, Sink, Stream};
use pin_project::pin_project;

use super::route::{RouteIter, RouteMessage, RouteStream};

#[pin_project]
pub struct Router(#[pin] RouteStream<async_zmq::Router<RouteIter, Message>>);

impl std::fmt::Debug for Router {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("Router").finish()
    }
}

pub trait IntoRouter {
    fn into_router<I: Unpin + Iterator<Item: Into<Message>>>(
        self,
    ) -> async_zmq::Result<async_zmq::Router<I, I::Item>>;
}

impl IntoRouter for Socket {
    fn into_router<I: Unpin + Iterator<Item: Into<Message>>>(
        self,
    ) -> async_zmq::Result<async_zmq::Router<I, I::Item>> {
        Ok(self.into())
    }
}

impl Router {
    pub fn new(router: impl IntoRouter) -> async_zmq::Result<Self> {
        Ok(Self(RouteStream(router.into_router()?)))
    }
}

impl Stream for Router {
    type Item = Result<RouteMessage>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.project().0.poll_next(cx)
    }
}

impl Sink<RouteMessage> for Router {
    type Error = Error;

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<()>> {
        self.project().0.poll_ready(cx)
    }

    fn start_send(self: Pin<&mut Self>, item: RouteMessage) -> Result<()> {
        self.project().0.start_send(item)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<()>> {
        self.project().0.poll_flush(cx)
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<()>> {
        self.project().0.poll_close(cx)
    }
}
