use std::{
    pin::Pin,
    task::{Context, Poll},
};

use async_zmq::{zmq::Socket, Error, Message, Result, Sink, Stream};
use pin_project::pin_project;

use super::route::{RouteIter, RouteMessage, RouteStream};

#[pin_project]
pub struct Dealer(#[pin] RouteStream<async_zmq::Dealer<RouteIter, Message>>);

impl std::fmt::Debug for Dealer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("Dealer").finish()
    }
}

pub trait IntoDealer {
    fn into_dealer<I: Unpin + Iterator<Item: Into<Message>>>(
        self,
    ) -> async_zmq::Result<async_zmq::Dealer<I, I::Item>>;
}

impl IntoDealer for Socket {
    fn into_dealer<I: Unpin + Iterator<Item: Into<Message>>>(
        self,
    ) -> async_zmq::Result<async_zmq::Dealer<I, I::Item>> {
        Ok(self.into())
    }
}

impl Dealer {
    pub fn new(dealer: impl IntoDealer) -> async_zmq::Result<Self> {
        Ok(Self(RouteStream(dealer.into_dealer()?)))
    }
}

impl Stream for Dealer {
    type Item = Result<RouteMessage>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.project().0.poll_next(cx)
    }
}

impl Sink<RouteMessage> for Dealer {
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
