use std::{
    pin::Pin,
    task::{ready, Context, Poll},
};

use async_zmq::{Result, Stream};
use pin_project::pin_project;

use super::topic::TopicMessage;

#[pin_project]
pub struct Subscribe(#[pin] async_zmq::Subscribe);

impl std::fmt::Debug for Subscribe {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("Subscribe").finish()
    }
}

impl Subscribe {
    pub fn new(subscribe: impl Into<async_zmq::Subscribe>) -> Self {
        Self(subscribe.into())
    }

    pub fn sub(&self, topic: &str) -> Result<()> {
        self.0.set_subscribe(topic)?;
        Ok(())
    }

    pub fn unsub(&self, topic: &str) -> Result<()> {
        self.0.set_unsubscribe(topic)?;
        Ok(())
    }
}

impl Stream for Subscribe {
    type Item = Result<TopicMessage>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut stream = self.project().0;
        while let Some(message) = ready!(stream.as_mut().poll_next(cx)?) {
            if let Some((topic, data)) = message.split_first() {
                if let Some(topic) = topic.as_str() {
                    return Poll::Ready(Some(Ok((
                        topic.into(),
                        data.iter().map(|m| (*m).to_owned()).collect(),
                    ))));
                }
            }
        }
        Poll::Ready(None)
    }
}
