use std::mem;
use std::sync::Arc;

use parking_lot::Mutex;

use crate::Channel;

pub struct FlumeBoundedMpscChannel<T> {
    sender: Arc<Mutex<flume::Sender<T>>>,
    receiver: Arc<Mutex<Option<flume::Receiver<T>>>>,
}

impl<T> FlumeBoundedMpscChannel<T> {
    pub fn new(buffer: usize) -> Self {
        let (sender, receiver) = flume::bounded(buffer);
        Self {
            sender: Arc::new(Mutex::new(sender)),
            receiver: Arc::new(Mutex::new(Some(receiver))),
        }
    }

    pub fn into_inner(self) -> (flume::Sender<T>, Option<flume::Receiver<T>>) {
        let (sender, receiver) = flume::bounded(1);
        (
            mem::replace(&mut *self.sender.lock(), sender),
            mem::replace(&mut *self.receiver.lock(), Some(receiver)),
        )
    }
}

impl<T: Send> Channel for FlumeBoundedMpscChannel<T> {
    type Sender = flume::Sender<T>;
    type Receiver = flume::Receiver<T>;

    fn create() -> Self {
        Self::new(1024)
    }

    fn sender(&self) -> Self::Sender {
        self.sender.lock().clone()
    }

    fn receiver(&self) -> Self::Receiver {
        self.receiver.lock().take().unwrap_or_else(|| flume::bounded(1).1)
    }

    fn is_closed(&self) -> bool {
        self.sender.lock().is_disconnected()
    }
}

pub struct FlumeUnboundedMpscChannel<T> {
    sender: Arc<Mutex<flume::Sender<T>>>,
    receiver: Arc<Mutex<Option<flume::Receiver<T>>>>,
}

impl<T: Send> Default for FlumeUnboundedMpscChannel<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: Send> FlumeUnboundedMpscChannel<T> {
    pub fn new() -> Self {
        let (sender, receiver) = flume::unbounded();
        Self {
            sender: Arc::new(Mutex::new(sender)),
            receiver: Arc::new(Mutex::new(Some(receiver))),
        }
    }

    pub fn into_inner(self) -> (flume::Sender<T>, Option<flume::Receiver<T>>) {
        let (sender, receiver) = flume::unbounded();
        (
            mem::replace(&mut *self.sender.lock(), sender),
            mem::replace(&mut *self.receiver.lock(), Some(receiver)),
        )
    }
}

impl<T: Send> Channel for FlumeUnboundedMpscChannel<T> {
    type Sender = flume::Sender<T>;
    type Receiver = flume::Receiver<T>;

    fn create() -> Self {
        Self::new()
    }

    fn sender(&self) -> Self::Sender {
        self.sender.lock().clone()
    }

    fn receiver(&self) -> Self::Receiver {
        self.receiver.lock().take().unwrap_or_else(|| flume::unbounded().1)
    }

    fn is_closed(&self) -> bool {
        self.sender.lock().is_disconnected()
    }
}

#[cfg(test)]
mod tests {
    use flume::SendError;

    use crate::{DefaultContext, FlumeUnboundedMpscChannel, Message};

    #[tokio::test]
    async fn extract_unbounded_channel() {
        struct Value(&'static str);

        impl Message for Value {
            type Channel = FlumeUnboundedMpscChannel<Self>;
        }

        let ctx = DefaultContext::new();

        assert!(matches!(ctx.extract_channel::<Value>(), None));

        let sender = ctx.sender::<Value>();
        let mut receiver = ctx.receiver::<Value>();

        sender.send(Value("inside")).ok().unwrap();
        assert_eq!(receiver.recv_async().await.unwrap().0, "inside");

        let (extracted_sender, _) = ctx.extract_channel::<Value>().unwrap().into_inner();

        extracted_sender.send(Value("extracted")).ok().unwrap();
        assert_eq!(receiver.recv_async().await.unwrap().0, "extracted");

        drop(sender);
        drop(extracted_sender);
        // TODO: tokio channel returns None, flume Err(_)
        assert!(matches!(receiver.recv_async().await.ok(), None));

        let sender = ctx.sender::<Value>();
        drop(sender);

        let (sender, receiver) = ctx.extract_channel::<Value>().unwrap().into_inner();
        let mut receiver = receiver.unwrap();

        sender.send(Value("extracted")).ok().unwrap();
        assert_eq!(receiver.recv_async().await.unwrap().0, "extracted");

        drop(sender);
        assert!(matches!(receiver.recv_async().await.ok(), None));

        let mut receiver = ctx.receiver::<Value>();
        let (sender, _) = ctx.extract_channel::<Value>().unwrap().into_inner();

        sender.send(Value("extracted")).ok().unwrap();
        assert_eq!(receiver.recv_async().await.unwrap().0, "extracted");

        drop(sender);
        assert!(matches!(receiver.recv_async().await.ok(), None));

        let mut receiver = ctx.receiver::<Value>();
        let (sender, _) = ctx.extract_channel::<Value>().unwrap().into_inner();

        sender.send(Value("extracted")).ok().unwrap();
        assert_eq!(receiver.recv_async().await.unwrap().0, "extracted");

        drop(receiver);
        assert!(matches!(sender.send(Value("closed")), Err(SendError(Value("closed")))));
    }
}
