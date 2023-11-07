use std::sync::Arc;

use log::error;
use tokio::sync::mpsc::error::SendError;

use crate::executor::message::MessageExecutor;
use crate::executor::node::AddNodeExecutor;
use crate::executor::response::{ChanneledMessage, MessageResponse};
use crate::net::connection::AsyncTcpConnection;
use crate::net::message::Message;

use crate::net::node::Node;
use crate::net::wait::WaitingList;
use crate::routing::Table;
use crate::store::Store;

struct AsyncConnectionHandler {
    message_executor: MessageExecutor,
    add_node_executor: AddNodeExecutor,
}
impl AsyncConnectionHandler {
    pub(crate) fn new(
        current_node: Node,
        store: Arc<dyn Store>,
        waiting_list: Arc<WaitingList>,
        routing_table: Arc<Table>,
    ) -> Self {
        AsyncConnectionHandler {
            message_executor: MessageExecutor::new(
                current_node.clone(),
                store,
                waiting_list.clone(),
                routing_table.clone(),
            ),
            add_node_executor: AddNodeExecutor::new(current_node, waiting_list, routing_table),
        }
    }

    pub(crate) async fn handle(&self, mut connection: AsyncTcpConnection) {
        match connection.read().await {
            Ok(message) => {
                let source = message.source();
                Self::log_error_if_any(self.message_executor.submit(message).await);

                if let Some(node) = source {
                    Self::log_error_if_any(
                        self.add_node_executor
                            .submit(Message::add_node_type(node))
                            .await,
                    )
                }
            }
            Err(err) => {
                error!(
                    "received an error while reading from the connection {:?}",
                    err
                )
            }
        }
    }

    fn log_error_if_any(result: Result<MessageResponse, SendError<ChanneledMessage>>) {
        if let Err(err) = result {
            error!(
                "received error in submitting the message to the executor {:?}",
                err
            )
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::thread;
    use std::time::Duration;

    use tokio::net::TcpListener;

    use crate::id::Id;
    use crate::net::connection::AsyncTcpConnection;
    use crate::net::endpoint::Endpoint;
    use crate::net::message::Message;
    use crate::net::node::Node;
    use crate::net::wait::{WaitingList, WaitingListOptions};
    use crate::routing::Table;
    use crate::server::AsyncConnectionHandler;
    use crate::store::{InMemoryStore, Store};
    use crate::time::SystemClock;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn handle_connection_with_store_message() {
        let listener_result = TcpListener::bind("localhost:9015").await;
        assert!(listener_result.is_ok());

        let node = Node::new_with_id(
            Endpoint::new("localhost".to_string(), 9090),
            Id::new(255u16.to_be_bytes().to_vec()),
        );
        let node_id = node.node_id();

        let store = Arc::new(InMemoryStore::new());
        let store_clone = store.clone();

        let routing_table = Table::new(node_id);
        let routing_table_clone = routing_table.clone();

        let handle = tokio::spawn(async move {
            let tcp_listener = listener_result.unwrap();
            let stream = tcp_listener.accept().await.unwrap();

            let connection = AsyncTcpConnection::new(stream.0);

            let connection_handler =
                AsyncConnectionHandler::new(node, store, waiting_list(), routing_table);

            connection_handler.handle(connection).await;
        });

        let endpoint = Endpoint::new("localhost".to_string(), 9015);
        let connection_result = AsyncTcpConnection::establish_with(&endpoint).await;
        assert!(connection_result.is_ok());

        let source_node = Node::new(Endpoint::new("localhost".to_string(), 8787));
        let store_message = Message::store_type(
            "kademlia".as_bytes().to_vec(),
            "distributed hash table".as_bytes().to_vec(),
            source_node.clone(),
        );

        let mut connection = connection_result.unwrap();
        let connection_write_result = connection.write(&store_message).await;
        assert!(connection_write_result.is_ok());

        handle.await.unwrap();
        thread::sleep(Duration::from_millis(100));

        let value = store_clone.get(&"kademlia".as_bytes().to_vec());
        assert!(value.is_some());
        assert_eq!(
            "distributed hash table",
            String::from_utf8(value.unwrap()).unwrap()
        );

        let (_, contains) = routing_table_clone.contains(&source_node);
        assert!(contains);
    }

    fn waiting_list() -> Arc<WaitingList> {
        WaitingList::new(
            WaitingListOptions::new(Duration::from_secs(120), Duration::from_millis(100)),
            SystemClock::new(),
        )
    }
}
