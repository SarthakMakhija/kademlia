use std::io::Error;

use tokio::io;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;

use crate::net::endpoint::Endpoint;

pub(crate) struct TcpConnection {
    tcp_stream: TcpStream,
}

impl TcpConnection {
    async fn establish_with(endpoint: Endpoint) -> Result<TcpConnection, Error> {
        TcpStream::connect(endpoint.address())
            .await
            .map(|tcp_stream| TcpConnection { tcp_stream })
    }

    async fn write(&mut self, payload: &[u8]) -> io::Result<()> {
        self.tcp_stream.write_all(payload).await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use tokio::net::TcpListener;

    use crate::net::connection::TcpConnection;
    use crate::net::endpoint::Endpoint;

    #[tokio::test]
    async fn write_to_connect_successfully() {
        let listener_result = TcpListener::bind("localhost:9898").await;
        assert!(listener_result.is_ok());

        let tcp_connection_result =
            TcpConnection::establish_with(Endpoint::new("localhost".to_string(), 9898)).await;
        assert!(tcp_connection_result.is_ok());

        let mut tcp_connection = tcp_connection_result.unwrap();
        let payload = b"Kademlia";

        let write_result = tcp_connection.write(payload).await;
        assert!(write_result.is_ok());
    }

    #[tokio::test]
    async fn connect_to_endpoint_fails() {
        let tcp_connection_result =
            TcpConnection::establish_with(Endpoint::new("localhost".to_string(), 1010)).await;
        assert!(tcp_connection_result.is_err());
    }
}
