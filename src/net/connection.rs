use std::io::{Error, Write};
use std::net::TcpStream;
use crate::net::endpoint::Endpoint;

pub(crate) struct TcpConnection {
   tcp_stream: TcpStream
}

impl TcpConnection {
    fn connect_to(endpoint: Endpoint) -> Result<TcpConnection, Error> {
        TcpStream::connect(endpoint.address())
            .map(|tcp_stream| TcpConnection { tcp_stream })
    }

    fn write(&mut self, payload: &[u8]) -> std::io::Result<usize> {
        self.tcp_stream.write(payload)
    }
}

#[cfg(test)]
mod tests {
    use std::net::TcpListener;
    use crate::net::connection::TcpConnection;
    use crate::net::endpoint::Endpoint;

    #[test]
    fn write_to_connect_successfully() {
        let _listener = TcpListener::bind("127.0.0.1:9898").unwrap();

        let tcp_connection_result = TcpConnection::connect_to(Endpoint::new("127.0.0.1".to_string(), 9898));
        assert!(tcp_connection_result.is_ok());

        let mut tcp_connection = tcp_connection_result.unwrap();
        let payload = b"Kademlia";
        let write_result = tcp_connection.write(payload);

        assert!(write_result.is_ok());
        assert_eq!(payload.len(), write_result.unwrap())
    }

    #[test]
    fn connect_to_endpoint_fails() {
        let tcp_connection_result = TcpConnection::connect_to(Endpoint::new("127.0.0.1".to_string(), 1010));
        assert!(tcp_connection_result.is_err());
    }
}