use std::fmt::{Display, Formatter};

use serde::Deserialize;
use serde::Serialize;

#[derive(Debug, Eq, PartialEq, Clone, Serialize, Deserialize)]
pub(crate) struct Endpoint {
    host: String,
    port: u16,
}

impl Display for Endpoint {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{}", self.address())
    }
}

impl Endpoint {
    pub(crate) fn new(host: String, port: u16) -> Self {
        return Endpoint { host, port };
    }

    pub(crate) fn address(&self) -> String {
        format!("{}:{}", self.host, self.port)
    }
}

#[cfg(test)]
mod tests {
    use crate::net::endpoint::Endpoint;

    #[test]
    fn endpoint_with_localhost() {
        let endpoint = Endpoint::new("localhost".to_string(), 2379);
        assert_eq!("localhost:2379", endpoint.address())
    }

    #[test]
    fn endpoint_with_ip() {
        let endpoint = Endpoint::new("127.0.0.1".to_string(), 2379);
        assert_eq!("127.0.0.1:2379", endpoint.address())
    }
}
