use std::io;

#[derive(Debug)]
pub enum Error {
    Io(io::Error),
    UnsupportedMethod,
    InvalidRequest,
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Io(err) => Some(err),
            _ => None,
        }
    }
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::Io(err) => write!(f, "IO error: {}", err),
            Error::UnsupportedMethod => write!(f, "Unsupported RPC method"),
            Error::InvalidRequest => write!(f, "Invalid request"),
        }
    }
}

impl From<io::Error> for Error {
    fn from(err: io::Error) -> Self {
        Error::Io(err)
    }
}
