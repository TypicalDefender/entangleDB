use crate::error::{Error, Result};
use crate::raft;
use crate::sql;
use crate::sql::engine::Engine as _;
use crate::sql::execution::ResultSet;
use crate::sql::schema::{Catalog as _, Table};
use crate::sql::types::Row;

use ::log::{debug, error, info};
use futures::sink::SinkExt as _;
use serde_derive::{Deserialize, Serialize};
use std::collections::HashMap;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio_stream::wrappers::TcpListenerStream;
use tokio_stream::StreamExt as _;
use tokio_util::codec::{Framed, LengthDelimitedCodec};

/// A entangledb server.
/// It encapsulates the Raft consensus server and SQL server functionalities.
/// The server manages both Raft and SQL client connections, processing incoming
/// requests and dispatching them to the appropriate internal components.
pub struct Server {
    raft: raft::Server,
    raft_listener: Option<TcpListener>,
    sql_listener: Option<TcpListener>,
}

impl Server {
    /// Creates a new entangledb server.
    /// Initializes a new server instance with the provided Raft configuration.
    /// 
    /// # Arguments
    /// * `id` - The unique identifier for the Raft node.
    /// * `peers` - A map of peer node IDs to their associated network addresses.
    /// * `raft_log` - The Raft log implementation.
    /// * `raft_state` - The persistent state storage for the Raft consensus algorithm.
    ///
    /// # Returns
    /// A result containing the new server instance or an error if initialization fails.
    pub async fn new(
        id: raft::NodeID,
        peers: HashMap<raft::NodeID, String>,
        raft_log: raft::Log,
        raft_state: Box<dyn raft::State>,
    ) -> Result<Self> {
        Ok(Server {
            raft: raft::Server::new(id, peers, raft_log, raft_state).await?,
            raft_listener: None,
            sql_listener: None,
        })
    }

    /// Starts listening on the given ports. Must be called before serve.
    /// Sets up the TCP listeners for both SQL and Raft communication.
    ///
    /// # Arguments
    /// * `sql_addr` - The address to listen for SQL client connections.
    /// * `raft_addr` - The address to listen for Raft peer connections.
    ///
    /// # Returns
    /// A result containing the server instance with listeners configured or an error if listening fails.
    pub async fn listen(mut self, sql_addr: &str, raft_addr: &str) -> Result<Self> {
        let (sql, raft) =
            tokio::try_join!(TcpListener::bind(sql_addr), TcpListener::bind(raft_addr),)?;
        info!("Listening on {} (SQL) and {} (Raft)", sql.local_addr()?, raft.local_addr()?);
        self.sql_listener = Some(sql);
        self.raft_listener = Some(raft);
        Ok(self)
    }

    /// Serves Raft and SQL requests until the returned future is dropped. Consumes the server.
    /// Starts the event loop for handling incoming Raft and SQL connections.
    /// This function will run indefinitely until the server is shut down.
    ///
    /// # Returns
    /// A result indicating the success or failure of the server event loop.
    pub async fn serve(self) -> Result<()> {
        let sql_listener = self
            .sql_listener
            .ok_or_else(|| Error::Internal("Must listen before serving".into()))?;
        let raft_listener = self
            .raft_listener
            .ok_or_else(|| Error::Internal("Must listen before serving".into()))?;
        let (raft_tx, raft_rx) = mpsc::unbounded_channel();
        let sql_engine = sql::engine::Raft::new(raft_tx);

        tokio::try_join!(
            self.raft.serve(raft_listener, raft_rx),
            Self::serve_sql(sql_listener, sql_engine),
        )?;
        Ok(())
    }

    /// Serves SQL clients.
    /// Accepts incoming SQL client connections and handles their requests in separate tasks.
    ///
    /// # Arguments
    /// * `listener` - The TCP listener for SQL client connections.
    /// * `engine` - The SQL engine instance used for executing SQL commands.
    ///
    /// # Returns
    /// A result indicating the success or failure of serving SQL clients.
    async fn serve_sql(listener: TcpListener, engine: sql::engine::Raft) -> Result<()> {
        let mut listener = TcpListenerStream::new(listener);
        while let Some(socket) = listener.try_next().await? {
            let peer = socket.peer_addr()?;
            let session = Session::new(engine.clone())?;
            tokio::spawn(async move {
                info!("Client {} connected", peer);
                match session.handle(socket).await {
                    Ok(()) => info!("Client {} disconnected", peer),
                    Err(err) => error!("Client {} error: {}", peer, err),
                }
            });
        }
        Ok(())
    }
}

/// A client request.
/// Enumerates the different types of requests that a client can send to the server.
#[derive(Debug, Serialize, Deserialize)]
pub enum Request {
    Execute(String),
    GetTable(String),
    ListTables,
    Status,
}

/// A server response.
/// Enumerates the different types of responses that the server can send back to the client.
#[derive(Debug, Serialize, Deserialize)]
pub enum Response {
    Execute(ResultSet),
    Row(Option<Row>),
    GetTable(Table),
    ListTables(Vec<String>),
    Status(sql::engine::Status),
}

/// A client session coupled to a SQL session.
/// Manages the state and communication for a single client's connection to the SQL server.
pub struct Session {
    engine: sql::engine::Raft,
    sql: sql::engine::Session<sql::engine::Raft>,
}

impl Session {
    /// Creates a new client session.
    /// Initializes a new session for a client connected to the SQL server.
    ///
    /// # Arguments
    /// * `engine` - The SQL engine instance used for executing SQL commands.
    ///
    /// # Returns
    /// A result containing the new session instance or an error if initialization fails.
    fn new(engine: sql::engine::Raft) -> Result<Self> {
        Ok(Self { sql: engine.session()?, engine })
    }

    /// Handles a client connection.
    /// Processes incoming requests from the client and sends appropriate responses.
    ///
    /// # Arguments
    /// * `socket` - The TCP stream representing the client's connection.
    ///
    /// # Returns
    /// A result indicating the success or failure of handling the client connection.
    async fn handle(mut self, socket: TcpStream) -> Result<()> {
        let mut stream = tokio_serde::Framed::new(
            Framed::new(socket, LengthDelimitedCodec::new()),
            tokio_serde::formats::Bincode::default(),
        );
        while let Some(request) = stream.try_next().await? {
            let mut response = tokio::task::block_in_place(|| self.request(request));
            let mut rows: Box<dyn Iterator<Item = Result<Response>> + Send> =
                Box::new(std::iter::empty());
            if let Ok(Response::Execute(ResultSet::Query { rows: ref mut resultrows, .. })) =
                &mut response
            {
                rows = Box::new(
                    std::mem::replace(resultrows, Box::new(std::iter::empty()))
                        .map(|result| result.map(|row| Response::Row(Some(row))))
                        .chain(std::iter::once(Ok(Response::Row(None))))
                        .scan(false, |err_sent, response| match (&err_sent, &response) {
                            (true, _) => None,
                            (_, Err(error)) => {
                                *err_sent = true;
                                Some(Err(error.clone()))
                            }
                            _ => Some(response),
                        })
                        .fuse(),
                );
            }
            stream.send(response).await?;
            stream.send_all(&mut tokio_stream::iter(rows.map(Ok))).await?;
        }
        Ok(())
    }

    /// Executes a request.
    /// Processes a single request from the client and generates the corresponding response.
    ///
    /// # Arguments
    /// * `request` - The client request to be processed.
    ///
    /// # Returns
    /// A result containing the server response to the request or an error if processing fails.
    pub fn request(&mut self, request: Request) -> Result<Response> {
        debug!("Processing request {:?}", request);
        let response = match request {
            Request::Execute(query) => Response::Execute(self.sql.execute(&query)?),
            Request::GetTable(table) => {
                Response::GetTable(self.sql.read_with_txn(|txn| txn.must_read_table(&table))?)
            }
            Request::ListTables => Response::ListTables(
                self.sql.read_with_txn(|txn| Ok(txn.scan_tables()?.map(|t| t.name).collect()))?,
            ),
            Request::Status => Response::Status(self.engine.status()?),
        };
        debug!("Returning response {:?}", response);
        Ok(response)
    }
}

impl Drop for Session {
    fn drop(&mut self) {
        /// Automatically rolls back any active transaction when the session is dropped.
        tokio::task::block_in_place(|| self.sql.execute("ROLLBACK").ok());
    }
}
