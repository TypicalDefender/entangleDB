use crate::error::{Error, Result};
use crate::server::{Request, Response};
use crate::sql::engine::Status;
use crate::sql::execution::QueryResult;
use crate::sql::schema::EntitySchema;

use futures::stream::StreamExt as _;
use rand::Rng as _;
use std::sync::Arc;
use tokio::net::{TcpStream, ToSocketAddrs};
use tokio::sync::{Mutex, RwLock};
use tokio_util::codec::{Framed, LengthDelimitedCodec};

type Connection = tokio_serde::Framed<
    Framed<TcpStream, LengthDelimitedCodec>,
    Result<Response>,
    Request,
    tokio_serde::formats::Json<Result<Response>, Request>,
>;

/// An entangleDB client
#[derive(Clone)]
pub struct Client {
    conn: Arc<RwLock<Connection>>,
}

impl Client {
    /// Creates a new client for entangleDB
    pub async fn new<A: ToSocketAddrs>(addr: A) -> Result<Self> {
        let connection = tokio_serde::Framed::new(
            Framed::new(TcpStream::connect(addr).await?, LengthDelimitedCodec::new()),
            tokio_serde::formats::Json::default(),
        );
        Ok(Self {
            conn: Arc::new(RwLock::new(connection)),
        })
    }

    /// Call a server method
    async fn call(&self, request: Request) -> Result<Response> {
        let mut conn = self.conn.write().await;
        conn.send(request).await?;
        conn.next().await.ok_or_else(|| Error::Internal("Server disconnected".into()))?
    }

    /// Executes a query
    pub async fn execute(&self, query: &str) -> Result<QueryResult> {
        let response = self.call(Request::Execute(query.to_string())).await?;
        match response {
            Response::Execute(result) => Ok(result),
            _ => Err(Error::Internal("Unexpected response type".into())),
        }
    }

    /// Fetches the entity schema
    pub async fn get_schema(&self, entity: &str) -> Result<EntitySchema> {
        let response = self.call(Request::GetSchema(entity.to_string())).await?;
        match response {
            Response::GetSchema(schema) => Ok(schema),
            _ => Err(Error::Internal("Unexpected response type".into())),
        }
    }

    /// Lists all entities in the database
    pub async fn list_entities(&self) -> Result<Vec<String>> {
        let response = self.call(Request::ListEntities).await?;
        match response {
            Response::ListEntities(entities) => Ok(entities),
            _ => Err(Error::Internal("Unexpected response type".into())),
        }
    }

    /// Checks server status
    pub async fn status(&self) -> Result<Status> {
        let response = self.call(Request::Status).await?;
        match response {
            Response::Status(status) => Ok(status),
            _ => Err(Error::Internal("Unexpected response type".into())),
        }
    }
}

pub struct Pool {
    clients: Vec<Arc<RwLock<Client>>>,
}

impl Pool {
    /// Creates a new connection pool for entangleDB, eagerly connecting clients.
    pub async fn new<A: ToSocketAddrs + Clone>(addrs: Vec<A>, size: u64) -> Result<Self> {
        let mut addrs = addrs.into_iter().cycle();
        let clients = futures::future::try_join_all(
            std::iter::from_fn(|| {
                Some(Client::new(addrs.next().unwrap()).map(|r| r.map(|client| Arc::new(RwLock::new(client)))))
            })
            .take(size as usize),
        )
        .await?;
        Ok(Self { clients })
    }

    /// Fetches a read-only client from the pool for entangleDB.
    pub async fn get_read(&self) -> Result<PoolClient<'_>> {
        let (client, index, _) = futures::future::select_all(
            self.clients.iter().map(|c| c.read().boxed())
        ).await;
        Ok(PoolClient::new(index, client))
    }

    /// Returns the size of the pool
    pub fn size(&self) -> usize {
        self.clients.len()
    }
}

/// A read-only client returned from the pool
pub struct PoolClient<'a> {
    id: usize,
    client: RwLockReadGuard<'a, Client>,
}

impl<'a> PoolClient<'a> {
    /// Creates a new PoolClient for entangleDB
    fn new(id: usize, client: RwLockReadGuard<'a, Client>) -> Self {
        Self { id, client }
    }

    /// Returns the ID of the client in the pool
    pub fn id(&self) -> usize {
        self.id
    }
}

impl<'a> Deref for PoolClient<'a> {
    type Target = Client;

    fn deref(&self) -> &Self::Target {
        &*self.client
    }
}

