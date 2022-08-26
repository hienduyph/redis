use crate::{Command, Connection, Db, Shutdown};

use std::future::Future;
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{broadcast, mpsc, Semaphore};
use tokio::time::{self, Duration};
use tracing::{debug, error, info, instrument};

#[derive(Debug)]
struct Listener {
    db: Db,

    listener: TcpListener,

    limit_connections: Arc<Semaphore>,

    notify_shutdown: broadcast::Sender<()>,

    shutdown_complete_rx: mpsc::Receiver<()>,
    shutdown_complete_tx: mpsc::Sender<()>,
}

/// Per-connection handler.
struct Handler {
    db: Db,

    connection: Connection,

    limit_connections: Arc<Semaphore>,

    shutdown: Shutdown,

    _shutdown_complete: mpsc::Sender<()>,
}

const MAX_CONNECTION: usize = 10240;

pub async fn run(listener: TcpListener, shutdown: impl Future) -> crate::Result<()> {
    let (notify_shutdown, _) = broadcast::channel(1);
    let (shutdown_complete_tx, shutdown_complete_rx) = mpsc::channel(1);

    let mut server = Listener{
        listener,
        db: Db::new(),
        limit_connections: Arc::new(Semaphore::new(MAX_CONNECTION)),
        notify_shutdown,
        shutdown_complete_tx,
        shutdown_complete_rx,

    };

    tokio::select! {
        res = server.run() => {
            if let Err(err) = res {
                error!(cause = %err, "failed to accept");
            }
        }
        _ = shutdown => {
            info!("shutdown");
        }
    }

    let Listener {
        mut shutdown_complete_rx,
        shutdown_complete_tx,
        notify_shutdown,
        ..
    } = server;

    drop(notify_shutdown);
    drop(shutdown_complete_tx);

    let _ = shutdown_complete_rx.recv().await;
    Ok(())

}

impl Listener {
    async fn run (&mut self) -> crate::Result<()> {
        info!("accept inbound connections");

        loop {
            // wait for permit available
            self.limit_connections.acquire().await.unwrap().forget();

            let socket = self.accept().await?;

            let mut handler = Handler{
                db: self.db.clone(),

                connection: Connection::new(socket),

                limit_connections: self.limit_connections.clone(),

                shutdown: Shutdown::new(self.notify_shutdown.subscribe()),
                _shutdown_complete: self.shutdown_complete_tx.clone(),
            };

            tokio::spawn(async move {
                if let Err(err) = handler.run().await {
                    error!(cause =?err, "connection error");
                }
            });
        }
    }

    async fn accept(&mut self) -> crate::Result<TcpStream> {
        let mut backoff = 1;

        // try to accept a few times.
        loop {
            match self.listener.accept().await {
                Ok((socket, _)) => return Ok(socket),
                Err(err) => {
                    if backoff > 64 {
                        return Err(err.into());
                    }
                }
            }

            time::sleep(Duration::from_secs(backoff)).await;
            backoff *= 2;
        }

    }
}

impl Handler {
    #[instrument(skip(self))]
    async fn run(&mut self) -> crate::Result<()> {
        while !self.shutdown.is_shutdown() {

            let maybe_frame = tokio::select! {
                res = self.connection.read_frame() => res?,
                _ = self.shutdown.recv()=> {
                    return Ok(());
                }
            };

            debug!("Process Frame {:?}", maybe_frame);

            let frame = match maybe_frame {
                Some(frame) => frame,
                None => return Ok(()),
            };

            let cmd = Command::from_frame(frame)?;


            debug!("Got CMD {:?}", cmd);
            debug!(?cmd);

            cmd.apply(&self.db, &mut self.connection, &mut self.shutdown).await?;
        }
        Ok(())
    }
}

impl Drop for Handler {
    fn drop(&mut self) {
        // release 1 the semaphore
        self.limit_connections.add_permits(1);
    }
}
