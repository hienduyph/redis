
use futures::future::join_all;
mod get;
pub use get::Get;

mod set;
pub use set::Set;

mod publish;
pub use publish::Publish;

mod subscribe;
pub use subscribe::Subscribe;

mod unknown;
pub use unknown::Unknown;

mod cmd;
use cmd::Cmd;

use crate::Frame;

pub use self::subscribe::Unsubscribe;

#[derive(Debug)]
pub enum Command {
    Cmd(Cmd),
    Get(Get),
    Set(Set),
    Publish(Publish),
    Subscribe(Subscribe),
    Unsubscribe(Unsubscribe),
    Unknown(Unknown),
    Pipe(Vec<Command>),
}

impl Command {
    pub fn from_frame(frame: crate::Frame) -> crate::Result<Command> {
        match frame {
            Frame::Pipe(vec) => {
                let cmds = vec.into_iter().map(|f| Self::_from_frame(f).unwrap()).collect();
                Ok(Command::Pipe(cmds))
            },
            _ => Self::_from_frame(frame),
        }
    }

    fn _from_frame(frame: crate::Frame) -> crate::Result<Command> {
        // parse the frame
        let mut parse = crate::Parse::new(frame)?;
        let command_name = parse.next_string()?.to_lowercase();
        let command = match &command_name[..] {
            "get" => Command::Get(Get::parse_frame(&mut parse)?),
            "command" => Command::Cmd(Cmd::parse_frame(&mut parse)?),
            "set" => Command::Set(Set::parse_frame(&mut parse)?),
            "publish" => Command::Publish(Publish::parse_frames(&mut parse)?),
            "subscribe" => Command::Subscribe(Subscribe::parse_frames(&mut parse)?),
            "unsubscribe" => Command::Unsubscribe(Unsubscribe::parse_frames(&mut parse)?),
            _ => {
                return Ok(Command::Unknown(Unknown::new(command_name)));
            }
        };

        parse.finish()?;
        Ok(command)

    }

    pub(crate) async fn handle(
        db: &crate::Db,
        cmd: Command,
    ) -> crate::Result<Frame> {
        match cmd {
            Command::Get(cmd) => cmd.apply(db).await,
            Command::Set(cmd) => cmd.apply(db).await,
            Command::Cmd(cmd) => cmd.apply(db).await,
            _ => Err("Should not be here!".into())
        }
    }

    pub(crate) async fn apply(
        self,
        db: &crate::Db,
        dst: &mut crate::Connection,
        shutdown: &mut crate::Shutdown,
    ) -> crate::Result<()> {
        match self {
            Command::Publish(cmd) => cmd.apply(db, dst).await,
            Command::Subscribe(cmd) => cmd.apply(db, dst, shutdown).await,
            Command::Unknown(cmd) => cmd.apply(dst).await,
            Command::Unsubscribe(_) => Err("`Unsubscribe` is unsupported in this context".into()),
            Command::Pipe(cmds) => {
                let fu = cmds.into_iter().map(|c| { Self::handle(db, c) });
                let results = join_all(fu).await.into_iter().map(|v| v.unwrap()).collect();
                dst.write_frame(&Frame::Pipe(results)).await?;
                Ok(())
            },
            _ => {
                let f = Self::handle(db, self).await?;
                dst.write_frame(&f).await?;
                Ok(())
            }
        }
    }

    pub(crate) fn get_name(&self) -> &str {
        match self {
            Command::Get(_) => "get",
            Command::Set(_) => "set",
            Command::Publish(_) => "publish",
            Command::Subscribe(_) => "subscribe",
            Command::Unsubscribe(_) => "unsubcribe",
            Command::Pipe(_) => "pipe",
            Command::Cmd(_) => "command",
            Command::Unknown(cmd) => cmd.get_name(),
        }
    }
}
