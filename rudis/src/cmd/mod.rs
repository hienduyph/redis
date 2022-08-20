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

pub use self::subscribe::Unsubscribe;

#[derive(Debug)]
pub enum Command {
    Get(Get),
    Set(Set),
    Publish(Publish),
    Subscribe(Subscribe),
    Unsubscribe(Unsubscribe),
    Unknown(Unknown),
}

impl Command {
    pub fn from_frame(frame: crate::Frame) -> crate::Result<Command> {
        // parse the frame
        let mut parse = crate::Parse::new(frame)?;

        let command_name = parse.next_string()?.to_lowercase();

        let command = match &command_name[..] {
            "get" => Command::Get(Get::parse_frame(&mut parse)?),
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

    pub(crate) async fn apply(
        self,
        db: &crate::Db,
        dst: &mut crate::Connection,
        shutdown: &mut crate::Shutdown,
    ) -> crate::Result<()> {
        match self {
            Command::Get(cmd) => cmd.apply(db, dst).await,
            Command::Set(cmd) => cmd.apply(db, dst).await,
            Command::Publish(cmd) => cmd.apply(db, dst).await,
            Command::Subscribe(cmd) => cmd.apply(db, dst, shutdown).await,
            Command::Unknown(cmd) => cmd.apply(dst).await,
            Command::Unsubscribe(_) => Err("`Unsubscribe` is unsupported in this context".into()),
        }
    }

    pub(crate) fn get_name(&self) -> &str {
        match self {
            Command::Get(_) => "get",
            Command::Set(_) => "set",
            Command::Publish(_) => "publish",
            Command::Subscribe(_) => "subscribe",
            Command::Unsubscribe(_) => "unsubcribe",
            Command::Unknown(cmd) => cmd.get_name(),
        }
    }
}
