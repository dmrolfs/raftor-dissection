use actix::prelude::*;
use actix_web_actors::ws;
use serde::{Deserialize, Serialize};
// use serde_json as json;
use std::time::{Duration, Instant};
use log::{info, error, warn};

use crate::server::{self, Server};

/// How often heartbeat pings are sent
const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(5);
/// How long before lack of client response causes a timeout
const CLIENT_TIMEOUT: Duration = Duration::from_secs(30); //dmr Duration::from_secs(10);

pub struct Session {
    id: String,
    room: String,
    count: u64,
    server: Addr<Server>,
    last_heartbeat: Instant,
}

impl Session {
    pub fn new(id: &str, room: &str, server: Addr<Server>) -> Self {
        Session {
            id: id.to_owned(),
            room: room.to_owned(),
            server: server,
            count: 0,
            last_heartbeat: Instant::now(),
        }
    }

    fn heartbeat(&self, ctx: &mut ws::WebsocketContext<Self>) {
        ctx.run_interval(HEARTBEAT_INTERVAL, |act, ctx| {
            // check client heartbeats
            if Instant::now().duration_since(act.last_heartbeat) > CLIENT_TIMEOUT {
                // heartbeat timed out
                warn!("Websocket Client heartbeat failed, disconnecting!");

                // notify chat server
                act.server.do_send(server::Disconnect(act.id.to_owned()));

                // stop actor
                ctx.stop();

                // don't try to send a ping
                return;
            }

            ctx.ping("");
        });
    }
}

impl Actor for Session {
    type Context = ws::WebsocketContext<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        self.heartbeat(ctx);

        let addr = ctx.address();

        self.server
            .send(server::Connect(self.id.to_owned(), addr.clone()))
            .into_actor(self)
            .then(|res, _act, ctx| {
                match res {
                    Ok(_) => (),
                    _ => ctx.stop(),
                }
                fut::ok(())
            })
            .wait(ctx);
    }

    fn stopping(&mut self, _: &mut Self::Context) -> Running {
        // notify server
        self.server.do_send(server::Disconnect(self.id.to_owned()));
        Running::Stop
    }
}

#[derive(Message)]
pub struct SendMessage(pub String);

impl Handler<SendMessage> for Session {
    type Result = ();

    fn handle(&mut self, msg: SendMessage, ctx: &mut Self::Context) {
        ctx.text(msg.0);
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub enum Cmds {
    /// Join(room_id)
    Join(String),
    /// Send(recipient_id)
    SendRecipient(String),
    /// Send(room_id)
    SendRoom(String),
}

#[derive(Message, Serialize, Deserialize, Debug)]
pub struct Message {
    content: String,
    cmd: Cmds,
}

#[derive(Message, Serialize, Deserialize, Debug)]
pub struct TextMessage {
    pub content: String,
    pub sender_id: String,
}

impl Handler<server::DisconnectSession> for Session {
    type Result = ();

    fn handle(&mut self, _: server::DisconnectSession, ctx: &mut Self::Context) {
        ctx.stop();
    }
}

impl Handler<TextMessage> for Session {
    type Result = ();

    fn handle(&mut self, msg: TextMessage, ctx: &mut Self::Context) {
        self.count += 1;
        info!("Text Message - Sending message to client #{}", self.count);
        //dmr let payload = serde_cbor::to_vec(&msg).unwrap_or(vec![] ); //dmr "no output".to_owned());
        //dmr ctx.binary(payload);
        let payload = serde_json::to_string(&msg).unwrap_or("no output".to_owned());
        ctx.text(payload);
    }
}

impl StreamHandler<ws::Message, ws::ProtocolError> for Session {
    fn handle(&mut self, msg: ws::Message, ctx: &mut Self::Context) {
        let uid = self.id.to_owned();

        match msg {
            ws::Message::Ping(msg) => {
                self.last_heartbeat = Instant::now();
                ctx.pong(&msg);
            }
            ws::Message::Pong(_) => {
                self.last_heartbeat = Instant::now();
            }
            ws::Message::Text(msg) => {
                // let msg = serde_cbor::from_slice::<Message>(msg.as_ref());
                let msg = serde_json::from_slice::<Message>(msg.as_ref());
                match msg {
                    Ok(msg) => match msg.cmd {
                        Cmds::Join(room_id) => {
                            self.server.do_send(server::Join {
                                room_id: room_id,
                                uid: uid,
                            });
                        }
                        Cmds::SendRecipient(recipient_id) => {
                            self.server.do_send(server::SendRecipient {
                                recipient_id: recipient_id,
                                uid: uid,
                                content: msg.content,
                            });
                        }
                        Cmds::SendRoom(room_id) => {
                            self.server.do_send(server::SendRoom {
                                room_id: room_id,
                                uid: uid,
                                content: msg.content,
                            });
                        }
                    },
                    Err(err) => error!("Websocket Text Error: {:?}", err),
                }
            }
            ws::Message::Binary(_) => error!("Unexpected binary message"),
            ws::Message::Close(_) => {
                ctx.stop();
            }
            ws::Message::Nop => (),
        }
    }
}
