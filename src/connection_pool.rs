use std::io;
use futures::{Future, Stream};
use futures::future::join_all;
use hyper;
use hyper::{Client, Request};
use hyper::client::HttpConnector;
use tokio_core::reactor::Core;

use serde_json;

use ::rpc::{ServerId, VoteReq, VoteResp, AppendEntriesReq, AppendEntriesResp};

pub struct ConnectionPool {
    core: Core,
    client: Client<HttpConnector>
}

impl ConnectionPool {
    pub fn new() -> Self {
        let core = Core::new().expect("init core");
        let client = Client::new(&core.handle());
        ConnectionPool {
            core: core,
            client: client,
        }
    }

    pub fn on_request_vote(&mut self, reqs: Vec<(ServerId, VoteReq)>) -> ::errors::Result<Vec<(ServerId, VoteReq, VoteResp)>> {
        let mut resps = Vec::new();
        for (server_id, vote_req) in reqs {
            let uri = format!("http://{}/raft/on_request_vote", server_id.addr()).parse().unwrap();
            let mut http_req: Request<hyper::Body> = Request::new(hyper::Method::Post, uri);
            let body = serde_json::to_string(&vote_req).unwrap();
            http_req.headers_mut().set(hyper::header::ContentLength(body.len() as u64));
            http_req.set_body(body);
            http_req.headers_mut().set(hyper::header::ContentType::json());


            let resp = self.client.request(http_req).and_then(|res| {
                res.body().concat2()
            }).and_then(|body| {
                serde_json::from_slice::<VoteResp>(&body)
                    .map(|resp| (server_id, vote_req, resp))
                    .map_err(|e| hyper::Error::from(
                        io::Error::new(
                            io::ErrorKind::Other,
                            e
                        )
                    ))
            });
            resps.push(resp);
        }

        Ok(self.core.run(join_all(resps))?)
    }

    pub fn on_append_entries(&mut self, reqs: Vec<(ServerId, AppendEntriesReq)>) -> ::errors::Result<Vec<(ServerId, AppendEntriesReq, AppendEntriesResp)>> {
        let mut resps = Vec::new();
        for (server_id, append_req) in reqs {
            let uri = format!("http://{}/raft/on_append_entries", server_id.addr()).parse().unwrap();
            let mut http_req = Request::new(hyper::Method::Post, uri);
            let body = serde_json::to_string(&append_req).unwrap();
            http_req.headers_mut().set(hyper::header::ContentLength(body.len() as u64));
            http_req.set_body(body);
            http_req.headers_mut().set(hyper::header::ContentType::json());

            let resp = self.client.request(http_req).and_then(|res| {
                res.body().concat2()
            }).and_then(|body| {
                let d: hyper::Result<(ServerId, AppendEntriesReq, AppendEntriesResp)> = serde_json::from_slice(&body).map(|resp| (server_id, append_req, resp))
                    .map_err(|e| hyper::Error::from(
                        io::Error::new(
                            io::ErrorKind::Other,
                            e
                        )
                    ));
                d
            });
            resps.push(resp);
        }

        Ok(self.core.run(join_all(resps))?)
    }
}
