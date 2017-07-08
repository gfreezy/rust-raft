use std::sync::mpsc::{Sender, channel};

use ::request::{Request, TickRequest};

pub struct Clock(Sender<Request>);


impl Clock {
    pub fn new(sender: Sender<Request>) -> Clock {
        Clock(sender)
    }

    pub fn tick(&self) {
        let (ret_sender, ret_receiver) = channel::<()>();
        {
            let req = Request::TickRequest(TickRequest {
                ret: ret_sender
            });
            self.0.send(req).expect("send vote req");
        }
        let _ = ret_receiver.recv().expect("receive tick resp");
    }
}
