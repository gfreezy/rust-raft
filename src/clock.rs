use std::sync::Mutex;
use std::sync::Arc;
use std::sync::mpsc::{Sender, channel};

use ::request::{Request, TickRequest};

pub struct Clock(Arc<Mutex<Sender<Request>>>);


impl Clock {
    pub fn new(sender: Arc<Mutex<Sender<Request>>>) -> Clock {
        Clock(sender)
    }

    pub fn tick(&self) {
        let (ret_sender, ret_receiver) = channel::<()>();
        {
            let sender = self.0.try_lock().expect("lock tick");
            let req = Request::TickRequest(TickRequest {
                ret: ret_sender
            });
            sender.send(req).expect("send vote req");
        }
        let _ = ret_receiver.recv().expect("receive tick resp");
    }
}
