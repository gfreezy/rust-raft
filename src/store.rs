use ::rpc::Entry;


pub trait Store: Send {
    fn apply(&mut self, entry: &Entry);
}
