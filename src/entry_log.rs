use ::rpc::{Entry, EntryIndex};


#[derive(Debug)]
pub struct EntryLog {
    entries: Vec<Entry>
}


impl EntryLog {
    pub fn new() -> EntryLog {
        EntryLog {
            entries: Vec::new()
        }
    }

    pub fn get(&self, index: EntryIndex) -> Option<&Entry> {
        self.entries.get(index.0 as usize)
    }

    pub fn push(&mut self, entry: Entry) {
        self.entries.push(entry)
    }

    pub fn truncate(&mut self, index: EntryIndex) {
        self.entries.truncate(index.0 as usize)
    }

    pub fn get_entries_since_index(&self, index: EntryIndex) -> Vec<Entry> {
        info!("last_index: {}, get entries since: {}", self.last_index(), index);
        if index > self.last_index() {
            Vec::new()
        } else {
            self.entries[index.0 as usize..].iter().cloned().collect::<Vec<Entry>>()
        }
    }

    pub fn last_index(&self) -> EntryIndex {
        EntryIndex(self.entries.len() as u64).prev_or_zero()
    }
}
