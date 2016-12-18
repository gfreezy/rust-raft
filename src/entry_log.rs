use ::rpc::{Entry, EntryIndex, Term};


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

    pub fn prev_last_index(&self) -> EntryIndex {
        self.last_index().prev_or_zero()
    }

    pub fn last_entry_term(&self) -> Term {
        let entry = self.last_entry();
        entry.map_or_else(|| Term(0), |e| e.term)
    }

    pub fn prev_last_entry_term(&self) -> Term {
        let entry = self.prev_last_entry();
        entry.map_or_else(|| Term(0), |e| e.term)
    }

    fn last_entry(&self) -> Option<&Entry> {
        self.get(self.last_index())
    }

    fn prev_last_entry(&self) -> Option<&Entry> {
        self.get(self.prev_last_index())
    }
}
