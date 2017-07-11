use ::rpc::{Entry, EntryIndex, Term};


#[derive(Debug)]
pub struct EntryLog {
    zero_index: EntryIndex,
    entries: Vec<Entry>
}


impl EntryLog {
    pub fn new() -> EntryLog {
        EntryLog {
            zero_index: EntryIndex(::ZERO_INDEX),
            entries: vec![
                Entry {
                    term: Term(1),
                    cmd: "".into(),
                    payload: "".into(),
                },
                Entry {
                    term: Term(1),
                    cmd: "".into(),
                    payload: "".into(),
                }
            ]
        }
    }

    pub fn get(&self, index: EntryIndex) -> Option<&Entry> {
        if index.0 >= ::ZERO_INDEX {
            self.entries.get(self.real_index(index))
        } else { None }
    }

    pub fn push(&mut self, entry: Entry) {
        self.entries.push(entry)
    }

    pub fn truncate(&mut self, index: EntryIndex) {
        if index.0 > ::ZERO_INDEX + 1 {
            let i = self.real_index(index);
            self.entries.truncate(i);
        }
    }

    pub fn get_entries_since_index(&self, index: EntryIndex) -> Vec<Entry> {
        if index > self.last_index() {
            Vec::new()
        } else {
            self.entries[self.real_index(index)..].iter().cloned().collect::<Vec<Entry>>()
        }
    }

    pub fn last_index(&self) -> EntryIndex {
        EntryIndex(self.entries.len() as u64 + ::ZERO_INDEX - 1)
    }

    pub fn prev_index(&self, index: EntryIndex) -> EntryIndex {
        index.prev_or_zero()
    }

    pub fn last_entry_term(&self) -> Term {
        let entry = self.last_entry();
        entry.map_or_else(|| Term(0), |e| e.term)
    }

    pub fn prev_entry_term(&self, index: EntryIndex) -> Term {
        let entry = self.prev_entry(index);
        entry.map_or_else(|| Term(0), |e| e.term)
    }

    fn last_entry(&self) -> Option<&Entry> {
        self.get(self.last_index())
    }

    fn prev_entry(&self, index: EntryIndex) -> Option<&Entry> {
        self.get(self.prev_index(index))
    }

    fn real_index(&self, entry_index: EntryIndex) -> usize {
        entry_index.0 as usize - ::ZERO_INDEX as usize
    }
}
