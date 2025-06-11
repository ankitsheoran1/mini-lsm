// Copyright (c) 2022-2025 Alex Chi Z
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::char::ToLowercase;
use std::collections::HashMap;
use std::ops::Bound;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;

use anyhow::Result;
use bytes::Bytes;
use nom::combinator::value;
use parking_lot::{Mutex, MutexGuard, RawRwLock, RwLock};

use crate::block::Block;
use crate::compact::{
    CompactionController, CompactionOptions, LeveledCompactionController, LeveledCompactionOptions,
    SimpleLeveledCompactionController, SimpleLeveledCompactionOptions, TieredCompactionController,
};
use crate::iterators::StorageIterator;
use crate::iterators::merge_iterator::MergeIterator;
use crate::iterators::two_merge_iterator::TwoMergeIterator;
use crate::key::{Key, KeyBytes, KeySlice};
use crate::lsm_iterator::{FusedIterator, LsmIterator};
use crate::manifest::Manifest;
use crate::mem_table::{MemTable, map_bound};
use crate::mvcc::LsmMvccInner;
use crate::table::{SsTable, SsTableBuilder, SsTableIterator};

pub type BlockCache = moka::sync::Cache<(usize, usize), Arc<Block>>;

/// Represents the state of the storage engine.
#[derive(Clone)]
pub struct LsmStorageState {
    /// The current memtable.
    pub memtable: Arc<MemTable>,
    /// Immutable memtables, from latest to earliest.
    pub imm_memtables: Vec<Arc<MemTable>>,
    /// L0 SSTs, from latest to earliest.
    pub l0_sstables: Vec<usize>,
    /// SsTables sorted by key range; L1 - L_max for leveled compaction, or tiers for tiered
    /// compaction.
    pub levels: Vec<(usize, Vec<usize>)>,
    /// SST objects.
    pub sstables: HashMap<usize, Arc<SsTable>>,
}

pub enum WriteBatchRecord<T: AsRef<[u8]>> {
    Put(T, T),
    Del(T),
}

impl LsmStorageState {
    fn create(options: &LsmStorageOptions) -> Self {
        let levels = match &options.compaction_options {
            CompactionOptions::Leveled(LeveledCompactionOptions { max_levels, .. })
            | CompactionOptions::Simple(SimpleLeveledCompactionOptions { max_levels, .. }) => (1
                ..=*max_levels)
                .map(|level| (level, Vec::new()))
                .collect::<Vec<_>>(),
            CompactionOptions::Tiered(_) => Vec::new(),
            CompactionOptions::NoCompaction => vec![(1, Vec::new())],
        };
        Self {
            memtable: Arc::new(MemTable::create(0)),
            imm_memtables: Vec::new(),
            l0_sstables: Vec::new(),
            levels,
            sstables: Default::default(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct LsmStorageOptions {
    // Block size in bytes
    pub block_size: usize,
    // SST size in bytes, also the approximate memtable capacity limit
    pub target_sst_size: usize,
    // Maximum number of memtables in memory, flush to L0 when exceeding this limit
    pub num_memtable_limit: usize,
    pub compaction_options: CompactionOptions,
    pub enable_wal: bool,
    pub serializable: bool,
}

impl LsmStorageOptions {
    pub fn default_for_week1_test() -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 2 << 20,
            compaction_options: CompactionOptions::NoCompaction,
            enable_wal: false,
            num_memtable_limit: 50,
            serializable: false,
        }
    }

    pub fn default_for_week1_day6_test() -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 2 << 20,
            compaction_options: CompactionOptions::NoCompaction,
            enable_wal: false,
            num_memtable_limit: 2,
            serializable: false,
        }
    }

    pub fn default_for_week2_test(compaction_options: CompactionOptions) -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 1 << 20, // 1MB
            compaction_options,
            enable_wal: false,
            num_memtable_limit: 2,
            serializable: false,
        }
    }
}

#[derive(Clone, Debug)]
pub enum CompactionFilter {
    Prefix(Bytes),
}

/// The storage interface of the LSM tree.
pub(crate) struct LsmStorageInner {
    pub(crate) state: Arc<RwLock<Arc<LsmStorageState>>>,
    pub(crate) state_lock: Mutex<()>,
    path: PathBuf,
    pub(crate) block_cache: Arc<BlockCache>,
    next_sst_id: AtomicUsize,
    pub(crate) options: Arc<LsmStorageOptions>,
    pub(crate) compaction_controller: CompactionController,
    pub(crate) manifest: Option<Manifest>,
    pub(crate) mvcc: Option<LsmMvccInner>,
    pub(crate) compaction_filters: Arc<Mutex<Vec<CompactionFilter>>>,
}

/// A thin wrapper for `LsmStorageInner` and the user interface for MiniLSM.
pub struct MiniLsm {
    pub(crate) inner: Arc<LsmStorageInner>,
    /// Notifies the L0 flush thread to stop working. (In week 1 day 6)
    flush_notifier: crossbeam_channel::Sender<()>,
    /// The handle for the flush thread. (In week 1 day 6)
    flush_thread: Mutex<Option<std::thread::JoinHandle<()>>>,
    /// Notifies the compaction thread to stop working. (In week 2)
    compaction_notifier: crossbeam_channel::Sender<()>,
    /// The handle for the compaction thread. (In week 2)
    compaction_thread: Mutex<Option<std::thread::JoinHandle<()>>>,
}

impl Drop for MiniLsm {
    fn drop(&mut self) {
        self.compaction_notifier.send(()).ok();
        self.flush_notifier.send(()).ok();
    }
}

impl MiniLsm {
    pub fn close(&self) -> Result<()> {
        let mut flush_thread = self.flush_thread.lock();
        if let Some(flush_thread) = flush_thread.take() {
            match flush_thread.join() {
                Ok(_) => (),
                Err(e) => eprintln!("error in flush thread: {:?}", e),
            }
        }
        Ok(())
    }

    /// Start the storage engine by either loading an existing directory or creating a new one if the directory does
    /// not exist.
    pub fn open(path: impl AsRef<Path>, options: LsmStorageOptions) -> Result<Arc<Self>> {
        let inner = Arc::new(LsmStorageInner::open(path, options)?);
        let (tx1, rx) = crossbeam_channel::unbounded();
        let compaction_thread = inner.spawn_compaction_thread(rx)?;
        let (tx2, rx) = crossbeam_channel::unbounded();
        let flush_thread = inner.spawn_flush_thread(rx)?;
        Ok(Arc::new(Self {
            inner,
            flush_notifier: tx2,
            flush_thread: Mutex::new(flush_thread),
            compaction_notifier: tx1,
            compaction_thread: Mutex::new(compaction_thread),
        }))
    }

    pub fn new_txn(&self) -> Result<()> {
        self.inner.new_txn()
    }

    pub fn write_batch<T: AsRef<[u8]>>(&self, batch: &[WriteBatchRecord<T>]) -> Result<()> {
        self.inner.write_batch(batch)
    }

    pub fn add_compaction_filter(&self, compaction_filter: CompactionFilter) {
        self.inner.add_compaction_filter(compaction_filter)
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        self.inner.get(key)
    }

    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.inner.put(key, value)
    }

    pub fn delete(&self, key: &[u8]) -> Result<()> {
        self.inner.delete(key)
    }

    pub fn sync(&self) -> Result<()> {
        self.inner.sync()
    }

    pub fn scan(
        &self,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
    ) -> Result<FusedIterator<LsmIterator>> {
        self.inner.scan(lower, upper)
    }

    /// Only call this in test cases due to race conditions
    pub fn force_flush(&self) -> Result<()> {
        if !self.inner.state.read().memtable.is_empty() {
            self.inner
                .force_freeze_memtable(&self.inner.state_lock.lock())?;
        }
        if !self.inner.state.read().imm_memtables.is_empty() {
            self.inner.force_flush_next_imm_memtable()?;
        }
        Ok(())
    }

    pub fn force_full_compaction(&self) -> Result<()> {
        self.inner.force_full_compaction()
    }
}

impl LsmStorageInner {
    pub(crate) fn next_sst_id(&self) -> usize {
        self.next_sst_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    }

    pub(crate) fn mvcc(&self) -> &LsmMvccInner {
        self.mvcc.as_ref().unwrap()
    }

    /// Start the storage engine by either loading an existing directory or creating a new one if the directory does
    /// not exist.
    pub(crate) fn open(path: impl AsRef<Path>, options: LsmStorageOptions) -> Result<Self> {
        let path = path.as_ref();
        let state = LsmStorageState::create(&options);

        let compaction_controller = match &options.compaction_options {
            CompactionOptions::Leveled(options) => {
                CompactionController::Leveled(LeveledCompactionController::new(options.clone()))
            }
            CompactionOptions::Tiered(options) => {
                CompactionController::Tiered(TieredCompactionController::new(options.clone()))
            }
            CompactionOptions::Simple(options) => CompactionController::Simple(
                SimpleLeveledCompactionController::new(options.clone()),
            ),
            CompactionOptions::NoCompaction => CompactionController::NoCompaction,
        };

        let storage = Self {
            state: Arc::new(RwLock::new(Arc::new(state))),
            state_lock: Mutex::new(()),
            path: path.to_path_buf(),
            block_cache: Arc::new(BlockCache::new(1024)),
            next_sst_id: AtomicUsize::new(1),
            compaction_controller,
            manifest: None,
            options: options.into(),
            mvcc: None,
            compaction_filters: Arc::new(Mutex::new(Vec::new())),
        };

        Ok(storage)
    }

    pub fn sync(&self) -> Result<()> {
        unimplemented!()
    }

    pub fn add_compaction_filter(&self, compaction_filter: CompactionFilter) {
        let mut compaction_filters = self.compaction_filters.lock();
        compaction_filters.push(compaction_filter);
    }

    /// Get a key from the storage. In day 7, this can be further optimized by using a bloom filter.
    pub fn get(&self, _key: &[u8]) -> Result<Option<Bytes>> {
        let (active_memtable, imm_memtables) = {
            let guard = self.state.read();
            (
                Arc::clone(&guard.memtable),
                guard
                    .imm_memtables
                    .iter()
                    .map(|table| Arc::clone(table))
                    .collect::<Vec<_>>(),
            )
        };

        if let Some(value) = active_memtable.get(_key) {
            return Ok(if value.is_empty() { None } else { Some(value) });
        }

        for memtable in imm_memtables {
            if let Some(value) = memtable.get(_key) {
                return Ok(if value.is_empty() { None } else { Some(value) });
            }
        }

        let disk_lsm_storage = Arc::clone(&self.state.read());
        let mut sstable_iterators = Vec::new();
        let sstable_ids = &disk_lsm_storage.l0_sstables;
        for sstable_id in sstable_ids {
            let sstable = disk_lsm_storage.sstables.get(&sstable_id).unwrap();
            if _key < sstable.first_key().as_key_slice().into_inner()
                || _key > sstable.last_key().as_key_slice().into_inner()
            {
                continue;
            }
            let sstable_iter = SsTableIterator::create_and_seek_to_key(
                Arc::clone(sstable),
                KeySlice::from_slice(_key),
            )?;
            sstable_iterators.push(Box::new(sstable_iter));
        }
        let sstable_merge_iterator = MergeIterator::create(sstable_iterators);

        if sstable_merge_iterator.is_valid()
            && sstable_merge_iterator.key() == Key::from_slice(_key)
            && !sstable_merge_iterator.value().is_empty()
        {
            let value = sstable_merge_iterator.value();
            return Ok(Some(Bytes::copy_from_slice(value)));
        }

        Ok(None)
    }

    /// Write a batch of data into the storage. Implement in week 2 day 7.
    pub fn write_batch<T: AsRef<[u8]>>(&self, _batch: &[WriteBatchRecord<T>]) -> Result<()> {
        unimplemented!()
    }

    /// Put a key-value pair into the storage by writing into the current memtable.
    pub fn put(&self, _key: &[u8], _value: &[u8]) -> Result<()> {
        let guard = self.state.read();
        let res = guard.memtable.put(_key, _value);
        if guard.memtable.approximate_size() >= self.options.target_sst_size {
            let lock = self.state_lock.lock();
            if guard.memtable.approximate_size() >= self.options.target_sst_size {
                drop(guard);
                self.force_freeze_memtable(&lock)?
            }
        }
        res
    }

    /// Remove a key from the storage by writing an empty value.
    pub fn delete(&self, _key: &[u8]) -> Result<()> {
        self.put(_key, &[])
    }

    pub(crate) fn path_of_sst_static(path: impl AsRef<Path>, id: usize) -> PathBuf {
        path.as_ref().join(format!("{:05}.sst", id))
    }

    pub(crate) fn path_of_sst(&self, id: usize) -> PathBuf {
        Self::path_of_sst_static(&self.path, id)
    }

    pub(crate) fn path_of_wal_static(path: impl AsRef<Path>, id: usize) -> PathBuf {
        path.as_ref().join(format!("{:05}.wal", id))
    }

    pub(crate) fn path_of_wal(&self, id: usize) -> PathBuf {
        Self::path_of_wal_static(&self.path, id)
    }

    pub(super) fn sync_dir(&self) -> Result<()> {
        unimplemented!()
    }

    /// Force freeze the current memtable to an immutable memtable
    pub fn force_freeze_memtable(&self, _state_lock_observer: &MutexGuard<'_, ()>) -> Result<()> {
        let curr_memtable = self.state.read().memtable.clone();
        let memtable = MemTable::create(self.next_sst_id());
        let mut write_guard = self.state.write();
        let write_guard = Arc::make_mut(&mut write_guard);
        write_guard.imm_memtables.insert(0, curr_memtable);
        //let mut state = self.state.write();
        write_guard.memtable = Arc::new(memtable);
        Ok(())
    }

    /// Force flush the earliest-created immutable memtable to disk
    pub fn force_flush_next_imm_memtable(&self) -> Result<()> {
        let state_lock = self.state_lock.lock();

        let (oldest_table, memtable_id) = {
            let guard = self.state.read();
            if guard.imm_memtables.is_empty() {
                return Ok(()); // Nothing to flush
            }
            let oldest_table = guard.imm_memtables.last().unwrap().clone();
            let memtable_id = oldest_table.id();
            (oldest_table, memtable_id)
        };

        let mut sst_builder = SsTableBuilder::new(self.options.block_size);
        oldest_table.flush(&mut sst_builder)?;
        let sst = Arc::new(sst_builder.build(
            memtable_id,
            Some(Arc::clone(&self.block_cache)),
            self.path_of_sst(memtable_id),
        )?);
        {
            let mut state_guard = self.state.write();

            if state_guard.imm_memtables.is_empty() {
                return Ok(());
            }

            let last_memtable_id = state_guard.imm_memtables.last().unwrap().id();
            if last_memtable_id != memtable_id {
                return Err(anyhow::anyhow!(
                    "Memtable changed during flush: expected {}, found {}",
                    memtable_id,
                    last_memtable_id
                ));
            }
            let mut new_state = state_guard.as_ref().clone();
            let flushed_memtable = new_state.imm_memtables.pop().unwrap();
            assert_eq!(flushed_memtable.id(), memtable_id);
            new_state.l0_sstables.insert(0, memtable_id);
            new_state.sstables.insert(memtable_id, sst);
            *state_guard = Arc::new(new_state);
        }
        Ok(())
    }

    pub fn new_txn(&self) -> Result<()> {
        // no-op
        Ok(())
    }

    /// Create an iterator over a range of keys.
    pub fn scan(
        &self,
        _lower: Bound<&[u8]>,
        _upper: Bound<&[u8]>,
    ) -> Result<FusedIterator<LsmIterator>> {
        let mut memtables = Vec::new();
        let guard = self.state.read();
        memtables.push(Arc::clone(&guard.memtable));
        memtables.extend(
            guard
                .imm_memtables
                .iter()
                .map(|memtable| Arc::clone(memtable)),
        );
        let mut iterators = Vec::new();
        for memtable in memtables {
            //  create a memtable iterator for each memtable
            let iterator = memtable.scan(_lower, _upper);
            iterators.push(Box::new(iterator));
        }

        let merge_iterator = MergeIterator::create(iterators);
        let sstable_iterators = self.create_sst_iterators(_lower, _upper).unwrap();
        let lsm_iterator = LsmIterator::new(
            TwoMergeIterator::create(merge_iterator, MergeIterator::create(sstable_iterators))?,
            map_bound(_upper),
        )?;
        Ok(FusedIterator::new(lsm_iterator))
    }

    fn create_sst_iterators(
        &self,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
    ) -> Result<Vec<Box<SsTableIterator>>> {
        let storage = Arc::clone(&self.state.read());
        let ss_ids = &storage.l0_sstables;
        let mut sstable_iterators = Vec::new();

        for ss_id in ss_ids {
            let table = storage.sstables.get(ss_id).unwrap();
            if !self.range_overlap(lower, upper, table.first_key(), table.last_key()) {
                continue;
            }
            let iter = match lower {
                Bound::Included(key) => SsTableIterator::create_and_seek_to_key(
                    Arc::clone(table),
                    KeySlice::from_slice(key),
                )?,
                Bound::Excluded(key) => {
                    let mut iter = SsTableIterator::create_and_seek_to_key(
                        Arc::clone(table),
                        KeySlice::from_slice(key),
                    )?;
                    if iter.is_valid() && iter.key().raw_ref() == key {
                        iter.next()?;
                    }
                    iter
                }
                Bound::Unbounded => SsTableIterator::create_and_seek_to_first(Arc::clone(table))?,
            };
            sstable_iterators.push(Box::new(iter));
        }

        Ok(sstable_iterators)
    }

    fn range_overlap(
        &self,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
        table_first: &KeyBytes,
        table_last: &KeyBytes,
    ) -> bool {
        match lower {
            Bound::Excluded(key) if key >= table_last.as_key_slice().into_inner() => {
                return false;
            }
            Bound::Included(key) if key > table_last.as_key_slice().into_inner() => {
                return false;
            }
            _ => {}
        }
        match upper {
            Bound::Excluded(key) if key <= table_first.as_key_slice().into_inner() => {
                return false;
            }
            Bound::Included(key) if key < table_first.as_key_slice().into_inner() => {
                return false;
            }
            _ => {}
        }
        true
    }
}
