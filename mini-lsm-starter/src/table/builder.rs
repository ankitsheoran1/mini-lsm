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

use std::path::Path;
use std::sync::Arc;

use super::{BlockMeta, FileObject, SsTable};
use crate::block::Block;
use crate::key::Key;
use crate::table::bloom::Bloom;
use crate::{block::BlockBuilder, key::KeySlice, lsm_storage::BlockCache};
use anyhow::Result;
use nom::AsBytes;

/// Builds an SSTable from key-value pairs.
pub struct SsTableBuilder {
    builder: BlockBuilder,
    first_key: Vec<u8>,
    last_key: Vec<u8>,
    data: Vec<u8>,
    pub(crate) meta: Vec<BlockMeta>,
    block_size: usize,
    key_hashes: Vec<u32>,
}

impl SsTableBuilder {
    /// Create a builder based on target block size.
    pub fn new(block_size: usize) -> Self {
        SsTableBuilder {
            builder: BlockBuilder::new(block_size),
            first_key: Key::new().into_inner(),
            last_key: Key::new().into_inner(),
            data: Vec::new(),
            meta: Vec::new(),
            block_size,
            key_hashes: Vec::new(),
        }
    }

    /// Adds a key-value pair to SSTable.
    ///
    /// Note: You should split a new block when the current block is full.(`std::mem::replace` may
    /// be helpful here)

    fn bytes_to_string(bytes: &[u8]) -> String {
        String::from_utf8_lossy(bytes).to_string()
    }
    pub fn add(&mut self, key: KeySlice, value: &[u8]) {
        if self.first_key.is_empty() {
            self.first_key = key.to_key_vec().raw_ref().to_vec();
        }

        self.key_hashes
            .push(farmhash::fingerprint32(key.into_inner()));

        if self.builder.add(key, value) {
            println!("Setting last key {} ", Self::bytes_to_string(key.raw_ref()));
            self.last_key = key.to_key_vec().raw_ref().to_vec();
            return;
        }

        println!("New Block key {} ", Self::bytes_to_string(key.raw_ref()));
        self.rotate_block();

        assert!(self.builder.add(key, value));
        self.first_key = key.to_key_vec().raw_ref().to_vec();
        self.last_key = key.to_key_vec().raw_ref().to_vec();
    }

    fn rotate_block(&mut self) {
        let builder = std::mem::replace(&mut self.builder, BlockBuilder::new(self.block_size));
        let block = builder.build();
        let bytes = block.encode();

        self.meta.push(BlockMeta {
            first_key: Key::from_vec(self.first_key.clone()).into_key_bytes(),
            last_key: Key::from_vec(self.last_key.clone()).into_key_bytes(),
            offset: self.data.len(),
        });
        self.data.extend_from_slice(&bytes);
    }

    /// Get the estimated size of the SSTable.
    ///
    /// Since the data blocks contain much more data than meta blocks, just return the size of data
    /// blocks here.
    pub fn estimated_size(&self) -> usize {
        self.data.len()
    }

    /// Builds the SSTable and writes it to the given path. Use the `FileObject` structure to manipulate the disk objects.
    pub fn build(
        mut self,
        id: usize,
        block_cache: Option<Arc<BlockCache>>,
        path: impl AsRef<Path>,
    ) -> Result<SsTable> {
        self.rotate_block();
        let mut data = Vec::new();
        data.extend_from_slice(&self.data);
        let mut meta: Vec<u8> = Vec::new();
        BlockMeta::encode_block_meta(&self.meta, &mut meta);
        data.extend_from_slice(&meta);
        data.extend_from_slice(&(self.data.len() as u32).to_le_bytes());
        let bloom_bits_per_key = Bloom::bloom_bits_per_key(self.key_hashes.len(), 0.01);
        let bloom = Bloom::build_from_key_hashes(&self.key_hashes, bloom_bits_per_key);

        let bloom_filter_offset = data.len() as u32;
        bloom.encode(&mut data);
        data.extend_from_slice(&bloom_filter_offset.to_le_bytes());
        let file = FileObject::create(path.as_ref(), data)?;
        Ok(SsTable {
            file,
            id,
            block_cache,
            first_key: self.meta.first().unwrap().first_key.clone(),
            last_key: self.meta.last().unwrap().last_key.clone(),
            block_meta: self.meta,
            bloom: Some(bloom),
            max_ts: 0,
            block_meta_offset: self.data.len(),
        })
    }

    #[cfg(test)]
    pub(crate) fn build_for_test(self, path: impl AsRef<Path>) -> Result<SsTable> {
        self.build(0, None, path)
    }
}
