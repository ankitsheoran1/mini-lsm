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

use crate::key::{Key, KeySlice, KeyVec};
use nom::AsChar;

use super::Block;

/// Builds a block.
pub struct BlockBuilder {
    /// Offsets of each key-value entries.
    offsets: Vec<u16>,
    /// All serialized key-value pairs in the block.
    data: Vec<u8>,
    /// The expected block size.
    block_size: usize,
    /// The first key in the block
    first_key: KeyVec,
}

impl BlockBuilder {
    /// Creates a new block builder.
    pub fn new(block_size: usize) -> Self {
        let first_key = Key::new();
        BlockBuilder {
            offsets: Vec::new(),
            data: Vec::new(),
            block_size,
            first_key,
        }
        // unimplemented!()
    }

    fn get_overlap_with_first_key(&self, key: KeySlice) -> u16 {
        let overlap_len = key
            .raw_ref()
            .iter()
            .zip(self.first_key.raw_ref().iter())
            .take_while(|(a, b)| a == b)
            .count() as u16;
        overlap_len
    }

    /// Adds a key-value pair to the block. Returns false when the block is full.
    /// You may find the `bytes::BufMut` trait useful for manipulating binary data.
    #[must_use]
    pub fn add(&mut self, key: KeySlice, value: &[u8]) -> bool {
        //check if it would fit in size or not
        //Adding 6 = key_length + value_length + offset
        let overlap_len = self.get_overlap_with_first_key(key);
        let overlap_len_bytes = overlap_len.to_le_bytes();
        let rem_key = &(key.raw_ref())[overlap_len as usize..];
        let rem_key_len = (rem_key.len() as u16).to_le_bytes();

        let total_size = rem_key.len() + value.len() + 6;
        if self.first_key.raw_ref().len() > 0
            && total_size + self.data.len() + self.offsets.len() > self.block_size
        {
            return false;
        }
        self.offsets.push(self.data.len() as u16);
        self.data.extend_from_slice(&overlap_len_bytes);
        self.data.extend_from_slice(&rem_key_len);
        self.data.extend_from_slice(rem_key);
        self.data
            .extend_from_slice(&(value.len() as u16).to_le_bytes());
        self.data.extend_from_slice(value);
        if self.first_key.is_empty() {
            self.first_key = key.to_key_vec()
        }
        true
    }

    /// Check if there is no key-value pair in the block.
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    /// Finalize the block.
    pub fn build(self) -> Block {
        Block {
            data: self.data,
            offsets: self.offsets,
        }

        // unimplemented!()
    }
}
