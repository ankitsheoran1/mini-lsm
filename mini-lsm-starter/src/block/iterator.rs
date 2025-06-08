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
use nom::character::complete::u16;
use std::sync::Arc;

use super::Block;

/// Iterates on a block.
pub struct BlockIterator {
    /// The internal `Block`, wrapped by an `Arc`
    block: Arc<Block>,
    /// The current key, empty represents the iterator is invalid
    key: KeyVec,
    /// the current value range in the block.data, corresponds to the current key
    value_range: (usize, usize),
    /// Current index of the key-value pair, should be in range of [0, num_of_elements)
    idx: usize,
    /// The first key in the block
    first_key: KeyVec,
}

impl BlockIterator {
    fn new(block: Arc<Block>) -> Self {
        Self {
            block,
            key: Key::new(),
            value_range: (0, 0),
            idx: 0,
            first_key: Key::new(),
        }
    }

    /// Creates a block iterator and seek to the first entry.
    pub fn create_and_seek_to_first(block: Arc<Block>) -> Self {
        let mut itr = BlockIterator::new(block);
        itr.seek_to_first();

        itr
    }

    /// Creates a block iterator and seek to the first key that >= `key`.
    pub fn create_and_seek_to_key(block: Arc<Block>, key: KeySlice) -> Self {
        let mut itr = BlockIterator::new(block);
        itr.seek_to_key(key);

        itr
    }

    /// Returns the key of the current entry.
    pub fn key(&self) -> KeySlice {
        self.key.as_key_slice()
    }

    /// Returns the value of the current entry.
    pub fn value(&self) -> &[u8] {
        &self.block.data[self.value_range.0..self.value_range.1]
    }

    /// Returns true if the iterator is valid.
    /// Note: You may want to make use of `key`
    pub fn is_valid(&self) -> bool {
        if self.key.is_empty() {
            return false;
        }

        true
    }

    /// Seeks to the first key in the block.
    pub fn seek_to_first(&mut self) {
        self.move_to(0);
        self.idx = 0;
    }

    /// Move to the next key in the block.
    pub fn next(&mut self) {
        self.idx += 1;
        if self.idx >= self.block.offsets.len() {
            self.key.clear();
            self.value_range = (0, 0);
            return;
        }

        self.move_to(self.idx)
    }

    /// Seek to the first key that >= `key`.
    /// Note: You should assume the key-value pairs in the block are sorted when being added by
    /// callers.
    pub fn seek_to_key(&mut self, key: KeySlice) {
        let mut l = 0;
        let mut r = self.block.offsets.len() - 1;
        while l <= r {
            let mid = l + (r - l) / 2;
            let mid_key = self.find_key_at(mid);
            match mid_key.cmp(&key) {
                std::cmp::Ordering::Less => l = mid + 1,
                std::cmp::Ordering::Greater => {
                    if mid == 0 {
                        break;
                    }
                    r = mid - 1;
                }
                std::cmp::Ordering::Equal => {
                    l = mid;
                    self.block.print_as_number_or_string(key.into_inner());
                    break;
                }
            }
        }

        if l >= self.block.offsets.len() {
            self.key.clear();
            self.value_range = (0, 0);
            return;
        }
        self.idx = l;
        self.move_to(l);
    }

    pub fn find_key_at(&mut self, idx: usize) -> KeySlice {
        let offset = self.block.offsets[idx];
        let useful_data = &self.block.data[offset as usize..];
        let (key_len, rem) = useful_data.split_at(2);
        let actual_key_len = u16::from_le_bytes(key_len.try_into().unwrap());
        let (key, remain) = rem.split_at(actual_key_len as usize);
        KeySlice::from_slice(key)
    }

    pub fn move_to(&mut self, idx: usize) {
        if idx >= self.block.offsets.len() {
            return;
        }

        let nxt_entry = self.value_range.1;
        let entry_start = self.block.offsets[idx] as usize;

        let actual_data = &self.block.data[entry_start..];
        let (key_len, remain) = actual_data.split_at(2);
        let actual_key_len = u16::from_le_bytes(key_len.try_into().unwrap());
        let (key, remain) = remain.split_at(actual_key_len as usize);
        self.key = KeyVec::from_vec(key.to_vec());
        let (value_len, remain) = remain.split_at(2);
        let actual_value_len = u16::from_le_bytes(value_len.try_into().unwrap());
        let (value, _) = remain.split_at(actual_value_len as usize);
        self.value_range = (
            entry_start + 2 + (actual_key_len as usize) + 2,
            entry_start + 2 + actual_key_len as usize + 2 + actual_value_len as usize,
        );
    }
}
