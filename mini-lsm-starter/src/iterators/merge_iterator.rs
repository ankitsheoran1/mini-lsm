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

use std::cmp::{self};
use std::collections::BinaryHeap;
use std::collections::binary_heap::PeekMut;

use crate::key::KeySlice;
use anyhow::Result;
use nom::multi::length_count;

use super::StorageIterator;

struct HeapWrapper<I: StorageIterator>(pub usize, pub Box<I>);

impl<I: StorageIterator> PartialEq for HeapWrapper<I> {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == cmp::Ordering::Equal
    }
}

impl<I: StorageIterator> Eq for HeapWrapper<I> {}

impl<I: StorageIterator> PartialOrd for HeapWrapper<I> {
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<I: StorageIterator> Ord for HeapWrapper<I> {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        self.1
            .key()
            .cmp(&other.1.key())
            .then(self.0.cmp(&other.0))
            .reverse()
    }
}

/// Merge multiple iterators of the same type. If the same key occurs multiple times in some
/// iterators, prefer the one with smaller index.
pub struct MergeIterator<I: StorageIterator> {
    iters: BinaryHeap<HeapWrapper<I>>,
    current: Option<HeapWrapper<I>>,
}

impl<I: StorageIterator> MergeIterator<I> {
    pub fn create(iters: Vec<Box<I>>) -> Self {
        let mut heap = BinaryHeap::new();
        if iters.len() == 0 {
            return MergeIterator {
                iters: heap,
                current: None,
            };
        };

        for (idx, iter) in iters.into_iter().enumerate() {
            if iter.is_valid() {
                heap.push(HeapWrapper(idx, iter))
            }
        }

        let curr = heap.pop();
        if curr == None {
            return MergeIterator {
                iters: heap,
                current: None,
            };
        }

        MergeIterator {
            iters: heap,
            current: curr,
        }
    }
}

impl<I: 'static + for<'a> StorageIterator<KeyType<'a> = KeySlice<'a>>> StorageIterator
    for MergeIterator<I>
{
    type KeyType<'a> = KeySlice<'a>;

    fn key(&self) -> KeySlice {
        self.current.as_ref().unwrap().1.key()
    }

    fn value(&self) -> &[u8] {
        self.current.as_ref().unwrap().1.value()
    }

    fn is_valid(&self) -> bool {
        self.current
            .as_ref()
            .map(|curr| curr.1.is_valid())
            .unwrap_or(false)
    }

    fn next(&mut self) -> Result<()> {
        let mut curr = self.current.as_mut().unwrap();

        // sanitize top of heap
        loop {
            if let Some(mut node) = self.iters.peek_mut() {
                if node.1.key() == curr.1.key() {
                    if let Err(e) = node.1.next() {
                        PeekMut::pop(node);
                        return Err(e);
                    }
                    if !node.1.is_valid() {
                        PeekMut::pop(node);
                    }
                } else {
                    break;
                }
            } else {
                break;
            }
        }
        // next to current
        curr.1.next()?;
        if !curr.1.is_valid() {
            if let Some(replacement) = self.iters.pop() {
                self.current = Some(replacement);
            } else {
                self.current = None;
            }
            return Ok(());
        }
        // if we are on wrong curr

        if let Some(mut node) = self.iters.peek_mut() {
            if curr < &mut node {
                std::mem::swap(curr, &mut *node);
            }
        }
        Ok(())
    }
}
