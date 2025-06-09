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

use crate::iterators::two_merge_iterator::TwoMergeIterator;
use crate::table::SsTableIterator;
use crate::{
    iterators::{StorageIterator, merge_iterator::MergeIterator},
    mem_table::MemTableIterator,
};
use anyhow::Result;
use bytes::Bytes;
use std::io;
use std::io::ErrorKind;
use std::ops::Bound;

/// Represents the internal type for an LSM iterator. This type will be changed across the course for multiple times.
type LsmIteratorInner =
    TwoMergeIterator<MergeIterator<MemTableIterator>, MergeIterator<SsTableIterator>>;

pub struct LsmIterator {
    inner: LsmIteratorInner,
    end_bound: Bound<Bytes>,
    ended: bool,
}

impl LsmIterator {
    pub(crate) fn new(iter: LsmIteratorInner, end_bound: Bound<Bytes>) -> Result<Self> {
        let mut itr = Self {
            end_bound,
            ended: iter.is_valid(),
            inner: iter,
        };
        // while itr.inner.value().is_empty() && itr.inner.is_valid()  {
        //     itr.inner.next()?;
        // }
        itr.validation()?;
        Ok(itr)
    }

    fn validation(&mut self) -> Result<()> {
        while self.inner.is_valid() && self.inner.value().is_empty() {
            self.inner.next()?;
            if !self.inner.is_valid() {
                self.ended = false;
                return Ok(());
            }
            let current_key = self.inner.key().raw_ref();
            match &self.end_bound {
                Bound::Included(end_key) => {
                    if current_key > end_key.as_ref() {
                        self.ended = false;
                    }
                }
                Bound::Excluded(end_key) => {
                    if current_key >= end_key.as_ref() {
                        self.ended = false;
                    }
                }
                Bound::Unbounded => {}
            }
        }

        Ok(())
    }
}

impl StorageIterator for LsmIterator {
    type KeyType<'a> = &'a [u8];

    fn is_valid(&self) -> bool {
        self.inner.is_valid()
    }

    fn key(&self) -> &[u8] {
        self.inner.key().into_inner()
    }

    fn value(&self) -> &[u8] {
        self.inner.value()
    }

    fn next(&mut self) -> Result<()> {
        self.inner.next()?;
        if !self.inner.is_valid() {
            self.ended = false;
            return Ok(());
        }
        let current_key = self.inner.key().raw_ref();
        match &self.end_bound {
            Bound::Included(end_key) => {
                if current_key > end_key.as_ref() {
                    self.ended = false;
                }
            }
            Bound::Excluded(end_key) => {
                if current_key >= end_key.as_ref() {
                    self.ended = false;
                }
            }
            Bound::Unbounded => {}
        }
        self.validation()?;
        Ok(())
    }
}

/// A wrapper around existing iterator, will prevent users from calling `next` when the iterator is
/// invalid. If an iterator is already invalid, `next` does not do anything. If `next` returns an error,
/// `is_valid` should return false, and `next` should always return an error.
pub struct FusedIterator<I: StorageIterator> {
    iter: I,
    has_errored: bool,
}

impl<I: StorageIterator> FusedIterator<I> {
    pub fn new(iter: I) -> Self {
        Self {
            iter,
            has_errored: false,
        }
    }
}

impl<I: StorageIterator> StorageIterator for FusedIterator<I> {
    type KeyType<'a>
        = I::KeyType<'a>
    where
        Self: 'a;

    fn is_valid(&self) -> bool {
        if self.has_errored {
            return false;
        }
        self.iter.is_valid()
    }

    fn key(&self) -> Self::KeyType<'_> {
        self.iter.key()
    }

    fn value(&self) -> &[u8] {
        self.iter.value()
    }

    fn next(&mut self) -> Result<()> {
        if self.has_errored {
            return Err(anyhow::anyhow!("Already iterator error occurred"));
        }
        if !self.iter.is_valid() {
            //self.has_errored = true;
            return Ok(());
        }
        match self.iter.next() {
            Ok(()) => Ok(()),
            Err(e) => {
                self.has_errored = true;
                Err(e)
            }
        }
    }
}
