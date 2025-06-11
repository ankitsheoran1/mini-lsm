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

use anyhow::Result;

use super::StorageIterator;

/// Merges two iterators of different types into one. If the two iterators have the same key, only
/// produce the key once and prefer the entry from A.
pub struct TwoMergeIterator<A: StorageIterator, B: StorageIterator> {
    a: A,
    b: B,
    pref: i32,
    // Add fields as need
}

impl<
    A: 'static + StorageIterator,
    B: 'static + for<'a> StorageIterator<KeyType<'a> = A::KeyType<'a>>,
> TwoMergeIterator<A, B>
{
    pub fn create(a: A, b: B) -> Result<Self> {
        let itr = Self::decide(&a, &b);
        Ok(TwoMergeIterator { a, b, pref: itr })
    }

    fn decide(a: &A, b: &B) -> i32 {
        if !a.is_valid() && !b.is_valid() {
            return -1;
        }
        if !a.is_valid() && b.is_valid() {
            return 1;
        }
        if a.is_valid() && !b.is_valid() {
            return 0;
        }

        if a.key() < b.key() {
            0
        } else if a.key() > b.key() {
            1
        } else {
            2
        }
    }
}

impl<
    A: 'static + StorageIterator,
    B: 'static + for<'a> StorageIterator<KeyType<'a> = A::KeyType<'a>>,
> StorageIterator for TwoMergeIterator<A, B>
{
    type KeyType<'a> = A::KeyType<'a>;

    fn key(&self) -> Self::KeyType<'_> {
        if self.pref == 1 {
            return self.b.key();
        }

        self.a.key()
    }

    fn value(&self) -> &[u8] {
        if self.pref == 1 {
            return self.b.value();
        }

        self.a.value()
    }

    fn is_valid(&self) -> bool {
        if self.pref < 0 {
            false
        } else if self.pref == 1 {
            self.b.is_valid()
        } else {
            self.a.is_valid()
        }
    }

    fn next(&mut self) -> Result<()> {
        if self.pref == 1 {
            if self.b.is_valid() {
                self.b.next()?;
            }
        } else if self.pref == 2 {
            if self.a.is_valid() {
                self.a.next()?;
            }
            if self.b.is_valid() {
                self.b.next()?;
            }
        } else {
            if self.a.is_valid() {
                self.a.next()?;
            }
        }
        self.pref = TwoMergeIterator::decide(&self.a, &self.b);
        Ok(())
    }

    fn num_active_iterators(&self) -> usize {
        self.a.num_active_iterators() + self.b.num_active_iterators()
    }
}
