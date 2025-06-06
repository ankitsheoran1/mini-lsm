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

mod builder;
mod iterator;

pub use builder::BlockBuilder;
use bytes::{BufMut, Bytes, BytesMut};
pub use iterator::BlockIterator;

/// A block is the smallest unit of read and caching in LSM tree. It is a collection of sorted key-value pairs.
pub struct Block {
    pub(crate) data: Vec<u8>,
    pub(crate) offsets: Vec<u16>,
}

impl Block {
    /// Encode the internal data to the data layout illustrated in the course
    /// Note: You may want to recheck if any of the expected field is missing from your output
    pub fn encode(&self) -> Bytes {
        let mut buffer = BytesMut::new();
        for v in &self.data {
            buffer.put_u8(*v);
        }

        for offset in &self.offsets {
            buffer.put_u16_le(*offset);
        }
        buffer.put_u16_le(self.offsets.len() as u16);

        buffer.into()
    }

    /// Decode from the data layout, transform the input `data` to a single `Block`
    pub fn decode(data: &[u8]) -> Self {
        let len = data.len();
        let count = u16::from_le_bytes([data[len - 2], data[len - 1]]);
        let offset_idx = len - 2 - (2 * count as usize);
        let offset = &data[offset_idx..len - 2];
        let mut i = 0;
        let mut offset_data = Vec::new();
        while i < offset.len() {
            let bytes_data = u16::from_le_bytes([offset[i], offset[i + 1]]);
            offset_data.push(bytes_data);
            i += 2;
        }
        let rdt = &data[0..(len - 2 - (count * 2) as usize)];
        Self {
            data: rdt.to_vec(),
            offsets: offset_data,
        }
    }
}
