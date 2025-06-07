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

    // Note: This is just for Debugging purpose someone wants to see actual data storage, its not used any where
    pub fn print_values(&self) {
        println!("Number of key-value pairs: {}", self.offsets.len());
        println!("Total data length: {}", self.data.len());
        println!("Offsets: {:?}", self.offsets);
        println!();

        for (i, &offset) in self.offsets.iter().enumerate() {
            let start = offset as usize;

            println!("Pair {} (offset: {}):", i, offset);

            if start >= self.data.len() {
                println!(
                    "  Error: Offset {} is beyond data length {}",
                    offset,
                    self.data.len()
                );
                continue;
            }

            // Check if we have at least 4 bytes for the two length fields
            if start + 4 > self.data.len() {
                println!(
                    "  Error: Not enough data for length headers at offset {}",
                    offset
                );
                println!("  Available bytes from offset: {}", self.data.len() - start);
                continue;
            }

            // Read key_len (2 bytes, little-endian)
            let key_len = u16::from_le_bytes([self.data[start], self.data[start + 1]]) as usize;

            println!("  Key length: {}", key_len);

            // Check if we have enough data for the key
            let key_start = start + 2;
            let key_end = key_start + key_len;

            if key_end > self.data.len() {
                println!(
                    "  Error: Key extends beyond data (key_end: {}, data_len: {})",
                    key_end,
                    self.data.len()
                );
                continue;
            }

            // Check if we have space for value_len after the key
            if key_end + 2 > self.data.len() {
                println!("  Error: Not enough space for value length after key");
                continue;
            }

            // Read value_len (2 bytes, little-endian, after the key)
            let value_len =
                u16::from_le_bytes([self.data[key_end], self.data[key_end + 1]]) as usize;

            println!("  Value length: {}", value_len);

            let value_start = key_end + 2;
            let value_end = value_start + value_len;

            if value_end > self.data.len() {
                println!(
                    "  Error: Value extends beyond data (value_end: {}, data_len: {})",
                    value_end,
                    self.data.len()
                );
                continue;
            }

            // Extract key and value
            let key_bytes = &self.data[key_start..key_end];
            let value_bytes = &self.data[value_start..value_end];

            print!("  Key: ");
            self.print_as_number_or_string(key_bytes);

            print!("  Value: ");
            self.print_as_number_or_string(value_bytes);

            println!();
        }
    }

    fn print_as_number_or_string(&self, bytes: &[u8]) {
        match bytes.len() {
            0 => {
                println!("(empty)");
            }
            1 => {
                println!("{}", bytes[0]);
            }
            2 => {
                let val = u16::from_le_bytes([bytes[0], bytes[1]]);
                println!("{}", val);
            }
            4 => {
                let val = u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
                println!("{}", val);
            }
            8 => {
                let val = u64::from_le_bytes([
                    bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
                ]);
                println!("{}", val);
            }
            _ => {
                // Try to parse as UTF-8 string first
                if let Ok(s) = std::str::from_utf8(bytes) {
                    // Check if it's printable
                    if s.chars().all(|c| !c.is_control() || c.is_whitespace()) {
                        println!("\"{}\"", s);
                        return;
                    }
                }

                // If not a valid string, show as hex and try as number
                print!("[");
                for (i, &byte) in bytes.iter().enumerate() {
                    if i > 0 {
                        print!(", ");
                    }
                    print!("{}", byte);
                }
                println!("]");
            }
        }
    }

    // Debug function to show raw data around an offset
    pub fn debug_offset(&self, offset_index: usize) {
        if offset_index >= self.offsets.len() {
            println!("Invalid offset index: {}", offset_index);
            return;
        }

        let offset = self.offsets[offset_index] as usize;
        let start = offset.saturating_sub(10);
        let end = (offset + 20).min(self.data.len());

        println!(
            "Debug data around offset {} (index {}):",
            offset, offset_index
        );
        print!("Bytes: ");
        for i in start..end {
            if i == offset {
                print!("[{}] ", self.data[i]);
            } else {
                print!("{} ", self.data[i]);
            }
        }
        println!();
    }

    // Alternative standalone function if you prefer:

    // Usage example:
    // let decoded = YourStructName::decode(&your_byte_data);
    // decoded.print_values();

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
