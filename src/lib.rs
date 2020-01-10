use std::io::prelude::*;
use serde::Serialize;
use std::collections::HashMap;
use byteorder::{ReadBytesExt, WriteBytesExt, BigEndian, LittleEndian};
use std::time::Duration;

#[derive(Debug, Clone)]
/// Forms part of a complete message.
pub struct Chunk {
    // Total chunk size (including this header), i.e. 24 bytes + size of data field.
    length: u32,

    // For the first chunk of a message, the low bit of the second u32 is set, for all subsequent
    // ones it is reset. In the first chunk of a message, the number "chunk" is the total number of
    // chunks in the message, in all subsequent chunks, the number "chunk" is the current number of
    // this chunk.
    chunk_x: u32,

    // Unique identifier, responsibility of sender to generate this (zero is reserved for not set).
    message_id: u64,

    // Total size of the message, of which this chunk is a part.
    message_length: u64,

    // Data payload.
    data: Vec<u8>,
}

impl Chunk {
    pub fn data(&self) -> &[u8] {
        &self.data
    }

    pub fn from_data(data: Vec<u8>) -> Self {
        Self {
            length: (24 + data.len()) as u32,
            chunk_x: Self::encode_chunk_x(0, 1),
            message_id: 11, // FIXME: caller must generate ID
            message_length: data.len() as u64, // TODO: check if this includes header or not
            data
        }
    }

    pub fn from_bytes(data: &[u8]) -> Self {
        let mut buf: [u8; 4] = Default::default();
        buf.copy_from_slice(&data[0..4]);
        let length = u32::from_le_bytes(buf);

        buf.copy_from_slice(&data[4..8]);
        let chunk_x = u32::from_le_bytes(buf);

        let mut buf: [u8; 8] = Default::default();
        buf.copy_from_slice(&data[8..16]);
        let message_id = u64::from_le_bytes(buf);

        buf.copy_from_slice(&data[16..24]);
        let message_length = u64::from_le_bytes(buf);

        Self {
            length,
            chunk_x,
            message_id,
            message_length,
            data: data[24..].to_vec()
        }
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        let mut writer = Vec::new();
        writer.extend_from_slice(&self.length.to_le_bytes());
        writer.extend_from_slice(&self.chunk_x.to_le_bytes());
        writer.extend_from_slice(&self.message_id.to_le_bytes());
        writer.extend_from_slice(&self.message_length.to_le_bytes());
        writer.extend_from_slice(&self.data);
        writer
    }

    fn encode_chunk_x(chunk_index: u32, num_chunks: u32) -> u32 {
        if num_chunks == 1 {
            3 // last byte: 0000 0011
        } else if chunk_index == 0 {
            (num_chunks << 1) + 1
        } else {
            chunk_index << 1
        }
    }

    fn get_chunk_number(&self) -> u32 {
        if self.is_first_chunk() {
            1
        } else {
            self.get_chunk()
        }
    }

    fn get_chunk(&self) -> u32 {
        assert!(self.is_first_chunk());
        self.chunk_x >> 1
    }

    fn is_first_chunk(&self) -> bool {
         (self.chunk_x & 0x01) == 1
    }
}

#[derive(Copy, Clone, Debug)]
pub enum RequestType {
    Delete = 0,
    Get = 1,
    Post = 2,
    Put = 3,
    // Head = 4, (not used)
    Patch = 5,
    // Options = 6, (not used)
}

#[derive(Copy, Clone, Debug)]
pub enum MessageType {
    Request = 1,

    // Last response for this message ID.
    FinalResponse = 2,

    // Response for this message ID, indicating that more responses will follow.
    Response = 3,

    Authentication = 1000,
}

pub struct RequestMessage {
    pub version: u32,
    pub message_type: MessageType,
    pub database: String,
    pub request_type: RequestType,
    pub request_path: String,
    pub parameters: HashMap<String, String>,
    pub meta: HashMap<String, String>,
}

impl RequestMessage {
    pub fn to_bytes(&self) -> velocypack::Result<Vec<u8>> {
        let mut arr: Vec<Box<dyn erased_serde::Serialize>> = Vec::with_capacity(7);

        arr.push(Box::new(&self.version));
        arr.push(Box::new(self.message_type as i32));
        arr.push(Box::new(&self.database));
        arr.push(Box::new(self.request_type as i32));
        arr.push(Box::new(&self.request_path));
        arr.push(Box::new(&self.parameters));
        arr.push(Box::new(&self.meta));

        velocypack::to_bytes(&arr)
    }
}

impl Default for RequestMessage {
    fn default() -> Self {
        Self {
            version: 1,
            message_type: MessageType::Request,
            database: "_system".to_owned(),
            request_type: RequestType::Get,
            request_path: "/_admin/echo".to_owned(),
            parameters: HashMap::new(),
            meta: HashMap::new(),
        }
    }
}
