use crate::rpc::client::NodeId;
use std::path::{Path, PathBuf};

// 节点数据目录：ruft-data/node-{hash(node_id)}。
pub fn node_storage_dir(root: &Path, node_id: NodeId) -> PathBuf {
    root.join(format!("node-{:08x}", node_id_hash(node_id)))
}

pub(crate) fn node_id_hash(node_id: NodeId) -> u32 {
    checksum(&node_id.to_le_bytes())
}

// 标准 CRC32(IEEE) 实现；避免额外依赖，同时保持 WAL 记录可校验。
pub fn checksum(payload: &[u8]) -> u32 {
    let mut crc = 0xffff_ffff_u32;
    for byte in payload {
        crc ^= u32::from(*byte);
        for _ in 0..8 {
            let mask = 0_u32.wrapping_sub(crc & 1);
            crc = (crc >> 1) ^ (0xedb8_8320 & mask);
        }
    }
    !crc
}
