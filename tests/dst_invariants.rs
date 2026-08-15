//! Crash / bitrot contracts for the append-log snapshot.
//!
//! These assert the *correct* behavior. They fail on current main (B1/B3/B4).

use slipstream::snapshot::{load, SnapshotError, SnapshotWriter};
use slipstream::{AppendLogSnapshot, KvEntry, KvUpdate, SnapshotStore, VersionToken, WatchCursor};
use std::path::Path;
use tempfile::TempDir;

fn put(key: &str, value: &[u8], rev: u64) -> KvUpdate {
    KvUpdate::Put(KvEntry {
        key: key.to_string(),
        value: value.to_vec(),
        version: VersionToken::from_u64(rev),
    })
}

fn write_three(path: &Path) {
    let mut w = SnapshotWriter::open(path, u64::MAX).unwrap();
    w.write_update(&put("alpha", b"value-alpha-xxxxxxxx", 1))
        .unwrap();
    w.checkpoint(&WatchCursor::from_u64(1)).unwrap();
    w.write_update(&put("bravo", b"value-bravo-yyyyyyyy", 2))
        .unwrap();
    w.checkpoint(&WatchCursor::from_u64(2)).unwrap();
    w.write_update(&put("charlie", b"value-charlie-zzzzzz", 3))
        .unwrap();
    w.checkpoint(&WatchCursor::from_u64(3)).unwrap();
}

/// Second PUT's value_len field (absolute offset), or None.
fn second_put_value_len_off(data: &[u8]) -> Option<usize> {
    if data.len() < 6 || &data[0..4] != b"PGSS" {
        return None;
    }
    let mut pos = 6usize;
    let mut puts = 0u32;
    while pos + 7 <= data.len() {
        let kind = data[pos + 4];
        if kind != 0x01 {
            // cursor: crc4 + type1 + len1 + bytes
            if kind == 0x03 && pos + 6 <= data.len() {
                pos += 6 + data[pos + 5] as usize;
                continue;
            }
            if kind == 0x02 && pos + 7 <= data.len() {
                let klen = u16::from_le_bytes([data[pos + 5], data[pos + 6]]) as usize;
                if pos + 7 + klen + 1 > data.len() {
                    break;
                }
                let vlen = data[pos + 7 + klen] as usize;
                pos += 7 + klen + 1 + vlen;
                continue;
            }
            break;
        }
        let klen = u16::from_le_bytes([data[pos + 5], data[pos + 6]]) as usize;
        let vl = pos + 7 + klen;
        if vl + 4 > data.len() {
            break;
        }
        puts += 1;
        if puts == 2 {
            return Some(vl);
        }
        let value_len = u32::from_le_bytes(data[vl..vl + 4].try_into().ok()?) as usize;
        let ver_off = vl + 4 + value_len;
        if ver_off + 1 > data.len() {
            break;
        }
        let ver_len = data[ver_off] as usize;
        pos = ver_off + 1 + ver_len;
    }
    None
}

#[test]
fn mid_file_length_bitrot_is_not_a_silent_tail() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("s.snap");
    write_three(&path);
    let mut data = std::fs::read(&path).unwrap();
    let vl = second_put_value_len_off(&data).expect("second PUT");
    data[vl..vl + 4].copy_from_slice(&0x7fff_ffffu32.to_le_bytes());
    std::fs::write(&path, &data).unwrap();

    match load(&path) {
        Err(SnapshotError::Corrupted) | Err(SnapshotError::InvalidFormat(_)) => {}
        Ok(Some(snap)) => {
            assert!(
                snap.entries.contains_key("bravo") && snap.entries.contains_key("charlie"),
                "length bitrot must not drop later checkpointed keys; got {:?}",
                snap.entries.keys().collect::<Vec<_>>()
            );
        }
        other => panic!("unexpected {other:?}"),
    }
}

#[test]
fn torn_first_record_then_apply_is_visible_after_reopen() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("s.snap");
    let mut torn = b"PGSS\x02\x00".to_vec();
    torn.extend_from_slice(&[0x11, 0x22, 0x33, 0x44, 0x01, 0x00]);
    std::fs::write(&path, &torn).unwrap();

    let (_cur, mut store) = AppendLogSnapshot::open(&path, u64::MAX).unwrap();
    store
        .apply(
            &[put("charlie", b"should-survive", 3)],
            &WatchCursor::from_u64(3),
        )
        .unwrap();
    drop(store);

    let (_cur, store) = AppendLogSnapshot::open(&path, u64::MAX).unwrap();
    assert!(
        store.get("charlie").unwrap().is_some(),
        "apply after a torn first record must survive reopen"
    );
}

#[test]
fn trailing_zero_bytes_do_not_reject_the_prefix() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("s.snap");
    write_three(&path);
    let mut data = std::fs::read(&path).unwrap();
    data.extend_from_slice(&[0u8; 8]);
    std::fs::write(&path, &data).unwrap();

    let snap = load(&path)
        .unwrap_or_else(|e| panic!("trailing zeros must not fail load: {e}"))
        .expect("prefix still a snapshot");
    assert_eq!(snap.entries.len(), 3, "all three keys must survive");
    assert_eq!(snap.cursor.as_u64(), Some(3));
}

/// Five NULs at a record boundary look like `crc=0, type=0`. After the B4
/// fix that is `Truncated`, so a *mid-file* hole (prealloc / leftover
/// version bytes / torn copy) stops replay and `load` rewrites the suffix
/// away. Each PUT/CURSOR on either side is CRC-valid — the zeros are not
/// inside any frame.
#[test]
fn mid_file_nuls_must_not_drop_a_crc_valid_suffix() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("s.snap");

    let mut w = SnapshotWriter::open(&path, u64::MAX).unwrap();
    w.write_update(&put("a", b"x", 1)).unwrap();
    w.checkpoint(&WatchCursor::from_u64(1)).unwrap();
    w.write_update(&put("b", b"y", 2)).unwrap();
    w.checkpoint(&WatchCursor::from_u64(2)).unwrap();
    drop(w);

    let orig = std::fs::read(&path).unwrap();
    // First record: PUT a (key 1, value 1, ver 8) = 4+1+2+1+4+1+1+8 = 22,
    // then CURSOR 1 = 4+1+1+8 = 14. Insert after those two.
    let rec1 = 22usize;
    let cur1 = 14usize;
    let insert_at = 6 + rec1 + cur1;
    assert!(
        insert_at < orig.len(),
        "need a suffix after the first put+cursor"
    );
    let mut punched = orig[..insert_at].to_vec();
    punched.extend_from_slice(&[0u8; 5]);
    punched.extend_from_slice(&orig[insert_at..]);
    std::fs::write(&path, &punched).unwrap();

    match load(&path) {
        Err(SnapshotError::Corrupted) | Err(SnapshotError::InvalidFormat(_)) => {
            // Fail-stop on mid-file junk: file must stay intact so a suffix
            // of CRC-valid records is not burned by compact-on-load.
            let stayed = std::fs::read(&path).unwrap();
            assert_eq!(
                stayed, punched,
                "fail-stop must not rewrite the suffix away"
            );
        }
        Ok(Some(snap)) => {
            assert!(
                snap.entries.contains_key("b") && snap.cursor.as_u64() == Some(2),
                "mid-file NULs must not silent-drop a CRC-valid suffix; got keys={:?} cursor={:?}",
                snap.entries.keys().collect::<Vec<_>>(),
                snap.cursor.as_u64()
            );
        }
        other => panic!("unexpected {other:?}"),
    }
}
