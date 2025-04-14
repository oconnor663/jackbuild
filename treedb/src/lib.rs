use anyhow::{Context, bail, ensure};
use rusqlite::{OptionalExtension, TransactionBehavior::Immediate};
use std::collections::BTreeMap;
use std::fs::{self, File};
use std::io::prelude::*;
use std::path::{Path, PathBuf};

#[cfg(test)]
mod test;

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum NodeType {
    Blob { executable: bool },
    Tree,
}

#[derive(Copy, Clone, Debug)]
pub struct Child<'a> {
    name: &'a str,
    id: &'a blake3::Hash,
    node_type: NodeType,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Tree {
    children: BTreeMap<String, (blake3::Hash, NodeType)>,
}

impl Tree {
    pub fn new() -> Self {
        Self {
            children: BTreeMap::new(),
        }
    }

    pub fn len(&self) -> usize {
        self.children.len()
    }

    pub fn get_child(&mut self, name: &str) -> Option<Child<'_>> {
        debug_assert!(!name.is_empty());
        debug_assert!(!name.contains("/"));
        debug_assert!(!name.contains("\0"));
        match self.children.get_key_value(name) {
            Some((name, &(ref id, node_type))) => Some(Child {
                name,
                id,
                node_type,
            }),
            None => None,
        }
    }

    pub fn add_child(&mut self, name: impl Into<String>, id: &blake3::Hash, node_type: NodeType) {
        let name = name.into();
        assert!(!name.is_empty());
        assert!(!name.contains("/"));
        assert!(!name.contains("\0"));
        self.children.insert(name, (*id, node_type));
    }

    pub fn iter(&self) -> impl Iterator<Item = Child<'_>> {
        self.children
            .iter()
            .map(|(name, &(ref id, node_type))| Child {
                name,
                id,
                node_type,
            })
    }

    pub fn id(&self) -> blake3::Hash {
        let mut hasher = blake3::Hasher::new_derive_key("tree_id");
        // Note that self.children is sorted.
        for child in self.iter() {
            debug_assert!(!child.name.is_empty());
            debug_assert!(!child.name.contains("/"));
            debug_assert!(!child.name.contains("\0"));
            let node_type_bytes = match child.node_type {
                NodeType::Blob { executable: false } => [0, 0],
                NodeType::Blob { executable: true } => [0, 1],
                NodeType::Tree => [1, 0],
            };
            hasher.update(child.id.as_bytes());
            hasher.update(&node_type_bytes);
            hasher.update(child.name.as_bytes());
            hasher.update(b"\0");
        }
        hasher.finalize()
    }
}

// Semi-arbitrary cutoff based on https://www.sqlite.org/intern-v-extern-blob.html.
const LARGE_BLOB_THRESHOLD: usize = 1 << 16; // 64 KiB

#[derive(Debug)]
pub struct TreeDb {
    conn: rusqlite::Connection,
    blobs_dir: PathBuf,
}

impl TreeDb {
    /// open or create
    pub fn open(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        // Create the blobs/ directory if it doesn't already exist. This also asserts that `path`
        // is in fact a directory.
        let blobs_dir = path.as_ref().join("blobs");
        fs::create_dir_all(&blobs_dir).with_context(|| {
            format!(
                "failed to open/create directory at {}",
                blobs_dir.to_string_lossy(),
            )
        })?;
        let db_path = path.as_ref().join("db");
        let conn = rusqlite::Connection::open(&db_path).with_context(|| {
            format!(
                "failed to open/create SQLite database at {}",
                db_path.to_string_lossy(),
            )
        })?;
        conn.pragma_update(None, "journal_mode", "WAL")?;
        conn.execute(
            "CREATE TABLE IF NOT EXISTS blobs (
                 blob_id BLOB NOT NULL,
                 data BLOB,  -- NULL means data is in the large blobs dir
                 PRIMARY KEY (blob_id))",
            (),
        )?;
        conn.execute(
            "CREATE TABLE IF NOT EXISTS trees (
                tree_id BLOB NOT NULL,
                child_name TEXT NOT NULL,
                child_id BLOB NOT NULL,
                node_type TINYINT NOT NULL,
                executable BOOLEAN NOT NULL,
                PRIMARY KEY (tree_id, child_name))",
            (),
        )?;
        Ok(Self { blobs_dir, conn })
    }

    pub fn contains_blob(&self, blob_id: blake3::Hash) -> anyhow::Result<bool> {
        let exists: u64 = self.conn.query_row(
            "SELECT COUNT(*) FROM blobs WHERE blob_id = ?",
            (blob_id.as_bytes(),),
            |row| row.get(0),
        )?;
        debug_assert!(exists <= 1);
        Ok(exists == 1)
    }

    // Not a method for borrowck reasons.
    fn blob_path(&self, blob_id: blake3::Hash) -> PathBuf {
        self.blobs_dir.join(blob_id.to_hex().as_str())
    }

    pub fn insert_blob(&mut self, blob: &[u8]) -> anyhow::Result<blake3::Hash> {
        // Do this first to avoid borrowck errors.
        let blob_id = blake3::hash(blob);
        let blob_path = self.blob_path(blob_id);

        // Deferred transactions are vulnerable to BUSY errors if there are concurrent writers.
        // See: https://fractaledmind.github.io/2024/04/15/sqlite-on-rails-the-how-and-why-of-optimal-performance/
        let tx = self.conn.transaction_with_behavior(Immediate)?;

        // Short-circuit if this blob already exists.
        let exists: u64 = tx.query_row(
            "SELECT COUNT(*) FROM blobs WHERE blob_id = ?",
            (blob_id.as_bytes(),),
            |row| row.get(0),
        )?;
        assert!(exists <= 1);
        let exists = exists == 1;
        if exists {
            return Ok(blob_id);
        }

        // Small blobs go in the blobs table.
        if blob.len() < LARGE_BLOB_THRESHOLD {
            tx.execute(
                "INSERT INTO blobs (blob_id, data) VALUES (?, ?)",
                (blob_id.as_bytes(), blob),
            )?;
            tx.commit()?;
            return Ok(blob_id);
        }

        // Large blobs go in the blobs dir.
        tx.execute(
            // NULL data means the data is in the blobs dir. Note that this write won't be
            // observable to concurrent readers until we commit.
            "INSERT INTO blobs (blob_id, data) VALUES (?, NULL)",
            (blob_id.as_bytes(),),
        )?;
        // The IMMEDIATE mode transaction above should exclude any other writers, so we don't need
        // to create a randomly-named tempfile and atomically rename it.
        let mut file = File::create(&blob_path)
            .with_context(|| format!("creating file at {}", blob_path.to_string_lossy()))?;
        file.write_all(blob)?;
        // Commit!
        tx.commit()?;
        // Finally, make the file read-only. If anything fails before this, a later File::create
        // operation can silently overwrite this file and recover.
        let mut permissions = file.metadata()?.permissions();
        permissions.set_readonly(true);
        file.set_permissions(permissions)?;
        Ok(blob_id)
    }

    pub fn insert_file(&mut self, source_path: impl AsRef<Path>) -> anyhow::Result<blake3::Hash> {
        let source_file = File::open(&source_path).with_context(|| {
            format!(
                "failed to open file at {}",
                source_path.as_ref().to_string_lossy(),
            )
        })?;
        let metadata_before = source_file.metadata()?;
        ensure!(metadata_before.is_file());

        // Small blobs go in the blobs table.
        if metadata_before.len() < LARGE_BLOB_THRESHOLD as u64 {
            // Small blobs go in the blobs table.
            let mut blob = Vec::with_capacity(metadata_before.len() as usize);
            // Using .take() guarantees that we won't read more than metadata.len() bytes, even if
            // there's an FS race going on and some other process is growing the file.
            source_file
                .take(metadata_before.len())
                .read_to_end(&mut blob)
                .with_context(|| {
                    format!(
                        "failed to read file at {}",
                        source_path.as_ref().to_string_lossy()
                    )
                })?;
            return self.insert_blob(&blob);
        }

        // Large blobs go in the blobs dir. Hash the file first to avoid an expensive copy if it's
        // a duplicate. We'll trust the mtime (and on Unix, the inode) of the source file and bail
        // if it changes across the whole hash+copy operation.
        let blob_id = blake3::Hasher::new()
            .update_mmap_rayon(&source_path)
            .with_context(|| {
                format!(
                    "failed to hash file at {}",
                    source_path.as_ref().to_string_lossy()
                )
            })?
            .finalize();
        let blob_path = self.blob_path(blob_id);

        // Deferred transactions are vulnerable to BUSY errors if there are concurrent writers.
        // See: https://fractaledmind.github.io/2024/04/15/sqlite-on-rails-the-how-and-why-of-optimal-performance/
        let tx = self.conn.transaction_with_behavior(Immediate)?;

        // Short-circuit if this blob already exists.
        let exists: u64 = tx.query_row(
            "SELECT COUNT(*) FROM blobs WHERE blob_id = ?",
            (blob_id.as_bytes(),),
            |row| row.get(0),
        )?;
        assert!(exists <= 1);
        let exists = exists == 1;
        if exists {
            return Ok(blob_id);
        }

        // NULL data means the data is in the blobs dir. Note that this write won't be observable
        // to concurrent readers until we commit.
        tx.execute(
            "INSERT INTO blobs (blob_id, data) VALUES (?, NULL)",
            (blob_id.as_bytes(),),
        )?;

        // Copy the file into the blobs dir. Use a cheap reflink if possible on filesystems that
        // support it, e.g. BTRFS. The IMMEDIATE mode transaction above should exclude any other
        // writers, so we don't need to create a randomly-named tempfile and atomically rename it.
        reflink_copy::reflink_or_copy(&source_path, &blob_path).with_context(|| {
            format!(
                "failed to copy {} to {}",
                source_path.as_ref().to_string_lossy(),
                blob_path.to_string_lossy(),
            )
        })?;

        // Double check the mtime and (on Unix) inode of the original file, to guard against FS
        // races. You can spoof mtime if you want to, so this isn't bulletproof, but at that point
        // you deserve what you get. (You can also just corrupt the blobs dir yourself if you feel
        // like it.) On Windows, the fact that we're holding `file` open prevents renaming
        // shenanigans.
        let metadata_after = source_file.metadata()?;
        ensure!(
            metadata_before.modified()? == metadata_after.modified()?,
            "{} was modified while it was being read",
            source_path.as_ref().to_string_lossy(),
        );
        #[cfg(not(windows))]
        {
            use std::os::unix::fs::MetadataExt;
            ensure!(
                metadata_before.ino() == metadata_after.ino(),
                "{} was modified while it was being read",
                source_path.as_ref().to_string_lossy(),
            );
        }

        // Commit!
        tx.commit()?;

        // Finally, make the copied file read-only. If anything fails before this, a later
        // File::create operation can silently overwrite this file and recover.
        let copied_file = File::open(&blob_path)?;
        let mut permissions = copied_file.metadata()?.permissions();
        permissions.set_readonly(true);
        source_file.set_permissions(permissions)?;
        Ok(blob_id)
    }

    pub fn get_blob(&mut self, blob_id: &blake3::Hash) -> anyhow::Result<Option<Vec<u8>>> {
        // First try the blobs directory. Large blobs live here.
        let blob_path = self.blobs_dir.join(blob_id.to_hex().as_str());
        if fs::exists(&blob_path)? {
            let bytes = fs::read(&blob_path)?;
            debug_assert!(bytes.len() >= LARGE_BLOB_THRESHOLD);
            return Ok(Some(fs::read(&blob_path)?));
        }
        // Second try the blobs table. Small blobs live here.
        let data: Option<Vec<u8>> = self
            .conn
            .query_row(
                "SELECT data FROM blobs WHERE blob_id = ?",
                (blob_id.as_bytes(),),
                |row| row.get(0),
            )
            .optional()?;
        if let Some(data) = &data {
            debug_assert_eq!(blob_id, &blake3::hash(data));
            debug_assert!(data.len() < LARGE_BLOB_THRESHOLD);
        }
        Ok(data)
    }

    pub fn get_tree(&mut self, tree_id: &blake3::Hash) -> anyhow::Result<Option<Tree>> {
        let mut tree = Tree::new();
        let mut query = self.conn.prepare(
            "SELECT child_name, child_id, node_type, executable FROM trees WHERE tree_id = ?",
        )?;
        let rows = query.query_map((tree_id.as_bytes(),), |row| {
            let child_name: String = row.get(0)?;
            let child_id: [u8; 32] = row.get(1)?;
            let node_type: u8 = row.get(2)?;
            let executable: bool = row.get(3)?;
            Ok((child_name, child_id, node_type, executable))
        })?;
        for row in rows {
            let (child_name, child_id, node_type, executable) = row?;
            let node_type = match (node_type, executable) {
                (0, _) => NodeType::Blob { executable },
                (1, false) => NodeType::Tree,
                _ => bail!("unknown node type: {} {}", node_type, executable),
            };
            tree.add_child(child_name, &child_id.into(), node_type);
        }
        if tree.len() > 0 {
            Ok(Some(tree))
        } else {
            Ok(None)
        }
    }

    pub fn insert_tree(&mut self, tree: &Tree) -> anyhow::Result<blake3::Hash> {
        assert_ne!(tree.len(), 0, "can't insert empty trees");
        let tree_id = tree.id();
        let tx = self.conn.transaction()?;
        for child in tree.iter() {
            match child.node_type {
                NodeType::Blob { .. } => {
                    let blob_count: u64 = tx.query_row(
                        "SELECT COUNT(*) FROM blobs WHERE blob_id = ?",
                        (child.id.as_bytes(),),
                        |row| row.get(0),
                    )?;
                    assert!(blob_count <= 1, "duplicate blobs?");
                    ensure!(blob_count == 1, "blob {} does not exist", child.id);
                }
                NodeType::Tree { .. } => {
                    let tree_count: u64 = tx.query_row(
                        "SELECT COUNT(*) FROM trees WHERE tree_id = ?",
                        (child.id.as_bytes(),),
                        |row| row.get(0),
                    )?;
                    ensure!(tree_count > 0, "tree {} does not exist", child.id);
                }
            }
            let (node_type, executable) = match child.node_type {
                NodeType::Blob { executable } => (0u8, executable),
                NodeType::Tree => (1u8, false),
            };
            tx.execute(
                "INSERT INTO trees (tree_id, child_name, child_id, node_type, executable) VALUES (?, ?, ?, ?, ?)",
                (tree_id.as_bytes(), child.name, child.id.as_bytes(), node_type, executable),
            )?;
        }
        tx.commit()?;
        Ok(tree_id)
    }
}
