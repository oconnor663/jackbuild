use anyhow::{bail, ensure};
use rusqlite::OptionalExtension;
use std::collections::BTreeMap;
use std::path::Path;

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

#[derive(Debug)]
pub struct TreeDb {
    conn: rusqlite::Connection,
}

impl TreeDb {
    pub fn open(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        let conn = rusqlite::Connection::open(path.as_ref())?;
        conn.pragma_update(None, "journal_mode", "WAL")?;
        conn.execute(
            "CREATE TABLE IF NOT EXISTS blobs (
                 blob_id BLOB,
                 data BLOB,
                 PRIMARY KEY (blob_id))",
            (),
        )?;
        conn.execute(
            "CREATE TABLE IF NOT EXISTS trees (
                tree_id BLOB,
                child_name TEXT,
                child_id BLOB,
                node_type TINYINT,
                executable BOOLEAN,
                PRIMARY KEY (tree_id, child_name))",
            (),
        )?;
        Ok(Self { conn })
    }

    pub fn get_blob(&mut self, blob_id: &blake3::Hash) -> anyhow::Result<Option<Vec<u8>>> {
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
        }
        Ok(data)
    }

    pub fn insert_blob(&mut self, blob: &[u8]) -> anyhow::Result<blake3::Hash> {
        let blob_id = blake3::hash(blob);
        self.conn.execute(
            "INSERT INTO blobs (blob_id, data) VALUES (?, ?)",
            (blob_id.as_bytes(), blob),
        )?;
        Ok(blob_id)
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
