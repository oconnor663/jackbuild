use super::*;
use std::fs;
use tempfile::NamedTempFile;

fn big_blob_tempfile() -> anyhow::Result<NamedTempFile> {
    let mut blob_bytes = vec![0u8; LARGE_BLOB_THRESHOLD];
    rand::fill(&mut blob_bytes[..]);
    let file = NamedTempFile::new()?;
    fs::write(file.path(), &blob_bytes)?;
    Ok(file)
}

#[test]
fn test_basic() -> anyhow::Result<()> {
    // Test data:
    // - a: b"foo"
    // - b: b"foo"
    // - c/d: <LARGE_BLOB_THRESHOLD random bytes>

    let dir = tempfile::tempdir()?;
    let db_path = dir.path().join("db");
    dbg!(&db_path);
    let mut conn = TreeDb::open(db_path)?;

    let foo_id = conn.insert_blob(b"foo")?;
    let big_file = big_blob_tempfile()?;
    let big_bytes = fs::read(big_file.path())?;
    let big_id = conn.insert_file(&big_file.path())?;
    let mut c_tree = Tree::new();
    c_tree.add_child("d", &big_id, NodeType::Blob { executable: false });
    let c_id = conn.insert_tree(&c_tree)?;
    let mut root = Tree::new();
    root.add_child("a", &foo_id, NodeType::Blob { executable: false });
    root.add_child("b", &foo_id, NodeType::Blob { executable: false });
    root.add_child("c", &c_id, NodeType::Tree);
    let root_id = conn.insert_tree(&root)?;

    assert_eq!(conn.get_tree(&root_id)?.unwrap(), root);
    assert_eq!(conn.get_tree(&c_id)?.unwrap(), c_tree);
    assert_eq!(conn.get_blob(&foo_id)?, b"foo");
    assert_eq!(conn.get_blob(&big_id)?, big_bytes);

    // Test get_file.
    let foo2 = NamedTempFile::new()?;
    conn.get_file(&foo_id, foo2.path())?;
    assert_eq!(fs::read(foo2.path())?, b"foo");
    let big2 = NamedTempFile::new()?;
    conn.get_file(&big_id, big2.path())?;
    assert_eq!(fs::read(big2.path())?, big_bytes);

    Ok(())
}

#[test]
fn test_children_must_exist() -> anyhow::Result<()> {
    // Test data:
    // - a: b"foo"
    // - b/c: b"bar"

    let dir = tempfile::tempdir()?;
    let db1_path = dir.path().join("db1");
    dbg!(&db1_path);
    let mut conn1 = TreeDb::open(db1_path)?;

    let foo_id = conn1.insert_blob(b"foo")?;
    let bar_id = conn1.insert_blob(b"bar")?;
    let mut b_tree = Tree::new();
    b_tree.add_child("d", &bar_id, NodeType::Blob { executable: false });
    let b_id = conn1.insert_tree(&b_tree)?;
    let mut root = Tree::new();
    root.add_child("a", &foo_id, NodeType::Blob { executable: false });
    root.add_child("b", &b_id, NodeType::Tree);
    let root_id = conn1.insert_tree(&root)?;
    drop(conn1);

    // Inserting `root` into a DB that doesn't have `foo` should fail.
    let db2_path = dir.path().join("db2");
    dbg!(&db2_path);
    let mut conn2 = TreeDb::open(db2_path)?;
    let id = conn2.insert_blob(b"bar")?;
    assert_eq!(id, bar_id);
    let id = conn2.insert_tree(&b_tree)?;
    assert_eq!(id, b_id);
    conn2.insert_tree(&root).unwrap_err();
    let id = conn2.insert_blob(b"foo")?;
    assert_eq!(id, foo_id);
    let id = conn2.insert_tree(&root)?;
    assert_eq!(id, root_id);
    drop(conn2);

    // Same if the DB doesn't have `b`.
    let db3_path = dir.path().join("db3");
    dbg!(&db3_path);
    let mut conn3 = TreeDb::open(db3_path)?;
    let id = conn3.insert_blob(b"foo")?;
    assert_eq!(id, foo_id);
    let id = conn3.insert_blob(b"bar")?;
    assert_eq!(id, bar_id);
    conn3.insert_tree(&root).unwrap_err();
    let id = conn3.insert_tree(&b_tree)?;
    assert_eq!(id, b_id);
    let id = conn3.insert_tree(&root)?;
    assert_eq!(id, root_id);
    drop(conn3);

    Ok(())
}
