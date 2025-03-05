use super::*;

#[test]
fn test_basic() -> anyhow::Result<()> {
    // Test data:
    // - a: b"foo"
    // - b: b"foo"
    // - c/d: b"bar"

    let dir = tempfile::tempdir()?;
    let db_path = dir.path().join("db");
    dbg!(&db_path);
    let mut conn = TreeDb::open(db_path)?;

    let foo_id = conn.insert_blob(b"foo")?;
    let bar_id = conn.insert_blob(b"bar")?;
    let mut c_tree = Tree::new();
    c_tree.add_child("d", &bar_id, NodeType::Blob { executable: false });
    let c_id = conn.insert_tree(&c_tree)?;
    let mut root = Tree::new();
    root.add_child("a", &foo_id, NodeType::Blob { executable: false });
    root.add_child("b", &foo_id, NodeType::Blob { executable: false });
    root.add_child("c", &c_id, NodeType::Tree);
    let root_id = conn.insert_tree(&root)?;

    assert_eq!(conn.get_tree(&root_id)?.unwrap(), root);
    assert_eq!(conn.get_tree(&c_id)?.unwrap(), c_tree);
    assert_eq!(conn.get_blob(&foo_id)?.unwrap(), b"foo");
    assert_eq!(conn.get_blob(&bar_id)?.unwrap(), b"bar");

    Ok(())
}
