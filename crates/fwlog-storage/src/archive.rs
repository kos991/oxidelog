use std::{
    fs,
    path::{Path, PathBuf},
};

use anyhow::{Context, Result};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArchiveFile {
    pub path: PathBuf,
    pub bytes: u64,
}

pub fn list_archive_files(dir: impl AsRef<Path>) -> Result<Vec<ArchiveFile>> {
    let mut files = Vec::new();
    collect_archive_files(dir.as_ref(), &mut files)?;
    files.sort_by(|left, right| left.path.cmp(&right.path));
    Ok(files)
}

fn collect_archive_files(dir: &Path, files: &mut Vec<ArchiveFile>) -> Result<()> {
    for entry in
        fs::read_dir(dir).with_context(|| format!("read archive directory {}", dir.display()))?
    {
        let entry = entry.context("read archive directory entry")?;
        let path = entry.path();
        let metadata = entry
            .metadata()
            .with_context(|| format!("read archive metadata {}", path.display()))?;
        if metadata.is_dir() {
            collect_archive_files(&path, files)?;
        } else if metadata.is_file()
            && path
                .extension()
                .is_some_and(|extension| extension == "parquet")
        {
            files.push(ArchiveFile {
                path,
                bytes: metadata.len(),
            });
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn list_archive_files_returns_parquet_files_sorted_by_path() {
        let dir = tempfile::tempdir().unwrap();
        let root_parquet = dir.path().join("b.parquet");
        let nested_dir = dir.path().join("nested");
        let nested_parquet = nested_dir.join("a.parquet");
        let non_parquet = nested_dir.join("ignore.txt");
        std::fs::create_dir_all(&nested_dir).unwrap();
        std::fs::write(&root_parquet, b"root").unwrap();
        std::fs::write(&nested_parquet, b"nested").unwrap();
        std::fs::write(non_parquet, b"ignore").unwrap();

        let files = list_archive_files(dir.path()).unwrap();

        let paths: Vec<_> = files.iter().map(|file| file.path.clone()).collect();
        assert_eq!(paths, vec![root_parquet, nested_parquet]);
        assert_eq!(files[0].bytes, 4);
        assert_eq!(files[1].bytes, 6);
    }
}
