use std::{
    fs,
    path::{Path, PathBuf},
    time::{Duration, SystemTime},
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

pub fn prune_archive_files(dir: impl AsRef<Path>, retention: Duration) -> Result<usize> {
    let dir = dir.as_ref();
    if !dir.exists() {
        return Ok(0);
    }

    let cutoff = SystemTime::now()
        .checked_sub(retention)
        .unwrap_or(SystemTime::UNIX_EPOCH);
    let mut removed = 0;
    for file in list_archive_files(dir)? {
        let modified = fs::metadata(&file.path)
            .with_context(|| format!("read archive metadata {}", file.path.display()))?
            .modified()
            .with_context(|| format!("read archive modified time {}", file.path.display()))?;
        if modified < cutoff {
            fs::remove_file(&file.path)
                .with_context(|| format!("remove expired archive {}", file.path.display()))?;
            removed += 1;
        }
    }
    Ok(removed)
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

    #[test]
    fn prune_archive_files_ignores_files_inside_retention() {
        let dir = tempfile::tempdir().unwrap();
        let parquet = dir.path().join("events.parquet");
        std::fs::write(&parquet, b"root").unwrap();

        let removed =
            prune_archive_files(dir.path(), Duration::from_secs(365 * 24 * 3600)).unwrap();

        assert_eq!(removed, 0);
        assert!(parquet.exists());
    }
}
