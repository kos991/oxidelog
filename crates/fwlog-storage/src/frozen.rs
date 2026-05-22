use std::{
    fs::{self, File},
    io::{BufRead, BufReader, Write},
    path::{Path, PathBuf},
    time::{Duration, SystemTime},
};

use anyhow::{Context, Result};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FrozenFile {
    pub path: PathBuf,
    pub bytes: u64,
}

pub fn write_frozen_raw(output_path: impl AsRef<Path>, records: &[String]) -> Result<FrozenFile> {
    let output_path = output_path.as_ref();
    if let Some(parent) = output_path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        fs::create_dir_all(parent)
            .with_context(|| format!("create frozen parent directory {}", parent.display()))?;
    }

    let file = File::create(output_path)
        .with_context(|| format!("create frozen raw file {}", output_path.display()))?;
    let mut encoder = zstd::stream::write::Encoder::new(file, 0)
        .with_context(|| format!("create zstd encoder for {}", output_path.display()))?;

    for record in records {
        encoder
            .write_all(record.as_bytes())
            .with_context(|| format!("write frozen raw record {}", output_path.display()))?;
        encoder
            .write_all(b"\n")
            .with_context(|| format!("write frozen raw newline {}", output_path.display()))?;
    }

    encoder
        .finish()
        .with_context(|| format!("finish zstd encoder for {}", output_path.display()))?;
    let bytes = fs::metadata(output_path)
        .with_context(|| format!("read frozen raw metadata {}", output_path.display()))?
        .len();

    Ok(FrozenFile {
        path: output_path.to_path_buf(),
        bytes,
    })
}

pub fn read_frozen_raw(input_path: impl AsRef<Path>) -> Result<Vec<String>> {
    let input_path = input_path.as_ref();
    let file = File::open(input_path)
        .with_context(|| format!("open frozen raw file {}", input_path.display()))?;
    let decoder = zstd::stream::read::Decoder::new(file)
        .with_context(|| format!("create zstd decoder for {}", input_path.display()))?;
    let reader = BufReader::new(decoder);

    reader
        .lines()
        .collect::<std::io::Result<Vec<_>>>()
        .with_context(|| format!("read frozen raw lines {}", input_path.display()))
}

pub fn list_frozen_files(dir: impl AsRef<Path>) -> Result<Vec<FrozenFile>> {
    let mut files = Vec::new();
    collect_frozen_files(dir.as_ref(), &mut files)?;
    files.sort_by(|left, right| left.path.cmp(&right.path));
    Ok(files)
}

pub fn prune_frozen_files(dir: impl AsRef<Path>, retention: Duration) -> Result<usize> {
    let dir = dir.as_ref();
    if !dir.exists() {
        return Ok(0);
    }

    let cutoff = SystemTime::now()
        .checked_sub(retention)
        .unwrap_or(SystemTime::UNIX_EPOCH);
    let mut removed = 0;
    for file in list_frozen_files(dir)? {
        let modified = fs::metadata(&file.path)
            .with_context(|| format!("read frozen metadata {}", file.path.display()))?
            .modified()
            .with_context(|| format!("read frozen modified time {}", file.path.display()))?;
        if modified < cutoff {
            fs::remove_file(&file.path).with_context(|| {
                format!("remove expired frozen archive {}", file.path.display())
            })?;
            removed += 1;
        }
    }
    Ok(removed)
}

fn collect_frozen_files(dir: &Path, files: &mut Vec<FrozenFile>) -> Result<()> {
    for entry in
        fs::read_dir(dir).with_context(|| format!("read frozen directory {}", dir.display()))?
    {
        let entry = entry.context("read frozen directory entry")?;
        let path = entry.path();
        let metadata = entry
            .metadata()
            .with_context(|| format!("read frozen metadata {}", path.display()))?;
        if metadata.is_dir() {
            collect_frozen_files(&path, files)?;
        } else if metadata.is_file() && is_frozen_archive(&path) {
            files.push(FrozenFile {
                path,
                bytes: metadata.len(),
            });
        }
    }
    Ok(())
}

fn is_frozen_archive(path: &Path) -> bool {
    path.file_name()
        .and_then(|file_name| file_name.to_str())
        .is_some_and(|file_name| {
            file_name.ends_with(".raw.zst")
                || (file_name.starts_with("raw-import-") && file_name.ends_with(".tar.zst"))
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn write_frozen_raw_then_read_frozen_raw_returns_exact_raw_lines() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("nested").join("logs.raw.zst");
        let records = vec![
            "plain raw line".to_string(),
            "with spaces and symbols = value".to_string(),
            "非 ASCII 日志".to_string(),
        ];

        let frozen = write_frozen_raw(&path, &records).unwrap();
        let read = read_frozen_raw(&path).unwrap();

        assert_eq!(frozen.path, path);
        assert!(frozen.bytes > 0);
        assert_eq!(read, records);
    }

    #[test]
    fn list_frozen_files_returns_raw_zst_and_raw_import_tar_zst_sorted_by_path() {
        let dir = tempfile::tempdir().unwrap();
        let root_frozen = dir.path().join("b.raw.zst");
        let raw_import = dir.path().join("raw-import-20260516.tar.zst");
        let nested_dir = dir.path().join("nested");
        let nested_frozen = nested_dir.join("a.raw.zst");
        let not_raw_zst = nested_dir.join("ignore.zst");
        let not_zst = nested_dir.join("ignore.raw");
        std::fs::create_dir_all(&nested_dir).unwrap();
        std::fs::write(&root_frozen, b"root").unwrap();
        std::fs::write(&raw_import, b"raw-import").unwrap();
        std::fs::write(&nested_frozen, b"nested").unwrap();
        std::fs::write(not_raw_zst, b"ignore").unwrap();
        std::fs::write(not_zst, b"ignore").unwrap();

        let files = list_frozen_files(dir.path()).unwrap();

        let paths: Vec<_> = files.iter().map(|file| file.path.clone()).collect();
        assert_eq!(paths, vec![root_frozen, nested_frozen, raw_import]);
        assert_eq!(files[0].bytes, 4);
        assert_eq!(files[1].bytes, 6);
        assert_eq!(files[2].bytes, 10);
    }

    #[test]
    fn prune_frozen_files_ignores_files_inside_retention() {
        let dir = tempfile::tempdir().unwrap();
        let frozen = dir.path().join("events.raw.zst");
        std::fs::write(&frozen, b"root").unwrap();

        let removed = prune_frozen_files(dir.path(), Duration::from_secs(365 * 24 * 3600)).unwrap();

        assert_eq!(removed, 0);
        assert!(frozen.exists());
    }
}
