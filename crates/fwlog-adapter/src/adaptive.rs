use arrayvec::ArrayVec;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GenericPair<'a> {
    pub key: &'a str,
    pub value: &'a str,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GenericPairs<'a, const N: usize> {
    pub pairs: ArrayVec<GenericPair<'a>, N>,
}

impl<'a, const N: usize> Default for GenericPairs<'a, N> {
    fn default() -> Self {
        Self {
            pairs: ArrayVec::new(),
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ExtractDiagnostics {
    pub line_truncated: bool,
    pub pairs_truncated: bool,
    pub reason: Option<String>,
}

impl ExtractDiagnostics {
    pub fn clear(&mut self) {
        self.line_truncated = false;
        self.pairs_truncated = false;
        self.reason = None;
    }
}

pub fn extract_generic_pairs<'a, const N: usize>(
    raw: &'a str,
    max_generic_line_bytes: usize,
    diagnostics: &mut ExtractDiagnostics,
) -> GenericPairs<'a, N> {
    diagnostics.clear();

    let Some(scan) = bounded_scan_slice(raw, max_generic_line_bytes, diagnostics) else {
        return GenericPairs::default();
    };

    let bytes = scan.as_bytes();
    let mut pairs = GenericPairs::default();
    let mut cursor = 0;

    while cursor < bytes.len() {
        cursor = skip_field_separators(bytes, cursor);
        if cursor >= bytes.len() {
            break;
        }

        let key_start = cursor;
        let Some(separator) = find_assignment_separator(bytes, cursor) else {
            cursor = skip_to_next_field(bytes, cursor);
            continue;
        };

        let key = scan[key_start..separator].trim();
        let mut value_start = separator + 1;
        while value_start < bytes.len() && bytes[value_start].is_ascii_whitespace() {
            value_start += 1;
        }

        let value_end = find_value_end(bytes, value_start);
        let value = scan[value_start..value_end].trim();

        if !key.is_empty() && !value.is_empty() {
            if pairs.pairs.try_push(GenericPair { key, value }).is_err() {
                diagnostics.pairs_truncated = true;
                break;
            }
        }

        cursor = value_end;
    }

    pairs
}

fn bounded_scan_slice<'a>(
    raw: &'a str,
    max_generic_line_bytes: usize,
    diagnostics: &mut ExtractDiagnostics,
) -> Option<&'a str> {
    if raw.len() <= max_generic_line_bytes {
        return Some(raw);
    }

    let limit = max_generic_line_bytes.min(raw.len());
    let Some(boundary) = last_safe_boundary_before(raw.as_bytes(), limit) else {
        diagnostics.reason = Some("line_too_long_no_safe_boundary".to_string());
        return None;
    };

    diagnostics.line_truncated = true;
    Some(&raw[..boundary])
}

fn last_safe_boundary_before(bytes: &[u8], limit: usize) -> Option<usize> {
    let haystack = &bytes[..limit];
    let mut last = None;

    for delimiter in [b' ', b'\t', b'\r', b'\n', b',', b';', b'|'] {
        if let Some(index) = memchr::memchr_iter(delimiter, haystack).last() {
            last = Some(last.map_or(index, |current: usize| current.max(index)));
        }
    }

    last
}

fn skip_field_separators(bytes: &[u8], mut cursor: usize) -> usize {
    while cursor < bytes.len() && is_field_separator(bytes[cursor]) {
        cursor += 1;
    }
    cursor
}

fn skip_to_next_field(bytes: &[u8], mut cursor: usize) -> usize {
    while cursor < bytes.len() && !is_field_separator(bytes[cursor]) {
        cursor += 1;
    }
    cursor
}

fn find_assignment_separator(bytes: &[u8], mut cursor: usize) -> Option<usize> {
    while cursor < bytes.len() {
        match bytes[cursor] {
            b'=' | b':' => return Some(cursor),
            byte if is_field_separator(byte) => return None,
            _ => cursor += 1,
        }
    }
    None
}

fn find_value_end(bytes: &[u8], mut cursor: usize) -> usize {
    while cursor < bytes.len() && !is_field_separator(bytes[cursor]) {
        cursor += 1;
    }
    cursor
}

fn is_field_separator(byte: u8) -> bool {
    byte.is_ascii_whitespace() || matches!(byte, b',' | b';' | b'|')
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extractor_returns_borrowed_pairs() {
        let raw = "src=192.168.1.1 dst:10.0.0.1 action=allow";
        let mut diagnostics = ExtractDiagnostics::default();
        let pairs = extract_generic_pairs::<8>(raw, 8192, &mut diagnostics);

        assert_eq!(pairs.pairs.len(), 3);
        assert_eq!(pairs.pairs[0].key, "src");
        assert_eq!(pairs.pairs[0].value, "192.168.1.1");
        assert_eq!(pairs.pairs[1].key, "dst");
        assert_eq!(pairs.pairs[1].value, "10.0.0.1");
        assert!(!diagnostics.pairs_truncated);
    }

    #[test]
    fn extractor_truncates_pairs_without_panic() {
        let raw = "a=1 b=2 c=3";
        let mut diagnostics = ExtractDiagnostics::default();
        let pairs = extract_generic_pairs::<2>(raw, 8192, &mut diagnostics);

        assert_eq!(pairs.pairs.len(), 2);
        assert!(diagnostics.pairs_truncated);
    }

    #[test]
    fn long_line_without_safe_boundary_is_skipped() {
        let raw = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa=1";
        let mut diagnostics = ExtractDiagnostics::default();
        let pairs = extract_generic_pairs::<8>(raw, 8, &mut diagnostics);

        assert_eq!(pairs.pairs.len(), 0);
        assert_eq!(
            diagnostics.reason.as_deref(),
            Some("line_too_long_no_safe_boundary")
        );
    }

    #[test]
    fn extractor_handles_utf8_keys_without_splitting_boundaries() {
        let raw = "婧怚P:192.168.1.1 鐩殑IP:10.0.0.1";
        let mut diagnostics = ExtractDiagnostics::default();
        let pairs = extract_generic_pairs::<8>(raw, 8192, &mut diagnostics);

        assert_eq!(pairs.pairs.len(), 2);
        assert_eq!(pairs.pairs[0].key, "婧怚P");
        assert_eq!(pairs.pairs[1].key, "鐩殑IP");
    }
}
