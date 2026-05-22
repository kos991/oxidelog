use arrayvec::ArrayVec;
use fwlog_domain::CanonicalEvent;

pub const MAX_APPLIED_RULES: usize = 8;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParserId(pub &'static str);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuleId(pub String);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DetectScore {
    NoMatch,
    CompatibilityMatch,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DetectOutcome {
    pub score: DetectScore,
    pub reason: &'static str,
}

impl DetectOutcome {
    pub fn matched(reason: &'static str) -> Self {
        Self {
            score: DetectScore::CompatibilityMatch,
            reason,
        }
    }

    pub fn no_match(reason: &'static str) -> Self {
        Self {
            score: DetectScore::NoMatch,
            reason,
        }
    }

    pub fn is_match(&self) -> bool {
        matches!(self.score, DetectScore::CompatibilityMatch)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParserAttemptDiagnostic {
    pub parser_id: String,
    pub parser_name: String,
    pub detect_reason: String,
    pub status: String,
    pub error: Option<String>,
}

#[derive(Debug, Clone)]
pub struct ParseDiagnosticsBuffer {
    pub matched_parser_id: Option<String>,
    pub attempts: Vec<ParserAttemptDiagnostic>,
    pub failure_reason: Option<String>,
    pub pairs_truncated: bool,
    pub line_truncated: bool,
    pub applied_rules: ArrayVec<RuleId, MAX_APPLIED_RULES>,
    pub failed_rules: ArrayVec<RuleId, MAX_APPLIED_RULES>,
    pub applied_rules_truncated: bool,
    pub failed_rules_truncated: bool,
}

impl Default for ParseDiagnosticsBuffer {
    fn default() -> Self {
        Self {
            matched_parser_id: None,
            attempts: Vec::new(),
            failure_reason: None,
            pairs_truncated: false,
            line_truncated: false,
            applied_rules: ArrayVec::new(),
            failed_rules: ArrayVec::new(),
            applied_rules_truncated: false,
            failed_rules_truncated: false,
        }
    }
}

impl ParseDiagnosticsBuffer {
    pub fn clear(&mut self) {
        *self = Self::default();
    }

    pub fn push_attempt(
        &mut self,
        parser_id: impl Into<String>,
        parser_name: impl Into<String>,
        detect_reason: impl Into<String>,
        event: &CanonicalEvent,
    ) {
        self.attempts.push(ParserAttemptDiagnostic {
            parser_id: parser_id.into(),
            parser_name: parser_name.into(),
            detect_reason: detect_reason.into(),
            status: serde_json::to_string(&event.parse_status)
                .unwrap_or_else(|_| "\"failed\"".to_string())
                .trim_matches('"')
                .to_string(),
            error: event.parse_error.clone(),
        });
    }

    pub fn push_applied_rule(&mut self, rule_id: RuleId) {
        if self.applied_rules.try_push(rule_id).is_err() {
            self.applied_rules_truncated = true;
        }
    }

    pub fn push_failed_rule(&mut self, rule_id: RuleId) {
        if self.failed_rules.try_push(rule_id).is_err() {
            self.failed_rules_truncated = true;
        }
    }
}

#[derive(Debug, Clone)]
pub struct ParseResult {
    pub event: CanonicalEvent,
}
