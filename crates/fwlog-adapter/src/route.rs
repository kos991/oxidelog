use std::collections::BTreeMap;

pub const SANGFOR_PARSER_ID: &str = "parser:sangfor_nat_v1";
pub const GENERIC_KV_PARSER_ID: &str = "parser:generic_kv_v1";
pub const RULE_BASED_PARSER_ID: &str = "parser:rule_based_v1";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StaticRouteGroup {
    parser_ids: Vec<String>,
}

impl StaticRouteGroup {
    pub fn new(parser_ids: Vec<String>) -> Self {
        Self { parser_ids }
    }

    pub fn parser_ids(&self) -> &[String] {
        &self.parser_ids
    }

    fn insert_before_generic_fallback(&mut self, parser_id: &str) {
        if self.parser_ids.iter().any(|id| id == parser_id) {
            return;
        }
        let index = self
            .parser_ids
            .iter()
            .position(|id| id == GENERIC_KV_PARSER_ID || id == RULE_BASED_PARSER_ID)
            .unwrap_or(self.parser_ids.len());
        self.parser_ids.insert(index, parser_id.to_string());
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PinnedScopeParsers {
    pub scope_key: String,
    pub parser_ids: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct RouteSnapshot {
    default_groups: Vec<StaticRouteGroup>,
    scoped_pins: BTreeMap<String, StaticRouteGroup>,
}

impl RouteSnapshot {
    pub fn default_static() -> Self {
        Self {
            default_groups: vec![StaticRouteGroup::new(vec![
                SANGFOR_PARSER_ID.to_string(),
                GENERIC_KV_PARSER_ID.to_string(),
                RULE_BASED_PARSER_ID.to_string(),
            ])],
            scoped_pins: BTreeMap::new(),
        }
    }

    pub fn with_pins(pins: Vec<PinnedScopeParsers>) -> Self {
        let mut snapshot = Self::default_static();
        for pin in pins {
            snapshot.scoped_pins.insert(
                pin.scope_key,
                StaticRouteGroup::new(dedupe_preserving_order(pin.parser_ids)),
            );
        }
        snapshot
    }

    pub fn register_default_adapter(&mut self, parser_id: &str) {
        if let Some(group) = self.default_groups.first_mut() {
            group.insert_before_generic_fallback(parser_id);
        }
    }

    pub fn for_each_parser_id(&self, scope_key: &str, mut visit: impl FnMut(&str)) {
        let pinned = self.scoped_pins.get(scope_key);
        if let Some(group) = pinned {
            for id in group.parser_ids() {
                visit(id);
            }
        }

        for group in &self.default_groups {
            for id in group.parser_ids() {
                let already_pinned = pinned
                    .map(|pinned| pinned.parser_ids().iter().any(|pinned_id| pinned_id == id))
                    .unwrap_or(false);
                if !already_pinned {
                    visit(id);
                }
            }
        }
    }
}

impl Default for RouteSnapshot {
    fn default() -> Self {
        Self::default_static()
    }
}

fn dedupe_preserving_order(ids: Vec<String>) -> Vec<String> {
    let mut out = Vec::new();
    for id in ids {
        if !out.iter().any(|existing| existing == &id) {
            out.push(id);
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_route_snapshot_preserves_static_family_order() {
        let snapshot = RouteSnapshot::default_static();
        let mut ids = Vec::new();
        snapshot.for_each_parser_id("source:udp://192.168.1.10", |id| ids.push(id.to_string()));

        assert_eq!(
            ids,
            vec![
                "parser:sangfor_nat_v1",
                "parser:generic_kv_v1",
                "parser:rule_based_v1"
            ]
        );
    }

    #[test]
    fn pinned_scope_ids_are_first_without_sorting() {
        let snapshot = RouteSnapshot::with_pins(vec![PinnedScopeParsers {
            scope_key: "source:udp://192.168.1.10".to_string(),
            parser_ids: vec![
                "rule:default:CriticalCustomRule".to_string(),
                "parser:sangfor_nat_v1".to_string(),
            ],
        }]);

        let mut ids = Vec::new();
        snapshot.for_each_parser_id("source:udp://192.168.1.10", |id| ids.push(id.to_string()));
        assert_eq!(ids[0], "rule:default:CriticalCustomRule");
        assert_eq!(ids[1], "parser:sangfor_nat_v1");
        assert!(ids.iter().any(|id| id == "parser:generic_kv_v1"));
    }

    #[test]
    fn default_adapter_registration_keeps_adapters_before_generic_fallback() {
        let mut snapshot = RouteSnapshot::default_static();
        snapshot.register_default_adapter("parser:custom_v1");

        let mut ids = Vec::new();
        snapshot.for_each_parser_id("source:udp://192.168.1.10", |id| ids.push(id.to_string()));

        assert_eq!(
            ids,
            vec![
                "parser:sangfor_nat_v1",
                "parser:custom_v1",
                "parser:generic_kv_v1",
                "parser:rule_based_v1"
            ]
        );
    }
}
