use std::collections::HashSet;

/// A feature tag. Just a string — no central enum to maintain.
pub type Feature = &'static str;

// Built-in tag constants for convenience. Not exhaustive — callers can use any string.
pub const COPY: Feature = "copy";
pub const COPY_EXTENDED: Feature = "copy_extended";
pub const TRANSACTIONS: Feature = "transactions";
pub const SQL_PREPARE: Feature = "sql_prepare";
pub const PLPGSQL: Feature = "plpgsql";
pub const FUNCTION_CALL: Feature = "function_call";
pub const MULTI_STATEMENT: Feature = "multi_statement";

/// All built-in feature tags.
pub const ALL_TAGS: &[Feature] = &[
    COPY,
    COPY_EXTENDED,
    TRANSACTIONS,
    SQL_PREPARE,
    PLPGSQL,
    FUNCTION_CALL,
    MULTI_STATEMENT,
];

/// Controls which SQL templates and operations the generator can draw from.
/// Features are identified by string tags — adding a new feature is purely additive.
#[derive(Debug, Clone)]
pub struct FuzzProfile {
    enabled: HashSet<&'static str>,
}

impl FuzzProfile {
    /// Core protocol ops only. No optional features.
    pub fn minimal() -> Self {
        Self {
            enabled: HashSet::new(),
        }
    }

    /// Core + transactions, simple COPY, multi-statement.
    pub fn standard() -> Self {
        let mut p = Self::minimal();
        p.enable(TRANSACTIONS);
        p.enable(COPY);
        p.enable(MULTI_STATEMENT);
        p
    }

    /// Everything enabled.
    pub fn full() -> Self {
        let mut p = Self::minimal();
        for tag in ALL_TAGS {
            p.enable(tag);
        }
        p
    }

    pub fn enable(&mut self, tag: Feature) {
        self.enabled.insert(tag);
    }

    pub fn disable(&mut self, tag: &str) {
        self.enabled.remove(tag);
    }

    pub fn is_enabled(&self, tag: &str) -> bool {
        self.enabled.contains(tag)
    }

    /// Returns true if all of the given tags are enabled. An empty slice always returns true.
    pub fn all_enabled(&self, tags: &[Feature]) -> bool {
        tags.iter().all(|t| self.is_enabled(t))
    }
}
