use crate::profile::{self, Feature, FuzzProfile};

/// Which protocol path a template is designed for. Advisory — the generator may
/// deliberately violate it at low probability to test "wrong" usage.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Affinity {
    /// Works in both simple and extended query paths.
    Any,
    /// Designed for simple query (Query message). Multi-statement, COPY via simple, etc.
    Simple,
    /// Designed for extended query (Parse/Bind/Execute). Parameterized queries, etc.
    Extended,
}

/// A SQL template in the registry. Declarative data — adding a new template is just
/// appending an entry to `TEMPLATES`.
#[derive(Debug, Clone)]
pub struct SqlEntry {
    pub sql: &'static str,
    pub affinity: Affinity,
    /// Feature tags required. All must be present in the profile for this template
    /// to be included in the generator's draw pool.
    pub requires: &'static [Feature],
    /// Number of bind parameters. 0 for non-parameterized statements.
    pub param_count: usize,
    /// Setup SQL to run once per connection before fuzzing begins.
    /// The runner deduplicates these across all enabled templates.
    pub setup: &'static [&'static str],
}

/// The full template registry. To add a template, append an entry here.
pub static TEMPLATES: &[SqlEntry] = &[
    // -- Core (no feature tags required) --
    SqlEntry {
        sql: "SELECT 1",
        affinity: Affinity::Any,
        requires: &[],
        param_count: 0,
        setup: &[],
    },
    SqlEntry {
        sql: "SELECT $1::int",
        affinity: Affinity::Extended,
        requires: &[],
        param_count: 1,
        setup: &[],
    },
    SqlEntry {
        sql: "SELECT $1::int, $2::text",
        affinity: Affinity::Extended,
        requires: &[],
        param_count: 2,
        setup: &[],
    },
    SqlEntry {
        sql: "SELECT * FROM pg_type LIMIT 5",
        affinity: Affinity::Any,
        requires: &[],
        param_count: 0,
        setup: &[],
    },
    // Parse error (typo)
    SqlEntry {
        sql: "SLECT 1",
        affinity: Affinity::Any,
        requires: &[],
        param_count: 0,
        setup: &[],
    },
    // Runtime error
    SqlEntry {
        sql: "SELECT 1/0",
        affinity: Affinity::Any,
        requires: &[],
        param_count: 0,
        setup: &[],
    },
    // -- multi_statement --
    SqlEntry {
        sql: "SELECT 1; SELECT 2",
        affinity: Affinity::Simple,
        requires: &[profile::MULTI_STATEMENT],
        param_count: 0,
        setup: &[],
    },
    // -- transactions --
    SqlEntry {
        sql: "BEGIN",
        affinity: Affinity::Any,
        requires: &[profile::TRANSACTIONS],
        param_count: 0,
        setup: &[],
    },
    SqlEntry {
        sql: "COMMIT",
        affinity: Affinity::Any,
        requires: &[profile::TRANSACTIONS],
        param_count: 0,
        setup: &[],
    },
    SqlEntry {
        sql: "ROLLBACK",
        affinity: Affinity::Any,
        requires: &[profile::TRANSACTIONS],
        param_count: 0,
        setup: &[],
    },
    // -- copy (simple query path) --
    SqlEntry {
        sql: "COPY (SELECT 1) TO STDOUT",
        affinity: Affinity::Simple,
        requires: &[profile::COPY],
        param_count: 0,
        setup: &[],
    },
    SqlEntry {
        sql: "COPY copy_test FROM STDIN",
        affinity: Affinity::Simple,
        requires: &[profile::COPY],
        param_count: 0,
        setup: &["CREATE TABLE IF NOT EXISTS copy_test (id int)"],
    },
    // -- copy_extended (extended query path) --
    SqlEntry {
        sql: "COPY (SELECT 1) TO STDOUT",
        affinity: Affinity::Extended,
        requires: &[profile::COPY_EXTENDED],
        param_count: 0,
        setup: &[],
    },
    SqlEntry {
        sql: "COPY copy_test FROM STDIN",
        affinity: Affinity::Extended,
        requires: &[profile::COPY_EXTENDED],
        param_count: 0,
        setup: &["CREATE TABLE IF NOT EXISTS copy_test (id int)"],
    },
    // -- plpgsql --
    SqlEntry {
        sql: "DO $$ BEGIN RAISE NOTICE 'hi'; END $$",
        affinity: Affinity::Any,
        requires: &[profile::PLPGSQL],
        param_count: 0,
        setup: &[],
    },
    // -- sql_prepare --
    SqlEntry {
        sql: "PREPARE fuzz_stmt AS SELECT 1",
        affinity: Affinity::Simple,
        requires: &[profile::SQL_PREPARE],
        param_count: 0,
        setup: &[],
    },
    SqlEntry {
        sql: "DEALLOCATE fuzz_stmt",
        affinity: Affinity::Simple,
        requires: &[profile::SQL_PREPARE],
        param_count: 0,
        setup: &[],
    },
    SqlEntry {
        sql: "EXECUTE fuzz_stmt",
        affinity: Affinity::Simple,
        requires: &[profile::SQL_PREPARE],
        param_count: 0,
        setup: &[],
    },
    SqlEntry {
        sql: "DEALLOCATE ALL",
        affinity: Affinity::Simple,
        requires: &[profile::SQL_PREPARE],
        param_count: 0,
        setup: &[],
    },
];

/// Returns all templates whose required tags are enabled in the profile.
pub fn enabled_templates(profile: &FuzzProfile) -> Vec<&'static SqlEntry> {
    TEMPLATES
        .iter()
        .filter(|t| profile.all_enabled(t.requires))
        .collect()
}

/// Returns deduplicated setup SQL from all enabled templates.
pub fn setup_sql(profile: &FuzzProfile) -> Vec<&'static str> {
    let mut seen = std::collections::HashSet::new();
    let mut result = Vec::new();
    for t in enabled_templates(profile) {
        for sql in t.setup {
            if seen.insert(*sql) {
                result.push(*sql);
            }
        }
    }
    result
}
