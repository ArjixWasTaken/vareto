//! psql's `\d…` list commands as catalog queries, modelled on what `psql -E`
//! shows, with the `S` (system objects), `+` (more detail) and `x` (expanded)
//! modifiers. `ver` is the server's `server_version_num`.

use anyhow::{bail, Result};

/// The modifiers after a `\d` command: `S`, `+`, `x`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Mods {
    pub system: bool,
    pub verbose: bool,
    pub expanded: bool,
}

/// Split `dtS+` into `dt` and its modifiers. `\dx` stays the extensions command.
pub fn parse_mods(cmd: &str) -> (&str, Mods) {
    let mut m = Mods::default();
    let mut base = cmd;
    while let Some(last) = base.chars().last() {
        let rest = &base[..base.len() - 1];
        match last {
            'S' if !rest.is_empty() && rest != "dR" => m.system = true,
            '+' => m.verbose = true,
            // `\dx` is extensions, not `\d` expanded.
            'x' if !rest.is_empty() && rest != "d" => m.expanded = true,
            _ => break,
        }
        base = rest;
    }
    (base, m)
}

/// A catalog query and how to show it.
pub struct Listing {
    pub title: String,
    pub sql: String,
    /// What to print when nothing matches (instead of an empty table).
    pub none: Option<String>,
}

/// A string literal (standard_conforming_strings: backslashes are literal).
pub use crate::db::sql::quote_lit as lit;

/// A psql pattern (`*`, `?`, optional `schema.`) as conditions on the schema and
/// name columns. Without a pattern: `visible` (and no system schemas unless S).
pub fn pattern(pat: Option<&str>, schema_col: &str, name_col: &str, visible: &str, m: Mods) -> String {
    // Unquoted names fold to lower case, as psql does; "Quoted" ones keep theirs.
    let re = |p: &str| {
        let p = if p.contains('"') { p.replace('"', "") } else { p.to_lowercase() };
        format!("^({})$", regex::escape(&p).replace("\\*", ".*").replace("\\?", "."))
    };
    match pat {
        Some(p) => match p.split_once('.') {
            Some((sc, name)) if !schema_col.is_empty() => {
                format!("{schema_col} ~ {} AND {name_col} ~ {}", lit(&re(sc)), lit(&re(name)))
            }
            _ => format!("{name_col} ~ {}{}", lit(&re(p)), if visible.is_empty() { String::new() } else { format!(" AND {visible}") }),
        },
        None => {
            let mut c = vec![if visible.is_empty() { "TRUE".to_string() } else { visible.to_string() }];
            if !m.system && !schema_col.is_empty() {
                c.push(format!("{schema_col} <> 'pg_catalog' AND {schema_col} <> 'information_schema' AND {schema_col} !~ '^pg_toast'"));
            }
            c.join(" AND ")
        }
    }
}

/// `\d[tivmsE]` and their combinations (`\dti`).
pub fn relations(letters: &str, pat: Option<&str>, m: Mods, ver: u32) -> Listing {
    let letters = if letters.is_empty() { "tvmsE" } else { letters };
    let mut kinds: Vec<&str> = Vec::new();
    for c in letters.chars() {
        kinds.extend(match c {
            't' => &["'r'", "'p'"][..],
            'v' => &["'v'"][..],
            'm' => &["'m'"][..],
            'i' => &["'i'", "'I'"][..],
            's' => &["'S'"][..],
            'E' => &["'f'"][..],
            _ => &[][..],
        });
    }
    let what = match letters {
        "t" => "tables",
        "v" => "views",
        "m" => "materialized views",
        "i" => "indexes",
        "s" => "sequences",
        "E" => "foreign tables",
        _ => "relations",
    };
    let index = letters.contains('i');
    let mut cols = vec![
        "n.nspname AS \"Schema\"".to_string(),
        "c.relname AS \"Name\"".into(),
        "CASE c.relkind WHEN 'r' THEN 'table' WHEN 'v' THEN 'view' WHEN 'm' THEN 'materialized view' WHEN 'i' THEN 'index' \
         WHEN 'S' THEN 'sequence' WHEN 't' THEN 'TOAST table' WHEN 'f' THEN 'foreign table' WHEN 'p' THEN 'partitioned table' \
         WHEN 'I' THEN 'partitioned index' END AS \"Type\""
            .into(),
        "pg_catalog.pg_get_userbyid(c.relowner) AS \"Owner\"".into(),
    ];
    if index {
        cols.push("c2.relname AS \"Table\"".into());
    }
    if m.verbose {
        cols.push(
            "CASE c.relpersistence WHEN 'p' THEN 'permanent' WHEN 't' THEN 'temporary' WHEN 'u' THEN 'unlogged' END AS \"Persistence\"".into(),
        );
        if ver >= 120000 {
            cols.push("am.amname AS \"Access method\"".into());
        }
        cols.push("pg_catalog.pg_size_pretty(pg_catalog.pg_table_size(c.oid)) AS \"Size\"".into());
        cols.push("pg_catalog.obj_description(c.oid, 'pg_class') AS \"Description\"".into());
    }
    let mut from = "pg_catalog.pg_class c LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace".to_string();
    if index {
        from.push_str(
            " LEFT JOIN pg_catalog.pg_index i ON i.indexrelid = c.oid LEFT JOIN pg_catalog.pg_class c2 ON i.indrelid = c2.oid",
        );
    }
    if m.verbose && ver >= 120000 {
        from.push_str(" LEFT JOIN pg_catalog.pg_am am ON am.oid = c.relam");
    }
    let sql = format!(
        "SELECT {} FROM {from} WHERE c.relkind IN ({}) AND {} ORDER BY 1, 2",
        cols.join(", "),
        kinds.join(","),
        pattern(pat, "n.nspname", "c.relname", "pg_catalog.pg_table_is_visible(c.oid)", m),
    );
    let none = match pat {
        Some(p) => format!("Did not find any {what} named \"{p}\"."),
        None => format!("Did not find any {what}."),
    };
    Listing { title: "List of relations".into(), sql, none: Some(none) }
}

/// The other list commands, by name (without `\` and modifiers). `args` are the
/// command's arguments (a pattern, sometimes two).
pub fn listing(cmd: &str, args: &[String], m: Mods, ver: u32) -> Option<Result<Listing>> {
    let pat = args.first().map(String::as_str);
    let p = |schema: &str, name: &str, visible: &str| pattern(pat, schema, name, visible, m);
    let v = m.verbose;
    let plus = |cond: bool, cols: &str| if cond { format!(", {cols}") } else { String::new() };
    let l = |title: &str, sql: String| Some(Ok(Listing { title: title.into(), sql, none: None }));
    match cmd {
        "l" | "list" => l(
            "List of databases",
            format!(
                "SELECT d.datname AS \"Name\", pg_catalog.pg_get_userbyid(d.datdba) AS \"Owner\", \
                 pg_catalog.pg_encoding_to_char(d.encoding) AS \"Encoding\"{}, d.datcollate AS \"Collate\", d.datctype AS \"Ctype\", \
                 pg_catalog.array_to_string(d.datacl, E'\\n') AS \"Access privileges\"{} \
                 FROM pg_catalog.pg_database d LEFT JOIN pg_catalog.pg_tablespace t ON d.dattablespace = t.oid WHERE {} ORDER BY 1",
                plus(ver >= 150000, "CASE d.datlocprovider WHEN 'c' THEN 'libc' WHEN 'i' THEN 'icu' WHEN 'b' THEN 'builtin' END AS \"Locale Provider\""),
                plus(
                    v,
                    "CASE WHEN pg_catalog.has_database_privilege(d.datname, 'CONNECT') \
                     THEN pg_catalog.pg_size_pretty(pg_catalog.pg_database_size(d.datname)) ELSE 'No Access' END AS \"Size\", \
                     t.spcname AS \"Tablespace\", pg_catalog.shobj_description(d.oid, 'pg_database') AS \"Description\""
                ),
                pattern(pat, "", "d.datname", "", m),
            ),
        ),
        "dn" => l(
            "List of schemas",
            format!(
                "SELECT n.nspname AS \"Name\", pg_catalog.pg_get_userbyid(n.nspowner) AS \"Owner\"{} \
                 FROM pg_catalog.pg_namespace n WHERE {} ORDER BY 1",
                plus(v, "pg_catalog.array_to_string(n.nspacl, E'\\n') AS \"Access privileges\", pg_catalog.obj_description(n.oid, 'pg_namespace') AS \"Description\""),
                if pat.is_some() || m.system {
                    pattern(pat, "", "n.nspname", "", m)
                } else {
                    "n.nspname !~ '^pg_' AND n.nspname <> 'information_schema'".into()
                },
            ),
        ),
        "du" | "dg" => l(
            "List of roles",
            format!(
                "SELECT r.rolname AS \"Role name\", concat_ws(', ', \
                 CASE WHEN r.rolsuper THEN 'Superuser' END, CASE WHEN NOT r.rolinherit THEN 'No inheritance' END, \
                 CASE WHEN r.rolcreaterole THEN 'Create role' END, CASE WHEN r.rolcreatedb THEN 'Create DB' END, \
                 CASE WHEN NOT r.rolcanlogin THEN 'Cannot login' END, CASE WHEN r.rolreplication THEN 'Replication' END, \
                 CASE WHEN r.rolbypassrls THEN 'Bypass RLS' END, \
                 CASE WHEN r.rolconnlimit >= 0 THEN r.rolconnlimit || ' connection' || CASE WHEN r.rolconnlimit <> 1 THEN 's' ELSE '' END END, \
                 CASE WHEN r.rolvaliduntil IS NOT NULL THEN 'Password valid until ' || r.rolvaliduntil END) AS \"Attributes\"{} \
                 FROM pg_catalog.pg_roles r WHERE {} ORDER BY 1",
                plus(v, "pg_catalog.shobj_description(r.oid, 'pg_authid') AS \"Description\""),
                if pat.is_some() || m.system { pattern(pat, "", "r.rolname", "", m) } else { "r.rolname !~ '^pg_'".into() },
            ),
        ),
        c if c.starts_with("df") && c[2..].chars().all(|c| "anptw".contains(c)) => {
            let kinds: Vec<&str> = c[2..]
                .chars()
                .map(|k| match k {
                    'a' => "'a'",
                    'p' => "'p'",
                    'w' => "'w'",
                    _ => "'f'",
                })
                .collect();
            let mut cond = vec![p("n.nspname", "p.proname", "pg_catalog.pg_function_is_visible(p.oid)")];
            if !kinds.is_empty() {
                let mut k = format!("p.prokind IN ({})", kinds.join(","));
                if c.contains('t') {
                    k = format!("({k} OR p.prorettype = 'pg_catalog.trigger'::pg_catalog.regtype)");
                }
                if c.contains('n') {
                    k = format!("({k} OR (p.prokind = 'f' AND p.prorettype <> 'pg_catalog.trigger'::pg_catalog.regtype))");
                }
                cond.push(k);
            }
            l(
                "List of functions",
                format!(
                    "SELECT n.nspname AS \"Schema\", p.proname AS \"Name\", pg_catalog.pg_get_function_result(p.oid) AS \"Result data type\", \
                     pg_catalog.pg_get_function_arguments(p.oid) AS \"Argument data types\", \
                     CASE p.prokind WHEN 'a' THEN 'agg' WHEN 'w' THEN 'window' WHEN 'p' THEN 'proc' \
                     ELSE CASE WHEN p.prorettype = 'pg_catalog.trigger'::pg_catalog.regtype THEN 'trigger' ELSE 'func' END END AS \"Type\"{} \
                     FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace \
                     LEFT JOIN pg_catalog.pg_language lang ON lang.oid = p.prolang WHERE {} ORDER BY 1, 2, 4",
                    plus(
                        v,
                        "CASE p.provolatile WHEN 'i' THEN 'immutable' WHEN 's' THEN 'stable' WHEN 'v' THEN 'volatile' END AS \"Volatility\", \
                         CASE p.proparallel WHEN 'r' THEN 'restricted' WHEN 's' THEN 'safe' WHEN 'u' THEN 'unsafe' END AS \"Parallel\", \
                         pg_catalog.pg_get_userbyid(p.proowner) AS \"Owner\", \
                         CASE WHEN p.prosecdef THEN 'definer' ELSE 'invoker' END AS \"Security\", \
                         pg_catalog.array_to_string(p.proacl, E'\\n') AS \"Access privileges\", lang.lanname AS \"Language\", \
                         pg_catalog.obj_description(p.oid, 'pg_proc') AS \"Description\""
                    ),
                    cond.join(" AND "),
                ),
            )
        }
        "da" => l(
            "List of aggregate functions",
            format!(
                "SELECT n.nspname AS \"Schema\", p.proname AS \"Name\", pg_catalog.format_type(p.prorettype, NULL) AS \"Result data type\", \
                 CASE WHEN p.pronargs = 0 THEN '*' ELSE pg_catalog.pg_get_function_arguments(p.oid) END AS \"Argument data types\", \
                 pg_catalog.obj_description(p.oid, 'pg_proc') AS \"Description\" \
                 FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace \
                 WHERE p.prokind = 'a' AND {} ORDER BY 1, 2, 4",
                p("n.nspname", "p.proname", "pg_catalog.pg_function_is_visible(p.oid)"),
            ),
        ),
        "dx" => l(
            "List of installed extensions",
            format!(
                "SELECT e.extname AS \"Name\", e.extversion AS \"Version\", n.nspname AS \"Schema\", \
                 pg_catalog.obj_description(e.oid, 'pg_extension') AS \"Description\" \
                 FROM pg_catalog.pg_extension e LEFT JOIN pg_catalog.pg_namespace n ON n.oid = e.extnamespace WHERE {} ORDER BY 1",
                pattern(pat, "", "e.extname", "", m),
            ),
        ),
        "dT" => l(
            "List of data types",
            format!(
                "SELECT n.nspname AS \"Schema\", pg_catalog.format_type(t.oid, NULL) AS \"Name\"{}, \
                 pg_catalog.obj_description(t.oid, 'pg_type') AS \"Description\" \
                 FROM pg_catalog.pg_type t LEFT JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace \
                 WHERE (t.typrelid = 0 OR (SELECT c.relkind = 'c' FROM pg_catalog.pg_class c WHERE c.oid = t.typrelid)) \
                 AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_type el WHERE el.oid = t.typelem AND el.typarray = t.oid) AND {} ORDER BY 1, 2",
                plus(
                    v,
                    "t.typname AS \"Internal name\", CASE WHEN t.typrelid != 0 THEN 'tuple' WHEN t.typlen < 0 THEN 'var' ELSE t.typlen::text END AS \"Size\", \
                     pg_catalog.array_to_string(ARRAY(SELECT e.enumlabel FROM pg_catalog.pg_enum e WHERE e.enumtypid = t.oid ORDER BY e.enumsortorder), E'\\n') AS \"Elements\", \
                     pg_catalog.pg_get_userbyid(t.typowner) AS \"Owner\", pg_catalog.array_to_string(t.typacl, E'\\n') AS \"Access privileges\""
                ),
                p("n.nspname", "t.typname", "pg_catalog.pg_type_is_visible(t.oid)"),
            ),
        ),
        "dD" => l(
            "List of domains",
            format!(
                "SELECT n.nspname AS \"Schema\", t.typname AS \"Name\", pg_catalog.format_type(t.typbasetype, t.typtypmod) AS \"Type\", \
                 (SELECT c.collname FROM pg_catalog.pg_collation c, pg_catalog.pg_type bt \
                  WHERE c.oid = t.typcollation AND bt.oid = t.typbasetype AND t.typcollation <> bt.typcollation) AS \"Collation\", \
                 CASE WHEN t.typnotnull THEN 'not null' END AS \"Nullable\", t.typdefault AS \"Default\", \
                 pg_catalog.array_to_string(ARRAY(SELECT pg_catalog.pg_get_constraintdef(r.oid, true) FROM pg_catalog.pg_constraint r \
                  WHERE t.oid = r.contypid), ' ') AS \"Check\"{} \
                 FROM pg_catalog.pg_type t LEFT JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace \
                 WHERE t.typtype = 'd' AND {} ORDER BY 1, 2",
                plus(v, "pg_catalog.array_to_string(t.typacl, E'\\n') AS \"Access privileges\", pg_catalog.obj_description(t.oid, 'pg_type') AS \"Description\""),
                p("n.nspname", "t.typname", "pg_catalog.pg_type_is_visible(t.oid)"),
            ),
        ),
        "dp" | "z" => l(
            "Access privileges",
            format!(
                "SELECT n.nspname AS \"Schema\", c.relname AS \"Name\", \
                 CASE c.relkind WHEN 'r' THEN 'table' WHEN 'v' THEN 'view' WHEN 'm' THEN 'materialized view' WHEN 'S' THEN 'sequence' \
                 WHEN 'f' THEN 'foreign table' WHEN 'p' THEN 'partitioned table' END AS \"Type\", \
                 pg_catalog.array_to_string(c.relacl, E'\\n') AS \"Access privileges\", \
                 pg_catalog.array_to_string(ARRAY(SELECT attname || E':\\n  ' || pg_catalog.array_to_string(attacl, E'\\n  ') \
                  FROM pg_catalog.pg_attribute a WHERE attrelid = c.oid AND NOT attisdropped AND attacl IS NOT NULL), E'\\n') AS \"Column privileges\", \
                 pg_catalog.array_to_string(ARRAY(SELECT polname || CASE WHEN polcmd != '*' THEN \
                  ' (' || polcmd::pg_catalog.text || '):' ELSE ':' END FROM pg_catalog.pg_policy pol WHERE polrelid = c.oid), E'\\n') AS \"Policies\" \
                 FROM pg_catalog.pg_class c LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace \
                 WHERE c.relkind IN ('r','v','m','S','f','p') AND {} ORDER BY 1, 2",
                p("n.nspname", "c.relname", "pg_catalog.pg_table_is_visible(c.oid)"),
            ),
        ),
        "ddp" => l(
            "Default access privileges",
            format!(
                "SELECT pg_catalog.pg_get_userbyid(d.defaclrole) AS \"Owner\", n.nspname AS \"Schema\", \
                 CASE d.defaclobjtype WHEN 'r' THEN 'table' WHEN 'S' THEN 'sequence' WHEN 'f' THEN 'function' WHEN 'T' THEN 'type' \
                 WHEN 'n' THEN 'schema' END AS \"Type\", pg_catalog.array_to_string(d.defaclacl, E'\\n') AS \"Access privileges\" \
                 FROM pg_catalog.pg_default_acl d LEFT JOIN pg_catalog.pg_namespace n ON n.oid = d.defaclnamespace \
                 WHERE {} ORDER BY 1, 2, 3",
                match pat {
                    Some(p) => format!("(n.nspname ~ {0} OR pg_catalog.pg_get_userbyid(d.defaclrole) ~ {0})", lit(&format!("^({})$", regex::escape(p).replace("\\*", ".*")))),
                    None => "TRUE".into(),
                },
            ),
        ),
        "dy" => l(
            "List of event triggers",
            format!(
                "SELECT evtname AS \"Name\", evtevent AS \"Event\", pg_catalog.pg_get_userbyid(e.evtowner) AS \"Owner\", \
                 CASE evtenabled WHEN 'O' THEN 'enabled' WHEN 'R' THEN 'replica' WHEN 'A' THEN 'always' WHEN 'D' THEN 'disabled' END AS \"Enabled\", \
                 e.evtfoid::pg_catalog.regproc AS \"Function\", pg_catalog.array_to_string(array(SELECT x FROM pg_catalog.unnest(evttags) AS t(x)), ', ') AS \"Tags\"{} \
                 FROM pg_catalog.pg_event_trigger e WHERE {} ORDER BY 1",
                plus(v, "pg_catalog.obj_description(e.oid, 'pg_event_trigger') AS \"Description\""),
                pattern(pat, "", "evtname", "", m),
            ),
        ),
        "dL" => l(
            "List of languages",
            format!(
                "SELECT l.lanname AS \"Name\", pg_catalog.pg_get_userbyid(l.lanowner) AS \"Owner\", l.lanpltrusted AS \"Trusted\"{}, \
                 pg_catalog.obj_description(l.oid, 'pg_language') AS \"Description\" FROM pg_catalog.pg_language l WHERE {} ORDER BY 1",
                plus(
                    v,
                    "NOT l.lanispl AS \"Internal language\", l.lanplcallfoid::pg_catalog.regprocedure AS \"Call handler\", \
                     l.lanvalidator::pg_catalog.regprocedure AS \"Validator\", l.laninline::pg_catalog.regprocedure AS \"Inline handler\", \
                     pg_catalog.array_to_string(l.lanacl, E'\\n') AS \"Access privileges\""
                ),
                if m.system || pat.is_some() { pattern(pat, "", "l.lanname", "", m) } else { "l.lanplcallfoid != 0".into() },
            ),
        ),
        "dO" => l(
            "List of collations",
            format!(
                "SELECT n.nspname AS \"Schema\", c.collname AS \"Name\", \
                 CASE c.collprovider WHEN 'd' THEN 'default' WHEN 'c' THEN 'libc' WHEN 'i' THEN 'icu' WHEN 'b' THEN 'builtin' END AS \"Provider\", \
                 c.collcollate AS \"Collate\", c.collctype AS \"Ctype\"{}{} \
                 FROM pg_catalog.pg_collation c, pg_catalog.pg_namespace n \
                 WHERE n.oid = c.collnamespace AND c.collencoding IN (-1, pg_catalog.pg_char_to_encoding(pg_catalog.getdatabaseencoding())) \
                 AND {} ORDER BY 1, 2",
                if ver >= 170000 { ", c.colllocale AS \"Locale\"" } else if ver >= 150000 { ", c.colliculocale AS \"Locale\"" } else { "" },
                plus(ver >= 120000, "CASE WHEN c.collisdeterministic THEN 'yes' ELSE 'no' END AS \"Deterministic?\""),
                p("n.nspname", "c.collname", "pg_catalog.pg_collation_is_visible(c.oid)"),
            ),
        ),
        "dP" | "dPt" | "dPi" | "dPn" | "dPti" | "dPit" => {
            let kinds = if cmd.contains('i') && !cmd.contains('t') {
                "'I'"
            } else if cmd.contains('t') && !cmd.contains('i') {
                "'p'"
            } else {
                "'p','I'"
            };
            l(
                "List of partitioned relations",
                format!(
                    "SELECT n.nspname AS \"Schema\", c.relname AS \"Name\", pg_catalog.pg_get_userbyid(c.relowner) AS \"Owner\", \
                     CASE c.relkind WHEN 'p' THEN 'partitioned table' WHEN 'I' THEN 'partitioned index' END AS \"Type\", \
                     c2.relname AS \"Table\"{} \
                     FROM pg_catalog.pg_class c LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace \
                     LEFT JOIN pg_catalog.pg_index i ON i.indexrelid = c.oid LEFT JOIN pg_catalog.pg_class c2 ON i.indrelid = c2.oid \
                     WHERE c.relkind IN ({kinds}) {} AND {} ORDER BY 1, 2",
                    plus(
                        v && ver >= 120000,
                        "(SELECT pg_catalog.pg_size_pretty(sum(pg_catalog.pg_table_size(ppt.relid))) FROM pg_catalog.pg_partition_tree(c.oid) ppt) AS \"Total size\", \
                         pg_catalog.obj_description(c.oid, 'pg_class') AS \"Description\""
                    ),
                    if cmd.contains('n') { "" } else { "AND NOT c.relispartition" },
                    p("n.nspname", "c.relname", "pg_catalog.pg_table_is_visible(c.oid)"),
                ),
            )
        }
        "dX" => l(
            "List of extended statistics",
            format!(
                "SELECT es.stxnamespace::pg_catalog.regnamespace::pg_catalog.text AS \"Schema\", es.stxname AS \"Name\", \
                 {} AS \"Definition\", \
                 CASE WHEN 'd' = any(es.stxkind) THEN 'defined' END AS \"Ndistinct\", \
                 CASE WHEN 'f' = any(es.stxkind) THEN 'defined' END AS \"Dependencies\", \
                 CASE WHEN 'm' = any(es.stxkind) THEN 'defined' END AS \"MCV\" \
                 FROM pg_catalog.pg_statistic_ext es WHERE {} ORDER BY 1, 2",
                if ver >= 140000 {
                    "pg_catalog.pg_get_statisticsobjdef_columns(es.oid) || ' FROM ' || es.stxrelid::pg_catalog.regclass"
                } else {
                    "es.stxrelid::pg_catalog.regclass::pg_catalog.text"
                },
                p("es.stxnamespace::pg_catalog.regnamespace::pg_catalog.text", "es.stxname", "pg_catalog.pg_statistics_obj_is_visible(es.oid)"),
            ),
        ),
        "db" => l(
            "List of tablespaces",
            format!(
                "SELECT spcname AS \"Name\", pg_catalog.pg_get_userbyid(spcowner) AS \"Owner\", \
                 pg_catalog.pg_tablespace_location(oid) AS \"Location\"{} FROM pg_catalog.pg_tablespace WHERE {} ORDER BY 1",
                plus(
                    v,
                    "pg_catalog.array_to_string(spcacl, E'\\n') AS \"Access privileges\", spcoptions AS \"Options\", \
                     pg_catalog.pg_size_pretty(pg_catalog.pg_tablespace_size(oid)) AS \"Size\", \
                     pg_catalog.shobj_description(oid, 'pg_tablespace') AS \"Description\""
                ),
                pattern(pat, "", "spcname", "", m),
            ),
        ),
        "dconfig" => l(
            "List of configuration parameters",
            format!(
                "SELECT s.name AS \"Parameter\", pg_catalog.current_setting(s.name) AS \"Value\"{} \
                 FROM pg_catalog.pg_settings s WHERE {} ORDER BY 1",
                plus(v, "s.vartype AS \"Type\", s.context AS \"Context\""),
                match pat {
                    Some(p) => format!("pg_catalog.lower(s.name) ~ {}", lit(&format!("^({})$", regex::escape(&p.to_lowercase()).replace("\\*", ".*").replace("\\?", ".")))),
                    None => "s.source <> 'default' AND s.setting IS DISTINCT FROM s.boot_val".into(),
                },
            ),
        ),
        "drds" => {
            let role = args.first().map(|r| pattern(Some(r), "", "r.rolname", "", m)).unwrap_or_else(|| "TRUE".into());
            let db = args.get(1).map(|d| pattern(Some(d), "", "d.datname", "", m)).unwrap_or_else(|| "TRUE".into());
            l(
                "List of settings",
                format!(
                    "SELECT r.rolname AS \"Role\", d.datname AS \"Database\", pg_catalog.array_to_string(s.setconfig, E'\\n') AS \"Settings\" \
                     FROM pg_catalog.pg_db_role_setting s LEFT JOIN pg_catalog.pg_database d ON d.oid = s.setdatabase \
                     LEFT JOIN pg_catalog.pg_roles r ON r.oid = s.setrole WHERE {role} AND {db} ORDER BY 1, 2"
                ),
            )
        }
        "drg" => l(
            "List of role grants",
            format!(
                "SELECT m.rolname AS \"Role name\", r.rolname AS \"Member of\", {} AS \"Options\", g.rolname AS \"Grantor\" \
                 FROM pg_catalog.pg_roles m JOIN pg_catalog.pg_auth_members pam ON (pam.member = m.oid) \
                 LEFT JOIN pg_catalog.pg_roles r ON (pam.roleid = r.oid) LEFT JOIN pg_catalog.pg_roles g ON (pam.grantor = g.oid) \
                 WHERE {} ORDER BY 1, 2, 4",
                if ver >= 160000 {
                    "pg_catalog.concat_ws(', ', CASE WHEN pam.admin_option THEN 'ADMIN' END, \
                     CASE WHEN pam.inherit_option THEN 'INHERIT' END, CASE WHEN pam.set_option THEN 'SET' END)"
                } else {
                    "CASE WHEN pam.admin_option THEN 'ADMIN' END"
                },
                if pat.is_some() || m.system { pattern(pat, "", "m.rolname", "", m) } else { "m.rolname !~ '^pg_'".into() },
            ),
        ),
        "dRp" => l(
            "List of publications",
            format!(
                "SELECT pubname AS \"Name\", pg_catalog.pg_get_userbyid(pubowner) AS \"Owner\", puballtables AS \"All tables\", \
                 pubinsert AS \"Inserts\", pubupdate AS \"Updates\", pubdelete AS \"Deletes\"{}{} \
                 FROM pg_catalog.pg_publication WHERE {} ORDER BY 1",
                plus(ver >= 110000, "pubtruncate AS \"Truncates\""),
                plus(ver >= 130000, "pubviaroot AS \"Via root\""),
                pattern(pat, "", "pubname", "", m),
            ),
        ),
        "dRs" => l(
            "List of subscriptions",
            format!(
                "SELECT subname AS \"Name\", pg_catalog.pg_get_userbyid(subowner) AS \"Owner\", subenabled AS \"Enabled\", \
                 subpublications AS \"Publication\" FROM pg_catalog.pg_subscription \
                 WHERE subdbid = (SELECT oid FROM pg_catalog.pg_database WHERE datname = pg_catalog.current_database()) AND {} ORDER BY 1",
                pattern(pat, "", "subname", "", m),
            ),
        ),
        "dA" => l(
            "List of access methods",
            format!(
                "SELECT amname AS \"Name\", CASE amtype WHEN 'i' THEN 'Index' WHEN 't' THEN 'Table' END AS \"Type\"{} \
                 FROM pg_catalog.pg_am WHERE {} ORDER BY 1",
                plus(v, "amhandler AS \"Handler\", pg_catalog.obj_description(oid, 'pg_am') AS \"Description\""),
                pattern(pat, "", "amname", "", m),
            ),
        ),
        "dAc" => {
            let am = pat.map(|a| pattern(Some(a), "", "am.amname", "", m)).unwrap_or_else(|| "TRUE".into());
            let ty = args.get(1).map(|t| pattern(Some(t), "", "pg_catalog.format_type(c.opcintype, NULL)", "", m)).unwrap_or_else(|| "TRUE".into());
            l(
                "List of operator classes",
                format!(
                    "SELECT am.amname AS \"AM\", pg_catalog.format_type(c.opcintype, NULL) AS \"Input type\", \
                     CASE WHEN c.opckeytype <> 0 AND c.opckeytype <> c.opcintype THEN pg_catalog.format_type(c.opckeytype, NULL) END AS \"Storage type\", \
                     CASE WHEN pg_catalog.pg_opclass_is_visible(c.oid) THEN c.opcname ELSE n.nspname || '.' || c.opcname END AS \"Operator class\", \
                     CASE WHEN c.opcdefault THEN 'yes' ELSE 'no' END AS \"Default?\" \
                     FROM pg_catalog.pg_opclass c LEFT JOIN pg_catalog.pg_am am ON am.oid = c.opcmethod \
                     LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.opcnamespace WHERE {am} AND {ty} ORDER BY 1, 2, 4"
                ),
            )
        }
        "dAf" => {
            let am = pat.map(|a| pattern(Some(a), "", "am.amname", "", m)).unwrap_or_else(|| "TRUE".into());
            l(
                "List of operator families",
                format!(
                    "SELECT am.amname AS \"AM\", CASE WHEN pg_catalog.pg_opfamily_is_visible(f.oid) THEN f.opfname \
                     ELSE n.nspname || '.' || f.opfname END AS \"Operator family\", \
                     (SELECT pg_catalog.string_agg(pg_catalog.format_type(oc.opcintype, NULL), ', ') FROM pg_catalog.pg_opclass oc \
                      WHERE oc.opcfamily = f.oid) AS \"Applicable types\" \
                     FROM pg_catalog.pg_opfamily f LEFT JOIN pg_catalog.pg_am am ON am.oid = f.opfmethod \
                     LEFT JOIN pg_catalog.pg_namespace n ON n.oid = f.opfnamespace WHERE {am} ORDER BY 1, 2"
                ),
            )
        }
        "dAo" | "dAp" => {
            let am = pat.map(|a| pattern(Some(a), "", "am.amname", "", m)).unwrap_or_else(|| "TRUE".into());
            let fam = args.get(1).map(|f| pattern(Some(f), "", "of.opfname", "", m)).unwrap_or_else(|| "TRUE".into());
            if cmd == "dAo" {
                l(
                    "List of operators of operator families",
                    format!(
                        "SELECT am.amname AS \"AM\", of.opfname AS \"Operator family\", o.amopopr::pg_catalog.regoperator AS \"Operator\", \
                         o.amopstrategy AS \"Strategy\", CASE o.amoppurpose WHEN 'o' THEN 'ordering' WHEN 's' THEN 'search' END AS \"Purpose\" \
                         FROM pg_catalog.pg_amop o JOIN pg_catalog.pg_opfamily of ON of.oid = o.amopfamily \
                         JOIN pg_catalog.pg_am am ON am.oid = of.opfmethod WHERE {am} AND {fam} ORDER BY 1, 2, 4"
                    ),
                )
            } else {
                l(
                    "List of support functions of operator families",
                    format!(
                        "SELECT am.amname AS \"AM\", of.opfname AS \"Operator family\", \
                         pg_catalog.format_type(ap.amproclefttype, NULL) AS \"Registered left type\", \
                         pg_catalog.format_type(ap.amprocrighttype, NULL) AS \"Registered right type\", ap.amprocnum AS \"Number\", \
                         ap.amproc::pg_catalog.regprocedure AS \"Function\" \
                         FROM pg_catalog.pg_amproc ap JOIN pg_catalog.pg_opfamily of ON of.oid = ap.amprocfamily \
                         JOIN pg_catalog.pg_am am ON am.oid = of.opfmethod WHERE {am} AND {fam} ORDER BY 1, 2, 3, 4, 5"
                    ),
                )
            }
        }
        "dc" => l(
            "List of conversions",
            format!(
                "SELECT n.nspname AS \"Schema\", c.conname AS \"Name\", pg_catalog.pg_encoding_to_char(c.conforencoding) AS \"Source\", \
                 pg_catalog.pg_encoding_to_char(c.contoencoding) AS \"Destination\", CASE WHEN c.condefault THEN 'yes' ELSE 'no' END AS \"Default?\"{} \
                 FROM pg_catalog.pg_conversion c JOIN pg_catalog.pg_namespace n ON n.oid = c.connamespace WHERE {} ORDER BY 1, 2",
                plus(v, "pg_catalog.obj_description(c.oid, 'pg_conversion') AS \"Description\""),
                p("n.nspname", "c.conname", "pg_catalog.pg_conversion_is_visible(c.oid)"),
            ),
        ),
        "dC" => l(
            "List of casts",
            format!(
                "SELECT pg_catalog.format_type(castsource, NULL) AS \"Source type\", pg_catalog.format_type(casttarget, NULL) AS \"Target type\", \
                 CASE WHEN c.castmethod = 'b' THEN '(binary coercible)' WHEN c.castmethod = 'i' THEN '(with inout)' ELSE p.proname END AS \"Function\", \
                 CASE WHEN c.castcontext = 'e' THEN 'no' WHEN c.castcontext = 'a' THEN 'in assignment' ELSE 'yes' END AS \"Implicit?\"{} \
                 FROM pg_catalog.pg_cast c LEFT JOIN pg_catalog.pg_proc p ON c.castfunc = p.oid WHERE {} ORDER BY 1, 2",
                plus(v, "pg_catalog.obj_description(c.oid, 'pg_cast') AS \"Description\""),
                match pat {
                    Some(pt) => {
                        let re = lit(&format!("^({})$", regex::escape(pt).replace("\\*", ".*").replace("\\?", ".")));
                        format!("(pg_catalog.format_type(castsource, NULL) ~ {re} OR pg_catalog.format_type(casttarget, NULL) ~ {re})")
                    }
                    None => "TRUE".into(),
                },
            ),
        ),
        "dd" => l(
            "Object descriptions",
            format!(
                "SELECT * FROM ( \
                 SELECT n.nspname AS \"Schema\", pgc.conname AS \"Name\", 'table constraint' AS \"Object\", d.description AS \"Description\" \
                 FROM pg_catalog.pg_constraint pgc JOIN pg_catalog.pg_class c ON c.oid = pgc.conrelid \
                 LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace \
                 JOIN pg_catalog.pg_description d ON d.objoid = pgc.oid AND d.classoid = 'pg_catalog.pg_constraint'::pg_catalog.regclass \
                 UNION ALL \
                 SELECT n.nspname, t.tgname, 'trigger', d.description FROM pg_catalog.pg_trigger t \
                 JOIN pg_catalog.pg_class c ON c.oid = t.tgrelid LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace \
                 JOIN pg_catalog.pg_description d ON d.objoid = t.oid AND d.classoid = 'pg_catalog.pg_trigger'::pg_catalog.regclass \
                 UNION ALL \
                 SELECT n.nspname, r.rulename, 'rule', d.description FROM pg_catalog.pg_rewrite r \
                 JOIN pg_catalog.pg_class c ON c.oid = r.ev_class LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace \
                 JOIN pg_catalog.pg_description d ON d.objoid = r.oid AND d.classoid = 'pg_catalog.pg_rewrite'::pg_catalog.regclass \
                 WHERE r.rulename <> '_RETURN') x WHERE {} ORDER BY 1, 2, 3",
                p("\"Schema\"", "\"Name\"", ""),
            ),
        ),
        "do" => {
            let mut cond = vec![p("n.nspname", "o.oprname", "pg_catalog.pg_operator_is_visible(o.oid)")];
            for (i, side) in ["o.oprleft", "o.oprright"].iter().enumerate() {
                if let Some(t) = args.get(i + 1) {
                    cond.push(format!("pg_catalog.format_type({side}, NULL) ~ {}", lit(&format!("^({})$", regex::escape(t)))));
                }
            }
            l(
                "List of operators",
                format!(
                    "SELECT n.nspname AS \"Schema\", o.oprname AS \"Name\", \
                     CASE WHEN o.oprkind = 'l' THEN NULL ELSE pg_catalog.format_type(o.oprleft, NULL) END AS \"Left arg type\", \
                     CASE WHEN o.oprkind = 'r' THEN NULL ELSE pg_catalog.format_type(o.oprright, NULL) END AS \"Right arg type\", \
                     pg_catalog.format_type(o.oprresult, NULL) AS \"Result type\"{}, \
                     coalesce(pg_catalog.obj_description(o.oid, 'pg_operator'), pg_catalog.obj_description(o.oprcode, 'pg_proc')) AS \"Description\" \
                     FROM pg_catalog.pg_operator o LEFT JOIN pg_catalog.pg_namespace n ON n.oid = o.oprnamespace WHERE {} ORDER BY 1, 2, 3, 4",
                    plus(v, "o.oprcode AS \"Function\""),
                    cond.join(" AND "),
                ),
            )
        }
        "dF" | "dFd" | "dFp" | "dFt" => {
            let (title, table, name, ns) = match cmd {
                "dF" => ("List of text search configurations", "pg_catalog.pg_ts_config", "cfgname", "cfgnamespace"),
                "dFd" => ("List of text search dictionaries", "pg_catalog.pg_ts_dict", "dictname", "dictnamespace"),
                "dFp" => ("List of text search parsers", "pg_catalog.pg_ts_parser", "prsname", "prsnamespace"),
                _ => ("List of text search templates", "pg_catalog.pg_ts_template", "tmplname", "tmplnamespace"),
            };
            let visible = match cmd {
                "dF" => "pg_catalog.pg_ts_config_is_visible(x.oid)",
                "dFd" => "pg_catalog.pg_ts_dict_is_visible(x.oid)",
                "dFp" => "pg_catalog.pg_ts_parser_is_visible(x.oid)",
                _ => "pg_catalog.pg_ts_template_is_visible(x.oid)",
            };
            let extra = match (cmd, v) {
                ("dFd", true) => ", (SELECT nt.nspname || '.' || t.tmplname FROM pg_catalog.pg_ts_template t \
                                  LEFT JOIN pg_catalog.pg_namespace nt ON nt.oid = t.tmplnamespace WHERE x.dicttemplate = t.oid) AS \"Template\", \
                                  x.dictinitoption AS \"Init options\"",
                ("dFt", true) => ", x.tmplinit::pg_catalog.regproc AS \"Init\", x.tmpllexize::pg_catalog.regproc AS \"Lexize\"",
                _ => "",
            };
            l(
                title,
                format!(
                    "SELECT n.nspname AS \"Schema\", x.{name} AS \"Name\"{extra}, pg_catalog.obj_description(x.oid, {}) AS \"Description\" \
                     FROM {table} x LEFT JOIN pg_catalog.pg_namespace n ON n.oid = x.{ns} WHERE {} ORDER BY 1, 2",
                    lit(table.trim_start_matches("pg_catalog.")),
                    p("n.nspname", &format!("x.{name}"), visible),
                ),
            )
        }
        "des" => l(
            "List of foreign servers",
            format!(
                "SELECT s.srvname AS \"Name\", pg_catalog.pg_get_userbyid(s.srvowner) AS \"Owner\", f.fdwname AS \"Foreign-data wrapper\"{} \
                 FROM pg_catalog.pg_foreign_server s JOIN pg_catalog.pg_foreign_data_wrapper f ON f.oid = s.srvfdw WHERE {} ORDER BY 1",
                plus(
                    v,
                    "pg_catalog.array_to_string(s.srvacl, E'\\n') AS \"Access privileges\", s.srvtype AS \"Type\", s.srvversion AS \"Version\", \
                     pg_catalog.array_to_string(s.srvoptions, ', ') AS \"FDW options\", pg_catalog.obj_description(s.oid, 'pg_foreign_server') AS \"Description\""
                ),
                pattern(pat, "", "s.srvname", "", m),
            ),
        ),
        "det" => l(
            "List of foreign tables",
            format!(
                "SELECT n.nspname AS \"Schema\", c.relname AS \"Table\", s.srvname AS \"Server\"{} \
                 FROM pg_catalog.pg_foreign_table ft JOIN pg_catalog.pg_class c ON c.oid = ft.ftrelid \
                 JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace JOIN pg_catalog.pg_foreign_server s ON s.oid = ft.ftserver \
                 WHERE {} ORDER BY 1, 2",
                plus(v, "pg_catalog.array_to_string(ft.ftoptions, ', ') AS \"FDW options\", pg_catalog.obj_description(c.oid, 'pg_class') AS \"Description\""),
                p("n.nspname", "c.relname", "pg_catalog.pg_table_is_visible(c.oid)"),
            ),
        ),
        "deu" => l(
            "List of user mappings",
            format!(
                "SELECT um.srvname AS \"Server\", um.usename AS \"User name\"{} FROM pg_catalog.pg_user_mappings um WHERE {} ORDER BY 1, 2",
                plus(v, "pg_catalog.array_to_string(um.umoptions, ', ') AS \"FDW options\""),
                pattern(pat, "", "um.usename", "", m),
            ),
        ),
        "dew" => l(
            "List of foreign-data wrappers",
            format!(
                "SELECT fdw.fdwname AS \"Name\", pg_catalog.pg_get_userbyid(fdw.fdwowner) AS \"Owner\", \
                 fdw.fdwhandler::pg_catalog.regproc AS \"Handler\", fdw.fdwvalidator::pg_catalog.regproc AS \"Validator\"{} \
                 FROM pg_catalog.pg_foreign_data_wrapper fdw WHERE {} ORDER BY 1",
                plus(
                    v,
                    "pg_catalog.array_to_string(fdwacl, E'\\n') AS \"Access privileges\", pg_catalog.array_to_string(fdw.fdwoptions, ', ') AS \"FDW options\", \
                     pg_catalog.obj_description(fdw.oid, 'pg_foreign_data_wrapper') AS \"Description\""
                ),
                pattern(pat, "", "fdw.fdwname", "", m),
            ),
        ),
        "dl" | "lo_list" => l(
            "Large objects",
            "SELECT oid AS \"ID\", pg_catalog.pg_get_userbyid(lomowner) AS \"Owner\", \
             pg_catalog.obj_description(oid, 'pg_largeobject') AS \"Description\" FROM pg_catalog.pg_largeobject_metadata ORDER BY oid"
                .into(),
        ),
        _ => None,
    }
}

/// `\sf` / `\ef`: a function's OID expression (by name or with argument types).
pub fn function_oid(name: &str) -> Result<String> {
    if name.trim().is_empty() {
        bail!("function name is required");
    }
    Ok(if name.contains('(') {
        format!("{}::pg_catalog.regprocedure", lit(name))
    } else {
        format!("{}::pg_catalog.regproc", lit(name))
    })
}

/// psql's `\sf+` / `\sv+` numbering: a function's header lines are not numbered
/// and its body starts at 1 (the line starting with `AS`); views from the first line.
pub fn number_lines(def: &str, function: bool) -> String {
    let mut out = Vec::new();
    let mut n = 0;
    let mut in_body = !function;
    for line in def.lines() {
        if !in_body && (line.starts_with("AS ") || line.starts_with("BEGIN ATOMIC")) {
            in_body = true;
        }
        if in_body {
            n += 1;
            out.push(format!("{n:<7} {line}"));
        } else {
            out.push(format!("        {line}"));
        }
    }
    out.join("\n")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn modifiers() {
        assert_eq!(parse_mods("dtS+"), ("dt", Mods { system: true, verbose: true, expanded: false }));
        assert_eq!(parse_mods("dx"), ("dx", Mods::default()));
        assert_eq!(parse_mods("dx+"), ("dx", Mods { verbose: true, ..Mods::default() }));
        assert_eq!(parse_mods("dS"), ("d", Mods { system: true, ..Mods::default() }));
        assert_eq!(parse_mods("dRs"), ("dRs", Mods::default()));
        assert_eq!(parse_mods("dtx"), ("dt", Mods { expanded: true, ..Mods::default() }));
        assert_eq!(parse_mods("l+"), ("l", Mods { verbose: true, ..Mods::default() }));
    }

    #[test]
    fn patterns() {
        let m = Mods::default();
        assert_eq!(pattern(None, "n.nspname", "c.relname", "vis", m), "vis AND n.nspname <> 'pg_catalog' AND n.nspname <> 'information_schema' AND n.nspname !~ '^pg_toast'");
        assert_eq!(pattern(None, "n.nspname", "c.relname", "vis", Mods { system: true, ..m }), "vis");
        assert_eq!(pattern(Some("us*"), "n.nspname", "c.relname", "vis", m), "c.relname ~ '^(us.*)$' AND vis");
        assert_eq!(pattern(Some("app.u?"), "n.nspname", "c.relname", "vis", m), "n.nspname ~ '^(app)$' AND c.relname ~ '^(u.)$'");
    }

    #[test]
    fn every_command_builds() {
        let cmds = [
            "l", "dn", "du", "df", "dfa", "dfnt", "da", "dx", "dT", "dD", "dp", "z", "ddp", "dy", "dL", "dO", "dP", "dPi", "dX", "db",
            "dconfig", "drds", "drg", "dRp", "dRs", "dA", "dAc", "dAf", "dAo", "dAp", "dc", "dC", "dd", "do", "dF", "dFd", "dFp", "dFt",
            "des", "det", "deu", "dew", "dl",
        ];
        for c in cmds {
            for ver in [100000, 160000, 180000] {
                let l = listing(c, &[], Mods { verbose: true, ..Mods::default() }, ver).unwrap_or_else(|| panic!("{c}")).unwrap();
                assert!(l.sql.starts_with("SELECT"), "{c}");
                assert_eq!(l.sql.matches('(').count(), l.sql.matches(')').count(), "{c}: unbalanced parentheses");
            }
        }
        assert!(listing("nope", &[], Mods::default(), 180000).is_none());
        assert!(relations("ti", None, Mods::default(), 180000).sql.contains("c.relkind IN ('r','p','i','I')"));
    }

    #[test]
    fn numbering() {
        let def = "CREATE OR REPLACE FUNCTION f()\n RETURNS int\n LANGUAGE sql\nAS $function$\nselect 1\n$function$";
        assert_eq!(
            number_lines(def, true),
            "        CREATE OR REPLACE FUNCTION f()\n         RETURNS int\n         LANGUAGE sql\n1       AS $function$\n2       select 1\n3       $function$"
        );
    }
}
