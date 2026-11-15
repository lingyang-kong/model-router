use std::{
    borrow::Cow,
    collections::{BTreeMap, HashSet},
};

use globset::{GlobSet, GlobSetBuilder};
use regex_automata::meta::Regex;
use serde::{Deserialize, Deserializer, de::Error as _};

use crate::{
    error::{Error, Result},
    model_pattern::compile_glob,
};

mod analysis;

const MAX_OVERRIDE_TRANSITIONS: usize = 64;

#[derive(Default)]
pub(crate) struct ModelOverrides {
    patterns: GlobSet,
    rules: Vec<Rule>,
    resolved: BTreeMap<String, String>,
}

struct Rule {
    pattern: String,
    target: Vec<String>,
    captures: Option<Regex>,
    source: Option<Vec<String>>,
    inverse: Option<Regex>,
}

impl<'de> Deserialize<'de> for ModelOverrides {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> std::result::Result<Self, D::Error> {
        let entries = BTreeMap::<String, String>::deserialize(deserializer)?;
        Self::compile(entries).map_err(|error| D::Error::custom(format!("{error:#}")))
    }
}

impl ModelOverrides {
    fn compile(entries: BTreeMap<String, String>) -> anyhow::Result<Self> {
        let mut builder = GlobSetBuilder::new();
        let mut rules = Vec::with_capacity(entries.len());
        for (pattern, replacement) in entries {
            anyhow::ensure!(
                !pattern.trim().is_empty(),
                "Model override patterns must be nonempty"
            );
            builder.add(compile_glob(&pattern)?);
            let target = split_wildcards(&replacement, false)?;
            for chunk in &target {
                anyhow::ensure!(
                    !chunk.contains(';') && !chunk.bytes().any(|b| b.is_ascii_control()),
                    "Model override targets must not contain semicolons or control characters"
                );
            }
            anyhow::ensure!(
                target.len() > 1 || !target[0].trim().is_empty(),
                "Model override targets must be nonempty"
            );
            let source = split_wildcards(&pattern, true).ok();
            let (captures, inverse) = if target.len() > 1 {
                let source = source.as_ref().ok_or_else(|| {
                    anyhow::anyhow!(
                        "Substitution patterns support literal characters and single stars only"
                    )
                })?;
                anyhow::ensure!(
                    source.len() == target.len(),
                    "Model override wildcard counts must match: {pattern}"
                );
                (
                    Some(template_regex(source)?),
                    Some(template_regex(&target)?),
                )
            } else {
                (None, None)
            };
            rules.push(Rule {
                pattern,
                target,
                captures,
                source,
                inverse,
            });
        }
        let mut overrides = Self {
            patterns: builder.build()?,
            rules,
            resolved: BTreeMap::new(),
        };
        overrides.resolved = analysis::analyze(&overrides)?;
        Ok(overrides)
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.rules.is_empty()
    }

    pub(crate) fn resolve<'a>(&self, model: &'a str) -> Result<Cow<'a, str>> {
        match self.resolved.get(model) {
            Some(target) => Ok(Cow::Owned(target.clone())),
            None => self.resolve_uncached(model),
        }
    }

    pub(crate) fn resolved(&self) -> &BTreeMap<String, String> {
        &self.resolved
    }

    fn resolve_uncached<'a>(&self, model: &'a str) -> Result<Cow<'a, str>> {
        self.resolve_chain(model)?.ok_or_else(|| {
            Error::bad_request(format!(
                "Model override exceeded {MAX_OVERRIDE_TRANSITIONS} steps (possible loop)"
            ))
        })
    }

    /// None means the transition budget was exhausted, rather than a proven cycle.
    fn resolve_chain<'a>(&self, model: &'a str) -> Result<Option<Cow<'a, str>>> {
        let mut current = Cow::Borrowed(model);
        let mut visited = None;
        let mut transitions = 0;
        loop {
            let mut selected: Option<(String, &str)> = None;
            for index in self.patterns.matches(current.as_ref()) {
                let rule = &self.rules[index];
                let target = rule.expand(current.as_ref())?;
                if let Some((previous, pattern)) = &selected {
                    if previous != &target {
                        return Err(Error::bad_request(format!(
                            "Conflicting model overrides {pattern:?} and {:?} for {:?}",
                            rule.pattern, current
                        )));
                    }
                } else {
                    selected = Some((target, &rule.pattern));
                }
            }
            let Some((target, _)) = selected else {
                return Ok(Some(current));
            };
            if target == current {
                return Ok(Some(current));
            }
            if transitions == MAX_OVERRIDE_TRANSITIONS {
                return Ok(None);
            }
            let visited = visited.get_or_insert_with(|| HashSet::from([model.to_owned()]));
            if !visited.insert(target.clone()) {
                return Err(Error::bad_request(format!(
                    "Model override cycle at {target:?}"
                )));
            }
            current = Cow::Owned(target);
            transitions += 1;
        }
    }
}

impl Rule {
    fn expand(&self, model: &str) -> Result<String> {
        let result = match &self.captures {
            Some(regex) => substitute(regex, &self.target, model)
                .ok_or_else(|| Error::bad_request("Model override capture did not match"))?,
            None => self.target[0].clone(),
        };
        if result.trim().is_empty()
            || result.contains(';')
            || result.bytes().any(|b| b.is_ascii_control())
        {
            return Err(Error::bad_request(
                "Model override produced an invalid target",
            ));
        }
        Ok(result)
    }

    fn predecessor(&self, target: &str) -> Option<String> {
        let source = substitute(self.inverse.as_ref()?, self.source.as_ref()?, target)?;
        // Reverse captures can be ambiguous; accept only verified forward images.
        (self.expand(&source).ok()?.as_str() == target).then_some(source)
    }
}

fn template_regex(chunks: &[String]) -> anyhow::Result<Regex> {
    Ok(Regex::new(&format!(
        "(?s)^{}$",
        chunks
            .iter()
            .map(|chunk| regex_syntax::escape(chunk))
            .collect::<Vec<_>>()
            .join("(.*)")
    ))?)
}

fn substitute(regex: &Regex, chunks: &[String], input: &str) -> Option<String> {
    let mut captures = regex.create_captures();
    regex.captures(input, &mut captures);
    let mut result = chunks[0].clone();
    for (index, suffix) in chunks.iter().enumerate().skip(1) {
        result.push_str(input.get(captures.get_group(index)?.range())?);
        result.push_str(suffix);
    }
    Some(result)
}

fn split_wildcards(value: &str, source: bool) -> anyhow::Result<Vec<String>> {
    let mut chunks = vec![String::new()];
    let mut chars = value.chars();
    while let Some(ch) = chars.next() {
        match ch {
            '\\' => chunks.last_mut().unwrap().push(
                chars
                    .next()
                    .ok_or_else(|| anyhow::anyhow!("Trailing escape in model override"))?,
            ),
            '*' => {
                if source && chunks.len() > 1 && chunks.last().unwrap().is_empty() {
                    anyhow::bail!("Adjacent stars are not supported in substitution patterns");
                }
                chunks.push(String::new());
            }
            '?' | '[' | ']' | '{' | '}' if source => anyhow::bail!(
                "Substitution patterns support literal characters and single stars only"
            ),
            _ => chunks.last_mut().unwrap().push(ch),
        }
    }
    Ok(chunks)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rules(entries: &[(&str, &str)]) -> anyhow::Result<ModelOverrides> {
        ModelOverrides::compile(
            entries
                .iter()
                .map(|(a, b)| (a.to_string(), b.to_string()))
                .collect(),
        )
    }

    #[test]
    fn supports_fixed_globs_and_substitution() {
        let overrides = rules(&[("gpt-{5,6}.?-*", "fixed"), (r"literal-\*", "escaped")]).unwrap();
        assert_eq!(overrides.resolve("gpt-5.6-luna").unwrap(), "fixed");
        assert_eq!(overrides.resolve("literal-*").unwrap(), "escaped");
        let overrides = rules(&[("old-*-*", "new-*-*"), ("new-sol-x", "final")]).unwrap();
        assert_eq!(overrides.resolve("old-sol-x").unwrap(), "final");
        assert_eq!(overrides.resolve("old-雪/a-猫").unwrap(), "new-雪/a-猫");
        assert_eq!(overrides.resolve("unmatched").unwrap(), "unmatched");
    }

    #[test]
    fn fixed_globs_match_hidden_model_syntax() {
        let overrides = rules(&[
            ("vendor/{模型,other}-[0-2]?/*", "matched"),
            (r"literal-\*", "escaped"),
        ])
        .unwrap();
        for model in ["vendor/模型-1x/part/nested", "vendor/other-2a/雪"] {
            assert_eq!(overrides.resolve(model).unwrap(), "matched", "{model}");
        }
        for model in [
            "vendor/模型-3x/a",
            "vendor/Other-2a/雪",
            "vendor/模型-1/part",
        ] {
            assert_eq!(overrides.resolve(model).unwrap(), model, "{model}");
        }
        assert_eq!(overrides.resolve("literal-*").unwrap(), "escaped");
        assert_eq!(overrides.resolve("literal-x").unwrap(), "literal-x");
    }

    #[test]
    fn transition_limit_allows_exactly_64_changes() {
        let make_chain = |end| {
            (0..end)
                .map(|i| (format!("model-{i}"), format!("model-{}", i + 1)))
                .collect()
        };
        let exact = ModelOverrides::compile(make_chain(MAX_OVERRIDE_TRANSITIONS)).unwrap();
        assert_eq!(exact.resolve("model-0").unwrap(), "model-64");
        assert!(ModelOverrides::compile(make_chain(MAX_OVERRIDE_TRANSITIONS + 1)).is_err());
    }

    #[test]
    fn overlap_and_loops_are_detected() {
        assert_eq!(
            rules(&[("a*", "b"), ("*a", "b")])
                .unwrap()
                .resolve("aa")
                .unwrap(),
            "b"
        );
        assert!(rules(&[("a*", "b"), ("*a", "c")]).is_err());
        assert!(rules(&[("a", "b"), ("b", "a")]).is_err());
        assert_eq!(rules(&[("*", "*")]).unwrap().resolve("a").unwrap(), "a");
        assert!(rules(&[("a*", "b*"), ("b*", "a*")]).is_err());
        assert!(rules(&[("a*", "aa*")]).is_err());
    }

    #[test]
    fn rejects_invalid_patterns_and_substitutions() {
        for (source, target) in [
            ("[", "x"),
            ("a?*", "b*"),
            ("a**", "b*"),
            ("a*", "b**"),
            ("a", " "),
            ("a", "x;y"),
        ] {
            assert!(rules(&[(source, target)]).is_err(), "{source:?} {target:?}");
        }
        let overrides = rules(&[("a*", r"literal-\*")]).unwrap();
        assert_eq!(overrides.resolve("abc").unwrap(), "literal-*");
    }
}
