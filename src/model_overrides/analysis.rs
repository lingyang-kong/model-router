use std::{
    borrow::Cow,
    collections::{BTreeMap, BTreeSet, VecDeque},
};

use anyhow::Context;

use super::ModelOverrides;

// Reverse substitutions can generate infinitely many predecessors. Keep startup
// work bounded; request-time resolution still checks every actual model ID.
const MAX_CANDIDATES: usize = 1024;

pub(super) fn analyze(overrides: &ModelOverrides) -> anyhow::Result<BTreeMap<String, String>> {
    let mut seeds = BTreeSet::new();
    let mut witnesses = BTreeSet::new();
    for rule in &overrides.rules {
        if let Some(source) = &rule.source {
            if source.len() == 1 {
                seeds.insert(source[0].clone());
            } else {
                witnesses.insert(source.join(""));
                witnesses.insert(source.join("x"));
            }
        }
        if rule.captures.is_none() && !rule.target[0].is_empty() {
            seeds.insert(rule.target[0].clone());
        }
    }

    for witness in witnesses.difference(&seeds) {
        if !witness.trim().is_empty() {
            overrides
                .resolve_uncached(witness)
                .with_context(|| format!("Invalid model override for {witness:?}"))?;
        }
    }

    let mut queue: VecDeque<_> = seeds.iter().cloned().map(|seed| (seed, true)).collect();
    let mut seen = seeds;
    let mut resolved = BTreeMap::new();
    while let Some((model, configured)) = queue.pop_front() {
        if model.trim().is_empty() {
            continue;
        }
        let target = if configured {
            overrides.resolve_uncached(&model).map(Some)
        } else {
            overrides.resolve_chain(&model)
        }
        .with_context(|| format!("Invalid model override for {model:?}"))?;
        let Some(target) = target else { continue };
        if let Cow::Owned(target) = target {
            resolved.insert(model.clone(), target);
        }
        for (index, rule) in overrides.rules.iter().enumerate() {
            if seen.len() >= MAX_CANDIDATES {
                break;
            }
            if let Some(previous) = rule.predecessor(&model)
                && !previous.trim().is_empty()
                && overrides.patterns.matches(&previous).contains(&index)
                && seen.insert(previous.clone())
            {
                queue.push_back((previous, false));
            }
        }
    }
    Ok(resolved)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rules(entries: &[(&str, &str)]) -> anyhow::Result<ModelOverrides> {
        ModelOverrides::compile(
            entries
                .iter()
                .map(|(source, target)| (source.to_string(), target.to_string()))
                .collect(),
        )
    }

    #[test]
    fn derives_specific_inputs_through_several_wildcard_rules() {
        let overrides = rules(&[
            ("gpt-5.6-*", "gpt-6-*"),
            ("gpt-6-sol", "gpt-6.1-sol"),
            ("older-*", "gpt-5.6-*"),
        ])
        .unwrap();
        let resolved = analyze(&overrides).unwrap();
        assert_eq!(resolved.get("gpt-5.6-sol").unwrap(), "gpt-6.1-sol");
        assert_eq!(resolved.get("older-sol").unwrap(), "gpt-6.1-sol");
        assert_eq!(resolved.get("gpt-6-sol").unwrap(), "gpt-6.1-sol");
    }

    #[test]
    fn detects_witnessed_cycles_and_growth() {
        assert!(rules(&[("a*", "b*"), ("b*", "a*")]).is_err());
        assert!(rules(&[("a*", "aa*")]).is_err());
        assert!(rules(&[("a*", "one"), ("*x", "two")]).is_err());
    }

    #[test]
    fn accepts_shrinking_rules_and_bounds_reverse_expansion() {
        let overrides = rules(&[("aa*", "a*"), ("a", "")]).unwrap();
        assert_eq!(overrides.resolve_uncached("aaaa").unwrap(), "a");
        assert!(analyze(&overrides).unwrap().len() <= MAX_CANDIDATES);
    }

    #[test]
    fn reverse_derivation_never_changes_greedy_capture_behavior() {
        let overrides = rules(&[("a*b*c", "X*Y*Z"), ("XfooYbarbquxZ", "final")]).unwrap();
        let candidate = "afoobbarbquxc";
        assert!(!overrides.resolved.contains_key(candidate));
        assert_eq!(overrides.resolve(candidate).unwrap(), "XfoobbarYquxZ");
        for (model, target) in &overrides.resolved {
            assert_eq!(overrides.resolve_uncached(model).unwrap().as_ref(), target);
        }
    }
}
