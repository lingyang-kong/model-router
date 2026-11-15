use globset::{Glob, GlobBuilder};

/// Model IDs use whole, case-sensitive globs, with `*` also matching `/`.
pub(crate) fn compile_glob(pattern: &str) -> Result<Glob, globset::Error> {
    GlobBuilder::new(pattern)
        .backslash_escape(true)
        .empty_alternates(true)
        .build()
}
