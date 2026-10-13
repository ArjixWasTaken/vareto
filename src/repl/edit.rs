//! `\e` / `edit`: change text in the user's editor.

use anyhow::{bail, Context, Result};

/// The editor command: `$VISUAL`, then `$EDITOR`, else `vi`.
fn editor() -> String {
    ["VISUAL", "EDITOR"]
        .iter()
        .find_map(|v| super::get_env(v).filter(|e| !e.trim().is_empty()))
        .unwrap_or_else(|| "vi".into())
}

/// Open `text` in the editor and return what was saved.
pub fn edit_text(text: &str, extension: &str) -> Result<String> {
    let path = std::env::temp_dir().join(format!("vareto-edit-{}.{extension}", std::process::id()));
    std::fs::write(&path, text).with_context(|| format!("writing {}", path.display()))?;
    let res = edit_file(&path.to_string_lossy(), None).and_then(|()| Ok(std::fs::read_to_string(&path)?));
    std::fs::remove_file(&path).ok();
    res
}

/// Open a file in the editor (at `line`, for editors that take `+N`).
pub fn edit_file(path: &str, line: Option<usize>) -> Result<()> {
    let ed = editor();
    let at = line.map(|n| format!(" +{n}")).unwrap_or_default();
    // Through the shell, so EDITOR="code --wait" works.
    let status = super::shell_command(&format!("{ed}{at} \"$1\""))
        .arg("vareto-edit")
        .arg(path)
        .status()
        .with_context(|| format!("could not run the editor ({ed})"))?;
    if !status.success() {
        bail!("the editor ({ed}) exited with {status}");
    }
    Ok(())
}
