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
    use std::io::Write as _;
    // A fresh, private file with a random name (never a predictable path in a
    // shared temp directory). It's removed when `file` drops.
    let mut file = tempfile::Builder::new()
        .prefix("vareto-edit-")
        .suffix(&format!(".{extension}"))
        .tempfile()
        .context("creating a file to edit")?;
    file.write_all(text.as_bytes()).and_then(|()| file.flush()).context("writing the file to edit")?;
    let path = file.path().to_path_buf();
    edit_file(&path.to_string_lossy(), None)?;
    // By path: editors often save by replacing the file.
    Ok(std::fs::read_to_string(&path)?)
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
