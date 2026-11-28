//! Minimal CSV writer for console exports.
//!
//! Only RFC 4180 needs are covered: fields containing a comma, quote, CR or LF
//! are quoted and inner quotes are doubled. A UTF-8 BOM is prepended so Excel
//! opens Chinese headers correctly.

pub const BOM: &str = "\u{feff}";

pub fn to_csv(headers: &[&str], rows: &[Vec<String>]) -> String {
    let mut out = String::from(BOM);
    out.push_str(&join(headers.iter().map(|value| escape(value)).collect()));
    out.push_str("\r\n");
    for row in rows {
        out.push_str(&join(row.iter().map(|value| escape(value)).collect()));
        out.push_str("\r\n");
    }
    out
}

fn join(fields: Vec<String>) -> String {
    fields.join(",")
}

fn escape(value: &str) -> String {
    if value.contains(',') || value.contains('"') || value.contains('\n') || value.contains('\r') {
        format!("\"{}\"", value.replace('"', "\"\""))
    } else {
        value.to_owned()
    }
}

/// Renders `None` as an empty field and `Some` via `to_string`.
pub fn cell<T: ToString>(value: Option<T>) -> String {
    value.map(|value| value.to_string()).unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn plain_values_are_not_quoted() {
        let csv = to_csv(
            &["code", "price"],
            &[vec!["sz000001".into(), "12.3".into()]],
        );
        assert!(csv.starts_with(BOM));
        assert!(csv.contains("code,price\r\nsz000001,12.3\r\n"));
    }

    #[test]
    fn special_characters_are_escaped() {
        let csv = to_csv(
            &["name", "note"],
            &[vec!["平安,银行".into(), "he said \"hi\"\nnext".into()]],
        );
        assert!(csv.contains("\"平安,银行\""));
        assert!(csv.contains("\"he said \"\"hi\"\"\nnext\""));
    }

    #[test]
    fn missing_values_become_empty_cells() {
        assert_eq!(cell(None::<f64>), "");
        assert_eq!(cell(Some(1.5)), "1.5");
        let csv = to_csv(&["a", "b"], &[vec![String::new(), "".into()]]);
        assert!(csv.contains("a,b\r\n,\r\n"));
    }
}
