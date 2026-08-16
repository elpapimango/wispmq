//! Topic Names, Topic Filters, wildcard matching (Section 4.7) and shared
//! subscription parsing (4.8.2).

/// Validate a Topic Name used in a PUBLISH (4.7.3). Topic Names MUST NOT
/// contain wildcards and MUST be at least one character.
pub fn valid_topic_name(topic: &str) -> bool {
    !topic.is_empty() && !topic.contains('+') && !topic.contains('#') && !topic.contains('\u{0000}')
}

/// Validate a Topic Filter used in a SUBSCRIBE (4.7). `#` must be the last
/// character and occupy its own level; `+` must occupy its own level.
pub fn valid_topic_filter(filter: &str) -> bool {
    if filter.is_empty() || filter.contains('\u{0000}') {
        return false;
    }
    let levels: Vec<&str> = filter.split('/').collect();
    let last = levels.len() - 1;
    for (i, level) in levels.iter().enumerate() {
        if level.contains('#') {
            // '#' must be the entire level and the final level.
            if *level != "#" || i != last {
                return false;
            }
        }
        if level.contains('+') && *level != "+" {
            // '+' must be the entire level.
            return false;
        }
    }
    true
}

/// Parsed form of a possibly-shared subscription filter.
pub struct ParsedFilter<'a> {
    /// The share name if this is a shared subscription (`$share/{name}/...`).
    pub share_name: Option<&'a str>,
    /// The actual topic filter used for matching.
    pub filter: &'a str,
}

/// Parse `$share/{ShareName}/{filter}` into its components. Returns `None` if
/// the shared subscription form is malformed.
pub fn parse_filter(input: &str) -> Option<ParsedFilter<'_>> {
    if let Some(rest) = input.strip_prefix("$share/") {
        let slash = rest.find('/')?;
        let share_name = &rest[..slash];
        let filter = &rest[slash + 1..];
        if share_name.is_empty()
            || share_name.contains('+')
            || share_name.contains('#')
            || filter.is_empty()
        {
            return None;
        }
        Some(ParsedFilter {
            share_name: Some(share_name),
            filter,
        })
    } else {
        Some(ParsedFilter {
            share_name: None,
            filter: input,
        })
    }
}

/// Does `filter` match `topic` per the MQTT wildcard rules (4.7.1/4.7.2)?
///
/// - `+` matches exactly one topic level.
/// - `#` matches the parent level and any number of child levels.
/// - A leading `$` topic is not matched by a filter starting with `#` or `+`
///   at the first level (4.7.2).
pub fn matches(filter: &str, topic: &str) -> bool {
    let filter_levels: Vec<&str> = filter.split('/').collect();
    let topic_levels: Vec<&str> = topic.split('/').collect();

    // A topic starting with '$' is excluded from wildcard matches at level 0.
    if let Some(first) = topic_levels.first() {
        if first.starts_with('$') {
            match filter_levels.first() {
                Some(&"#") | Some(&"+") => return false,
                _ => {}
            }
        }
    }

    let mut fi = 0;
    let mut ti = 0;
    while fi < filter_levels.len() {
        let f = filter_levels[fi];
        if f == "#" {
            // Matches the remainder, including zero levels.
            return true;
        }
        if ti >= topic_levels.len() {
            return false;
        }
        if f != "+" && f != topic_levels[ti] {
            return false;
        }
        fi += 1;
        ti += 1;
    }
    // All filter levels consumed; match only if topic is also fully consumed.
    ti == topic_levels.len()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wildcard_plus() {
        assert!(matches("sport/+/player1", "sport/tennis/player1"));
        assert!(!matches("sport/+/player1", "sport/tennis/player2"));
        assert!(matches("+/+", "/finance"));
        assert!(matches("sport/+", "sport/"));
    }

    #[test]
    fn wildcard_hash() {
        assert!(matches("sport/#", "sport"));
        assert!(matches("sport/#", "sport/tennis/player1"));
        assert!(matches("#", "a/b/c"));
    }

    #[test]
    fn dollar_topics() {
        assert!(!matches("#", "$SYS/broker"));
        assert!(!matches("+/monitor", "$SYS/monitor"));
        assert!(matches("$SYS/#", "$SYS/broker/load"));
    }

    #[test]
    fn filter_validation() {
        assert!(valid_topic_filter("sport/tennis/#"));
        assert!(!valid_topic_filter("sport/tennis#"));
        assert!(!valid_topic_filter("sport/#/ranking"));
        assert!(valid_topic_filter("+/+/+"));
        assert!(!valid_topic_filter("sp+ort/#"));
    }

    #[test]
    fn shared_parse() {
        let p = parse_filter("$share/g1/sport/#").unwrap();
        assert_eq!(p.share_name, Some("g1"));
        assert_eq!(p.filter, "sport/#");
        assert!(parse_filter("$share//sport").is_none());
    }
}
