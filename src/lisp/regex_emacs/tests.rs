use super::*;

/// A buffer-free environment: ASCII syntax, Unicode case and classes.
#[derive(Default)]
struct TestEnv {
    point: Option<usize>,
}

impl MatchEnv for TestEnv {
    fn translate(&mut self, c: u32) -> u32 {
        char::from_u32(c)
            .and_then(|ch| ch.to_lowercase().next())
            .map_or(c, |lower| lower as u32)
    }

    fn syntax(&mut self, c: u32, _at: usize) -> u8 {
        self.buffer_syntax(c)
    }

    fn buffer_syntax(&mut self, c: u32) -> u8 {
        match char::from_u32(c) {
            Some(' ' | '\t' | '\n') => syntax_code::WHITESPACE,
            Some('_' | '-') => syntax_code::SYMBOL,
            Some(ch) if ch.is_alphanumeric() => syntax_code::WORD,
            _ => syntax_code::PUNCT,
        }
    }

    fn has_category(&mut self, _c: u32, _category: u8) -> bool {
        false
    }

    fn word_boundary_p(&mut self, _c1: u32, _c2: u32) -> bool {
        false
    }

    fn alphabeticp(&mut self, c: u32) -> bool {
        char::from_u32(c).is_some_and(char::is_alphabetic)
    }

    fn alphanumericp(&mut self, c: u32) -> bool {
        char::from_u32(c).is_some_and(char::is_alphanumeric)
    }

    fn graphicp(&mut self, c: u32) -> bool {
        char::from_u32(c).is_some_and(|ch| !ch.is_whitespace() && !ch.is_control())
    }

    fn printablep(&mut self, c: u32) -> bool {
        char::from_u32(c).is_some_and(|ch| !ch.is_control())
    }

    fn blankp(&mut self, c: u32) -> bool {
        char::from_u32(c).is_some_and(|ch| ch.is_whitespace() && ch != '\n')
    }

    fn lowercasep(&mut self, c: u32) -> bool {
        char::from_u32(c).is_some_and(char::is_lowercase)
    }

    fn uppercasep(&mut self, c: u32) -> bool {
        char::from_u32(c).is_some_and(char::is_uppercase)
    }

    fn point(&self) -> Option<usize> {
        self.point
    }

    fn side_table_char(&mut self, _at: usize) -> Option<u32> {
        None
    }

    fn maybe_quit(&mut self) -> Result<(), crate::lisp::types::LispError> {
        Ok(())
    }
}

fn codes(pattern: &str) -> Vec<u32> {
    pattern.chars().map(|c| c as u32).collect()
}

fn compiled(pattern: &str, posix: bool, fold: bool) -> Pattern {
    compile(
        &codes(pattern),
        true,
        posix,
        None,
        fold,
        &mut TestEnv::default(),
    )
    .unwrap_or_else(|error| panic!("{pattern:?}: {}", error.message()))
}

/// The groups of the first match, as character offsets.
fn search_with(
    pattern: &str,
    text: &str,
    posix: bool,
    fold: bool,
) -> Option<Vec<Option<(usize, usize)>>> {
    let mut pattern = compiled(pattern, posix, fold);
    let t = Text::from_str(text, true, false);
    let regs = re_search(
        &mut pattern,
        &t,
        0,
        t.len() as isize,
        t.len(),
        &mut TestEnv::default(),
    )
    .expect("no overflow")?;
    Some(
        regs.regs
            .into_iter()
            .map(|reg| reg.map(|(start, end)| (t.char_offset(start), t.char_offset(end))))
            .collect(),
    )
}

fn search(pattern: &str, text: &str) -> Option<(usize, usize)> {
    search_with(pattern, text, false, false).map(|regs| regs[0].expect("group 0"))
}

fn groups(pattern: &str, text: &str) -> Vec<Option<(usize, usize)>> {
    search_with(pattern, text, false, false).expect("a match")
}

#[test]
fn literals_and_dot() {
    assert_eq!(search("foo", "xxfooyy"), Some((2, 5)));
    assert_eq!(search("foo", "xxfoyy"), None);
    assert_eq!(search("f.o", "xxfaoyy"), Some((2, 5)));
    assert_eq!(search("a.b", "a\nb"), None);
    assert_eq!(search("", "abc"), Some((0, 0)));
    assert_eq!(search("é+", "aééb"), Some((1, 3)));
    assert_eq!(search("日本", "これは日本語"), Some((3, 5)));
}

#[test]
fn repetition_and_alternation() {
    assert_eq!(search("ab*c", "xacx"), Some((1, 3)));
    assert_eq!(search("ab*c", "xabbbcx"), Some((1, 6)));
    assert_eq!(search("ab+c", "xacx abcx"), Some((5, 8)));
    assert_eq!(search("ab?c", "abbc ac"), Some((5, 7)));
    assert_eq!(search("a*a", "aaa"), Some((0, 3)));
    assert_eq!(search("x\\(a\\|b\\)+c", "zxabac"), Some((1, 6)));
    assert_eq!(search("cat\\|dog", "hotdog"), Some((3, 6)));
    assert_eq!(search("a.*b", "a1b2b3"), Some((0, 5)));
    assert_eq!(search("a.*?b", "a1b2b3"), Some((0, 3)));
    assert_eq!(search("a\\{2,3\\}", "a aa aaaa"), Some((2, 4)));
    assert_eq!(search("a\\{3\\}", "aa aaaa"), Some((3, 6)));
    assert_eq!(search("\\(ab\\)\\{2,\\}", "ab abab"), Some((3, 7)));
    assert_eq!(search("a\\{,2\\}b", "aaab"), Some((1, 4)));
}

#[test]
fn groups_and_back_references() {
    assert_eq!(
        groups("\\(a+\\)\\(b*\\)c", "xaabbc"),
        vec![Some((1, 6)), Some((1, 3)), Some((3, 5))]
    );
    assert_eq!(
        groups("\\(a\\)\\|\\(b\\)", "b"),
        vec![Some((0, 1)), None, Some((0, 1))]
    );
    assert_eq!(search("\\(a+\\)b\\1", "aab aaba"), Some((5, 8)));
    assert_eq!(
        groups("\\(?:x\\)\\(?3:y\\)", "xy"),
        vec![Some((0, 2)), None, None, Some((1, 2))]
    );
    assert_eq!(
        groups("\\(x?\\)*y\\1z", "xxyz"),
        vec![Some((0, 4)), Some((2, 2))]
    );
}

#[test]
fn bracket_expressions() {
    assert_eq!(search("[a-c]+", "xxbcay"), Some((2, 5)));
    assert_eq!(search("[^a-c]+", "abxyc"), Some((2, 4)));
    assert_eq!(search("[]a]+", "x]a]"), Some((1, 4)));
    assert_eq!(search("[a-]+", "x-a-"), Some((1, 4)));
    assert_eq!(search("[[:alpha:]]+", "12éab3"), Some((2, 5)));
    assert_eq!(search("[[:digit:]]+", "ab123"), Some((2, 5)));
    assert_eq!(search("[[:space:]]+", "ab \t c"), Some((2, 5)));
    assert_eq!(search("[^[:space:]]+", "  ab "), Some((2, 4)));
    assert_eq!(search("[α-ω]+", "abγδe"), Some((2, 4)));
}

#[test]
fn anchors() {
    assert_eq!(search("^b", "ab\nb"), Some((3, 4)));
    assert_eq!(search("a$", "ab\na"), Some((3, 4)));
    assert_eq!(search("a$", "a\nb"), Some((0, 1)));
    assert_eq!(search("\\`a", "ba"), None);
    assert_eq!(search("a\\'", "ab a"), Some((3, 4)));
    assert_eq!(search("x^", "x^"), Some((0, 2)));
    assert_eq!(search("$x", "$x"), Some((0, 2)));
}

#[test]
fn word_and_symbol_boundaries() {
    assert_eq!(search("\\bfoo\\b", "foobar foo"), Some((7, 10)));
    assert_eq!(search("\\<bar", "foobar bar"), Some((7, 10)));
    assert_eq!(search("foo\\>", "foobar foo"), Some((7, 10)));
    assert_eq!(search("\\_<a-b\\_>", "xa-b a-b"), Some((5, 8)));
    assert_eq!(search("\\Bo", "o fo"), Some((3, 4)));
    assert_eq!(search("\\w+", "  ab1 "), Some((2, 5)));
    assert_eq!(search("\\W+", "ab, c"), Some((2, 4)));
    assert_eq!(search("\\s-+", "ab  c"), Some((2, 4)));
    assert_eq!(search("\\s_", "ab_c"), Some((2, 3)));
}

#[test]
fn case_folding() {
    assert_eq!(
        search_with("foo", "xFOO", false, true).map(|r| r[0]),
        Some(Some((1, 4)))
    );
    assert_eq!(
        search_with("[a-c]+", "xBCa", false, true).map(|r| r[0]),
        Some(Some((1, 4)))
    );
    assert_eq!(search("foo", "xFOO"), None);
}

#[test]
fn posix_longest_match() {
    assert_eq!(
        search_with("a\\|ab", "ab", true, false).map(|r| r[0]),
        Some(Some((0, 2)))
    );
    assert_eq!(
        search_with("a\\|ab", "ab", false, false).map(|r| r[0]),
        Some(Some((0, 1)))
    );
    // A POSIX pattern ends without `succeed'; a loop at its end must not
    // read past it (GNU: groups (1 4) (1 2) (2 4)).
    assert_eq!(
        search_with("\\(a\\|aa\\)\\(a*\\)", "xaaa", true, false),
        Some(vec![Some((1, 4)), Some((1, 2)), Some((2, 4))])
    );
}

#[test]
fn backward_search() {
    let mut pattern = compiled("ab", false, false);
    let text = "ab ab ab";
    let t = Text::from_str(text, true, false);
    let found = re_search(&mut pattern, &t, 7, -7, 8, &mut TestEnv::default())
        .expect("no overflow")
        .expect("a match");
    assert_eq!(found.regs[0], Some((6, 8)));
    let found = re_search(&mut pattern, &t, 5, -5, 5, &mut TestEnv::default())
        .expect("no overflow")
        .expect("a match");
    assert_eq!(found.regs[0], Some((3, 5)));
    let mut pattern = compiled("é", false, false);
    let t = Text::from_str("aéaé", true, false);
    let found = re_search(
        &mut pattern,
        &t,
        t.len(),
        -(t.len() as isize),
        t.len(),
        &mut TestEnv::default(),
    )
    .expect("no overflow")
    .expect("a match");
    assert_eq!(found.regs[0], Some((4, 6)));
}

#[test]
fn match_at_position() {
    let mut pattern = compiled("b+", false, false);
    let t = Text::from_str("abbc", true, false);
    let env = &mut TestEnv::default();
    assert_eq!(
        re_match(&mut pattern, &t, 0, 4, env).expect("no overflow"),
        None
    );
    let regs = re_match(&mut pattern, &t, 1, 4, env)
        .expect("no overflow")
        .expect("a match");
    assert_eq!(regs.regs[0], Some((1, 3)));
    let regs = re_match(&mut pattern, &t, 1, 2, env)
        .expect("no overflow")
        .expect("a match");
    assert_eq!(regs.regs[0], Some((1, 2)));
}

#[test]
fn point_operator() {
    let mut pattern = compiled("\\=b", false, false);
    let t = Text::from_str("abab", true, false);
    let env = &mut TestEnv { point: Some(3) };
    let found = re_search(&mut pattern, &t, 0, 4, 4, env)
        .expect("no overflow")
        .expect("a match");
    assert_eq!(found.regs[0], Some((3, 4)));
}

#[test]
fn compile_errors() {
    let error = |pattern: &str| {
        compile(
            &codes(pattern),
            true,
            false,
            None,
            false,
            &mut TestEnv::default(),
        )
        .err()
        .map(RegError::message)
    };
    assert_eq!(error("\\("), Some("Unmatched ( or \\("));
    assert_eq!(error("\\)"), Some("Unmatched ) or \\)"));
    assert_eq!(error("[a"), Some("Unmatched [ or [^"));
    assert_eq!(error("a\\"), Some("Trailing backslash"));
    assert_eq!(error("[[:foo:]]"), Some("Invalid character class name"));
    assert_eq!(error("\\(a\\)\\2"), Some("Invalid back reference"));
    assert_eq!(error("a\\{2"), Some("Unmatched \\{"));
    assert_eq!(error("a\\{3,2\\}"), Some("Invalid content of \\{\\}"));
    assert_eq!(error("[a-"), Some("Premature end of regular expression"));
    assert_eq!(error("*a"), None);
    assert_eq!(error("\\{1\\}"), None);
}

#[test]
fn failure_stack_overflow() {
    let mut pattern = compiled("\\(?:a\\|b\\)*c", false, false);
    let text = "a".repeat(200_000);
    let t = Text::from_str(&text, true, false);
    let result = re_search(&mut pattern, &t, 0, 0, t.len(), &mut TestEnv::default());
    assert!(matches!(result, Err(MatchError::Overflow)));
}

#[test]
fn rope_chunks_are_read_in_place() {
    let mut text = "x".repeat(5000);
    text.push_str("needle");
    text.push_str(&"y".repeat(5000));
    let rope = ropey::Rope::from_str(&text);
    let t = Text::from_rope(rope.slice(..), true, false);
    let mut pattern = compiled("ne+dle\\|zz", false, false);
    let found = re_search(
        &mut pattern,
        &t,
        0,
        t.len() as isize,
        t.len(),
        &mut TestEnv::default(),
    )
    .expect("no overflow")
    .expect("a match");
    assert_eq!(found.regs[0], Some((5000, 5006)));
    let mut pattern = compiled("\\(x+\\)needle\\(y\\{4990,\\}\\)", false, false);
    let found = re_search(
        &mut pattern,
        &t,
        0,
        t.len() as isize,
        t.len(),
        &mut TestEnv::default(),
    )
    .expect("no overflow")
    .expect("a match");
    assert_eq!(found.regs[1], Some((0, 5000)));
    assert_eq!(found.regs[2], Some((5006, 10006)));
}
