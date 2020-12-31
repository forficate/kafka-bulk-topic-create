pub fn read_line(s: &str) -> Option<(String, Vec<(String, String)>)>{
    let mut state = State::Begin;
    let mut title = String::new();
    let mut a = String::new();
    let mut b = String::new();
    let mut pairs: Vec<(String, String)> = Vec::new();

    for (i, c) in s.chars().enumerate() {
        match state {
            State::Begin =>
                if (c.is_alphabetic()) {
                    if(state == State::Begin) {
                        state = State::MatchingTitle;
                    }
                    title.push(c);
                } else if (!c.is_whitespace()) {
                    return None
                }
            State::MatchingTitle =>
                if (c.is_alphanumeric() || c == '_' || c == '-') {
                    title.push(c);
                } else if (c == ',') {
                    state = State::ParsePair_0
                } else {
                    return None
                }
            State::ParsePair_0 =>
                if (c.is_alphanumeric() || c == '.') {
                    a.push(c);
                } else if (c == '=') {
                    state = State::ParsePair_1;
                } else if (!a.is_empty() && c.is_whitespace()) {
                    return None;
                }
            State::ParsePair_1 =>
                if (c.is_alphanumeric()) {
                    b.push(c);
                } else if (c == ',') {
                    pairs.push((a.to_owned(),b.to_owned()));
                    a.clear();
                    b.clear();
                    state = State::ParsePair_0;
                } else if (!a.is_empty() && !c.is_whitespace()) {
                    return None;
                }
        }
    }

    if(!a.is_empty() && !b.is_empty()) {
        pairs.push((a.to_owned(),b.to_owned()));
        Option::Some((title, pairs))
    } else if(title.is_empty() || !a.is_empty()) {
        Option::None
    } else {
        Option::Some((title, pairs))
    }
}

#[test]
fn test_read_line() {
    assert_eq!(read_line(""), Option::None);
    assert_eq!(read_line("     "), Option::None);
    assert_eq!(read_line("#hello"), Option::None);
    assert_eq!(read_line("#  hello"), Option::None);
    assert_eq!(read_line("123"), Option::None);
    assert_eq!(read_line("Hello World"), Option::None);
    assert_eq!(read_line("hello_world"), Option::Some(("hello_world".to_string(), Vec::new())));
    assert_eq!(read_line("hello_world,jkjk"), Option::None);
    assert_eq!(read_line("hello_world,a=b"), Option::Some(("hello_world".to_string(), vec![("a".to_string(), "b".to_string())])));
    assert_eq!(read_line("hello_world,a=b "), Option::Some(("hello_world".to_string(), vec![("a".to_string(), "b".to_string())])));
}

#[derive(Debug,Eq, PartialEq)]
enum State {
    Begin,
    MatchingTitle,
    ParsePair_0,
    ParsePair_1
}
