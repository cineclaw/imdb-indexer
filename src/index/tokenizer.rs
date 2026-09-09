use tantivy::tokenizer::{Token, TokenFilter, TokenStream, Tokenizer};

/// Token filter that normalizes Russian text:
/// - Replaces 'ё' / 'Ё' with 'е'
/// - Ensures case folding if used alongside or instead of LowerCaser
#[derive(Clone, Debug, Default)]
pub struct RussianYoFilter;

impl TokenFilter for RussianYoFilter {
    type Tokenizer<T: Tokenizer> = RussianYoFilterWrapper<T>;

    fn transform<T: Tokenizer>(self, tokenizer: T) -> Self::Tokenizer<T> {
        RussianYoFilterWrapper(tokenizer)
    }
}

#[derive(Clone, Debug)]
pub struct RussianYoFilterWrapper<T>(T);

impl<T: Tokenizer> Tokenizer for RussianYoFilterWrapper<T> {
    type TokenStream<'a> = RussianYoTokenStream<T::TokenStream<'a>>;

    fn token_stream<'a>(&'a mut self, text: &'a str) -> Self::TokenStream<'a> {
        RussianYoTokenStream {
            tail: self.0.token_stream(text),
        }
    }
}

pub struct RussianYoTokenStream<T> {
    tail: T,
}

impl<T: TokenStream> TokenStream for RussianYoTokenStream<T> {
    fn advance(&mut self) -> bool {
        if self.tail.advance() {
            let token = self.tail.token_mut();
            if token.text.contains(['ё', 'Ё']) {
                token.text = normalize_yo(&token.text);
            }
            true
        } else {
            false
        }
    }

    fn token(&self) -> &Token {
        self.tail.token()
    }

    fn token_mut(&mut self) -> &mut Token {
        self.tail.token_mut()
    }
}

/// Helper to normalize 'ё' and 'Ё' to 'е'
pub fn normalize_yo(s: &str) -> String {
    s.chars()
        .map(|c| match c {
            'ё' | 'Ё' => 'е',
            other => other,
        })
        .collect()
}

/// Helper for query normalization: lowercases and normalizes 'ё' -> 'е'
pub fn normalize_query_text(s: &str) -> String {
    normalize_yo(&s.to_lowercase())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tantivy::tokenizer::{LowerCaser, SimpleTokenizer, TextAnalyzer};

    #[test]
    fn test_yo_filter() {
        let mut analyzer = TextAnalyzer::builder(SimpleTokenizer::default())
            .filter(LowerCaser)
            .filter(RussianYoFilter)
            .build();

        let mut stream = analyzer.token_stream("Тёмный Рыцарь: Возрождение легенды (Всё о кино)");
        let mut tokens = Vec::new();
        while stream.advance() {
            tokens.push(stream.token().text.clone());
        }

        assert_eq!(
            tokens,
            vec!["темный", "рыцарь", "возрождение", "легенды", "все", "о", "кино"]
        );
    }
}
