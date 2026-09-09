use crate::index::schema::{MovieDoc, MovieSchema};
use crate::index::tokenizer::normalize_query_text;
use tantivy::collector::TopDocs;
use tantivy::query::{
    BooleanQuery, BoostQuery, DisjunctionMaxQuery, FuzzyTermQuery, Occur, PhraseQuery, Query,
    RangeQuery, TermQuery,
};
use tantivy::schema::{IndexRecordOption, Term};
use tantivy::IndexReader;

#[derive(Debug, Clone)]
pub struct SearchParams {
    pub query: String,
    pub limit: usize,
    pub title_type: Option<String>,
    pub year_from: Option<u64>,
    pub year_to: Option<u64>,
    pub min_votes: Option<u64>,
    pub popularity_boost_weight: f64,
    pub rating_boost_weight: f64,
}

impl Default for SearchParams {
    fn default() -> Self {
        Self {
            query: String::new(),
            limit: 20,
            title_type: None,
            year_from: None,
            year_to: None,
            min_votes: None,
            popularity_boost_weight: 2.0,
            rating_boost_weight: 0.5,
        }
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct PosterUrls {
    pub thumbnail: String,
    pub small: String,
    pub medium: String,
    pub large: String,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct SearchHit {
    pub movie: MovieDoc,
    pub score: f32,
    pub bm25_score: f32,
    pub popularity_multiplier: f32,
    pub poster_url: String,
    pub posters: PosterUrls,
}

pub struct SearchEngine {
    reader: std::sync::Arc<IndexReader>,
    schema: MovieSchema,
}

impl SearchEngine {
    pub fn new(reader: std::sync::Arc<IndexReader>, schema: MovieSchema) -> Self {
        Self { reader, schema }
    }

    pub fn search(&self, params: &SearchParams) -> anyhow::Result<Vec<SearchHit>> {
        let searcher = self.reader.searcher();
        let query_str = params.query.trim();

        if query_str.is_empty() {
            return Ok(vec![]);
        }

        let query = self.build_query(query_str, params)?;

        let normalized = normalize_query_text(query_str);
        let detected_year: Option<u64> = normalized
            .split(|c: char| !c.is_alphanumeric())
            .find_map(|t| t.parse::<u64>().ok().filter(|&y| (1880..=2040).contains(&y)));

        let search_terms: Vec<&str> = normalized
            .split(|c: char| !c.is_alphanumeric())
            .filter(|s| {
                if let Some(y) = detected_year {
                    if let Ok(val) = s.parse::<u64>() {
                        if val == y {
                            return false;
                        }
                    }
                }
                !s.is_empty()
            })
            .collect();
        let query_clean_no_year = search_terms.join(" ");

        // Retrieve top candidates (e.g. 200 candidates) for smart popularity re-ranking
        let candidate_limit = (params.limit * 10).clamp(100, 500);
        let top_docs = searcher.search(&query, &TopDocs::with_limit(candidate_limit))?;

        let mut hits = Vec::with_capacity(top_docs.len());

        for (bm25_score, doc_address) in top_docs {
            let doc: tantivy::TantivyDocument = searcher.doc(doc_address)?;
            let movie = self.schema.from_tantivy_doc(&doc);

            // 1. Calculate structural title match quality (Exact Title, Starts-With, Word, Fuzzy)
            let match_quality = calculate_title_match_quality(
                &movie,
                &query_clean_no_year,
                &search_terms,
            );

            // 2. Damped BM25 to prevent length-penalty bias on short vs long titles
            let bm25_factor = (bm25_score.max(0.1f32)).powf(0.6);

            // 3. Smart popularity multiplier
            let pop_mult = calculate_popularity_multiplier(
                movie.num_votes,
                movie.rating,
                params.popularity_boost_weight,
                params.rating_boost_weight,
            );

            // 4. Boost score if the query specifically contained the movie's release year
            let year_mult = if let Some(query_year) = detected_year {
                if movie.year == Some(query_year as u32) {
                    2.5f32
                } else {
                    1.0f32
                }
            } else {
                1.0f32
            };

            let final_score = bm25_factor * match_quality * pop_mult * year_mult;
            let poster_url = format!("/poster/{}?size=w185&v=2", movie.tconst);
            let posters = PosterUrls {
                thumbnail: format!("/poster/{}?size=w92&v=2", movie.tconst),
                small: format!("/poster/{}?size=w154&v=2", movie.tconst),
                medium: format!("/poster/{}?size=w185&v=2", movie.tconst),
                large: format!("/poster/{}?size=w342&v=2", movie.tconst),
            };

            hits.push(SearchHit {
                movie,
                score: final_score,
                bm25_score,
                popularity_multiplier: pop_mult,
                poster_url,
                posters,
            });
        }

        // Re-sort by final smart score descending
        hits.sort_by(|a, b| {
            b.score
                .partial_cmp(&a.score)
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        // Truncate to requested limit
        hits.truncate(params.limit);

        Ok(hits)
    }

    /// Builds a multi-tier query with exact match, phrase match, and accurate adaptive fuzzy match
    pub fn build_query(&self, raw_query: &str, params: &SearchParams) -> anyhow::Result<Box<dyn Query>> {
        let trimmed = raw_query.trim();
        // Exact tconst lookup (e.g. "tt0114746")
        if trimmed.starts_with("tt") && trimmed.len() >= 7 && trimmed[2..].chars().all(|c| c.is_ascii_digit()) {
            let term = Term::from_field_text(self.schema.f_tconst, trimmed);
            return Ok(Box::new(TermQuery::new(term, IndexRecordOption::Basic)));
        }

        let normalized = normalize_query_text(raw_query);
        let terms: Vec<&str> = normalized
            .split(|c: char| !c.is_alphanumeric())
            .filter(|s| !s.is_empty())
            .collect();

        if terms.is_empty() {
            anyhow::bail!("Query contains no searchable words");
        }

        let mut text_clauses: Vec<(Occur, Box<dyn Query>)> = Vec::new();

        // Detect if user typed a specific release year in the query (e.g. "Матрица 1999" or "Дюна 2021")
        let detected_year: Option<u64> = terms
            .iter()
            .find_map(|t| t.parse::<u64>().ok().filter(|&y| (1880..=2040).contains(&y)));

        if let Some(y) = detected_year {
            let year_range = RangeQuery::new_u64("year".to_string(), y..(y + 1));
            text_clauses.push((Occur::Should, Box::new(BoostQuery::new(Box::new(year_range), 25.0))));
        }

        // 1. Full phrase query if multi-term
        if terms.len() > 1 {
            // Phrase on Russian title
            let ru_phrase_terms: Vec<Term> = terms
                .iter()
                .map(|t| Term::from_field_text(self.schema.f_title_ru, t))
                .collect();
            let ru_phrase = PhraseQuery::new(ru_phrase_terms);
            text_clauses.push((Occur::Should, Box::new(BoostQuery::new(Box::new(ru_phrase), 15.0))));

            // Phrase on Original title
            let orig_phrase_terms: Vec<Term> = terms
                .iter()
                .map(|t| Term::from_field_text(self.schema.f_title_orig, t))
                .collect();
            let orig_phrase = PhraseQuery::new(orig_phrase_terms);
            text_clauses.push((Occur::Should, Box::new(BoostQuery::new(Box::new(orig_phrase), 10.0))));
        }

        // 2. Per-term queries (Exact, Prefix, and Adaptive Fuzzy)
        for term_str in &terms {
            let term_len = term_str.chars().count();
            let mut term_subqueries: Vec<Box<dyn Query>> = Vec::new();

            // Search across title_ru, title_orig, and title_primary
            let target_fields = [
                (self.schema.f_title_ru, 3.0f32),
                (self.schema.f_title_orig, 2.0f32),
                (self.schema.f_title_primary, 1.5f32),
            ];

            for (field, field_boost) in target_fields {
                let term = Term::from_field_text(field, term_str);

                // Exact match
                let exact_query = TermQuery::new(term.clone(), IndexRecordOption::WithFreqsAndPositions);
                term_subqueries.push(Box::new(BoostQuery::new(
                    Box::new(exact_query),
                    field_boost * 4.0,
                )));

                // Adaptive accurate fuzzy query:
                // len <= 3: distance = 0 (exact only, no fuzzy)
                // 4 <= len <= 6: distance = 1 (1 typo allowed)
                // len >= 7: distance = 2 (up to 2 typos allowed)
                let max_distance = match term_len {
                    0..=3 => 0,
                    4..=6 => 1,
                    _ => 2,
                };

                if max_distance > 0 {
                    let fuzzy_query = FuzzyTermQuery::new_prefix(
                        term,
                        max_distance,
                        true, // transposition_cost_one = true (Damerau-Levenshtein)
                    );
                    term_subqueries.push(Box::new(BoostQuery::new(
                        Box::new(fuzzy_query),
                        field_boost * 1.0,
                    )));
                }
            }

            // DisjunctionMaxQuery: take max score among alternatives for this term
            let term_disjunction = DisjunctionMaxQuery::new(term_subqueries);
            text_clauses.push((Occur::Should, Box::new(term_disjunction)));
        }

        let main_text_query = BooleanQuery::new(text_clauses);

        // Apply filters (type, year, min_votes)
        let mut filter_clauses: Vec<(Occur, Box<dyn Query>)> = Vec::new();
        filter_clauses.push((Occur::Must, Box::new(main_text_query)));

        // Title type filter
        if let Some(ref t_type) = params.title_type {
            let type_term = Term::from_field_text(self.schema.f_title_type, t_type);
            filter_clauses.push((
                Occur::Must,
                Box::new(TermQuery::new(type_term, IndexRecordOption::Basic)),
            ));
        }

        // Year filter
        if params.year_from.is_some() || params.year_to.is_some() {
            let from = params.year_from.unwrap_or(1800);
            let to = params.year_to.unwrap_or(2100);
            let year_range = RangeQuery::new_u64("year".to_string(), from..(to + 1));
            filter_clauses.push((Occur::Must, Box::new(year_range)));
        }

        // Min votes filter
        if let Some(min_votes) = params.min_votes {
            if min_votes > 0 {
                let votes_range = RangeQuery::new_u64("num_votes".to_string(), min_votes..u64::MAX);
                filter_clauses.push((Occur::Must, Box::new(votes_range)));
            }
        }

        Ok(Box::new(BooleanQuery::new(filter_clauses)))
    }
}

fn levenshtein_distance(a: &str, b: &str) -> usize {
    let a_chars: Vec<char> = a.chars().collect();
    let b_chars: Vec<char> = b.chars().collect();
    let m = a_chars.len();
    let n = b_chars.len();

    if m == 0 {
        return n;
    }
    if n == 0 {
        return m;
    }

    let mut dp = vec![vec![0; n + 1]; m + 1];

    for i in 0..=m {
        dp[i][0] = i;
    }
    for j in 0..=n {
        dp[0][j] = j;
    }

    for i in 1..=m {
        for j in 1..=n {
            let cost = if a_chars[i - 1] == b_chars[j - 1] { 0 } else { 1 };
            dp[i][j] = (dp[i - 1][j] + 1)
                .min(dp[i][j - 1] + 1)
                .min(dp[i - 1][j - 1] + cost);
        }
    }

    dp[m][n]
}

/// Computes a structural match quality multiplier:
/// - 1.5x: Exact Title Match (title == query)
/// - 1.3x: Title Starts With Query with word boundary (e.g. "Клик: с пультом по жизни" starts with "клик")
/// - 1.1x: Exact Word Match (all query terms present as whole words in title)
/// - Graduated penalty for fuzzy/typo matches based on Levenshtein distance
pub fn calculate_title_match_quality(
    movie: &MovieDoc,
    query_clean: &str,
    query_terms: &[&str],
) -> f32 {
    if query_terms.is_empty() {
        return 1.0;
    }

    let mut all_titles: Vec<&str> = Vec::with_capacity(4 + movie.russian_titles.len());
    if let Some(ref ru) = movie.title_ru {
        all_titles.push(ru.as_str());
    }
    all_titles.push(movie.title_orig.as_str());
    all_titles.push(movie.title_primary.as_str());
    for t in &movie.russian_titles {
        all_titles.push(t.as_str());
    }

    let mut best_quality: f32 = 0.4;

    for raw_title in all_titles {
        let norm_title = normalize_query_text(raw_title);
        let trimmed_title = norm_title.trim();

        if trimmed_title.is_empty() {
            continue;
        }

        let title_words: Vec<&str> = trimmed_title
            .split(|c: char| !c.is_alphanumeric())
            .filter(|s| !s.is_empty())
            .collect();

        if title_words.is_empty() {
            continue;
        }

        // Calculate average term score
        let mut term_scores_sum = 0.0f32;
        for qt in query_terms {
            let mut best_term_score = 0.35f32;
            for tw in &title_words {
                if tw == qt {
                    best_term_score = best_term_score.max(1.0);
                } else if tw.starts_with(qt) || qt.starts_with(tw) {
                    best_term_score = best_term_score.max(0.85);
                } else {
                    let dist = levenshtein_distance(qt, tw);
                    if dist == 1 {
                        best_term_score = best_term_score.max(0.75);
                    } else if dist == 2 && qt.chars().count() >= 6 {
                        best_term_score = best_term_score.max(0.60);
                    }
                }
            }
            term_scores_sum += best_term_score;
        }

        let avg_term_score = term_scores_sum / query_terms.len() as f32;

        // Position / exactness multiplier
        let position_mult = if trimmed_title == query_clean {
            1.5f32
        } else if trimmed_title.starts_with(query_clean) {
            let remainder = &trimmed_title[query_clean.len()..];
            if remainder.is_empty() || remainder.starts_with(|c: char| !c.is_alphanumeric()) {
                1.3f32
            } else {
                1.1f32
            }
        } else if query_terms.iter().all(|qt| title_words.contains(qt)) {
            1.1f32
        } else {
            1.0f32
        };

        let current_quality = avg_term_score * position_mult;
        if current_quality > best_quality {
            best_quality = current_quality;
        }
    }

    best_quality
}

/// Calculate popularity multiplier based on number of votes and rating:
/// mult = (1.0 + weight_votes * log10(votes + 1)^1.2) * (rating / 10)^weight_rating
pub fn calculate_popularity_multiplier(
    num_votes: u32,
    rating: Option<f32>,
    weight_votes: f64,
    weight_rating: f64,
) -> f32 {
    let votes_f = num_votes as f64;
    // log10(votes + 1) with progressive scaling
    let log_votes = (votes_f + 1.0).log10();
    let votes_factor = 1.0 + (weight_votes * log_votes.powf(1.2));

    let rating_val = rating.unwrap_or(5.0) as f64;
    let rating_norm = (rating_val / 10.0).clamp(0.1, 1.0);
    let rating_factor = rating_norm.powf(weight_rating);

    (votes_factor * rating_factor) as f32
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::index::manager::IndexManager;

    #[test]
    fn test_smart_popularity_ranking_with_typo() -> anyhow::Result<()> {
        let test_dir = std::env::temp_dir().join(format!("imdb_test_{}", std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH)?.as_millis()));
        let _ = std::fs::remove_dir_all(&test_dir);
        let mut manager = IndexManager::open_or_create(&test_dir)?;
        let (_index, mut writer, gen_path) = manager.create_new_generation(50)?;
        let schema = manager.schema().clone();

        // 1. Cult classic movie with 2 million votes: "Матрица" (1999)
        let matrix = MovieDoc {
            tconst: "tt0133093".to_string(),
            title_ru: Some("Матрица".to_string()),
            title_orig: "The Matrix".to_string(),
            title_primary: "The Matrix".to_string(),
            russian_titles: vec!["Матрица".to_string()],
            year: Some(1999),
            title_type: "movie".to_string(),
            rating: Some(8.7),
            num_votes: 2_050_000,
            genres: vec!["Action".to_string(), "Sci-Fi".to_string()],
            runtime_minutes: Some(136),
        };
        writer.add_document(schema.to_tantivy_doc(&matrix))?;

        // 2. Obscure 2-vote movie whose Russian title matches a typo "Матрицо"
        let obscure_match = MovieDoc {
            tconst: "tt9999999".to_string(),
            title_ru: Some("Матрицо".to_string()),
            title_orig: "Matritso".to_string(),
            title_primary: "Matritso".to_string(),
            russian_titles: vec!["Матрицо".to_string()],
            year: Some(2020),
            title_type: "movie".to_string(),
            rating: None,
            num_votes: 2,
            genres: vec!["Short".to_string()],
            runtime_minutes: Some(5),
        };
        writer.add_document(schema.to_tantivy_doc(&obscure_match))?;

        writer.commit()?;
        manager.activate_generation(&gen_path)?;

        let search_engine = SearchEngine::new(manager.reader(), schema);

        // User makes a typo: searches "матрицо"
        let hits = search_engine.search(&SearchParams {
            query: "матрицо".to_string(),
            limit: 10,
            ..Default::default()
        })?;

        assert!(!hits.is_empty(), "Should return hits");
        // Due to smart popularity boosting, the legendary "Матрица" should be #1!
        assert_eq!(
            hits[0].movie.tconst, "tt0133093",
            "Cult popular movie should rank #1 even when user typed a typo that matched obscure film!"
        );

        let _ = std::fs::remove_dir_all(&test_dir);
        Ok(())
    }

    #[test]
    fn test_search_with_year_in_query() -> anyhow::Result<()> {
        let test_dir = std::env::temp_dir().join(format!("imdb_year_test_{}", std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH)?.as_millis()));
        let _ = std::fs::remove_dir_all(&test_dir);
        let mut manager = IndexManager::open_or_create(&test_dir)?;
        let (_index, mut writer, gen_path) = manager.create_new_generation(50)?;
        let schema = manager.schema().clone();

        // Dune 1984 (David Lynch)
        let dune_1984 = MovieDoc {
            tconst: "tt0087182".to_string(),
            title_ru: Some("Дюна".to_string()),
            title_orig: "Dune".to_string(),
            title_primary: "Dune".to_string(),
            russian_titles: vec!["Дюна".to_string()],
            year: Some(1984),
            title_type: "movie".to_string(),
            rating: Some(6.3),
            num_votes: 180_000,
            genres: vec!["Action".to_string(), "Sci-Fi".to_string()],
            runtime_minutes: Some(137),
        };
        writer.add_document(schema.to_tantivy_doc(&dune_1984))?;

        // Dune 2021 (Denis Villeneuve, more popular)
        let dune_2021 = MovieDoc {
            tconst: "tt1160419".to_string(),
            title_ru: Some("Дюна".to_string()),
            title_orig: "Dune: Part One".to_string(),
            title_primary: "Dune".to_string(),
            russian_titles: vec!["Дюна".to_string()],
            year: Some(2021),
            title_type: "movie".to_string(),
            rating: Some(8.0),
            num_votes: 750_000,
            genres: vec!["Action".to_string(), "Sci-Fi".to_string()],
            runtime_minutes: Some(155),
        };
        writer.add_document(schema.to_tantivy_doc(&dune_2021))?;

        writer.commit()?;
        manager.activate_generation(&gen_path)?;

        let search_engine = SearchEngine::new(manager.reader(), schema);

        // When user explicitly searches "Дюна 1984", 1984 version must be #1!
        let hits_1984 = search_engine.search(&SearchParams {
            query: "Дюна 1984".to_string(),
            limit: 5,
            ..Default::default()
        })?;
        assert!(!hits_1984.is_empty());
        assert_eq!(hits_1984[0].movie.tconst, "tt0087182", "Dune 1984 must be #1");

        // When user explicitly searches "Дюна 2021", 2021 version must be #1!
        let hits_2021 = search_engine.search(&SearchParams {
            query: "Дюна 2021".to_string(),
            limit: 5,
            ..Default::default()
        })?;
        assert!(!hits_2021.is_empty());
        assert_eq!(hits_2021[0].movie.tconst, "tt1160419", "Dune 2021 must be #1");

        let _ = std::fs::remove_dir_all(&test_dir);
        Ok(())
    }
}

