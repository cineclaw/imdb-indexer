use serde::{Deserialize, Serialize};
use tantivy::schema::*;

pub const RUSSIAN_ANALYZER: &str = "russian_yo";

#[derive(Clone, Debug)]
pub struct MovieSchema {
    pub schema: Schema,
    pub f_tconst: Field,
    pub f_title_ru: Field,
    pub f_title_orig: Field,
    pub f_title_primary: Field,
    pub f_title_ru_raw: Field,
    pub f_year: Field,
    pub f_title_type: Field,
    pub f_rating: Field,
    pub f_num_votes: Field,
    pub f_genres: Field,
    pub f_runtime_minutes: Field,
}

impl Default for MovieSchema {
    fn default() -> Self {
        Self::new()
    }
}

impl MovieSchema {
    pub fn new() -> Self {
        let mut builder = Schema::builder();

        // Exact ID field (e.g. "tt0111161")
        let string_field_indexing = TextOptions::default()
            .set_indexing_options(
                TextFieldIndexing::default()
                    .set_tokenizer("raw")
                    .set_index_option(IndexRecordOption::Basic),
            )
            .set_stored()
            .set_fast(None);
        let f_tconst = builder.add_text_field("tconst", string_field_indexing.clone());

        // Text fields
        let ru_text_options = TextOptions::default()
            .set_indexing_options(
                TextFieldIndexing::default()
                    .set_tokenizer(RUSSIAN_ANALYZER)
                    .set_index_option(IndexRecordOption::WithFreqsAndPositions),
            )
            .set_stored();
        let f_title_ru = builder.add_text_field("title_ru", ru_text_options);

        let orig_text_options = TextOptions::default()
            .set_indexing_options(
                TextFieldIndexing::default()
                    .set_tokenizer("default")
                    .set_index_option(IndexRecordOption::WithFreqsAndPositions),
            )
            .set_stored();
        let f_title_orig = builder.add_text_field("title_orig", orig_text_options.clone());
        let f_title_primary = builder.add_text_field("title_primary", orig_text_options);

        // Raw Russian titles stored as JSON or string list
        let f_title_ru_raw = builder.add_text_field("title_ru_raw", STORED);

        // Numeric fields: year, rating, num_votes, runtime
        let u64_options = NumericOptions::default()
            .set_stored()
            .set_fast()
            .set_indexed();
        let f_year = builder.add_u64_field("year", u64_options.clone());
        let f_num_votes = builder.add_u64_field("num_votes", u64_options.clone());
        let f_runtime_minutes = builder.add_u64_field("runtime_minutes", u64_options);

        let f64_options = NumericOptions::default()
            .set_stored()
            .set_fast();
        let f_rating = builder.add_f64_field("rating", f64_options);

        let f_title_type = builder.add_text_field("title_type", string_field_indexing);
        let f_genres = builder.add_text_field("genres", STORED);

        let schema = builder.build();

        Self {
            schema,
            f_tconst,
            f_title_ru,
            f_title_orig,
            f_title_primary,
            f_title_ru_raw,
            f_year,
            f_title_type,
            f_rating,
            f_num_votes,
            f_genres,
            f_runtime_minutes,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MovieDoc {
    pub tconst: String,
    pub title_ru: Option<String>,
    pub title_orig: String,
    pub title_primary: String,
    pub russian_titles: Vec<String>,
    pub year: Option<u32>,
    pub title_type: String,
    pub rating: Option<f32>,
    pub num_votes: u32,
    pub genres: Vec<String>,
    pub runtime_minutes: Option<u32>,
}

impl MovieSchema {
    pub fn to_tantivy_doc(&self, movie: &MovieDoc) -> TantivyDocument {
        let mut doc = TantivyDocument::default();

        doc.add_text(self.f_tconst, &movie.tconst);

        if let Some(ru) = &movie.title_ru {
            doc.add_text(self.f_title_ru, ru);
        }
        doc.add_text(self.f_title_orig, &movie.title_orig);
        doc.add_text(self.f_title_primary, &movie.title_primary);

        if !movie.russian_titles.is_empty() {
            if let Ok(json) = serde_json::to_string(&movie.russian_titles) {
                doc.add_text(self.f_title_ru_raw, &json);
            }
        }

        if let Some(yr) = movie.year {
            doc.add_u64(self.f_year, yr as u64);
        }

        doc.add_text(self.f_title_type, &movie.title_type);

        if let Some(rt) = movie.rating {
            doc.add_f64(self.f_rating, rt as f64);
        }

        doc.add_u64(self.f_num_votes, movie.num_votes as u64);

        if !movie.genres.is_empty() {
            doc.add_text(self.f_genres, &movie.genres.join(", "));
        }

        if let Some(rtm) = movie.runtime_minutes {
            doc.add_u64(self.f_runtime_minutes, rtm as u64);
        }

        doc
    }

    pub fn from_tantivy_doc(&self, doc: &TantivyDocument) -> MovieDoc {
        let tconst = doc
            .get_first(self.f_tconst)
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string();

        let title_ru = doc
            .get_first(self.f_title_ru)
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());

        let title_orig = doc
            .get_first(self.f_title_orig)
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string();

        let title_primary = doc
            .get_first(self.f_title_primary)
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string();

        let russian_titles = doc
            .get_first(self.f_title_ru_raw)
            .and_then(|v| v.as_str())
            .and_then(|s| serde_json::from_str::<Vec<String>>(s).ok())
            .unwrap_or_else(|| {
                if let Some(ru) = &title_ru {
                    vec![ru.clone()]
                } else {
                    vec![]
                }
            });

        let year = doc
            .get_first(self.f_year)
            .and_then(|v| v.as_u64())
            .map(|y| y as u32);

        let title_type = doc
            .get_first(self.f_title_type)
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string();

        let rating = doc
            .get_first(self.f_rating)
            .and_then(|v| v.as_f64())
            .map(|r| r as f32);

        let num_votes = doc
            .get_first(self.f_num_votes)
            .and_then(|v| v.as_u64())
            .unwrap_or(0) as u32;

        let genres = doc
            .get_first(self.f_genres)
            .and_then(|v| v.as_str())
            .map(|s| s.split(", ").map(|g| g.to_string()).collect())
            .unwrap_or_default();

        let runtime_minutes = doc
            .get_first(self.f_runtime_minutes)
            .and_then(|v| v.as_u64())
            .map(|m| m as u32);

        MovieDoc {
            tconst,
            title_ru,
            title_orig,
            title_primary,
            russian_titles,
            year,
            title_type,
            rating,
            num_votes,
            genres,
            runtime_minutes,
        }
    }
}
