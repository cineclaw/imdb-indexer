use anyhow::Result;
use redb::{Database, ReadableTable, TableDefinition};
use std::path::Path;

const RATINGS_TABLE: TableDefinition<u32, u64> = TableDefinition::new("ratings");
const AKAS_RU_TABLE: TableDefinition<u32, &str> = TableDefinition::new("akas_ru");

/// Helper to parse "tt0111161" into numeric 111161
pub fn parse_tconst_id(tconst: &str) -> Option<u32> {
    if tconst.starts_with("tt") {
        tconst[2..].parse::<u32>().ok()
    } else {
        None
    }
}

pub struct TempIngestStore {
    db: Database,
}

impl TempIngestStore {
    pub fn create<P: AsRef<Path>>(path: P) -> Result<Self> {
        let db = Database::create(path)?;
        // Pre-create tables
        let write_txn = db.begin_write()?;
        {
            let _ = write_txn.open_table(RATINGS_TABLE)?;
            let _ = write_txn.open_table(AKAS_RU_TABLE)?;
        }
        write_txn.commit()?;
        Ok(Self { db })
    }

    pub fn insert_ratings_batch(&self, batch: &[(u32, u8, u32)]) -> Result<()> {
        let write_txn = self.db.begin_write()?;
        {
            let mut table = write_txn.open_table(RATINGS_TABLE)?;
            for &(id, rating_scaled, votes) in batch {
                let packed: u64 = ((rating_scaled as u64) << 32) | (votes as u64);
                table.insert(id, packed)?;
            }
        }
        write_txn.commit()?;
        Ok(())
    }

    pub fn insert_akas_batch(&self, batch: &[(u32, String)]) -> Result<()> {
        let write_txn = self.db.begin_write()?;
        {
            let mut table = write_txn.open_table(AKAS_RU_TABLE)?;
            for (id, new_title) in batch {
                let merged_title = if let Some(existing) = table.get(id)? {
                    let existing_str = existing.value();
                    if existing_str.split('\t').any(|t| t == new_title) {
                        existing_str.to_string()
                    } else {
                        format!("{}\t{}", existing_str, new_title)
                    }
                } else {
                    new_title.clone()
                };
                table.insert(id, merged_title.as_str())?;
            }
        }
        write_txn.commit()?;
        Ok(())
    }

    pub fn get_rating(&self, id: u32) -> Result<Option<(f32, u32)>> {
        let read_txn = self.db.begin_read()?;
        let table = read_txn.open_table(RATINGS_TABLE)?;
        if let Some(val) = table.get(id)? {
            let packed = val.value();
            let rating_scaled = (packed >> 32) as u8;
            let votes = (packed & 0xFFFF_FFFF) as u32;
            let rating = if rating_scaled > 0 {
                Some(rating_scaled as f32 / 10.0)
            } else {
                None
            };
            Ok(Some((rating.unwrap_or(0.0), votes)))
        } else {
            Ok(None)
        }
    }

    pub fn get_russian_titles(&self, id: u32) -> Result<Option<Vec<String>>> {
        let read_txn = self.db.begin_read()?;
        let table = read_txn.open_table(AKAS_RU_TABLE)?;
        if let Some(val) = table.get(id)? {
            let raw = val.value();
            let titles = raw.split('\t').map(|s| s.to_string()).collect();
            Ok(Some(titles))
        } else {
            Ok(None)
        }
    }

    /// Read transaction helper for fast sequential lookup during basics parsing
    pub fn begin_reader(&self) -> Result<TempIngestReader> {
        let read_txn = self.db.begin_read()?;
        Ok(TempIngestReader { read_txn })
    }
}

pub struct TempIngestReader {
    read_txn: redb::ReadTransaction,
}

impl TempIngestReader {
    pub fn lookup(&self, id: u32) -> (Option<(f32, u32)>, Option<Vec<String>>) {
        let rating_info = if let Ok(table) = self.read_txn.open_table(RATINGS_TABLE) {
            if let Ok(Some(val)) = table.get(id) {
                let packed = val.value();
                let rating_scaled = (packed >> 32) as u8;
                let votes = (packed & 0xFFFF_FFFF) as u32;
                let rating = rating_scaled as f32 / 10.0;
                Some((rating, votes))
            } else {
                None
            }
        } else {
            None
        };

        let ru_titles = if let Ok(table) = self.read_txn.open_table(AKAS_RU_TABLE) {
            if let Ok(Some(val)) = table.get(id) {
                let raw = val.value();
                let titles: Vec<String> = raw.split('\t').map(|s| s.to_string()).collect();
                Some(titles)
            } else {
                None
            }
        } else {
            None
        };

        (rating_info, ru_titles)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_temp_store() -> Result<()> {
        let test_path = std::env::temp_dir().join(format!("test_redb_{}.db", std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH)?.as_millis()));
        let store = TempIngestStore::create(&test_path)?;

        let ratings = vec![(111161, 93, 2_800_000), (133093, 87, 2_050_000)];
        store.insert_ratings_batch(&ratings)?;

        let akas = vec![
            (111161, "Побег из Шоушенка".to_string()),
            (133093, "Матрица".to_string()),
            (133093, "Матрица: Фильм".to_string()),
        ];
        store.insert_akas_batch(&akas)?;

        let reader = store.begin_reader()?;
        let (rating_info, ru_titles) = reader.lookup(133093);

        assert_eq!(rating_info, Some((8.7, 2_050_000)));
        assert_eq!(
            ru_titles,
            Some(vec!["Матрица".to_string(), "Матрица: Фильм".to_string()])
        );

        drop(reader);
        drop(store);
        let _ = std::fs::remove_file(&test_path);
        Ok(())
    }
}
