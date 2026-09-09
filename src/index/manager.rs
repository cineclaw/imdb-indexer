use crate::index::schema::{MovieSchema, RUSSIAN_ANALYZER};
use crate::index::tokenizer::RussianYoFilter;
use anyhow::{Context, Result};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tantivy::tokenizer::{LowerCaser, SimpleTokenizer, TextAnalyzer};
use tantivy::{Index, IndexReader, IndexWriter};
use tracing::info;

#[derive(Clone)]
pub struct IndexManager {
    base_dir: PathBuf,
    schema: MovieSchema,
    index: Arc<Index>,
    reader: Arc<IndexReader>,
}

impl IndexManager {
    /// Initialize or open current active index
    pub fn open_or_create<P: AsRef<Path>>(base_dir: P) -> Result<Self> {
        let base_dir = base_dir.as_ref().to_path_buf();
        fs::create_dir_all(&base_dir).context("Failed to create base index directory")?;

        let schema = MovieSchema::new();
        let active_path = Self::get_active_index_path(&base_dir);

        let index = if active_path.exists() {
            info!("Opening existing active index at {:?}", active_path);
            Index::open_in_dir(&active_path)?
        } else {
            info!("Creating initial index at {:?}", active_path);
            fs::create_dir_all(&active_path)?;
            Index::create_in_dir(&active_path, schema.schema.clone())?
        };

        Self::register_analyzers(&index);

        let reader = index
            .reader_builder()
            .reload_policy(tantivy::ReloadPolicy::OnCommitWithDelay)
            .try_into()?;

        Ok(Self {
            base_dir,
            schema,
            index: Arc::new(index),
            reader: Arc::new(reader),
        })
    }

    pub fn register_analyzers(index: &Index) {
        let analyzer = TextAnalyzer::builder(SimpleTokenizer::default())
            .filter(LowerCaser)
            .filter(RussianYoFilter)
            .build();
        index.tokenizers().register(RUSSIAN_ANALYZER, analyzer);
    }

    pub fn schema(&self) -> &MovieSchema {
        &self.schema
    }

    pub fn reader(&self) -> Arc<IndexReader> {
        self.reader.clone()
    }

    pub fn index(&self) -> Arc<Index> {
        self.index.clone()
    }

    /// Creates a fresh new index in a generation folder for Blue-Green indexing.
    /// Memory budget is strictly bounded to avoid RAM spikes.
    pub fn create_new_generation(&self, memory_budget_mb: usize) -> Result<(Index, IndexWriter, PathBuf)> {
        let timestamp = chrono::Utc::now().timestamp_millis();
        let gen_path = self.base_dir.join(format!("gen_{}", timestamp));
        fs::create_dir_all(&gen_path)?;

        let new_index = Index::create_in_dir(&gen_path, self.schema.schema.clone())?;
        Self::register_analyzers(&new_index);

        let budget_bytes = memory_budget_mb * 1024 * 1024;
        let writer = new_index.writer(budget_bytes)?;

        Ok((new_index, writer, gen_path))
    }

    /// Atomically activates a newly built generation index and cleans up old generations
    pub fn activate_generation(&mut self, gen_path: &Path) -> Result<()> {
        info!("Activating new index generation at {:?}", gen_path);

        let pointer_file = self.base_dir.join("active_gen.txt");
        let gen_folder_name = gen_path
            .file_name()
            .and_then(|n| n.to_str())
            .context("Invalid gen path name")?;

        // Write atomic pointer file
        let temp_pointer = self.base_dir.join("active_gen.txt.tmp");
        fs::write(&temp_pointer, gen_folder_name)?;
        fs::rename(&temp_pointer, &pointer_file)?;

        // Open newly activated index
        let new_index = Index::open_in_dir(gen_path)?;
        Self::register_analyzers(&new_index);

        let new_reader = new_index
            .reader_builder()
            .reload_policy(tantivy::ReloadPolicy::OnCommitWithDelay)
            .try_into()?;

        self.index = Arc::new(new_index);
        self.reader = Arc::new(new_reader);

        info!("New index generation activated successfully!");

        // Clean up old generations (keep only active one)
        self.cleanup_old_generations(gen_folder_name);

        Ok(())
    }

    fn get_active_index_path(base_dir: &Path) -> PathBuf {
        let pointer_file = base_dir.join("active_gen.txt");
        if pointer_file.exists() {
            if let Ok(gen_name) = fs::read_to_string(&pointer_file) {
                let trimmed = gen_name.trim();
                let gen_path = base_dir.join(trimmed);
                if gen_path.exists() {
                    return gen_path;
                }
            }
        }
        // Default to active directory
        base_dir.join("active")
    }

    fn cleanup_old_generations(&self, current_active_gen: &str) {
        if let Ok(entries) = fs::read_dir(&self.base_dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.is_dir() {
                    if let Some(folder_name) = path.file_name().and_then(|n| n.to_str()) {
                        if folder_name.starts_with("gen_") && folder_name != current_active_gen {
                            info!("Cleaning up old index generation: {:?}", path);
                            let _ = fs::remove_dir_all(&path);
                        }
                    }
                }
            }
        }
    }
}
