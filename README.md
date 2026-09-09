# CineClaw IMDb Indexer

[![Docker Image](https://github.com/cineclaw/imdb-indexer/actions/workflows/docker-publish.yml/badge.svg)](https://github.com/cineclaw/imdb-indexer/actions/workflows/docker-publish.yml)
[![Version](https://img.shields.io/badge/version-1.0.0-blue.svg)](https://github.com/cineclaw/imdb-indexer/releases)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

**CineClaw IMDb Indexer** — высокопроизводительный сервис на Rust (Axum, Tantivy, redb) для автономной загрузки, экономной индексации и сверхбыстрого полнотекстового поиска по базе IMDb (11+ млн записей) с упором на русскую локализацию, проксирование постеров TMDB и кураторские фиды.

---

## Ключевые возможности

1. **Экстремально низкое потребление оперативной памяти (Low RAM Footprint)**:
   - **Режим поиска**: < 50 MB RSS (Tantivy использует `mmap` сегментов индекса).
   - **Режим полной индексации миллионов записей**: < 100-150 MB RSS (жесткий лимит буфера памяти `IndexWriter` 50 MB, стриминговый gzip-парсинг и временное disk-backed хранилище `redb` вместо раздутых `HashMap` в RAM).
2. **Умный и аккуратный Fuzzy Search**:
   - **Адаптивное расстояние Левенштейна**: 
     - $\le 3$ символов: 0 (только точное/префиксное совпадение).
     - 4–6 символов: 1 (допускается 1 опечатка).
     - $\ge 7$ символов: 2 (допускается до 2 опечаток).
   - Damerau-Levenshtein с транспозицией букв (`transposition_cost_one = true`).
   - Поддержка нормализации русской буквы `ё` $\to$ `е` (кастомный токенизатор Tantivy `RussianYoFilter`).
   - Поиск как по русским локализациям (включая регионы `RU`, `SU` и язык `ru`), так и по оригинальным названиям.
3. **Умное ранжирование популярных фильмов при опечатках**:
   - При опечатках культовый блокбастер с миллионами голосов ранжируется **выше**, чем неизвестный низкорейтинговый проект с 2 голосами, случайно совпавший точнее.
   - Двухфазный скоринг:
     $$\text{FinalScore} = \text{BM25} \times \left(1.0 + w_{\text{votes}} \cdot \log_{10}(\text{num\_votes} + 1)\right) \times \left(\frac{\text{rating}}{10}\right)^{w_{\text{rating}}}$$
4. **Автономное скачивание и Blue-Green обновления**:
   - Периодическая проверка (`check_interval_hours: 24`) через HTTP `HEAD` с проверкой `ETag` и `Last-Modified`.
   - Blue-Green ротация поколений индекса (`gen_<timestamp>`): во время индексации поиск работает бесперебойно, переключение происходит атомарно.
5. **Метаданные TMDB & Кураторские полки**:
   - Домашние полки новинок, трендов и шедевров (`/api/feeds`).
   - Интеграция с создателями сериалов (Showrunners, `created_by`, episodic directors).
   - Постер-прокси с кэшированием на диск и отдачей `ETag` / `304 Not Modified`.

---

## Структура проекта

```
imdb-indexer/
├── Cargo.toml                  # Зависимости (Tantivy, Axum, Tokio, redb)
├── config.yaml                 # Файл конфигурации
├── Dockerfile                  # Multi-stage production сборка на Rust
├── src/
│   ├── main.rs                 # Точка входа, запуск сервера и фонового шедулера
│   ├── config.rs               # Конфигурация (поддержка ENV-переменных)
│   ├── lib.rs                  # Экспорт библиотечных модулей
│   ├── api/                    # Axum HTTP API
│   │   ├── handlers.rs         # /search, /status, /health, /feeds, /person
│   │   └── routes.rs           # Маршрутизация и CORS
│   ├── downloader/             # Стриминговый загрузчик с ETag-кэшированием
│   │   └── client.rs
│   ├── ingestion/              # Стриминговая обработка с нулевым расходом RAM
│   │   ├── temp_store.rs       # redb disk-backed KV для связки tconst -> ratings/akas
│   │   └── pipeline.rs         # Пайплайн потоковой подачи в Tantivy
│   ├── index/                  # Модуль Tantivy
│   │   ├── schema.rs           # Схема и MovieDoc
│   │   ├── tokenizer.rs        # Токенизатор с поддержкой ё->е
│   │   └── manager.rs          # Blue-Green менеджер индексов
│   ├── poster/                 # Кэширующий сервис постеров и метаданных TMDB
│   │   └── service.rs
│   ├── search/                 # Поисковый движок
│   │   └── fuzzy.rs            # Адаптивный fuzzy-поиск и умный реранкер
│   └── scheduler/              # Шедулер периодических фоновых обновлений
│       └── mod.rs
└── tests/
    └── integration_search_test.rs # Сквозные тесты
```

---

## Запуск и разработка

### Сборка из исходников
```bash
# Всегда компилируйте в release-режиме (Tantivy в 10 раз быстрее)
cargo build --release

# Запуск демона
./target/release/imdb-indexer
```

### Переменные окружения
| Переменная | Назначение |
| :--- | :--- |
| `CONFIG_PATH` | Путь к файлу конфигурации (по умолчанию `config.yaml`) |
| `DATA_DIR` | Каталог хранения индексов и дампов (по умолчанию `./data`) |
| `TMDB_CACHE_DIR`| Каталог кэша постеров (по умолчанию `./data/posters`) |
| `TMDB_API_KEY` | API-ключ themoviedb.org для постеров и метаданных |

---

## Docker-контейнер

Готовый многоплатформенный образ доступен в GitHub Container Registry:
```bash
docker pull ghcr.io/cineclaw/imdb-indexer:latest

# Запуск контейнера
docker run -d \
  -p 8090:8090 \
  -v $(pwd)/data:/data \
  -e TMDB_API_KEY="your_api_key" \
  ghcr.io/cineclaw/imdb-indexer:latest
```

---

## HTTP API

- `GET /status` — статус поискового индексатора, количество документов и версия (`1.0.0`).
- `GET /search?q=query&limit=20` — адаптивный fuzzy-поиск по фильмам и сериалам.
- `GET /feeds` — домашние полки (тренды, цифровые релизы, популярные сериалы, шедевры).
- `GET /feeds/:shelf_id?page=1` — постраничная загрузка фильмов с полки.
- `GET /poster/:tconst?size=w185` — кэшированный постер фильма по IMDb ID.
- `GET /series/:tconst/seasons` — сезоны и количество эпизодов сериала.
- `GET /person/:person_id` — детальная биография и фильмография персоны.
- `POST /api/index/update` — принудительный запуск обновления дампов IMDb.

---

## Лицензия
MIT License.
