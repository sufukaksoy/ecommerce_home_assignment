# E-Commerce Analytics Pipeline

Bu repo, 6 aylık e-ticaret CSV verisi için temel bir ELT hattı uygular.

## Proje Yapısı
```text
ecommerce_pipeline/
  data/raw/                        # Girdi CSV dosyaları (lokalde tutulur, git'e dahil edilmez)
  src/
    pipeline_duckdb.py             # Ana ELT hattı (DuckDB)
  sql/
    transform/
      dims.sql                     # Dimension dönüşümleri
      facts.sql                    # Fact dönüşümleri
      views.sql                    # BI view dönüşümleri
  scripts/
    dq_checks.py                   # Veri kalite kontrolleri
    start_duckdb_ui_container.py   # Opsiyonel DuckDB local UI başlatma
  dags/
    ecommerce_pipeline_dag.py      # Airflow DAG (otomasyon)
  docker/
    Dockerfile
    Dockerfile.airflow
    Dockerfile.duckdb-ui
  docker-compose.yml
  app_logging.py                   # Yapılandırılmış JSON loglama
```

## Veri Alımı ve Modelleme

### Veri Alımı
- Ham CSV dosyaları `staging.*_raw` tablolara yüklenir.
- Yükleme işlemi `src/pipeline_duckdb.py` ile yapılır.
- Şema çıkarımı DuckDB `read_csv_auto(...)` ile yapılır.

### Diyagram 1: Ingestion Akışı

### Hedef Model
Mart katmanı fact/dimension prensibiyle tasarlanmıştır:
- Fact tablolar: `mart.fact_orders`, `mart.fact_order_items`
- Dimension tablolar: `mart.dim_users`, `mart.dim_products`, `mart.dim_distribution_centers`

Not: Bu yapı star benzeri analitik modeldir (teknik olarak iki fact tablo ve `product -> distribution_center` bağı nedeniyle küçük bir fact constellation yapısıdır).

### Diyagram 2: Mart Modeli (Fact/Dim)
```mermaid
flowchart TB
  FO[(mart.fact_orders)]
  FOI[(mart.fact_order_items)]
  DU[dim_users]
  DP[dim_products]
  DC[dim_distribution_centers]

  DU --> FO
  DU --> FOI
  FO --> FOI
  DP --> FOI
  DC --> DP
```

## Veri Dönüşümü (ELT)

Dönüşüm mantığı `sql/transform/*.sql` dosyalarındadır (`dims.sql`, `facts.sql`, `views.sql`):
- Tekilleştirme: `ROW_NUMBER() ... WHERE rn = 1`
- Null/boş değer yönetimi: `COALESCE(NULLIF(TRIM(...), ''), ...)`
- Tip dönüşümleri: `BIGINT`, `DOUBLE`, `TIMESTAMP`, `DATE`
- Durum normalizasyonu: `LOWER(...)`

BI için hazır çıktılar:
- `mart.daily_commerce_metrics` (kategori bazlı günlük performans: satılan adet, kategori cirosu + günlük toplam metrikler)
- `mart.daily_summary_metrics` (günlük toplam ciro ve sipariş hacmi)
- `mart.daily_top_category` (gün bazında en çok gelir üreten kategori)

### Diyagram 3: Transform Çıktıları
```mermaid
flowchart LR
  S[(staging.*_raw)] --> TD[sql/transform/dims.sql]
  TD --> D1[(mart.dim_users)]
  TD --> D2[(mart.dim_products)]
  TD --> D3[(mart.dim_distribution_centers)]
  TD --> TF[sql/transform/facts.sql]
  TF --> F1[(mart.fact_orders)]
  TF --> F2[(mart.fact_order_items)]
  TF --> TV[sql/transform/views.sql]
  TV --> V1[[mart.daily_commerce_metrics]]
  TV --> V2[[mart.daily_summary_metrics]]
  TV --> V3[[mart.daily_top_category]]
```

## Pipeline Otomasyonu

### Tekrarlanabilir Pipeline
- Pipeline idempotent tasarlanmıştır (`CREATE OR REPLACE` yaklaşımı).
- Tekrar çalıştırıldığında mart tabloları/view’ları ham veriden deterministik şekilde yeniden üretilir.

### Airflow Orkestrasyonu 
DAG: `ecommerce_elt_pipeline`
1. `validate_inputs`
2. `load_users`
3. `load_orders`
4. `load_order_items`
5. `load_products`
6. `load_distribution_centers`
7. `load_inventory_items`
8. `load_events`
9. `transform_dims`
10. `transform_facts`
11. `transform_views`
12. `run_dq_checks`

Ek operasyonel kontroller:
- **Granüler Görev İzolasyonu:** Hata yönetimi (Retry) sırasında baştan başlamak yerine sadece hata alan adım (örneğin sadece Transform) tekrarlanarak gigabaytlarca I/O (CSV okuma) israfı önlenir.
- **Otomatik Retry Mekanizması:** Olası kilitlenmelere (File Lock) karşı her adım 2 dakika arayla 3 defa otomatik olarak tekrar denenir.
- Yapılandırılmış JSON loglar
- Pipeline sonrasında veri kalite kontrolü

### Diyagram 4: Airflow Orkestrasyon Akışı
```mermaid
flowchart TD
  V[validate_inputs] --> LU[load_users]
  LU --> LO[load_orders]
  LO --> LOI[load_order_items]
  LOI --> LP[load_products]
  LP --> LDC[load_distribution_centers]
  LDC --> LII[load_inventory_items]
  LII --> LE[load_events]
  LE --> TD[transform_dims]
  TD --> TF[transform_facts]
  TF --> TV[transform_views]
  TV --> Q[run_dq_checks]

  LU -. retry .-> LU
  LO -. retry .-> LO
  LOI -. retry .-> LOI
  LP -. retry .-> LP
  LDC -. retry .-> LDC
  LII -. retry .-> LII
  LE -. retry .-> LE
  TD -. retry .-> TD
  TF -. retry .-> TF
  TV -. retry .-> TV
```

### Lokalde Çalıştırma

#### 1. Proje dizinine gir
```bash
cd ecommerce_pipeline
```

#### 2. Veriyi `data/raw` klasörüne yerleştir
Bu repo boyut limiti nedeniyle raw CSV dosyalarını içermez. Aşağıdaki dosyaları `data/raw/` altına kopyala:
- `users.csv`
- `orders.csv`
- `order_items.csv`
- `products.csv`
- `distribution_centers.csv`
- `inventory_items.csv`
- `events.csv`

Doğrulama:
```bash
ls data/raw
```

#### 3. Airflow stack'i ayağa kaldır
```bash
docker compose down --remove-orphans
docker compose up --build -d
```
Airflow UI:
- `http://localhost:8080`
- kullanıcı/şifre: `admin/admin`

#### 4. DAG'i çalıştır
- Airflow UI'da `ecommerce_elt_pipeline` DAG'ini `Trigger DAG` ile tetikle.
- Task akışı: `validate_inputs -> load_users -> ... -> run_dq_checks`

#### 5. (Opsiyonel) DuckDB UI aç (tercih edilen yöntem: local Python)
```bash
docker compose stop duckdb-ui || true

source .venv/bin/activate
python - << 'PY'
import duckdb, time
con = duckdb.connect("ecommerce.duckdb")
con.execute("INSTALL ui; LOAD ui;")
print(con.execute("CALL start_ui_server();").fetchall())
time.sleep(99999)
PY
```
UI: `http://localhost:4213`
Beklenen çıktı:
`[('UI server started at http://localhost:4213/',)]`

Alternatif container yöntemi:
```bash
docker compose --profile duckui up --build duckdb-ui
```

#### 6. (Opsiyonel) Tek seferlik lokal pipeline çalıştırma
Airflow yerine doğrudan ELT çalıştırmak için:
```bash
docker compose --profile tools run --rm duckdb-build
```
Not: `pipeline_duckdb.py` içinde `--step transform`, `transform_dims -> transform_facts -> transform_views` zinciri için alias olarak çalışır.

### Beklenen Çıktı (Referans)
- `mart.dim_users`: `100000`
- `mart.dim_products`: `29120`
- `mart.dim_distribution_centers`: `10`
- `mart.fact_orders`: `50291`
- `mart.fact_order_items`: `73110`

## Mimari Tercihler
- DuckDB, local-first analitik kurulum kolaylığı için seçildi.
- SQL-first dönüşüm yaklaşımı iş kurallarını şeffaf ve gözden geçirilebilir tutar.
- Fact/dimension model BI sorgularını hızlandırır ve sadeleştirir.
- Airflow, otomasyon bonusu kapsamında eklendi.

## Veri Kalitesi Stratejisi
Uygulanan kontroller:
- Dönüşüm öncesi input dosya varlık kontrolü
- İş anahtarına göre tekilleştirme
- Null/default yönetimi
- Veri tipi normalizasyonu
- Dönüşüm sonrası Data Quality kontrolleri (`scripts/dq_checks.py`)

### Diyagram 5: Veri Kalite Kontrolü
```mermaid
flowchart LR
  M[(mart.fact_orders / mart.fact_order_items / mart.daily_commerce_metrics / mart.daily_summary_metrics)] --> C[scripts/dq_checks.py]
  C -->|başarılı| OK[Pipeline Success]
  C -->|hata| FAIL[Task Fail + Retry]
```

## Loglama
`app_logging.py` üzerinden yapılandırılmış JSON loglama kullanılır.
- Ortam değişkeni: `LOG_LEVEL` (`DEBUG`, `INFO`, `WARNING`, `ERROR`)
- Varsayılan: `INFO`

Örnek:
```bash
LOG_LEVEL=DEBUG docker compose up --build
```

## 100x Veri Büyümesinde Nasıl Evrilir?
100x ölçek için önerilen değişiklikler:
1. Local dosya yerine object storage kullanımı (GCS/S3).
2. CSV yerine partitioned Parquet formatı.
3. Full refresh yerine incremental load (watermark/CDC).
4. Eşzamanlı BI yükleri için managed warehouse (BigQuery/Snowflake/Redshift).
5. Metadata-driven orchestration, schema evolution ve data contract yönetimi.
6. Monitoring/alerting (SLA, freshness, row count drift, failure alert).
7. Dev/stage/prod ayrımı, CI/CD ve otomatik veri testleri.

### Diyagram 6: Uçtan Uca Mimari
```mermaid
flowchart LR
  RAW[data/raw CSV] --> STG[(staging)]
  STG --> MART[(mart fact/dim)]
  MART --> BI[[daily_commerce_metrics / daily_summary_metrics / daily_top_category]]
  AIR[Airflow DAG] --> STG
  AIR --> MART
  DQ[DQ Checks] --> MART
```
