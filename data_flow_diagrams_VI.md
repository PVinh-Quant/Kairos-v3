> 📖 **[English Version](data_flow_diagrams.md)**

# KAIROS v3.2 — Sơ đồ luồng dữ liệu & Workflow (Production & Live Core)

> **Ký hiệu:** ✅ = đã có | ❌ = cần build | 🔗 = tái dùng module có sẵn | **[DEFER]** = không cần cho first release

---

## A. Master Data Flow — Toàn cảnh

```mermaid
flowchart TB
    subgraph SOURCE["☁️ Nguồn dữ liệu"]
        WS_LIVE["WebSocket Gateway ✅\nbinance_ws / bybit_ws / okx_ws\nthu_thap/websocket/\nRunner: kich_ban/data_collector.py ✅ 2026-06-01\n→ python main.py collect"]
        REST_FB["REST API ✅\nthu_thap/rest_api/\nGap fill fallback | Historical backfill\nBackfill runner: kich_ban/backfill_history.py ✅\n→ python main.py backfill (2yr OHLCV+Funding+OI)"]
    end

    subgraph INGEST["📦 M-D1: Raw Ingestion ✅ (100%)"]
        direction TB
        OHLC["xu_ly_dong/bo_loc/ohlc_engine.py\nBar boundary: left-inclusive right-exclusive\nbar_start_ns <= event_time_ns < bar_end_ns"]
        OB["xu_ly_dong/bo_loc/orderbook_engine.py"]
        D1MODS["thu_thap/bar_processor.py ✅ • bar_types.py ✅\nsettlement_buffer.py ✅ • dead_letter_queue.py ✅\nfunding_interval_cache.py ✅ • liquidation_aggregator.py ✅\naux_parquet_writer.py ✅ • symbol_remapper.py ✅\nschema_validator.py ✅ • startup_routines.py ✅"]
        RAW[("ho_du_lieu/tho/{exchange}/{symbol}/year={Y}/month={M}/data.parquet\nho_du_lieu/tho/{exchange}/{symbol}/long_short/\nho_du_lieu/tho/onchain/{asset}/{metric}/\nImmutable — atomic write → M-D0 register")]
    end

    subgraph DATA_CORE["🗝️ DATA CORE"]
        direction TB
        subgraph MD0["📋 M-D0: Data Lineage ✅ (Phase 0 — 8 files built + tested)"]
            direction TB
            DSREC["quan_ly_phien_ban/dataset_record.py ✅\nDatasetRecord + VerificationRecord dataclasses\ncanonical Arrow IPC hash (column+row sorted)\ncompute_feature_logic_hash (AST-based)\nwrite_and_register() atomic + verify_fn hook"]
            LINREG["quan_ly_phien_ban/lineage_registry.py ✅\nAppend-only JSONL registry\nnext_version() atomic allocation\nstartup orphan sweep\nLINEAGE_ROOT env var (absolute path)"]
            VERREG["quan_ly_phien_ban/verification_registry.py ✅\nis_production_verified(dataset_id) → bool\nFAILED verifications PHẢI persist"]
            EXC["quan_ly_phien_ban/exceptions.py ✅\nLineageError hierarchy\nImmutabilityError, PITViolationError, ..."]
            LOCK["quan_ly_phien_ban/_locking.py ✅\ncross-platform file lock + fsync\nUnix fcntl / Windows msvcrt"]
            PITV["quan_ly_phien_ban/pit_verifier.py ✅\nverify_pit_production(sample≥500, random seed)\nstratified sampling: warmup/recent/edge/random"]
            SLLP["quan_ly_phien_ban/symbol_lifecycle_poller.py ✅\n[daemon] daily REST poll\n→ symbol_lifecycle_raw.parquet (state-based intervals)\nConsumer: M-D2 + M-D3"]
            DSREC --- LINREG --- VERREG
            EXC -.-> DSREC
            LOCK -.-> LINREG
            PITV -.-> VERREG
        end

        subgraph GAP["🔧 M-D2 ✅ Done (8 files) | M-D3 ✅ Done (3 files)"]
            direction TB
            MAINT["xu_ly_lo/maintenance_event_logger.py ✅\n[daemon] poll exchange status mỗi 5min\n→ maintenance_log_{date}.parquet + daemon_heartbeat.json\nconfidence: HIGH (daemon) | LOW (manual_override)"]
            GAPDET["xu_ly_lo/gap_detector.py ✅\nScan missing/suspect bars\nzombie volume: MAD robust_zscore (two-tail)\nbaseline = history only (no look-ahead)\ngap manifest TRƯỚC fill (detection fields immutable)"]
            RESTFILL["xu_ly_lo/rest_filler.py ✅\ngap ≤ max_gap_duration_s → REST fill (async)\nfill_gap() → (df, quality, reason)\nINTRA-GAP CONTINUITY + BOUNDARY (SYMMETRIC)\nRIGHT denom = bars[-1].close | HTTP 418/429 backoff"]
            RFUND["xu_ly_lo/reconcile_funding_rates.py ✅\nfunding_rate_raw + funding_interval_min\nfunding_published_ns (anti-lookahead)\nreceived_ns set SAU response | FAIL LOUD cache miss"]
            QTAG["xu_ly_lo/quality_tagger.py ✅\ndata_quality: 0/1/2/3/4\ndaemon stale → quality=3 (không skip)\ncoverage report aggregate từ batch runner"]
            SCHVAL["xu_ly_lo/schema_validator.py ✅\nMD2_SCHEMA explicit PyArrow (26 cols)\nschema_version mandatory trong mọi Parquet\nwrite_with_schema_version() atomic"]
            RECONCILE["xu_ly_lo/reconcile.py ✅\nMain orchestrator steps 0–7\nrun_reconciliation_batch() → coverage report\nMD2_SCHEMA ép type tại write time"]
            PITUNIV["xu_ly_lo/pit_universe.py ✅\n+ symbol_remapper.py ✅ + asset_registry.db ✅\nknown_at semantics | UUID v5 asset_id\nLifecycle: ACTIVE/SUSPECTED_DELIST/DELISTED\nin_entry_universe gate | circuit breaker\npit_audit() | universe_health_check()"]
            EXCMETA["cau_hinh/exchange_metadata.yaml ✅\nSINGLE SOURCE OF TRUTH:\nfunding schedule, min_order_size,\nfee_tiers, liquidation engine\nINV-EXC.1: adapters reference đây, không hardcode"]
            CLEAN[("ho_du_lieu/da_xu_ly/\n+ is_gap_filled, gap_duration_s\n+ data_quality (0/1/2/3/4)\n+ funding_rate_raw, funding_interval_min\n+ funding_published_ns, funding_rate_stale\n+ close_time_ns, funding_rate_annual")]
            MANIFEST[("ho_du_lieu/gap_manifest/{date}/\n{exchange}_{symbol}.json\nAudit artifact — detection fields IMMUTABLE")]
            COVERAGE[("ho_du_lieu/bao_cao_phu_song/{date}.json\nDaily coverage report — always generated")]
            MAINT --> GAPDET
            GAPDET --> RESTFILL
            RESTFILL --> QTAG
            RFUND --> RECONCILE
            QTAG --> RECONCILE
            SCHVAL --> RECONCILE
            RECONCILE --> CLEAN
            RECONCILE --> MANIFEST
            RECONCILE --> COVERAGE
            PITUNIV -->|"asset_id + sector"| CLEAN
            EXCMETA -.->|"reference"| GAPDET
            SLLP -.->|"symbol_lifecycle_raw"| GAPDET
        end
    end

    subgraph FEATURE_LAYER["🔩 M-D4: Feature Engine ✅ Done"]
        direction TB
        FB["xu_ly_lo/feature_cache.py ✅\nFeatureCache: get_or_compute(), atomic write\ncache_hash 12-char | idempotent skip\nBTC-first ordering (INV-D4.28)"]
        FB_L2["xu_ly_lo/pre_aggregate_l2.py ✅\nL2Snapshot binary store (KHÔNG delete)\nOFI formula (Cont 2014) | book_pressure_5min\naggregate_l2_features() batch per-day"]
        IE["ong_dan_dac_trung/online/incremental_engine.py ✅\nIncrementalFeatureEngine — live realtime\nSync emit: NaN all nếu bất kỳ feature chưa warm"]
        FEAT[("ho_du_lieu/kho_dac_trung/offline/\nfeatures_*.parquet\nschema_version mandatory")]
        MEM[("ho_du_lieu/kho_dac_trung/online/memory_store.py ✅\nLive in-memory")]
        FB --> FEAT
        IE --> MEM
    end

    subgraph DEPLOY["🚀 Live Core ✅/❌"]
        direction TB
        PAPER["moi_truong_chay/paper/ ✅ (~95%) — M-L1\nPaper trade + fill capture"]
        SIG["thuc_thi_lenh/dong_co_tin_hieu/ml_signal_engine.py ✅\nONNX inference"]
        EMS["thuc_thi_lenh/dong_co_thuc_thi/ems.py ✅"]
        RG["quan_tri_rui_ro/kiem_tra_truoc_lenh/risk_gate.py ✅ (~70%) — M-L5\nRG-1 to RG-17 pre-trade checks\nINV-EXC.1: exchange_metadata.yaml reference"]
        MON["giam_sat/ ❌ (Phase 2)\nreality_gap_monitor.py • auto_kill.py\ngiam_sat/canh_bao/alert_manager.py"]
    end

    WS_LIVE --> OHLC --> RAW
    WS_LIVE --> OB --> RAW
    REST_FB -.->|"fallback only"| GAP
    RAW --> GAP
    SLLP -.->|"symbol_lifecycle_raw.parquet"| GAP
    CLEAN --> FB
    CLEAN --> IE
    MEM --> SIG
    FEAT -.-> SIG
    PAPER --> SIG --> EMS --> RG
    MON -.->|"circuit breaker kill"| SIG

    style SOURCE fill:#1a1a2e,stroke:#e94560,color:#fff
    style DEPLOY fill:#0f3460,stroke:#16213e,color:#fff
    style GAP fill:#1a2e1a,stroke:#27ae60,color:#fff
    style MANIFEST fill:#0a1a0a,stroke:#27ae60,color:#aaa
    style COVERAGE fill:#0a1a0a,stroke:#27ae60,color:#aaa
    style MD0 fill:#2e1a3e,stroke:#9b59b6,color:#fff
    style DATA_CORE fill:#0a0a1a,stroke:#555,color:#fff
```

---

## B. Portfolio Construction & Risk Gate

```mermaid
flowchart TB
    subgraph ALPHAS["🔬 Live Signal Input"]
        SIG_IN["ml_signal_engine.py ✅\nONNX Signal Output per asset"]
    end

    subgraph ERC["📐 ERC Portfolio — M-L4 (Phase 3)\nhoc_may/to_hop_alpha/erc_optimizer.py ❌"]
        direction TB
        E1["Equal Risk Contribution\nMỗi alpha contribute equal volatility\nKhông MVO — không CVXPY"]
        E2["Constraints:\n  gross_leverage ≤ 2.0\n  net_leverage ≤ 0.5\n  |w_symbol| ≤ 0.20 per name\n  turnover ≤ 20%/ngày two-way\n  family_exposure[F] ≤ 60%"]
        E3["portfolio_rebalancer.py\nDiff current vs target\nGenerate rebalance orders"]
        E1 --> E2 --> E3
    end

    subgraph FUNDING["💸 Funding Settlement Rules"]
        direction TB
        FD1["raw_return = price_return + funding_income"]
        FD2["surprise_funding = realized - expected\nExpected từ Premium Index MA"]
        FD3["⚠️ Funding settlement-aligned:\nCharge CHỈ tại settlement bars (00:00/08:00/16:00 UTC)\nChỉ khi position[t] != 0 AND position[t-1] != 0\nKhông per-bar approximation"]
        FD1 --> FD2 --> FD3
    end

    subgraph RISK_GATE["🔴 Risk Gate — M-L5\nquan_tri_rui_ro/kiem_tra_truoc_lenh/risk_gate.py ✅"]
        direction TB
        RG1["RG-1→RG-12: core pre-trade checks"]
        RG2["RG-13: Inventory Skew (HHI < 0.30)\nRG-14: Cancel/Replace Storm (>10/min → pause)\nRG-15: Dead-Man Switch (heartbeat 300s)"]
        RG3["RG-16: ADL Risk Monitor ❌\nAlert nếu ADL rank top-10%\nReduce 30% nếu top-5% → block new entries\nINV-EXC.1: references exchange_metadata.yaml"]
        RG4["RG-17: Exchange Solvency Monitor ❌\nFTX-pattern: withdrawal freeze, insurance fund\nMax 20% capital per exchange\nHalt + initiate withdrawal 50% khi flag"]
        RG1 --> RG2 --> RG3 --> RG4
    end

    ALPHAS --> ERC
    FUNDING -.->|"position sizing adjust"| ERC
    ERC --> RISK_GATE
    RISK_GATE -->|"PASS → execute"| EXEC["thuc_thi_lenh/dong_co_thuc_thi/ems.py ✅"]

    style ERC fill:#1a3c2e,stroke:#27ae60,color:#fff
    style EXEC fill:#27ae60,color:#fff
    style RG3 fill:#3a1a1a,stroke:#e74c3c,color:#fff
    style RG4 fill:#3a1a1a,stroke:#e74c3c,color:#fff
```

---

## C. Production Monitoring & Recovery Protocols

```mermaid
flowchart TB
    subgraph DEPLOY["🚀 Production Execution"]
        D1["thuc_thi_lenh/dong_co_tin_hieu/ml_signal_engine.py ✅"]
        D2["thuc_thi_lenh/dong_co_thuc_thi/ems.py ✅"]
        D3["quan_tri_rui_ro/ ✅\nrisk_gate (RG-1→RG-17) + watchdog + emergency_flattener"]
        D1 --> D2 --> D3
    end

    subgraph MONITOR["📡 Performance & Health Monitoring"]
        direction TB
        M1["giam_sat/chi_so_hieu_suat/collector.py ✅\nLatency • throughput • resource"]
        M2["giam_sat/canh_bao/alert_manager.py ✅\n4-TIER ROUTING:\n  CRITICAL → Telegram 24/7 (không suppress)\n  ALERT → Telegram 00:00-20:00 UTC\n  WARNING → Daily digest 07:00 UTC\n  INFO → Log only\nSuppression: same-source 5 phút | flood >5/10min\nCool-down 30 phút sau HALT resume"]
        M3["giam_sat/reality_gap_monitor.py ❌\nSlippage vs model estimate\nFlag |z| > 2.0 per dimension"]
        M4["giam_sat/auto_kill.py ❌ — M-L3\nRolling 30d performance decay trigger\nCircuit breaker intervention"]
        M5{{"Trigger Action?"}}
        M1 --> M5
        M2 --> M5
        M3 --> M5
        M4 --> M5
    end

    subgraph EXEC_RISK["🔴 Execution Risk Layers"]
        RL1["RG-13: Inventory Skew\nHHI > 0.50 → reduce dominant 25%"]
        RL2["RG-14: Cancel/Replace Storm\n>10 cancel/min → pause symbol\n>30/min → halt all"]
        RL3["RG-15: Dead-Man Switch\nHeartbeat mất 300s → flatten all\nIndependent watchdog process"]
        RL4["RG-9: Exchange Failure\nTimeout ×3 → halt, wait reconnect\nReconcile trước khi resume"]
        RL5["RG-16: ADL Monitor ❌\nTop-5% → reduce 30%, block entries\n→ ALERT tier: ADL rank top-5%"]
        RL6["RG-17: Exchange Solvency ❌\nWithdrawal freeze → CRITICAL alert\nInitiate withdrawal + halt entries\n→ CRITICAL tier: exchange solvency flag"]
        RL1 --- RL2 --- RL3 --- RL4 --- RL5 --- RL6
    end

    subgraph ACTIONS["⚡ System Actions"]
        A1["🟡 SCALE DOWN\nscale_factor = 0.5 + alert"]
        A2["🔴 HALT / FLATTEN\nEmergencyFlattener ✅"]
        A3["😴 HIBERNATE\nScale to 0%, freeze trading"]
    end

    subgraph RECOVERY["🔁 Recovery Protocols"]
        direction TB
        REC1["RECOVERY-1 (Max DD >15%):\nPaper 14d → 25% → 50% (14d) → 100% (21d)"]
        REC2["RECOVERY-2 (Exchange outage):\nReconcile → resume ngay (không ramp-up)"]
        REC3["RECOVERY-3 (Feature drift CRITICAL):\nFix → paper 7d OK → resume full"]
        REC4["RECOVERY-4 (Depeg):\nResume ngay khi peg < 0.1% sustained 2h"]
        REC5["RECOVERY-5 (ADL top-5%):\n50% → 75% → 100% mỗi 4 giờ nếu ADL ổn định"]
        HALT_TREE["HALT DECISION TREE:\n  Infrastructure? → RECOVERY-2 or -3\n  Market condition? → RECOVERY-4 or -5\n  Signal quality? → RECOVERY-1\n  Never conflate infrastructure halt với signal decay"]
        REC1 --- REC2 --- REC3 --- REC4 --- REC5 --- HALT_TREE
    end

    D3 --> MONITOR
    EXEC_RISK -.->|"trigger halt"| A2
    M5 -->|"performance decay"| A1
    M5 -->|"DD breach / critical alert"| A2
    M5 -->|"crisis market regime"| A3
    A1 -->|"continue monitoring"| MONITOR
    A2 -.->|"apply"| RECOVERY

    style A1 fill:#f1c40f,color:#000
    style A2 fill:#e74c3c,color:#fff
    style A3 fill:#5d4037,color:#fff
    style EXEC_RISK fill:#2c2c2c,stroke:#e74c3c,color:#fff
    style RECOVERY fill:#1a2e1a,stroke:#27ae60,color:#fff
```

---

## D. Data Sourcing & Reconciliation Flow

```mermaid
flowchart TB
    subgraph SOURCES["Nguồn dữ liệu"]
        WS2["WS Gateway ✅\nthu_thap/websocket/\nPrimary — raw stream lưu liên tục\nRunner: kich_ban/data_collector.py ✅ (2026-06-01)\n  BinanceGateway → BarBuilder(1h) → Parquet\n  OI poll 5min | midnight flush\n  python main.py collect"]
        REST2["REST API ✅\nthu_thap/rest_api/\nGap fill ≤ 3 bars | Historical backfill\nBackfill: kich_ban/backfill_history.py ✅\n  /klines + /fundingRate + /openInterestHist\n  2yr history, 335K bars loaded (2026-06-01)\n  python main.py backfill"]
    end

    subgraph LINEAGE["📋 M-D0: Data Lineage ✅\ndong_co_du_lieu/quan_ly_phien_ban/"]
        direction TB
        LD1["dataset_record.py\nDatasetRecord: UUID, SHA-256, derivation_type\nfeature_logic_hash, derived_from DAG"]
        LD2["lineage_registry.py\nas_of_feature_snapshot(date) → dataset_id\nAppend-only — không delete hoặc modify"]
        LD3["Mọi data pipeline PHẢI log dataset_id_used\n[INV-D0.2] không được dùng 'current data'"]
        LD1 --> LD2 --> LD3
    end

    subgraph GAP_REC["dong_co_du_lieu/xu_ly_lo/ — M-D2 ✅ Done | M-D3 ✅ Done"]
        direction TB
        GD["gap_detector.py ✅\nScan missing bars per (exchange, symbol)\nzombie: MAD robust_zscore, baseline = history only\nExpected vs actual bar count"]
        RF["rest_filler.py ✅\nfill_gap() → (df, quality, reason)\nGap ≤ 3h → REST fill, is_gap_filled=True\nGap > 3h → data_quality=3 | continuity fail → quality=3"]
        QT["quality_tagger.py ✅\n0=perfect, 1=rest_filled, 2=suspect, 3=missing, 4=maintenance\ndaemon stale → quality=3 (không bỏ qua marking)"]
        REC["reconcile.py ✅\nOrchestrator steps 0–7\nMD2_SCHEMA explicit PyArrow schema\ncoverage report từ batch runner"]
        SCHV["schema_validator.py ✅\nMD2_SCHEMA: 26 columns explicit type\nschema_version=1 mandatory trong mọi output Parquet\nwrite_with_schema_version() atomic"]
        PIT["pit_universe.py ✅\n+ symbol_remapper.py ✅ + asset_registry.db ✅\nknown_at semantics | UUID v5 asset_id\nLifecycle 3 states + in_entry_universe\nseed_sector_assignments.py ✅"]
        GD --> RF --> QT --> REC
        SCHV --> REC
        PIT -.->|"Phase 1"| REC
    end

    subgraph STORAGE["ho_du_lieu/ — Storage"]
        RAW[("ho_du_lieu/tho/\n{exchange}/{symbol}/year/month/data.parquet\nImmutable — write-to-temp then atomic rename\nschema_version mandatory")]
        CLEAN[("ho_du_lieu/da_xu_ly/\n+ is_gap_filled, gap_duration_s, data_quality\nBars data_quality>1 excluded\nschema_version mandatory")]
        FEAT[("ho_du_lieu/kho_dac_trung/offline/\nFeatures — [DEFER Phase 1]\nschema_version mandatory")]
    end

    subgraph VALIDATE["Validation rules"]
        direction TB
        V1["[INV-D1.1] event_time_ns strictly monotonic"]
        V2["[INV-D1.2] No ffill trên raw OHLCV"]
        V3["[INV-D1.3] recv_time_ns >= event_time_ns"]
        V4["[INV-D2.2] data_quality ∈ {3,4} KHÔNG dùng trong signal\n(quality>1 → label=NaN)"]
        V5["All timestamps = UTC nanoseconds (không exception)"]
        V6["[INV-D1.6] Bar boundary: left-inclusive, right-exclusive\nbar_start_ns <= event_time_ns < bar_end_ns\nEvent tại exactly T+1 thuộc bar T+1, không T"]
        V7["[INV-D4.15] schema_version field mandatory\n  trong mọi Parquet file\n[INV-D4.16] Không hardcode column index\n[INV-D4.17] Required cols: crash fast | Optional: NaN"]
    end

    WS2 --> RAW
    REST2 -.->|"gap fill fallback"| GAP_REC
    RAW --> GAP_REC --> CLEAN --> FEAT
    LINEAGE -.->|"track dataset_id"| STORAGE

    style REST2 fill:#4a1a1a,stroke:#e74c3c,color:#fff
    style WS2 fill:#1a2e1a,stroke:#27ae60,color:#fff
    style LINEAGE fill:#2e1a3e,stroke:#9b59b6,color:#fff
```

---

## E. Live Dashboard Spec & Alert Routing

```mermaid
flowchart TB
    subgraph DASH["📊 Live Dashboard — Build TRƯỚC ngày 1 paper trade"]
        direction TB

        subgraph S1["SCREEN 1: HEALTH (daily, ≤60 giây)"]
            direction TB
            H1["Portfolio: PnL today/week | Gross/net leverage | Free collateral"]
            H2["Strategy Status: [F001 LIVE] PnL_30d=X ✅|⚠️ | fills=N"]
            H3["System: Pipeline OK | Feature OK | WAL size | Open orders"]
            H4["Alerts: N CRITICAL, M WARNING | [see digest]"]
            H1 --> H2 --> H3 --> H4
        end

        subgraph S2["SCREEN 2: STRATEGY DETAIL (khi có vấn đề)"]
            direction TB
            AD1["Live performance series last 30d (spark-line)"]
            AD2["Live IC vs baseline: current/target % — target ≥70%"]
            AD3["Fill rate maker actual vs target (≥0.55 BTC/ETH gate)"]
            AD4["TCA: timing_cost | impact_cost | fee split"]
            AD1 --> AD2 --> AD3 --> AD4
        end

        subgraph S3["SCREEN 3: TCA (weekly)"]
            direction TB
            T1["round_trip_cost actual vs model"]
            T2["adverse_selection actual vs model\n(update exchange_metadata khi gap > 30%)"]
            T3["maker_fill_rate by symbol, by hour-of-day"]
            T4["cancel_rate by symbol (alert if > 15%)"]
            T1 --> T2 --> T3 --> T4
        end

        subgraph S4["SCREEN 4: REGIME (weekly)"]
            direction TB
            R1["M-R11 state_vector last 7d:\n  regime: trending/ranging/stressed"]
            R2["ood_score max this week\n  (alert if > 2.5 on 3+ days)"]
            R3["btc_alt_corr rolling 7d\n  (alert if > 0.80 — crisis mode)"]
            R4["funding_extreme: any day funding_zscore > 2.5?"]
            R1 --> R2 --> R3 --> R4
        end

        BUILD_ORDER["Implementation priority:\nWeek 1 paper: Screen 1 + Alerts\nWeek 2: Screen 2 (Strategy Detail)\nWeek 4+: Screen 3 (TCA, sau ≥50 fills)\nMonth 2+: Screen 4 (Regime, sau M-R11 running)"]
    end

    subgraph ALERT_TIER["🔔 Alert Routing (4-tier)"]
        direction TB
        CR["CRITICAL → Telegram 24/7 (không suppress)\n  RG-1→RG-17 halt conditions\n  Dead-man switch fire (RG-15)\n  Exchange withdrawal freeze (RG-17)\n  Pipeline crash (live stream failure)\n  Response: human trong 15 phút"]
        AL["ALERT → Telegram (00:00–20:00 UTC)\n  effective_leverage > 1.8\n  feature drift pct_diff > 10%\n  ADL rank top-5% (RG-16)\n  Performance decay triggered\n  Response: human review trong 2 giờ"]
        WA["WARNING → Daily digest 07:00 UTC\n  feature drift 1–10%\n  clock drift > 500ms\n  cancel rate > 15% (sub-threshold)\n  symbol coverage < 95%"]
        IN["INFO → Log only (không Telegram)\n  normal fills, daily report\n  regime state update"]
        SUPP["Suppression rules:\n  Same-source 5 phút: suppress duplicate\n  Flood >5 ALERT/10 min: aggregate\n  Cool-down 30 phút sau HALT resume\n  CRITICAL: không bao giờ suppressed"]
        CR --- AL --- WA --- IN --- SUPP
    end

    S1 --> S2 --> S3 --> S4
    ALERT_TIER -.->|"feeds alerts"| S1

    style CR fill:#c0392b,stroke:#e74c3c,color:#fff
    style AL fill:#d35400,stroke:#e67e22,color:#fff
    style WA fill:#b7950b,stroke:#f39c12,color:#000
    style IN fill:#1a3c1a,stroke:#27ae60,color:#fff
    style SUPP fill:#1a1a2e,stroke:#3498db,color:#fff
```
