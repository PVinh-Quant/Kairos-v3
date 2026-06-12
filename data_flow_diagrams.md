# KAIROS v3.2 — Sơ đồ luồng dữ liệu & Workflow

> **Ký hiệu:** ✅ = đã có | ❌ = cần build | 🔗 = tái dùng module có sẵn | **[DEFER]** = không cần cho first alpha

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

    subgraph FEATURE_LAYER["🔩 M-D4: Feature Cache ✅ Done (5 files)"]
        direction TB
        FB["xu_ly_lo/feature_cache.py ✅\nFeatureCache: get_or_compute(), atomic write\ncache_hash 12-char | idempotent skip\nBTC-first ordering (INV-D4.28)"]
        FB_L2["xu_ly_lo/pre_aggregate_l2.py ✅\nL2Snapshot binary store (KHÔNG delete)\nOFI formula (Cont 2014) | book_pressure_5min\naggregate_l2_features() batch per-day"]
        FR["nghien_cuu/khung_alpha/feature_registry.py ✅\nFEATURE_REGISTRY: 12 FeatureFn\nWinsorize [1%,99%] return; 5×IQR funding/OI\nWelford funding_z; OLS 504 bars btc_neutral\nIncrementalFeatureEngine + FeatureSpecRegistry"]
        FSPEC["nghien_cuu/khung_alpha/feature_spec.py ✅\n12 FeatureSpec entries + FEATURE_SPEC_MAP\nDAG cycle validation tại import time\nMọi feature phải có entry trước T0/T1/T2"]
        IE["ong_dan_dac_trung/online/incremental_engine.py ✅\nIncrementalFeatureEngine — live realtime\nSync emit: NaN all nếu bất kỳ feature chưa warm"]
        FEAT[("ho_du_lieu/kho_dac_trung/offline/\nfeatures_*.parquet\nschema_version mandatory")]
        MEM[("ho_du_lieu/kho_dac_trung/online/memory_store.py ✅\nLive in-memory")]
        FB -->|"calls same functions"| FR
        IE -->|"calls same functions"| FR
        FB --> FEAT
        IE --> MEM
        FSPEC -.->|"validates"| FB
    end

    subgraph RESEARCH["🔬 Research Core ❌ (Phase 0–1)"]
        direction TB
        M_R1["hoc_may/huan_luyen/ — M-R1\nexperiment_record.py • reproducibility.py\nlabeler.py • trainer.py (Ridge default)"]
        M_R2["nghien_cuu/danh_gia/leakage_audit.py — M-R2\nGate bắt buộc trước T1"]
        M_R7["nghien_cuu/nha_may_alpha/cost_model.py — M-R7\nround_trip_cost_bps(symbol, side, size, regime)"]
        M_R6["nghien_cuu/dong_co_phat_lai/vectorized_backtest.py — M-R6\nPolars-based, single file"]
        M_R3["nghien_cuu/nha_may_alpha/t0_screen.py — M-R3 T0\n≤15 min | IC < 0.025 → KILL"]
        M_R4["nghien_cuu/nha_may_alpha/t1_validate.py — M-R4 T1\nPurgedKFold + DSR + ICIR + Regime | ≤2 hr"]
        M_R5["nghien_cuu/nha_may_alpha/t2_diligence.py — M-R5 T2\nStress + Walk-Forward | ≤1 day"]
        REGISTRY["nghien_cuu/nha_may_alpha/alpha_registry.py — M-R9\nalpha_cemetery.py — Kill tracking\nlifecycle.py — Stage gate validation"]
    end

    subgraph DEPLOY["🚀 Live Core ✅/❌"]
        direction TB
        PAPER["moi_truong_chay/paper/ ✅ (~95%) — M-L1\nPaper trade + fill capture"]
        SIG["thuc_thi_lenh/dong_co_tin_hieu/ml_signal_engine.py ✅\nONNX inference"]
        EMS["thuc_thi_lenh/dong_co_thuc_thi/ems.py ✅"]
        RG["quan_tri_rui_ro/kiem_tra_truoc_lenh/risk_gate.py ✅ (~70%) — M-L5\nRG-1 to RG-17 pre-trade checks\nINV-EXC.1: exchange_metadata.yaml reference"]
        MON["giam_sat/ ❌ (Phase 2)\nexecution_parity.py • reality_gap_monitor.py • auto_kill.py\ngiam_sat/canh_bao/alert_manager.py"]
    end

    WS_LIVE --> OHLC --> RAW
    WS_LIVE --> OB --> RAW
    REST_FB -.->|"fallback only"| GAP
    RAW --> GAP
    MD0 -.->|"dataset_id gate"| RESEARCH
    SLLP -.->|"symbol_lifecycle_raw.parquet"| GAP
    CLEAN --> FB
    FEAT --> M_R1
    M_R1 --> M_R3
    M_R7 --> M_R3
    M_R2 -->|"PASS gate"| M_R4
    M_R3 -->|"T0 PASS"| M_R4
    M_R4 -->|"T1 PASS"| M_R5
    M_R6 --> M_R5
    REGISTRY --> M_R4
    M_R5 -->|"T2 PASS"| PAPER
    PAPER --> SIG --> EMS --> RG
    MON -.->|"IC decay kill"| SIG

    style SOURCE fill:#1a1a2e,stroke:#e94560,color:#fff
    style DEPLOY fill:#0f3460,stroke:#16213e,color:#fff
    style GAP fill:#1a2e1a,stroke:#27ae60,color:#fff
    style MANIFEST fill:#0a1a0a,stroke:#27ae60,color:#aaa
    style COVERAGE fill:#0a1a0a,stroke:#27ae60,color:#aaa
    style RESEARCH fill:#1a1a3e,stroke:#3498db,color:#fff
    style MD0 fill:#2e1a3e,stroke:#9b59b6,color:#fff
    style DATA_CORE fill:#0a0a1a,stroke:#555,color:#fff
```

---

## B. Research Loop — T0 / T1 / T2

```mermaid
flowchart LR
    subgraph NB_STAGE0["📓 Stage -1 + 0 — 30 phút\nKhông code, không data"]
        direction TB
        S0_1["Mechanism: giải thích 2 câu\nCemetery similarity search: < 0.85\nFeature count ≤ 5"]
        S0_2["venue_type: agnostic / dependent\nHalflife sanity: < 2 bars → KILL\nListings cohort filter documented"]
        S0_3["Pre-register ExperimentRecord\nhypothesis_text (immutable)\nn_trials/family tracking start\nt0_override=False (default)\nnotes='' (cemetery override reason)"]
        S0_1 --> S0_2 --> S0_3
    end

    subgraph NB_T0["📓 T0 Screen — ≤15 phút\nnghien_cuu/nha_may_alpha/t0_screen.py"]
        direction TB
        T0_1["Gross BTC-neutral returns từ labeler.compute_gross()\n(btc_closes từ M-D2 — KHÔNG phải M-D4 features)\nSpearman IC — toàn sample, no CV"]
        T0_2["Rough cost = turnover × round_trip_bps (M-R7)\nK3: cost_bps / (|IC|×1000) > 0.40 → KILL"]
        T0_3["Dynamic threshold (Cochrane AR(1) effective_n):\n  n<300 strict: IC>0.030, CostAdj>0.020\n  300-600 std: IC>0.025, CostAdj>0.015\n  n>600 relaxed: IC>0.020, CostAdj>0.012"]
        T0_4{{"K1: |IC| < threshold → KILL\nK2: cost_adj_ic < threshold → KILL\nK3: cost > 40% IC edge → KILL\nBORDERLINE: |IC| ∈ [0.015, threshold) → manual review\nsignal_direction = -1 → T1 TỰ invert (caller pass raw signal)"}}
        T0_1 --> T0_2 --> T0_3 --> T0_4
    end

    subgraph NB_LEAK["📓 Leakage Audit — chạy 1 lần\nnghien_cuu/danh_gia/leakage_audit.py — M-R2"]
        direction TB
        L1["AUDIT-1: Future probe (14 checks total)\n  Forward: IC(signal[t], return[t+k]) ≈ 0 (k=1..10)\n  Backward baseline: IC(signal[t], return[t-1]) ≈ 0"]
        L2["AUDIT-2: Normalization PiT — expanding window only\nAUDIT-3/6/7/8/9/11-14: non-overridable integrity checks"]
        L3["AUDIT-4/5/10: overridable operational checks\n(max 2/năm, human sign-off required)"]
        L4{{"PASS / FAIL — gates T1\nFAIL → block T1 hoàn toàn\n[Non-overridable: AUDIT-1/2/3/6/7/8/9/11/12/13/14\nOverridable: AUDIT-4/5/10 — max 2/năm]"}}
        L1 --> L2 --> L3 --> L4
    end

    subgraph NB_T1["📓 T1 Validate — ≤2 giờ\nnghien_cuu/nha_may_alpha/t1_validate.py — M-R4"]
        direction TB
        T1_1["PurgedKFold (5-fold, 21-day embargo)\nmean_cv_ic > 0.020"]
        T1_1b["ICIR = mean(CV_IC_folds) / std(CV_IC_folds)\nKill nếu ICIR < 1.0\nWarn nếu ICIR < 1.5"]
        T1_2["DSR > 0.50 (n_trials_total từ ExperimentRecord)\nMỗi feature/horizon/filter tweak = +1 trial"]
        T1_3["Regime split IC (3 regimes)\nKill nếu bất kỳ regime < -0.010"]
        T1_4["T1.4: capacity ≥ $100K (rough ADV estimate)\nT1.5: corr vs registry < 0.60\nT1.6: joint OLS BTC+ETH |residual_beta| < 0.15\nT1.7: Ridge coeff stability > 2.0 per fold\nT1.8: ic_short < -0.01 → max_short_weight ≤ 50%\nT1.9: breadth_pct ≥ 0.30 (≥30% symbols có IC > 0)"]
        T1_5{{"ALL checks pass?"}}
        T1_1 --> T1_1b --> T1_2 --> T1_3 --> T1_4 --> T1_5
    end

    subgraph NB_T2["📓 T2 Full Diligence — ≤1 ngày\nnghien_cuu/nha_may_alpha/t2_diligence.py — M-R5"]
        direction TB
        T2_1["T2.1: Vectorized backtest 2 năm\nSharpe > 0.8 (aim 1.2) | max_dd < 25%\nir_vs_benchmark ≥ 0.20\nexecution_lag_bars=1, execution_price=open_next\nrolling_vol_window=21 bars\nFunding settlement-aligned (không per-bar):\n  charge CHỈ khi position open TRƯỚC và TẠI settlement"]
        T2_2["T2.2: Walk-forward n_windows (5 theo research.yaml)\nwf_ic_std < 0.50 × wf_ic_mean\n[WF dates PHẢI pre-defined trong research.yaml ✅\ncùng lúc với t1_fold_dates — BLOCKED nếu không có\ntest windows KHÔNG chạm holdout]"]
        T2_3["T2.3: Stress test\nLuna + FTX (mandatory) + 20 random high-vol (seed cố định)\npass ≥ 14/20 random periods"]
        T2_4["Dual fill: Scenario A ≥ 0.80 AND Scenario B ≥ 0.75\n(Scenario B gate thấp hơn 5% = fill rate haircut, không phải free pass)\nT2.7: Marginal Sharpe > 0.05\nT2.8: IC halflife[stressed] kill <2 bars, warn <5 bars\nT2.9: Cascade Exit — cascade_impact < 0.30\n(M-R11 Mode B live/backtest gap)"]
        T2_5{{"ALL checks pass?\nLive haircut expect: 30–60% từ backtest IC"}}
        T2_1 --> T2_2 --> T2_3 --> T2_4 --> T2_5
    end

    KILL["💀 KILL\nalpha_cemetery.py\nAlphaAutopsy → reason logged"]
    PAPER_GO["📝 → PAPER TRADE\nmoi_truong_chay/paper/ ✅"]

    NB_STAGE0 -->|"Stage 0 PASS"| NB_T0
    NB_T0 -->|"T0 PASS"| NB_LEAK
    NB_T0 -->|"KILL"| KILL
    NB_LEAK -->|"LEAK PASS"| NB_T1
    NB_LEAK -->|"FAIL → block"| KILL
    NB_T1 -->|"T1 PASS"| NB_T2
    NB_T1 -->|"KILL"| KILL
    NB_T2 -->|"T2 PASS"| PAPER_GO
    NB_T2 -->|"KILL"| KILL

    style KILL fill:#e74c3c,stroke:#c0392b,color:#fff
    style PAPER_GO fill:#27ae60,color:#fff
```

> **Throughput:**
> - Stage -1+0: ~10–20 ideas/week (15–30 phút/idea)
> - T0: ~5–10 PASS/week (automated, ≤15 phút)
> - T1: ~1–3 PASS/week (≤2 giờ + leakage gate)
> - T2: ~0.5–1 PASS/week (≤1 ngày)
> - Target kill rate: T0 kills 85–95% của ideas qua Stage 0

---

## C. Alpha Lifecycle — Kill Gates

```mermaid
flowchart TB
    IDEA["💡 IDEA\nTừ notebooks / anomaly_miner\nHypothesis seed"]

    STAGE_M1["STAGE -1 — 15 phút\nKhông code, không data"]
    GM1{{"Cemetery similarity < 0.85?\nFeature count ≤ 5?\nHalflife ≥ 2 bars?\nEconomic mechanism 2 câu?"}}

    STAGE0["STAGE 0 — 30 phút\nPre-registration"]
    G0{{"hypothesis_text locked?\nvenue_type declared?\nListing cohort filter documented?\nExperimentRecord created?"}}

    T0["T0 SCREEN — ≤15 phút\nt0_screen.py — M-R3\nIC + cost gate"]
    GT0{{"IC > threshold (dynamic, n-based)?\ncost_adj_ic > threshold?\ncost < 40% gross IC?"}}

    LEAK["LEAKAGE AUDIT — M-R2\nleakage_audit.py\nGate bắt buộc trước T1"]
    GL{{"Future probe ≈ 0?\nNormalization PiT?\nNo look-ahead?"}}

    T1["T1 VALIDATE — ≤2 giờ\nt1_validate.py — M-R4"]
    GT1{{"9 checks (TẤT CẢ phải pass):\nmean_cv_ic > 0.020? ICIR > 1.0?\nDSR > 0.50 (n_trials_total cross-family)?\nNo regime IC < -0.010? (T1.3)\ncapacity ≥ $100K? (T1.4)\ncorr_registry < 0.60? (T1.5)\njoint OLS |residual_beta| < 0.15? (T1.6)\nRidge stability > 2.0? (T1.7)\nbreadth_pct ≥ 0.30? (T1.9)"}}

    T2["T2 FULL DILIGENCE — ≤1 ngày\nt2_diligence.py — M-R5"]
    GT2{{"T2.1: Sharpe ≥ 0.80 (kill < 0.80), max_dd < 25%, ir ≥ 0.20?\nT2.2: wf_ic_std < 0.50 × wf_ic_mean? WF dates pre-defined?\nT2.3: Luna + FTX + 20 random (≥14/20)? Không âm cả 2?\nT2.4: Dual fill A≥0.80 AND B≥0.75?\nT2.5: time_to_recovery ≤ 120 ngày?\nT2.7: marginal_sharpe > 0.05?\nT2.8: IC halflife[stressed] ≥ 2 bars?\nT2.9: cascade_impact < 0.30? (PROXY→revalidate_t29=True)\nT2.10: Sharpe positive ≥ 3/5 market states?\nT2.11: feature drift KS p<0.01 AND ks>0.20 → KILL?"}}

    PAPER["📝 PAPER TRADE\nmoi_truong_chay/paper/ ✅\n≥ 60 calendar market days (INV-R9.2)\nPAPER_INTERIM check day 30 (TransitionEvent BẮT BUỘC)\nfill_capture.py → calibrate M-R7\n[ic_short < -0.01 → max_short_weight ≤ 50%]"]
    GP{{"ALL 6 SHADOW GATE (INV-R9.2):\n1. paper_trading_days_cal ≥ 60?\n2. paper_interim_logged_at NOT NULL?\n3. |paper_ic_live| ≥ 0.025?\n4. paper_ic_live ≥ 0.70 × t1_cv_ic_mean?\n5. paper_sharpe ≥ 0.60?\n6. paper_ic_min_rolling_10d ≥ 0.015?"}}

    SHADOW["🌑 SHADOW TRADE\nMicro-lot $1K–$5K per trade (real orders)\nReal adverse selection + queue dynamics\n≥ 90 calendar days"]
    GL_SHADOW{{"SHADOW → LIVE GATE:\nshadow_days ≥ 90?\nshadow_sharpe ≥ 0.80?\nmax_dd < 15%?\nmaker_fill_rate_actual ≥ 0.55 (BTC/ETH)?\ntca_impact_cost_bps < 4.0 avg?\nHuman sign-off + risk team confirm"}}

    LIVE["🚀 LIVE TRADE\nReal capital (small)\nAll risk gates active\nScheduled T2 re-val: 6 tháng"]
    GL2{{"Rolling 30d IC ≥ 25% inception\nsustained ≥ 14 ngày?\nDD < 15%?"}}
    GCRISIS{{"HARD_DECAY +\nM-R11 crisis regime?\n(btc_alt_corr > 0.8\nAND alt_xs_dispersion thấp)\n[vacuum → scale down 50%\nkhông phải HIBERNATE]"}}

    SCALE["📈 SCALE UP\n+25% capital/month\nM-L4 ERC khi ≥2 alphas\nFamily exposure ≤ 60%"]

    HIBERNATE["😴 HIBERNATE\nScale to 0%\nGiữ trong registry\nReactivate check sau 21 ngày\nMax 90 ngày"]

    RETIRE["📉 RETIRE\nCapital taper 25%/week\nPost-mortem → cemetery\nalpha_retirement.py"]

    KILL["💀 KILL\nalpha_cemetery.py\nAlphaAutopsy(kill_reason)\nSimilarity index updated"]

    IDEA --> STAGE_M1 --> GM1
    GM1 -->|"PASS"| STAGE0
    GM1 -->|"FAIL"| KILL
    STAGE0 --> G0
    G0 -->|"PASS"| T0
    G0 -->|"FAIL"| KILL
    T0 --> GT0
    GT0 -->|"PASS"| LEAK
    GT0 -->|"KILL"| KILL
    LEAK --> GL
    GL -->|"PASS"| T1
    GL -->|"FAIL — block"| KILL
    T1 --> GT1
    GT1 -->|"ALL PASS"| T2
    GT1 -->|"ANY FAIL"| KILL
    T2 --> GT2
    GT2 -->|"ALL PASS"| PAPER
    GT2 -->|"ANY FAIL"| KILL
    PAPER --> GP
    GP -->|"ALL 6 PASS"| SHADOW
    GP -->|"ANY FAIL"| KILL
    SHADOW --> GL_SHADOW
    GL_SHADOW -->|"ALL PASS + sign-off"| LIVE
    GL_SHADOW -->|"FAIL"| PAPER
    LIVE --> GL2
    GL2 -->|"HEALTHY"| SCALE
    GL2 -->|"HARD_DECAY"| GCRISIS
    GCRISIS -->|"YES — crisis"| HIBERNATE
    GCRISIS -->|"NO — normal market"| RETIRE
    HIBERNATE -->|"IC recover 30 ngày"| LIVE
    HIBERNATE -->|"90 ngày không recover"| RETIRE
    RETIRE -->|"post-mortem filed"| KILL
    SCALE -->|"continuous monitoring"| LIVE

    style KILL fill:#e74c3c,stroke:#c0392b,color:#fff
    style RETIRE fill:#e67e22,stroke:#d35400,color:#fff
    style HIBERNATE fill:#5d4037,stroke:#4e342e,color:#fff
    style LIVE fill:#27ae60,stroke:#229954,color:#fff
    style SCALE fill:#2ecc71,stroke:#27ae60,color:#000
    style IDEA fill:#3498db,stroke:#2980b9,color:#fff
    style STAGE_M1 fill:#8e44ad,stroke:#7d3c98,color:#fff
```

---

## D. Live vs Research — Code Parity

```mermaid
flowchart TB
    subgraph SHARED["🔗 Shared Code — CÙNG file, CÙNG function"]
        FR["nghien_cuu/khung_alpha/feature_registry.py ✅\nFEATURE_REGISTRY: 12 FeatureFn\nSingle source — batch + live import từ đây\nBit-exact: live == batch tolerance 1e-6\nKS test weekly — giam_sat/execution_parity.py"]
        FSPEC2["nghien_cuu/khung_alpha/feature_spec.py ✅\nFeatureSpec — 12 entries registered\npit_safe, live_ready, category\northogonal_group, decay_half_life_bars"]
        PS["thuc_thi_lenh/quan_ly_danh_muc/thoat_vi_the/position_sizer.py ✅"]
        ES["thuc_thi_lenh/quan_ly_danh_muc/thoat_vi_the/strategies/ ✅\nfixed_percent, trailing, time_based, atr_based"]
        CM["nghien_cuu/nha_may_alpha/cost_model.py ❌\nDùng cùng model cho backtest + live check"]
        EXCMETA2["cau_hinh/exchange_metadata.yaml ✅\nSINGLE SOURCE OF TRUTH — exchange constants\nINV-EXC.1: không hardcode trong adapter"]
    end

    subgraph LIVE["🟢 LIVE / PAPER ✅"]
        direction TB
        WS["thu_thap/websocket/ ✅\nbinance_ws / bybit_ws / okx_ws"]
        STREAM["xu_ly_dong/ohlc_engine.py ✅\norderbook_engine.py ✅"]
        IE["ong_dan_dac_trung/online/incremental_engine.py ✅"]
        MEM["ho_du_lieu/kho_dac_trung/online/memory_store.py ✅"]
        SIG["thuc_thi_lenh/dong_co_tin_hieu/ml_signal_engine.py ✅\nONNX inference (Ridge model exported)"]
        EMS["thuc_thi_lenh/dong_co_thuc_thi/ems.py ✅"]
        RG_LIVE["quan_tri_rui_ro/kiem_tra_truoc_lenh/risk_gate.py ✅\nRG-1→RG-17 pre-trade checks\nreferences exchange_metadata.yaml"]

        WS --> STREAM --> IE
        IE -->|"calls"| FR
        IE --> MEM --> SIG --> EMS --> RG_LIVE
    end

    subgraph RESEARCH["🔵 RESEARCH (Scripts + Notebooks) ❌"]
        direction TB
        PQ["ho_du_lieu/tho/ + ho_du_lieu/da_xu_ly/\nRaw + gap-flagged Parquet\nschema_version mandatory"]
        GAP_MOD["dong_co_du_lieu/xu_ly_lo/\ngap_detector • rest_filler • quality_tagger\npit_universe • symbol_remapper"]
        FB2["xu_ly_lo/feature_cache.py ✅\nFeatureCache.get_or_compute()\nVerify FeatureSpec + FEATURE_REGISTRY entry\nAtomic write (INV-D4.23) | cache_hash 12 chars"]
        FEAT2["ho_du_lieu/kho_dac_trung/offline/\n{cache_hash_12}/{asset_id}__{start}_{end}.parquet\nProvenance: feature_logic_hash, data_version"]
        BT2["nghien_cuu/dong_co_phat_lai/vectorized_backtest.py ❌\nPolars-based — M-R6\nFunding: settlement-aligned only"]

        PQ --> GAP_MOD --> FB2
        FB2 -->|"calls SAME functions"| FR
        FB2 -.->|"validates against"| FSPEC2
        FB2 --> FEAT2 --> BT2
        BT2 -->|"calls SAME exit logic"| ES
        BT2 -->|"calls SAME sizing"| PS
    end

    subgraph PARITY["🟣 Execution Parity Monitor ❌\ngiam_sat/execution_parity.py — M-L2"]
        direction TB
        EP1["signal_live_vs_research_diff()\nChạy hourly trong paper phase\nmean_abs_diff < 0.02σ → OK"]
        EP2["feature_parity_check()\nKS test weekly trên feature distributions"]
        EP3{{"status = OK?\nmean_abs_diff < 0.02σ?\ntiming_lag < 2 bars?"}}
        EP1 --> EP2 --> EP3
    end

    subgraph PAPER_ENV["📝 PAPER TRADE ✅ (~95%) — M-L1"]
        direction TB
        PM["moi_truong_chay/paper/paper_runner.py ✅"]
        PEA["moi_truong_chay/paper/paper_ems_adapter.py ✅"]
        FC["moi_truong_chay/paper/fill_capture.py ❌\nCapture fills → calibrate M-R7 adverse selection"]
        PM --> PEA --> FC
    end

    EP3 -->|"FAIL: PAUSE + debug"| FB2
    EP3 -->|"OK: continue"| PAPER_ENV

    style SHARED fill:#f39c12,stroke:#e67e22,color:#000
    style LIVE fill:#1a5276,stroke:#154360,color:#fff
    style RESEARCH fill:#1a3c5e,stroke:#154360,color:#fff
    style PAPER_ENV fill:#2e7d32,stroke:#1b5e20,color:#fff
    style PARITY fill:#4a235a,stroke:#7d3c98,color:#fff
```

---

## E. Backtest Pipeline — Gate Logic

```mermaid
flowchart LR
    IDEAS["Alpha ideas\nqua Stage -1 + 0\nT0 PASS"]

    subgraph T0_GATE["⚡ T0 Screen — ≤15 phút\nnghien_cuu/nha_may_alpha/t0_screen.py"]
        direction TB
        T0G1["Spearman IC (toàn sample)\nBTC-neutralized returns"]
        T0G2["Dynamic threshold (effective_n)\n  n<300: IC>0.030\n  300-600: IC>0.025\n  n>600: IC>0.020"]
        T0G3{{"IC pass?\ncost_adj_ic pass?\ncost < 40% gross IC?"}}
        T0G1 --> T0G2 --> T0G3
    end

    subgraph LEAK_GATE["🔍 Leakage Audit — Gate bắt buộc\nnghien_cuu/danh_gia/leakage_audit.py"]
        direction TB
        LG1["AUDIT-1: future probe multi-horizon\n  IC(signal[t], return[t+k]) ≈ 0 (k=1..10)\n  + backward baseline IC(signal[t], return[t-1])"]
        LG2["AUDIT-2: normalization PiT — expanding window only\n14 checks total (11 non-overridable)"]
        LG3{{"PASS?\n(FAIL = hard block T1)\nNon-overridable: AUDIT-1/2/3/6/7/8/9/11/12/13/14\nOverridable: AUDIT-4/5/10, max 2/năm"}}
        LG1 --> LG2 --> LG3
    end

    subgraph T1_GATE["🎯 T1 Validate — ≤2 giờ\nnghien_cuu/nha_may_alpha/t1_validate.py"]
        direction TB
        T1G1["PurgedKFold 5-fold, embargo 21 days\nmean_cv_ic > 0.020"]
        T1G1b["ICIR = mean(CV_IC) / std(CV_IC)\nKill nếu ICIR < 1.0\nWarn nếu ICIR < 1.5"]
        T1G2["DSR > 0.50 (n_trials_total — cross-family)\n+1 per feature/horizon/filter tweak"]
        T1G3["Regime split IC — commit thresholds trước T0\nAll 3 regimes IC > -0.010"]
        T1G4["T1.5: corr_registry < 0.60\nT1.6: joint OLS BTC+ETH |residual_beta| < 0.15\nT1.7: Ridge coeff stability > 2.0 per fold\nT1.8: ic_short < -0.01 → max_short_weight ≤ 50%\nT1.9: breadth_pct ≥ 0.30 (30% symbols IC > 0)\nT1Result: model_type, t0_override, m_r2_override propagated"]
        T1G5{{"ALL pass?"}}
        T1G1 --> T1G1b --> T1G2 --> T1G3 --> T1G4 --> T1G5
    end

    subgraph T2_GATE["📊 T2 Full Diligence — ≤1 ngày\nnghien_cuu/nha_may_alpha/t2_diligence.py"]
        direction TB
        T2G1["Vectorized backtest 2 năm (Polars)\nSharpe > 0.8 | aim 1.2 | max_dd < 25%\nir_vs_benchmark ≥ 0.20\nexecution_lag_bars=1 (open_next)\nrolling_vol_window=21\nFunding settlement-aligned (không per-bar):\n  charge chỉ tại bar settlement khi position open"]
        T2G2["Walk-forward n_windows (5 theo research.yaml)\nwf_ic_std < 0.50 × wf_ic_mean\n[BLOCKED nếu wf_test_dates không có\ntrong research.yaml ✅ trước T1\ntest windows KHÔNG chạm holdout]"]
        T2G3["Stress: Luna 2022-05 + FTX 2022-11\n+ 20 random high-vol (seed cố định) ≥ 14/20 pass\nNot negative BOTH mandatory periods"]
        T2G4["T2.7: marginal_sharpe > 0.05 (khi có existing alpha)\nT2.8: IC halflife[stressed] ≥ 2 bars (kill < 2)\nDual fill: Scenario A ≥ 0.80 AND Scenario B ≥ 0.75\nScenario B: taker=8bps adv_sel (2× maker=4bps)\nT2.9: Cascade Exit — cascade_impact < 0.30\n  ACTUAL (M-R11) hoặc PROXY (range+volume)\n  cascade_mode='PROXY' → revalidate_t29=True\nT2.10: Sharpe positive ≥ 3/5 market states\nT2.11: Feature drift KS kill nếu p<0.01 AND ks>0.20"]
        T2G5{{"ALL pass?\nExpect 30–60% IC haircut live"}}
        T2G1 --> T2G2 --> T2G3 --> T2G4 --> T2G5
    end

    DEAD0["💀 KILL — IC/cost gate"]
    DEAD1["💀 KILL — leakage"]
    DEAD2["💀 KILL — T1 fail"]
    DEAD3["💀 KILL — T2 fail"]
    PAPER_GO["📝 Paper Trade ≥ 60 ngày\nfill_capture → calibrate adverse selection"]

    IDEAS --> T0_GATE
    T0G3 -->|"FAIL"| DEAD0
    T0G3 -->|"PASS"| LEAK_GATE
    LG3 -->|"FAIL"| DEAD1
    LG3 -->|"PASS"| T1_GATE
    T1G5 -->|"FAIL"| DEAD2
    T1G5 -->|"PASS"| T2_GATE
    T2G5 -->|"FAIL"| DEAD3
    T2G5 -->|"PASS"| PAPER_GO

    style DEAD0 fill:#e74c3c,color:#fff
    style DEAD1 fill:#e74c3c,color:#fff
    style DEAD2 fill:#e74c3c,color:#fff
    style DEAD3 fill:#e74c3c,color:#fff
    style PAPER_GO fill:#27ae60,color:#fff
    style LEAK_GATE fill:#e67e22,stroke:#d35400,color:#fff
```

---

## F. Feature Development Workflow

```mermaid
flowchart TB
    subgraph DISCOVERY_SRC["🔍 Nguồn Hypothesis"]
        direction LR
        PUSH["PUSH: nghien_cuu/kham_pha_dac_trung/anomaly_miner.py ❌ [DEFER]\nObservation_log entries\nMarket replay notebooks"]
        MANUAL["Manual: notebooks Jupyter\nF001–F004, M001–M003, OB001–OB003"]
    end

    subgraph HYPOTHESIS["1️⃣ Hypothesis — Stage -1 (15 phút)"]
        H1["1 câu mechanism rõ ràng\nFeature count ≤ 5\nCemetery similarity < 0.85\nalpha_cemetery.py lookup"]
    end

    subgraph BUILD["2️⃣ Build"]
        direction TB
        B1["Viết trong Jupyter notebook\n(nghien_cuu/so_tay_jupyter/)"]
        B2["Phân loại:\nTier 1: cần live parity → feature_registry.py\nTier 2: research only → feature_builder.py"]
        B3["Funding signal?\n→ decompose: price_return vs funding_income\nsurprise_funding từ Premium Index MA"]
        B4["Normalization: expanding window ONLY\nKhông full-sample z-score"]
    end

    subgraph TEST["3️⃣ Quality checks"]
        direction TB
        T1["leakage_audit.py — M-R2\nFuture probe + normalization PiT"]
        T2["factor_neutralizer.py — M-R8\nBTC/ETH neutralization\nR²(BTC) > 0.60 → warn + document (INV-R8.3, KHÔNG auto-kill)"]
        T3["IC stability check — stable across time windows?"]
        T4["effective_n check\n< 300 → use strict thresholds"]
        T1 --> T2 --> T3 --> T4
    end

    subgraph FSPEC_REG["3b️⃣ Register FeatureSpec (TRƯỚC khi Register)"]
        direction TB
        FS1["nghien_cuu/khung_alpha/feature_spec.py ❌\nTạo FeatureSpec entry:\n  name, inputs, frequency\n  min_history_bars, pit_safe\n  live_ready, normalization"]
        FS2["Verify pit_safe=True (expanding window confirmed)\nVerify live_ready nếu Tier 1 feature"]
        FS1 --> FS2
    end

    subgraph DECIDE["4️⃣ Decision (T0 Screen)"]
        D1{{"leakage_audit PASS?\n|IC| > threshold (effective_n based)?\ncost_adj_ic > threshold?\nNot BTC-beta dominant?\nFeatureSpec registered?"}}
    end

    subgraph REGISTER["5️⃣ Register"]
        direction TB
        R1["Tier 2: feature_builder.py\nResearch only"]
        R2["Tier 1: feature_registry.py ✅\n+ incremental_engine.py update\nVerify: batch == live diff < 1e-6\nexecution_parity.py monitor drift"]
    end

    REJECT["❌ Reject — log lý do\nalpha_cemetery.py nếu hypothesis bị kill"]

    DISCOVERY_SRC --> HYPOTHESIS
    HYPOTHESIS --> BUILD --> TEST --> FSPEC_REG --> DECIDE
    DECIDE -->|"PASS"| REGISTER
    DECIDE -->|"FAIL"| REJECT

    style REJECT fill:#e74c3c,color:#fff
    style REGISTER fill:#27ae60,color:#fff
    style DISCOVERY_SRC fill:#2e1a2e,stroke:#9b59b6,color:#fff
    style FSPEC_REG fill:#1a2e3e,stroke:#3498db,color:#fff
```

---

## G. Walk-Forward Retraining Workflow

```mermaid
flowchart TB
    subgraph INITIAL["🔰 Initial Training — M-R1"]
        I1["hoc_may/huan_luyen/trainer.py\nDefault: Ridge Regression\nLightGBM chỉ khi OOS IC gain > 15% vs Ridge"]
        I2["reproducibility.py:\n  dataset SHA-256 locked\n  random_seed pinned\n  ExperimentRecord pre-registered"]
        I3["PurgedKFold 5-fold, embargo 21 days\nDSR > 0.50, effective_n warning < 500\nAdaptive thresholds: n<300 → strict"]
        I1 --> I2 --> I3
    end

    subgraph MONITOR["📡 Live Monitoring — M-L2, M-L3"]
        direction TB
        M1["giam_sat/execution_parity.py ❌\nHourly parity check\nFeature KS test weekly"]
        M2["giam_sat/reality_gap_monitor.py ❌\nDimensions: IC_live • fill_rate • slippage\nZ-score vs backtest — flag |z| > 2.0"]
        M3["giam_sat/auto_kill.py ❌ — M-L3\nRolling 30d IC < 25% inception for 14 days → kill\nHARD_DECAY + crisis → HIBERNATE (không kill)\nScheduled T2 re-val: 6 tháng OR ood > 2.5×10d"]
        M4["nghien_cuu/nha_may_alpha/alpha_retirement.py ❌\nReview mỗi 90 ngày\nTriggers: soft_decay, crowding, exec_saturation\n[Scheduled T2 re-val: mỗi 6 tháng\nOR ood_score > 2.5 × 10 ngày\nFAIL → SOFT_DECAY + investigate]"]
        M5{{"Kill / Retire / Retrain / Hibernate?"}}
        M1 --> M5
        M2 --> M5
        M3 --> M5
        M4 --> M5
    end

    subgraph RETRAIN["🔄 Walk-Forward Retrain"]
        direction TB
        W1["Slide window — AnchoredWFO\nretrain_freq = monthly per alpha_family"]
        W2["Train v2 — Ridge (same hyperparams)\nParameter freeze: không tune sau nhìn v2 result"]
        W3["Validate: PurgedKFold + DSR\nSo sánh IC_v2 vs IC_v1 trên holdout"]
        W4{{"v2 IC > v1 + DSR > 0.50?"}}
        W1 --> W2 --> W3 --> W4
    end

    subgraph SWAP["🔀 Model Swap"]
        direction TB
        S1["hoc_may/huan_luyen/onnx_exporter.py ❌ (Phase 2)\nExport v2 → ONNX"]
        S2["Shadow ≥ 21 ngày: v1 trade, v2 observe\n[min 21d — ~120 bars đủ statistical power]"]
        S3{{"v2 execution_parity OK?\nv2 reality_gap OK?"}}
        S4["Hot-swap: v1 → v2\nmodel_registry.py update"]
        S1 --> S2 --> S3 --> S4
    end

    RETIRE_PATH["📉 RETIRE\nCapital taper 25%/week → cemetery"]
    KEEP["Giữ v1 — thử lại next cycle"]

    I3 --> MONITOR
    M5 -->|"IC decay → RETRAIN"| RETRAIN
    M5 -->|"soft_decay / crowding → RETIRE"| RETIRE_PATH
    M5 -->|"healthy"| M2
    W4 -->|"YES"| SWAP
    W4 -->|"NO"| KEEP
    S3 -->|"YES"| S4
    S3 -->|"NO"| KEEP
    KEEP -->|"next cycle"| RETRAIN
    S4 -->|"v2 live"| MONITOR

    style KEEP fill:#f39c12,color:#000
    style SWAP fill:#27ae60,color:#fff
    style RETIRE_PATH fill:#e67e22,color:#fff
```

---

## H. Alpha Combination & Portfolio Construction

```mermaid
flowchart TB
    subgraph ALPHAS["🔬 Active Alphas — lifecycle = LIVE"]
        A1["Family 1: Funding Dislocation\nF001–F004 | IC 0.04–0.08\nPriority 1 — Phase 2"]
        A2["Family 2: OI-Weighted XS Momentum\nM001–M003 | IC 0.03–0.05\nPriority 2 — Phase 2"]
        A3["Family 3: OB Microstructure ❌\nOB001–OB003 | IC 0.04–0.09\nFull paper: Phase 3+ ONLY sau F1/F2 ≥100 fills\n[Capped paper 10% size OK nếu T2 PASS với 1.5× cost]"]
    end

    subgraph NEUTRAL["⚖️ Factor Neutralization — M-R8\nnghien_cuu/nha_may_alpha/factor_neutralizer.py ❌"]
        direction TB
        N1["Joint BTC+ETH neutralization\nMultivariate rolling OLS 21-day window\nresidual ⊥ (BTC,ETH) đồng thời — KHÔNG sequential\nR²(BTC/ETH) > 0.60 → warn + document (INV-R8.3)"]
        N2["Degenerate (corr BTC-ETH→1, det→0)\n→ fallback BTC-only, log warning"]
        N3["Cross-sectional sector demeaning\nper timestamp (mỗi bar), asset_id join PiT"]
    end

    subgraph REGISTRY_CHECK["📋 Alpha Registry Check — M-R9\nalpha_registry.py ❌"]
        direction TB
        RC1["Correlation vs existing alphas\nReturn series corr < 0.60"]
        RC2["Marginal Sharpe > 0.05\n(với existing portfolio)"]
        RC3["Similarity search vs cemetery\nKhông reinvent killed ideas"]
    end

    subgraph ERC["📐 ERC Portfolio — M-L4 (Phase 3)\nhoc_may/to_hop_alpha/erc_optimizer.py ❌"]
        direction TB
        E1["Equal Risk Contribution\nMỗi alpha contribute equal volatility\nKhông MVO — không CVXPY"]
        E2["Constraints:\n  gross_leverage ≤ 2.0\n  net_leverage ≤ 0.5\n  |w_symbol| ≤ 0.20 per name\n  turnover ≤ 20%/ngày two-way\n  family_exposure[F] ≤ 60%\n  (block new LIVE promotions nếu > 60%)\n  Portfolio IC kill nếu tất cả live alphas\n  trong family có rolling_ic_30d < 0.010"]
        E3["portfolio_rebalancer.py\nDiff current vs target\nGenerate rebalance orders"]
        COMB["combination_engine.py (Phase 3)\nIC-weighted average (không simple avg):\n  weight_i = max(0, rolling_ic_21d_i) / sum(weights)\n  Down-weight alphas đang decay tự động\n  Fallback: equal weight nếu < 21d data"]
        E1 --> E2 --> E3
        COMB -.->|"combined signal"| E3
    end

    subgraph FUNDING["💸 Funding Decomposition — M-R7"]
        direction TB
        FD1["raw_return = price_return + funding_income"]
        FD2["Test IC trên price_return\nKhông tổng return (funding contaminate)"]
        FD3["surprise_funding = realized - expected\nExpected từ Premium Index MA"]
        FD4["⚠️ Funding settlement-aligned:\nCharge CHỈ tại settlement bars (00:00/08:00/16:00 UTC)\nChỉ khi position[t] != 0 AND position[t-1] != 0\nKhông per-bar approximation"]
        FD1 --> FD2 --> FD3 --> FD4
    end

    subgraph RISK_GATE["🔴 Risk Gate — M-L5\nquan_tri_rui_ro/kiem_tra_truoc_lenh/risk_gate.py ✅"]
        direction TB
        RG1["RG-1→RG-12: existing checks"]
        RG2["RG-13: Inventory Skew (HHI < 0.30)\nRG-14: Cancel/Replace Storm (>10/min → pause)\nRG-15: Dead-Man Switch (heartbeat 300s)"]
        RG3["RG-16: ADL Risk Monitor ❌\nAlert nếu ADL rank top-10%\nReduce 30% nếu top-5% → block new entries\nINV-EXC.1: references exchange_metadata.yaml"]
        RG4["RG-17: Exchange Solvency Monitor ❌\nFTX-pattern: withdrawal freeze, insurance fund\nMax 20% capital per exchange\nHalt + initiate withdrawal 50% khi flag"]
        RG1 --> RG2 --> RG3 --> RG4
    end

    subgraph ATTR["🔎 Signal Attribution (Phase 3)\nnghien_cuu/danh_gia/signal_attribution.py ❌"]
        direction TB
        SA1["Build khi có ≥50 paper fills\nWHY signal fired per bar:\n  signal_score = sum(coef_i × feature_i)\n  Attribution: funding_extreme=+0.42, OI_div=+0.31"]
        SA2["Dùng để debug decay, live drift,\nfalse positives"]
        SA1 --> SA2
    end

    ALPHAS --> NEUTRAL --> REGISTRY_CHECK --> ERC
    FUNDING -->|"price_return IC"| ALPHAS
    ERC --> RISK_GATE
    RISK_GATE -->|"PASS → execute"| EXEC["thuc_thi_lenh/dong_co_thuc_thi/ems.py ✅"]
    ERC -.->|"Phase 3"| ATTR

    style A3 fill:#4a1a1a,stroke:#e74c3c,color:#fff
    style ERC fill:#1a3c2e,stroke:#27ae60,color:#fff
    style EXEC fill:#27ae60,color:#fff
    style ATTR fill:#1a2e4a,stroke:#3498db,color:#fff
    style RG3 fill:#3a1a1a,stroke:#e74c3c,color:#fff
    style RG4 fill:#3a1a1a,stroke:#e74c3c,color:#fff
```

---

## I. Production Monitoring & Auto-Kill Loop

```mermaid
flowchart TB
    subgraph DEPLOY["🚀 Production"]
        D1["thuc_thi_lenh/dong_co_tin_hieu/ml_signal_engine.py ✅"]
        D2["thuc_thi_lenh/dong_co_thuc_thi/ems.py ✅"]
        D3["quan_tri_rui_ro/ ✅\nrisk_gate (RG-1→RG-17) + watchdog + emergency_flattener"]
        D1 --> D2 --> D3
    end

    subgraph PARITY_MON["🟣 Execution Parity — M-L2\ngiam_sat/execution_parity.py ❌"]
        EP1["signal_live_vs_research_diff()\nHourly trong paper/shadow phase\nmean_abs_diff < 0.02σ"]
        EP2["feature_parity KS test weekly\nfill_rate, slippage vs model"]
        EP3{{"Parity OK?"}}
        EP1 --> EP2 --> EP3
    end

    subgraph MONITOR["📡 Performance Monitoring"]
        direction TB
        M1["giam_sat/chi_so_hieu_suat/collector.py ✅\nLatency • throughput • resource"]
        M2["giam_sat/canh_bao/alert_manager.py ✅\n4-TIER ROUTING:\n  CRITICAL → Telegram 24/7 (không suppress)\n  ALERT → Telegram 00:00-20:00 UTC\n  WARNING → Daily digest 07:00 UTC\n  INFO → Log only\nSuppression: same-source 5 phút | flood >5/10min\nCool-down 30 phút sau HALT resume"]
        M3["giam_sat/reality_gap_monitor.py ❌\nIC_live vs IC_backtest\nSlippage vs model estimate\nFlag |z| > 2.0 per dimension"]
        M4["giam_sat/auto_kill.py ❌ — M-L3\nRolling 30d IC < 25% inception × 14 days\nHARD_DECAY + crisis → HIBERNATE (không kill)\nScheduled T2 re-val: 6 tháng OR ood > 2.5×10d"]
        M5["nghien_cuu/nha_may_alpha/alpha_retirement.py ❌\nReview mỗi 90 ngày\nsoft_decay / crowding / exec_saturation\nFamily exhaustion: n_trials/T2_pass > 20 → alert"]
        M6{{"Kill / Retire / Scale / Retrain / Hibernate?"}}
        M1 --> M6
        M2 --> M6
        M3 --> M6
        M4 --> M6
        M5 --> M6
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

    subgraph ACTIONS["⚡ Actions"]
        A1["🟡 SCALE DOWN\nscale_factor = 0.5 + alert"]
        A2["🟠 KILL CANDIDATE\nMonitor 10 ngày thêm"]
        A3["🔴 HALT / FLATTEN\nEmergencyFlattener ✅"]
        A4["🔄 RETRAIN\nWalk-forward retrain (Workflow G)"]
        A5["📉 RETIRE\nCapital taper 25%/week\nPost-mortem → cemetery"]
        A6["😴 HIBERNATE\nScale to 0%, giữ registry\nReactivate check 21 ngày\nMax 90 ngày"]
    end

    subgraph RECOVERY["🔁 Recovery Protocols"]
        direction TB
        REC1["RECOVERY-1 (Max DD >15%):\nPaper 14d → 25% → 50% (14d) → 100% (21d)"]
        REC2["RECOVERY-2 (Exchange outage):\nReconcile → resume ngay (không ramp-up)"]
        REC3["RECOVERY-3 (Feature drift CRITICAL):\nFix → paper 7d OK → resume full"]
        REC4["RECOVERY-4 (Depeg):\nResume ngay khi peg < 0.1% sustained 2h"]
        REC5["RECOVERY-5 (ADL top-5%):\n50% → 75% → 100% mỗi 4 giờ nếu ADL ổn định"]
        REC6["RECOVERY-6 (IC SOFT_DECAY):\n25% → 50% (IC≥60% inception/14d) → 100% (IC≥75%/21d)"]
        HALT_TREE["HALT DECISION TREE:\n  Infrastructure? → RECOVERY-2 or -3\n  Market condition? → RECOVERY-4 or -5\n  Signal quality? → RECOVERY-1 or -6\n  Never conflate infrastructure halt với signal decay"]
        REC1 --- REC2 --- REC3 --- REC4 --- REC5 --- REC6 --- HALT_TREE
    end

    D3 --> PARITY_MON
    EP3 -->|"FAIL: PAUSE + debug"| D1
    EP3 -->|"OK"| MONITOR
    EXEC_RISK -.->|"trigger halt"| A3
    M6 -->|"IC decay scale_down"| A1
    M6 -->|"IC decay kill_candidate"| A2
    M6 -->|"DD breach"| A3
    M6 -->|"IC drift — sustainable"| A4
    M6 -->|"soft_decay / crowding"| A5
    M6 -->|"HARD_DECAY + crisis regime"| A6
    A4 -->|"new model"| D1
    A1 -->|"continue monitoring"| MONITOR
    A2 -->|"10 ngày"| MONITOR
    A6 -->|"IC recover → reactivate"| D1
    A3 -.->|"apply"| RECOVERY

    style A1 fill:#f1c40f,color:#000
    style A2 fill:#e67e22,color:#fff
    style A3 fill:#e74c3c,color:#fff
    style A4 fill:#3498db,color:#fff
    style A5 fill:#8e44ad,color:#fff
    style A6 fill:#5d4037,color:#fff
    style EXEC_RISK fill:#2c2c2c,stroke:#e74c3c,color:#fff
    style PARITY_MON fill:#4a235a,stroke:#7d3c98,color:#fff
    style RECOVERY fill:#1a2e1a,stroke:#27ae60,color:#fff
```

> **Dashboard Spec (4 screens — build trước ngày 1 paper trade):**
> - **Screen 1** (Health): Daily, ≤60 giây — PnL, leverage, alpha IC status, system OK
> - **Screen 2** (Alpha Detail): Khi có vấn đề — IC series, paper vs backtest IC, TCA breakdown
> - **Screen 3** (TCA): Weekly — actual vs model cost, maker fill rate, urgency distribution
> - **Screen 4** (Regime): Weekly — M-R11 state_vector, ood_score, btc_alt_corr, funding extremes

---

## J. Data Sourcing & Reconciliation Flow

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
        LD3["Mọi backtest PHẢI log dataset_id_used\n[INV-D0.2] không được dùng 'current data'"]
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
        CLEAN[("ho_du_lieu/da_xu_ly/\n+ is_gap_filled, gap_duration_s, data_quality\nBars data_quality>1 excluded từ IC calc\nschema_version mandatory")]
        FEAT[("ho_du_lieu/kho_dac_trung/offline/\nFeatures — [DEFER Phase 1]\nschema_version mandatory")]
    end

    subgraph VALIDATE["Validation rules"]
        direction TB
        V1["[INV-D1.1] event_time_ns strictly monotonic"]
        V2["[INV-D1.2] No ffill trên raw OHLCV"]
        V3["[INV-D1.3] recv_time_ns >= event_time_ns"]
        V4["[INV-D2.2] data_quality ∈ {3,4} KHÔNG dùng trong IC\n(labeler M-R1 nghiêm hơn: quality>1 → label=NaN)"]
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

## K. Alpha Triage — Research Velocity

```mermaid
flowchart LR
    POOL["Alpha Pool\nT0 quick screen: ~10–20/week\nFull T1+T2 eval: ~0.5–2/week\nPriority: Family 1 → Family 2 → Family 3 (blocked)"]

    subgraph SM1["STAGE -1 — 15 phút\nKhông code, không data"]
        direction TB
        SM1A["Mechanism 2 câu có economic basis?"]
        SM1B["Cemetery similarity < 0.85?\nalpha_cemetery.py auto-search"]
        SM1C["Feature count ≤ 5?\nHalflife ≥ 2 bars?"]
        SM1D["Listing cohort filter:\n  Family 2: exclude symbol < 30 ngày\n  Family 3: blocked pending F1/F2 fills"]
        SM1A --> SM1B --> SM1C --> SM1D
    end

    subgraph S0["STAGE 0 — 30 phút\nKhông code"]
        direction TB
        S0A["Hypothesis text (immutable sau sign-off)"]
        S0B["venue_type: agnostic / dependent"]
        S0C["ExperimentRecord.create()\nn_trials/family tracking START\nHoldout set designated (không touch)"]
        S0A --> S0B --> S0C
    end

    subgraph T0B["T0 SCREEN — ≤15 phút\nt0_screen.py — M-R3"]
        T0A["IC + cost gate\nDynamic threshold (effective_n)\nOverride: ExperimentRegistry.set_t0_override(id, reason)\n(append event — record frozen, registry.load() resolve)\nmax 2/family (honor system Phase 0)\nT1Result.t0_override propagate → AlphaRecord"]
    end

    subgraph T1B["T1 VALIDATE — ≤2 giờ\nPhải pass Leakage Audit trước\nt1_validate.py — M-R4"]
        T1A["PurgedKFold + DSR + ICIR (9 checks)\nICIR = mean(CV_IC_folds) / std(CV_IC_folds)\n  Kill nếu ICIR < 1.0, Warn nếu < 1.5\nT1.4 capacity | T1.5 corr_registry | T1.6 joint OLS beta\nT1.7 Ridge stability | T1.8 ic_short | T1.9 breadth ≥ 30%\nn_trials = line count counter per data_window\n(create dedup config_hash + increment events, cross-family)"]
    end

    subgraph T2B["T2 FULL DILIGENCE — ≤1 ngày\nt2_diligence.py — M-R5"]
        T2A["Backtest Sharpe > 0.8 (aim 1.2)\nStress Luna + FTX + 20 random high-vol\nWalk-forward stable\nHalflife[stressed] kill <2 bars (warn <5)\nFunding: settlement-aligned (không per-bar)"]
    end

    subgraph NULL_HYP["NULL HYPOTHESIS BASELINE (1 lần, Gate 1→2)"]
        direction TB
        NH1["100 random signals (seed=99, không tune)\nRun T0 screen → expected ≤5% PASS"]
        NH2["Run T1 trên T0-passers\n→ expected ≤2% PASS tổng"]
        NH3["FAIL: pipeline quá lỏng → tighten thresholds\nn_trials += 0 (random signals không count)"]
        NH1 --> NH2 --> NH3
    end

    KILL3["💀 KILL\nalpha_cemetery.py\nkill_reason + similarity_index update"]
    VAL["→ PAPER TRADE\nmoi_truong_chay/paper/\nfill_capture → calibrate M-R7"]

    POOL --> SM1
    SM1 -->|"FAIL"| KILL3
    SM1 -->|"PASS"| S0
    S0 -->|"FAIL"| KILL3
    S0 -->|"PASS"| T0B
    T0B -->|"KILL"| KILL3
    T0B -->|"PASS"| T1B
    T1B -->|"FAIL"| KILL3
    T1B -->|"PASS"| T2B
    T2B -->|"FAIL"| KILL3
    T2B -->|"PASS"| VAL
    NULL_HYP -.->|"validate pipeline strict enough"| T0B

    style KILL3 fill:#e74c3c,color:#fff
    style VAL fill:#27ae60,color:#fff
    style SM1 fill:#8e44ad,stroke:#7d3c98,color:#fff
    style S0 fill:#1a2e3e,stroke:#3498db,color:#fff
    style NULL_HYP fill:#2e2e1a,stroke:#f39c12,color:#fff
```

---

## L. Feature Discovery Engine

```mermaid
flowchart TB
    subgraph AUTOMATED["🤖 Automated — TIER-B\nnghien_cuu/kham_pha_dac_trung/anomaly_miner.py"]
        direction TB
        AM["Scans: IC_spikes • OI_divergence\nfunding_outliers • corr_breakdown\nbasis_anomaly • vol_regime_change"]
        FILTER["Filter: z_score > 2.5\nDedup: không log cùng anomaly 2 ngày"]
        LOG_AUTO["observation_log.py\nPriority: high z>3.0, medium z>2.5"]
        AM --> FILTER --> LOG_AUTO
    end

    subgraph MANUAL["👁️ Manual (Primary — Phase 0–1)"]
        direction TB
        NB_FAM["nghien_cuu/so_tay_jupyter/\nF001–F004 (Funding)\nM001–M003 (Momentum)\nOB001 (OB — blocked)"]
        REPLAY_R["Market event replay\nLoad L2 + funding + OI\nVisualize → structured observation"]
        LOG_MAN["observation_log.py\n{event, mechanism, priority}"]
        NB_FAM --> LOG_MAN
        REPLAY_R --> LOG_MAN
    end

    subgraph TRIAGE["📋 Weekly Hypothesis Triage"]
        direction TB
        PENDING["Pending hypotheses (≥ medium priority)\n10–30 entries/week target"]
        SORT["Sort: priority × economic_novelty × data_availability"]
        BATCH["Batch Stage -1\n~10–15 min/entry\nalpha_cemetery.py similarity check"]
        PENDING --> SORT --> BATCH
    end

    LOG_AUTO --> TRIAGE
    LOG_MAN --> TRIAGE
    BATCH -->|"PASS Stage -1 → ExperimentRecord"| QUEUE["→ Alpha pipeline\nrun_alpha_pipeline.py"]
    BATCH -->|"FAIL"| KILLED_FAST["💀 cemetery\n(< 15 phút/idea)"]

    style KILLED_FAST fill:#e74c3c,color:#fff
    style QUEUE fill:#27ae60,color:#fff
    style AUTOMATED fill:#1a2e1a,stroke:#27ae60,color:#fff
    style MANUAL fill:#1a1a2e,stroke:#3498db,color:#fff
```

---

## M. Live Dashboard Spec

```mermaid
flowchart TB
    subgraph DASH["📊 Live Dashboard — Build TRƯỚC ngày 1 paper trade"]
        direction TB

        subgraph S1["SCREEN 1: HEALTH (daily, ≤60 giây)"]
            direction TB
            H1["Portfolio: PnL today/week | Gross/net leverage | Free collateral"]
            H2["Alpha Status: [F001 LIVE] IC_30d=X ✅|⚠️ | fills=N"]
            H3["System: Pipeline OK | Feature OK | WAL size | Open orders"]
            H4["Alerts: N CRITICAL, M WARNING | [see digest]"]
            H1 --> H2 --> H3 --> H4
        end

        subgraph S2["SCREEN 2: ALPHA DETAIL (khi có vấn đề)"]
            direction TB
            AD1["IC series last 30d (spark-line)"]
            AD2["IC vs inception: current/inception % — target ≥70%"]
            AD3["Paper vs backtest IC ratio — target ≥70%"]
            AD4["Fill rate maker actual vs target (≥0.55 BTC/ETH gate)"]
            AD5["TCA: timing_cost | impact_cost | fee split"]
            AD1 --> AD2 --> AD3 --> AD4 --> AD5
        end

        subgraph S3["SCREEN 3: TCA (weekly)"]
            direction TB
            T1["round_trip_cost actual vs model"]
            T2["adverse_selection actual vs model\n(update research.yaml khi gap > 30%)"]
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

        BUILD_ORDER["Implementation priority:\nWeek 1 paper: Screen 1 + Alerts\nWeek 2: Screen 2 (Alpha Detail)\nWeek 4+: Screen 3 (TCA, sau ≥50 fills)\nMonth 2+: Screen 4 (Regime, sau M-R11 running)"]
    end

    subgraph ALERT_TIER["🔔 Alert Routing (4-tier)"]
        direction TB
        CR["CRITICAL → Telegram 24/7 (không suppress)\n  RG-1→RG-17 halt conditions\n  Dead-man switch fire (RG-15)\n  Exchange withdrawal freeze (RG-17)\n  Pipeline crash (live==0 khi research!=0)\n  Response: human trong 15 phút"]
        AL["ALERT → Telegram (00:00–20:00 UTC)\n  effective_leverage > 1.8\n  feature drift pct_diff > 10%\n  ADL rank top-5% (RG-16)\n  IC SOFT_DECAY triggered\n  Response: human review trong 2 giờ"]
        WA["WARNING → Daily digest 07:00 UTC\n  feature drift 1–10%\n  clock drift > 500ms\n  cancel rate > 15% (sub-threshold)\n  ICIR warn (< 1.5 trong paper)\n  symbol coverage < 95%"]
        IN["INFO → Log only (không Telegram)\n  normal fills, daily IC report\n  regime state update, backtest done"]
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
