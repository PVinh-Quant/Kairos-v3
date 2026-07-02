<div align="center">
 
<img width="124" height="124" alt="image" src="https://github.com/user-attachments/assets/bf450abd-f468-43fa-9750-464e3ef95651" />

# KAIROS v3
### **Hệ thống giao dịch định lượng (Quant Trading) tần suất trung bình (Mid-Frequency) tự động hóa toàn diện, với kiến trúc độ trễ thấp (Low-Latency)**

[![Python](https://img.shields.io/badge/Python-3.13+-blue?style=for-the-badge&logo=python)](https://www.python.org/)
[![Market](https://img.shields.io/badge/Market-Crypto-orange?style=for-the-badge)](https://www.binance.com/)
[![License](https://img.shields.io/badge/License-MIT-green?style=for-the-badge)](https://opensource.org/licenses/MIT)

`Python` • `ETL Pipeline` • `Backtesting` • `Quant Analysis`

| Trường | Giá trị |
|--------|---------|
| Phiên bản tài liệu | v3.8 |
| Ngày cập nhật | 2026-06-14 |
| Trạng thái | In Development — Layer 1 (Data Core) built & patched; các đợt consistency audit Layer 1/2/3 đã hoàn tất và đồng bộ tài liệu. *(Chi tiết audit findings nội bộ đã lược bỏ khỏi bản công khai.)* |
| Phạm vi | Architecture specification & implementation reference cho KAIROS v3 |
| Test coverage | 473 hàm test / 18 files trong `test/` (M-D0: 25, M-D1: 31, M-D2: 37, M-D3: 32, M-D4: 29, exits: 64, ...) + 149 Layer 2 spec test definitions |

<div align="left">
 
---

## Mục Lục

- [0. Đọc Trước Khi Tham Khảo](#read-this-first)
- [1. Luồng Dữ Liệu Thực Chiến (Data Flow)](#1--luồng-dữ-liệu-thực-chiến-data-flow)
- [2. Kiến Trúc Hệ Thống (Directory Map)](#2--kiến-trúc-hệ-thống-directory-map)
- [3. Đặc Tả Module (Module Specifications)](#3--đặc-tả-module-module-specifications)
  - [3.1. Cấu Hình & Môi Trường](#31--cấu-hình--môi-trường-config--runtime)
  - [3.2. Hồ Dữ Liệu](#32--hồ-dữ-liệu-data-lake)
  - [3.3. Động Cơ Dữ Liệu](#33--động-cơ-dữ-liệu-data-engine)
  - [3.4. Phòng Nghiên Cứu](#34--phòng-nghiên-cứu--kiểm-thử-research--replay-wip)
  - [3.5. Trí Tuệ Nhân Tạo](#35--trí-tuệ-nhân-tạo--mlops-machine-learning-wip)
  - [3.6. Thực Thi Chiến Dịch](#36--thực-thi-chiến-dịch-execution-core)
  - [3.7. Lưới Bảo Vệ](#37--lưới-bảo-vệ-risk-system)
  - [3.8. Hạ Tầng & Giao Tiếp](#38--hạ-tầng--giao-tiếp-infrastructure)
  - [3.9. Giám Sát & Kiểm Thử](#39--giám-sát--kiểm-thử-monitoring--testing)
- [4. Ràng Buộc & Quyết Định Thiết Kế (Design Constraints)](#4--ràng-buộc--quyết-định-thiết-kế-design-constraints--engineering-decisions)
- [8. Kiểm Tra Sẵn Sàng Production (Production Readiness Checklist)](#8--kiểm-tra-sẵn-sàng-production-production-readiness-checklist)

---

<a id="read-this-first"></a>
## 0. Đọc Trước Khi Tham Khảo

Kairos là một engine giao dịch tần suất trung bình (`mid-frequency`) với kiến trúc tối ưu rất mạnh cho độ trễ thấp. README này không nên được hiểu là xác nhận hệ thống đã đạt `formal correctness` cho real-money trading trong mọi failure mode. Ở nhiều điểm, thiết kế hiện tại ưu tiên `latency-first`, nên người tham khảo cần đọc rõ các giả định và ranh giới an toàn trước khi tái sử dụng.

**Cần hiểu đúng các khái niệm sau:**

- `Snapshot + WAL` hiện nên được hiểu là **persistence backbone / operational truth source** cho state trọng yếu. Chỉ nên gọi là `Single Source of Truth` tuyệt đối khi mọi mutation đều bắt buộc đi qua WAL và replay luôn rebuild ra cùng logical state.
- `Drop`, `coalescing`, `backpressure shedding`, và `fail-fast` là quyết định hiệu năng; chúng không mặc định an toàn cho real trading nếu event bị bỏ có thể làm sai quyết định hoặc làm lệch state. Khi đó phải có `gap detection`, `degraded mode`, hoặc `halt`.
- `Deterministic replay` chỉ có nghĩa khi input stream, ordering, seed, timestamp semantics, và side effects external đều được kiểm soát. Paper determinism không đồng nghĩa live determinism.
- `SPSC ring buffer`, `OCC retries`, và mô hình `thread + asyncio` đều dựa trên assumption chặt. Nếu assumption không được runtime-enforce hoặc stress-test, correctness có thể sai dù latency vẫn đẹp.
- `Integer-scaled arithmetic` chỉ giải quyết sai số `float`; nó không tự động giải quyết `currency normalization`, `funding sign conventions`, `fee model`, hay sự khác biệt giữa linear / inverse / coin-margined products.

**Production gate tối thiểu trước khi dùng làm tài liệu tham khảo cho engine tiền thật:**

| Nhóm | Yêu cầu tối thiểu |
|------|-------------------|
| Event correctness | `sequence_id`, tách `event_time` / `ingest_time`, gap detection, policy rõ cho out-of-order và drop |
| Persistence | mọi mutation state đi qua WAL, snapshot + replay rebuild ra cùng logical state |
| Determinism | cùng input -> cùng output ở mức logic, không phụ thuộc thread scheduling hay `time.time()` |
| Concurrency | ownership rõ hoặc lock strategy rõ; OCC chỉ áp cho operation idempotent và retry-safe |
| Risk stop | global kill switch atomic, engine state machine `INIT -> SYNC -> RUNNING -> HALT` rõ ràng |
| Accounting | base-currency normalization, funding/fee normalization, reconciliation với exchange |
| Integrity | invariant runtime checks cho order, position, balance, PnL |
| Testing | replay, chaos, fault injection, restart recovery, race stress, invariant assertions |

**Cách nên đọc phần còn lại của README:**

- Phần bên dưới mô tả chủ yếu là `kiến trúc`, `ý đồ thiết kế`, và `hướng hardening`.
- Những từ như `SSOT`, `deterministic`, `crash-safe`, `lock-free`, hay `production` nên được hiểu là có điều kiện, không phải bảo chứng vô điều kiện cho mọi đường chạy.
- Nếu bạn muốn dùng Kairos làm reference cho hệ thống thật, hãy kiểm tra kỹ: event sequencing, WAL coverage, idempotency, state machine, invariant enforcement, exchange reconciliation, và kill consistency.

---

## 1. Luồng Dữ Liệu Thực Chiến (Data Flow)

Mục tiêu thiết kế là không để state trọng yếu nằm rải rác. Trong README này, `Snapshot + WAL` nên được hiểu là **persistence backbone** và nguồn tham chiếu vận hành cho state quan trọng, không phải tuyên bố rằng mọi mutation đã được formalize thành `Single Source of Truth` tuyệt đối trên mọi đường chạy.

```mermaid
graph TD
    %% Định nghĩa các node
    EX[Sàn Giao Dịch]
    MD[Động Cơ Dữ Liệu]
    BUS((Event Bus ZMQ + Backpressure))
    STATE[(Operational State Store<br/>Snapshot + WAL)]
    
    SIG[Signal Engine]
    PORT[Portfolio Engine + Accounting]
    RISK[Risk Engine \n Cross-Strategy]
    OMS[OMS - Quản Lý Lệnh]
    EMS[EMS - Động Cơ Thực Thi]

    %% Luồng Dữ Liệu
    EX -- WebSocket Tick --> MD
    MD -- MarketEvent_v1 --> BUS
    
    BUS -.-> |Read Tick| SIG
    SIG -- SignalEvent_v1 --> BUS
    
    BUS -.-> |Read Signal| PORT
    PORT -.-> |Read Balances| STATE
    PORT -- TargetPortfolioEvent_v1 --> BUS
    
    BUS -.-> |Read Target| OMS
    OMS -.-> |Read Current Positions| STATE
    OMS -- ProposedOrderEvent_v1 --> BUS
    
    BUS -.-> |Read Proposed Order| RISK
    RISK -.-> |Cross-check Risk| STATE
    RISK -- ApprovedOrderEvent_v1 --> BUS
    
    BUS -.-> |Read Approved Order| EMS
    EMS -- Xé lệnh Limit/Market + Circuit Breaker --> EX
    
    EX -- FillEvent_v1 --> BUS
    BUS -.-> |Append WAL / refresh snapshot| STATE
```

> Khi dùng cho production, mỗi event nên mang tối thiểu `sequence_id`, `event_time`, và `ingest_time`. Nếu có gap, out-of-order ngoài cửa sổ, hoặc drop trên luồng causal, hệ thống nên chuyển sang `degraded` hoặc `halt` thay vì giả định state vẫn đúng.

---

## 2. Kiến Trúc Hệ Thống (Directory Map)

```text
KAIROS v3/
├── .env                                # Chứa API Keys, Passwords (KHÔNG commit)
├── .gitignore
├── Dockerfile
├── Makefile                            # Phím tắt thao tác (make train, make live)
├── main.py                             # Entry point tổng
├── requirements.txt                    # Quản lý thư viện Python
├── README.md                           # Bạn đang đọc file này
├── docs/                               # implementation_plan.md, data_flow_diagrams.md, muc_tieu_he_thong.md
├── prompt/                             # Spec modules: layer1_data (M-D), layer2_research (M-R), layer3_live (M-L), layer4_hft (M-H)
│
# ==========================================
# 1. CẤU HÌNH & MÔI TRƯỜNG (CONFIG & RUNTIME)
# ==========================================
├── cau_hinh/
│   ├── adapter_loader.py               # Factory nạp Exchange Adapters từ .env + universe.yaml
│   ├── config_loader.py                # Pydantic typed loaders cho các file YAML
│   ├── universe.yaml                   # Exchange adapters (timeout, rate) + Symbol master
│   ├── trading.yaml                    # Risk, OMS, PnL, Position Sync, Session, Strategy
│   ├── infra.yaml                      # Flow Control, Execution Protection, Alerting, Observability
│   ├── research.yaml                   # [Layer 2] Tham số research pipeline (T0/T1/T2, cost model)
│   ├── exchange_metadata.yaml          # Settlement times, funding intervals per exchange
│   ├── sector_map.yaml                 # [M-D3] Sector assignments seed
│   └── stablecoin_exclusion.yaml       # [M-D3] Stablecoin exclusion list cho universe
│
├── moi_truong_chay/                    # (RUNTIME ISOLATION) Tách biệt tuyệt đối
│   ├── live/                           # [NEW] Chạy tiền thật — Live Orchestrator (production-oriented)
│   │   ├── live_runner.py              # [NEW] 12-Step SRE Startup + Health/Reconcile Daemons (1038 dòng)
│   │   └── live_config.yaml            # [NEW] SRE-grade tuning: health, reconciliation, cancel policy
│   ├── paper/                          # Chạy tiền ảo (Testnet)
│   │   ├── audit_to_parquet.py         # Chuyển đổi dữ liệu kiểm toán sang Parquet
│   │   ├── microstructure_model.py     # Mô phỏng vi cấu trúc thị trường
│   │   ├── paper_ems_adapter.py        # Adapter thực thi cho Paper Trading (832 dòng)
│   │   ├── paper_runner.py             # Script chạy chính cho môi trường Paper
│   │   ├── paper_state_manager.py      # Quản lý trạng thái lệnh ảo
│   │   └── shock_simulator.py          # Giả lập các cú sốc thị trường
│   ├── backtest/                       # Chạy giả lập quá khứ
│   └── upstream_runner.py              # Script khởi chạy các tiến trình Data/Risk
│
# ==========================================
# 2. HỒ DỮ LIỆU (DATA LAKE)
# ==========================================
├── ho_du_lieu/
│   ├── tho/                            # (Raw) Immutable Parquet — M-D1 output
│   │   ├── {exchange}/{symbol}/        # [M-D1] 1h OHLCV bars + liquidation
│   │   │   └── year={Y}/month={M}/data.parquet
│   │   ├── {exchange}/{symbol}/long_short/  # [M-D1] 5min Long/Short ratio
│   │   │   └── year={Y}/month={M}/data.parquet
│   │   └── onchain/{asset}/{metric}/   # [M-D1] Hourly on-chain (CryptoQuant)
│   │       └── year={Y}/month={M}/data.parquet
│   ├── da_xu_ly/                       # Dữ liệu đã làm sạch & đồng bộ
│   ├── kho_dac_trung/                  # (Feature Store)
│   │   ├── offline/                    # Phục vụ train AI
│   │   └── online/                     # Cache trên RAM phục vụ chạy Live
│   │      └── memory_store.py          # OnlineFeatureStore
│   ├── thuc_thi/                       # Live execution state (WAL + snapshot + sessions)
│   │   ├── nhat_ky_wal/                # kairos.wal, wal_log.jsonl, funding_dedup.jsonl
│   │   ├── snapshot/                   # state.json (KairosState atomic checkpoint)
│   │   ├── phien_giao_dich/            # sessions_YYYY-MM.ndjson / .parquet (P&L ledger)
│   │   └── audit/                      # Exit audit trail (daily rotation, 30-day retention)
│   ├── gia_lap/                        # Paper trading output
│   │   ├── paper_wal.jsonl             # Paper WAL
│   │   ├── paper_snapshot.json         # Paper state snapshot
│   │   ├── paper_audit_*.jsonl         # Fill audit trail (line-buffered)
│   │   └── parquet/                    # Converted Parquet (audit_to_parquet)
│   ├── settlement_spool/{exchange}/    # [M-D1] Persistent spool cho settlement buffer
│   ├── dlq/{exchange}/                 # [M-D1] Dead Letter Queue per-exchange
│   ├── ket_noi/{exchange}/             # [M-D1] WS gap logs (gaps_{date}.jsonl)
│   ├── quarantine/                     # [M-D1] Orphan Parquet quarantine
│   ├── giam_sat/                       # Monitoring & alerting output
│   │   └── canh_bao/                   # Alert fallback logs (RotatingFileHandler)
│   └── he_thong/                       # System sentinel files (gitignored — runtime only)
│       ├── system.KILLED               # Kill-switch JSON (blocks all Gateway restarts)
│       └── FLATTEN_LOCK                # Written by EmergencyFlattener (blocks all new trades)
│
# ==========================================
# 3. ĐỘNG CƠ DỮ LIỆU (DATA ENGINE)
# ==========================================
├── dong_co_du_lieu/
│   ├── quan_ly_phien_ban/              # [M-D0] Data Lineage Registry (8 files)
│   │   ├── __init__.py                 # Public API export
│   │   ├── _locking.py                 # Cross-platform file lock (msvcrt/fcntl)
│   │   ├── dataset_record.py           # DatasetRecord + content/schema hash + write_and_register()
│   │   ├── exceptions.py               # Custom exception hierarchy (LineageError, PIT failures)
│   │   ├── lineage_registry.py         # Append-only JSONL registry, as_of() PIT query
│   │   ├── pit_verifier.py             # verify_pit_production() stratified sampling (500+ rows)
│   │   ├── symbol_lifecycle_poller.py   # Daemon daily poll → state-based interval tracking
│   │   └── verification_registry.py    # VerificationRecord + verification log
│   ├── thu_thap/                       # [M-D1] Raw Data Ingestion (11 modules)
│   │   ├── bar_types.py                # BarData dataclass — M-D1 schema đầy đủ
│   │   ├── bar_processor.py            # compute_dq_flags, attach_oi_to_bar, write_parquet_atomic
│   │   ├── settlement_buffer.py        # T+5min funding buffer + persistent spool
│   │   ├── dead_letter_queue.py        # Rolling 1h window per-exchange, route_to_dlq
│   │   ├── funding_interval_cache.py   # Settlement minutes cache + funding poll loop
│   │   ├── liquidation_aggregator.py   # Thread-safe per-bar LiquidationEvent aggregation
│   │   ├── aux_parquet_writer.py       # Long/Short ratio + On-chain Parquet writer
│   │   ├── schema_validator.py         # Schema validation: OHLCV + Long/Short + On-chain
│   │   ├── startup_routines.py         # Orphan scan + SIGTERM handler
│   │   ├── symbol_remapper.py          # Exchange symbol → canonical asset_id (date-range)
│   │   ├── websocket/                  # Real-time streaming
│   │   │   ├── binance_ws.py           # BinanceGateway
│   │   │   ├── okx_ws.py               # OkxGateway
│   │   │   └── bybit_ws.py             # BybitGateway
│   │   └── rest_api/                   # Polling định kỳ
│   │       ├── base_rest.py            # BaseRestPoller
│   │       ├── binance_rest.py         # BinanceRestPoller
│   │       ├── okx_rest.py             # OkxRestPoller
│   │       ├── bybit_rest.py           # BybitRestPoller
│   │       └── onchain_rest.py         # CryptoQuantPoller
│   ├── xu_ly_dong/                     # (Stream Processor)
│   │   └── bo_loc/
│   │       ├── orderbook_engine.py     # L2 Sync Engine
│   │       └── ohlc_engine.py          # OHLCV Aggregator
│   ├── xu_ly_lo/                       # [M-D2] Batch Processor — Gap Reconciliation (8 files)
│   │   ├── reconcile.py                # [M-D2] Main orchestrator steps 0–7, CLI + batch runner
│   │   ├── schema_validator.py         # [M-D2] MD2_SCHEMA explicit PyArrow, write_with_schema_version()
│   │   ├── maintenance_event_logger.py # [M-D2] Daemon poll → maintenance_log_{date}.parquet + heartbeat
│   │   ├── gap_detector.py             # [M-D2] Missing/zombie detection, gap manifest (immutable fields)
│   │   ├── rest_filler.py              # [M-D2] REST fill, continuity + boundary validation (async)
│   │   ├── quality_tagger.py           # [M-D2] data_quality 0–4, coverage report
│   │   ├── reconcile_funding_rates.py  # [M-D2] funding_rate_raw, anti-lookahead, stale propagation
│   │   ├── pit_universe.py             # [M-D3] PiTUniverseManager, AssetRegistry, snapshot builder
│   │   ├── symbol_remapper.py          # [M-D3] RENAME/FORK corporate actions, symbol history
│   │   ├── seed_sector_assignments.py  # [M-D3] One-time seeder: sector_map.yaml → DB
│   │   ├── feature_cache.py            # [M-D4] FeatureCache, atomic write, cache_hash, BTC-first
│   │   └── pre_aggregate_l2.py         # [M-D4] L2→OFI, binary snapshot store (KHÔNG delete; M-H1 NV2: + sequence/receive-ts + trade stream)
│   └── ong_dan_dac_trung/              # (Feature Pipeline) <10µs/tick
│       └── online/
│           ├── feature_registry.py     # FEATURE_REGISTRY + CompiledPlan
│           └── incremental_engine.py   # IncrementalFeatureEngine
│
# ==========================================
# 4. PHÒNG NGHIÊN CỨU (RESEARCH & REPLAY)
# ==========================================
├── nghien_cuu/
│   ├── khung_alpha/                    # [M-D4] Alpha Research Framework (3 files)
│   │   ├── feature_spec.py             # [M-D4] FeatureSpec dataclass, 12 entries, DAG validation
│   │   └── feature_registry.py         # [M-D4] FEATURE_REGISTRY 12 fns, IncrementalFeatureEngine
│   ├── so_tay_jupyter/                 # (Notebooks)
│   ├── nha_may_alpha/                  # (Alpha Factory)
│   ├── dong_co_phat_lai/               # (REPLAY ENGINE)
│   ├── kiem_thu_qua_khu/               # (Backtest Engine)
│   │   ├── ma_tran_sie_toc/            # (Vectorized) Polars
│   │   └── mo_phong_su_kien/           # (Event-driven)
│   └── danh_gia/                       # (Evaluation) Sharpe, MDD
│
# ==========================================
# 5. TRÍ TUỆ NHÂN TẠO (MACHINE LEARNING)
# ==========================================
├── hoc_may/
│   ├── mo_hinh/                        # (Models) LSTM, Transformer — [PLANNED, chưa tồn tại]
│   ├── huan_luyen/                     # [M-R1] Script train model — [CHƯA BUILD — viết code từ spec]
│   ├── suy_luan/                       # ONNX/TensorRT Inference
│   ├── to_hop_alpha/                   # (Alpha Combiner)
│   └── giam_sat_mo_hinh/               # (ML MONITORING)
│       ├── sai_lech_dac_trung/         # Feature Drift
│       └── sai_lech_du_doan/           # Prediction Drift
│
# ==========================================
# 6. THỰC THI CHIẾN DỊCH (EXECUTION CORE)
# ==========================================
├── thuc_thi_lenh/
│   ├── bo_nho_trang_thai/              # [PERSISTENCE BACKBONE / STATE STORE]
│   │   ├── snapshot/                   # Dump state định kỳ mỗi 5s
│   │   ├── nhat_ky_wal/                # Write-Ahead Log
│   │   │   └── durable_wal.py          # War-Grade WAL (mmap + CRC32)
│   │   ├── vi_the/                     # (Positions)
│   │   ├── so_lenh/                    # (Orders)
│   │   ├── so_du/                      # (Balances)
│   │   └── state_manager.py            # Quản lý trạng thái hệ thống
│   ├── cong_ket_noi/                   # (Gateway) Binance/Bybit/OKX
│   │   ├── base_adapter.py
│   │   ├── binance_adapter.py
│   │   ├── bybit_adapter.py
│   │   ├── okx_adapter.py
│   │   └── chien_luoc_thu_lai/         # [NEW] Smart Retry + Protection Stack
│   │       ├── circuit_breaker.py      # Weighted CB: CLOSED→OPEN→HALF_OPEN
│   │       ├── execution_wrapper.py    # Intent Lifecycle + Integrated Protection
│   │       ├── rate_limiter.py         # Adaptive Token Bucket + EWMA Latency
│   │       └── retry_policy.py         # Decorrelated Jitter + Deadline-Aware
│   ├── dong_co_tin_hieu/               # (Signal Engine)
│   │   ├── ml_signal_engine.py
│   │   └── mock_onnx_generator.py
│   ├── quan_ly_danh_muc/               # (Portfolio Engine)
│   │   ├── funding_collector.py        # [NEW] Periodic Funding Collection + NDJSON Dedup
│   │   ├── position_sync.py            # [NEW] Startup Sync + Drift Detection + Healing
│   │   ├── session_manager.py          # [NEW] Daily P&L Session Rotation + Archive
│   │   ├── ke_toan_pnl/                # [NEW] Accounting: Realized/Unrealized PnL
│   │   │   ├── pnl_aggregator.py       # Root PnL Orchestrator (734 dòng)
│   │   │   ├── pnl_tracker.py          # Integer-Scaled Realized PnL + OCC
│   │   │   ├── fee_ledger.py           # Trade Fees + Funding Payments
│   │   │   └── mark_to_market.py       # Live Mark Price + Unrealized PnL
│   │   └── thoat_vi_the/               # [NEW] Exit Management System (EMS Exit Layer)
│   │       ├── exit_coordinator.py     # [NEW] State machine + strategy dispatch (475 dòng)
│   │       ├── position_state.py       # [NEW] FLAT→OPEN→CLOSING→PARTIAL→CLOSED→QUARANTINED
│   │       ├── position_sizer.py       # [NEW] Anti-flip qty calculator
│   │       ├── audit_logger.py         # [NEW] Append-only exit audit trail (daily rotation)
│   │       ├── base_strategy.py        # [NEW] ExitStrategy ABC, ExitDecision, ExitContext
│   │       ├── composite.py            # [NEW] First-trigger-wins by urgency
│   │       └── strategies/
│   │           ├── fixed_percent.py    # [NEW] SL/TP fixed %
│   │           ├── atr_based.py        # [NEW] ATR-based SL/TP (per-symbol state)
│   │           ├── trailing_stop.py    # [NEW] Trailing stop with activation threshold
│   │           ├── breakeven.py        # [NEW] Move SL to breakeven after trigger %
│   │           ├── partial_exit.py     # [NEW] Partial close at multiple TP levels
│   │           └── time_based.py       # [NEW] Max hold time forced exit
│   ├── danh_ba_chien_luoc/             # (STRATEGY REGISTRY)
│   ├── quan_ly_lenh/                   # [NEW] (OMS) Order Management System
│   │   ├── order_book.py               # In-Memory Sổ Lệnh + Per-Symbol Lock (763 dòng)
│   │   ├── reconciler.py               # Exchange Reconciliation + Cancel-All
│   │   └── oms_serializer.py           # Binary WAL Payload Codec (32B)
│   ├── dong_co_thuc_thi/               # (EMS) Execution Management
│   │   ├── ems.py
│   │   └── execution_risk_engine.py
│   ├── theo_doi_do_tre/                # (LATENCY TRACKER)
│   └── vong_lap_su_kien.py             # (Event Loop) — 1251 dòng, trái tim hệ thống
│
# ==========================================
# 7. LƯỚI BẢO VỆ (RISK SYSTEM)
# ==========================================
├── quan_tri_rui_ro/
│   ├── rui_ro_cheo_chien_luoc/         # Cross-Strategy Risk
│   │   ├── bu_tru_vi_the/              # Exposure Netting
│   │   └── xung_dot_tin_hieu/          # Conflict Detector
│   ├── kiem_tra_truoc_lenh/            # Pre-trade Risk (risk_gate.py 646 dòng)
│   │   ├── rules/
│   │   │   ├── base_rule.py
│   │   │   ├── global_rules.py         # MaxDailyLoss, MaxDrawdown
│   │   │   ├── position_rules.py       # MaxOpenOrders, Concentration
│   │   │   └── rate_rules.py           # OrderRate, DuplicateGuard
│   │   ├── reconciliation.py
│   │   ├── risk_codes.py
│   │   └── risk_gate.py                # Cổng kiểm soát rủi ro chính
│   └── nguoi_gac_cong/                 # (Watchdog) Kill Switch
│       └── watchdog/
│           ├── watchdog.py
│           ├── emergency_flattener.py  # [NEW] Standalone REST flatten Binance/OKX/Bybit (438 dòng)
│           └── adapters/
│               ├── lite_rest.py
│               └── war_grade_rest.py
│
# ==========================================
# 8. HẠ TẦNG (INFRASTRUCTURE)
# ==========================================
├── ha_tang/
│   ├── bus_su_kien/                    # (Event Bus) ZeroMQ
│   │   ├── zmq_bus.py                  # AsyncEventBus PUB/SUB
│   │   ├── luoc_do_du_lieu/v1/         # Event Schema Versioning
│   │   │   ├── base_event.py
│   │   │   ├── market_schema.py
│   │   │   ├── state_schema.py
│   │   │   ├── feature_schema.py       # _FeatureEventRaw (192B ctypes)
│   │   │   ├── execution_schema.py
│   │   │   └── signal_schema.py        # _SignalEventRaw (64B ctypes)
│   │   └── kiem_soat_luu_luong/        # Backpressure Control
│   │       ├── drop_policy/
│   │       │   └── shedder.py          # Adaptive Shedder (EWMA)
│   │       └── priority_channel/
│   │           └── channel_manager.py  # SPSC Multi-Queue
│   ├── bo_nho_chung/                   # (Shared Memory)
│   └── dong_ho_thoi_gian/
│       └── time_validator.py           # PTP/NTP Clock Validation
│
# ==========================================
# 9. GIÁM SÁT (MONITORING & TESTING)
# ==========================================
├── giam_sat/
│   ├── chi_so_hieu_suat/               # System Metrics
│   │   ├── collector.py                # Dedicated OS Thread collector
│   │   └── reporter.py                 # ZMQ reporter
│   ├── canh_bao/                       # Alerts
│   │   ├── alert_manager.py
│   │   ├── alert_rules.py
│   │   └── telegram_sender.py
│   └── theo_doi_do_tre/                # Latency Tracking
│       ├── histogram.py                # HdrHistogram zero-alloc
│       ├── reporter.py
│       └── tracker.py                  # 4-segment latency tracker
├── kiem_thu/
│   └── san_gia_lap/                    # Mock Exchange
├── test/                               # Unit & Integration Tests (473 test functions / 18 files)
│   ├── test_live_runner.py             # Live Runner — 14 components, 966 dòng
│   ├── test_chaos_risk.py              # Pre-trade Risk Gate adversarial scenarios
│   ├── test_execution_pipeline.py      # Execution layer: Retry, TokenBucket, RiskEngine, Adapters
│   ├── test_feature_layer.py           # Feature engine pipeline
│   ├── test_rest_api.py                # REST API adapter tests
│   ├── test_signal_engine.py           # ML signal engine
│   ├── test_profiler.py                # Performance profiling
│   ├── test_state.py                   # KairosState WAL/snapshot
│   ├── test_position_sizer.py          # PositionSizer — 14 tests (qty, anti-flip, lot rounding)
│   ├── test_strategies.py              # Exit strategies — 32 tests (6 strategies + composite)
│   ├── test_exit_coordinator.py        # ExitCoordinator — 18 tests (state machine, cooldown, quarantine)
│   ├── test_exit_e2e.py                # Exit system end-to-end integration
│   ├── test_d0_lineage.py              # [M-D0] Data Lineage Registry — 25 tests
│   ├── test_d1_ingestion.py            # [M-D1] Raw Data Ingestion — 31 tests
│   ├── test_d2_gap_reconciliation.py   # [M-D2] Gap Reconciliation — 37 tests (T-D2.1→T-D2.32)
│   ├── test_d3_pit_universe.py         # [M-D3] PiT Universe Manager — 32 tests (T-D3.1→T-D3.30)
│   ├── test_d4_feature_cache.py        # [M-D4] Feature Cache — 29 tests (T-D4.1→T-D4.19)
│   ├── test_paper_adapter_queries.py   # Paper adapter query tests (10 tests)
│   ├── test_position_funding.py        # Position + Funding tests (13 tests)
│   └── test_session_pnl.py             # Session + PnL tests (20 tests)
│
# ==========================================
# 10. KỊCH BẢN VẬN HÀNH (SCRIPTS)
# ==========================================
└── kich_ban/
    ├── __init__.py                     # Package marker
    ├── khoi_dong.py                    # Stack launcher lõi: preflight, process manager, shutdown (181 dòng)
    ├── khoi_dong_live.py               # Entry point tiền thật: `make live`
    ├── khoi_dong_paper.py              # Entry point tiền ảo: `make paper-launch`
    └── dao_tao_lai_model.py            # Re-train ML model pipeline
```

---

## 3. Đặc Tả Module (Module Specifications)

### 3.0. Đặc Tả Chi Tiết Layer 3 và Layer 4 (Layer 3 & 4 Detailed Specifications)

#### Ranh Giới Kiến Trúc HFT (INV-STRATEGY.1)
Kairos-v3 vận hành theo mô hình Mid-Frequency Trading (MFT) với horizon giao dịch tối thiểu 30 phút. Mọi tối ưu hóa ở mức vi giây là không cần thiết. Nghĩa vụ hệ thống bao gồm ghi log vòng đời lệnh đầy đủ (submit/ack/cancel-ack kèm sequence number) và lưu trữ dữ liệu thô (L2 book depth, trade stream) không cắt xén để phục vụ huấn luyện mô hình HFT tương lai.

---

#### 3.0.1. CÁC MODULE ĐÃ TRIỂN KHAI (IMPLEMENTED MODULES)

*   **M-L1: Paper Trading Engine**
    *   *Mô tả:* Hệ thống giả lập giao dịch vi cấu trúc thị trường với khớp lệnh 19 bước.
    *   *Bất biến:* Cấu hình bị đóng băng trong Shadow window, IC/Sharpe parity gate, Seeded RNG (PCG64) đảm bảo tính tái lập.
    *   *Xử lý lỗi:* Quét gap định kỳ và tái dựng log qua audit tool nếu có lệch log.

*   **M-L5: Risk Gate (Pre-trade & Post-trade)**
    *   *Mô tả:* Cổng kiểm soát rủi ro kiểm tra biên an toàn của danh mục trước khi gửi lệnh.
    *   *Bất biến:* Không hardcode hằng số (dùng YAML), kiểm tra hedge imbalance (<0.5% NAV), nhất quán nguồn giá Spot giữa Live và Backtest.
    *   *Xử lý lỗi:* Stale NAV/PnL chuyển trạng thái sang Reduce-Only hoặc chặn hoàn toàn; vi phạm ngưỡng rủi ro kích hoạt cảnh báo Level 0.

*   **M-L6: Exit Management**
    *   *Mô tả:* Hệ thống quản lý thoát vị thế tự động với nhiều chiến lược (Trailing Stop, ATR, Breakeven).
    *   *Bất biến:* Lệnh luôn là Market reduce-only, chống đảo chiều vị thế, áp dụng Lot sizing đúng quy chuẩn sàn.
    *   *Xử lý lỗi:* Position Drift phát hiện bằng đối soát local-exchange; lệnh kẹt tự động hủy và đặt lại sau retry threshold.

*   **M-L7: Live Execution Machinery & SRE Startup**
    *   *Mô tả:* Core thực thi lệnh với pipeline SRE 12 bước khởi động nhanh.
    *   *Bất biến:* Độ trễ hot-path < 5ms, WAL bắt buộc ghi trước khi gửi API, định danh lệnh độc bản (UUID v4), Rate Limiting (Token Bucket).
    *   *Xử lý lỗi:* API error kích hoạt Circuit Breaker, mất WebSocket tự động fallback sang REST, crash mô hình halting toàn bộ.

*   **M-L8: PnL Accounting**
    *   *Mô tả:* Hệ thống kế toán PnL, phí giao dịch và funding rate.
    *   *Bất biến:* Phân tách Unrealized và Realized PnL, tính toán phí âm, đảm bảo idempotency khớp lệnh.
    *   *Xử lý lỗi:* File log hỏng được xử lý bằng cách cắt bỏ phần hỏng và đồng bộ lại; lệch PnL yêu cầu can thiệp thủ công từ operator.

*   **Alert Management**
    *   *Mô tả:* Quản lý phân cấp cảnh báo Telegram.
    *   *Bất biến:* Phân loại 4 cấp độ (CRITICAL tới INFO), tự động suppression các tin trùng lặp để tránh noise.
    *   *Xử lý lỗi:* Buffer log local khi Telegram API lỗi, ưu tiên gửi tin CRITICAL trước khi phục hồi.

---

#### 3.0.2. CÁC MODULE CHƯA TRIỂN KHAI (SPEC-ONLY MODULES)

*   **M-L2: Execution Parity Monitor**
    *   *Mô tả:* Đối chiếu đặc trưng online (live) vs offline (batch) per symbol per bar.
    *   *Bất biến:* Đồng bộ theo bar_id, check code logic hash trước khi so giá trị, status per-alpha.
    *   *Xử lý lỗi:* Thiếu cache M-D4 -> ALERT, không so sánh cache cũ; lệch logic hash -> CRITICAL, block alpha qua Risk Gate.

*   **M-L3: Alpha Decay Monitor**
    *   *Mô tả:* Theo dõi độ suy giảm hiệu năng của mô hình alpha (IC, Sharpe, Slippage).
    *   *Bất biến:* Tránh rò rỉ dữ liệu tương lai khi retrain (`training_data_cutoff = current_bar - horizon_bars`).
    *   *Xử lý lỗi:* Soft decay -> entry_mult = 0.75; Hard decay -> HIBERNATE hoặc HARD_KILL tùy trạng thái thị trường.

*   **M-L4: Portfolio Construction**
    *   *Mô tả:* Kết hợp nhiều tín hiệu alpha thông qua Inverse-Volatility Weighting (IVW).
    *   *Bất biến:* Trọng số alpha đơn lẻ <= 0.35, tổng trọng số <= 1.0, không sinh NaN, giảm beta hệ thống.
    *   *Xử lý lỗi:* Tối ưu hóa phân bổ không hội tụ -> Chuyển phần dư không phân bổ được về cash, ALERT.

*   **M-H1: Event Simulator (Layer 4)**
    *   *Mô tả:* Giả lập luồng sự kiện vi cấu trúc cho các nghiên cứu chiến lược siêu tốc trong tương lai.
    *   *Bất biến:* Giới hạn nghiêm ngặt tần suất trung bình (horizon >= 30m), nghĩa vụ ghi log order và lưu raw L2/trade stream.
    *   *Xử lý lỗi:* Mất dữ liệu raw depth/trade -> ALERT, switch source hoặc mua dữ liệu thay thế.

---

### Trạng Thái Triển Khai (Component Status)

| Module | File chính | Dòng | Trạng thái | Test |
|--------|-----------|------|------------|------|
| **M-D0: Data Lineage** | `quan_ly_phien_ban/` (8 files) | ~1600 | Implemented | `test_d0_lineage.py` (25 tests) |
| **M-D1: Raw Ingestion** | `thu_thap/` (11 modules) | ~2200 | Implemented | `test_d1_ingestion.py` (31 tests) |
| **M-D2: Gap Reconciliation** | `xu_ly_lo/` (8 files) | ~2800 | Implemented | `test_d2_gap_reconciliation.py` (37 tests) |
| **M-D3: PiT Universe Manager** | `xu_ly_lo/` (3 files) | ~750 | Implemented | `test_d3_pit_universe.py` (32 tests) |
| **M-D4: Feature Cache** | `xu_ly_lo/` (2 files) + `khung_alpha/` (3 files) | ~900 | Implemented | `test_d4_feature_cache.py` (29 tests) |
| Live Orchestrator | `live_runner.py` | 1038 | Implemented | `test_live_runner.py` (14 components) |
| Paper EMS Adapter | `paper_ems_adapter.py` | 832 | Implemented | `test_paper_adapter_queries.py` (10 tests) |
| Execution Gateway | `vong_lap_su_kien.py` | 1251 | Implemented | `test_execution_pipeline.py` |
| OMS — OrderBook | `order_book.py` | 763 | Implemented | — |
| PnL Aggregator | `pnl_aggregator.py` | 734 | Implemented | `test_session_pnl.py` (20 tests) |
| Session Manager | `session_manager.py` | 662 | Implemented | `test_session_pnl.py` |
| Risk Gate | `risk_gate.py` | 646 | Implemented | `test_chaos_risk.py` |
| Position Synchronizer | `position_sync.py` | 441 | Implemented | `test_position_funding.py` (13 tests) |
| Execution Wrapper | `execution_wrapper.py` | 435 | Implemented | — |
| Feature Engine | `incremental_engine.py` | 422 | Implemented | `test_feature_layer.py` |
| Reconciler | `reconciler.py` | 400 | Implemented | — |
| Durable WAL | `durable_wal.py` | 354 | Implemented | `test_state.py` |
| Funding Collector | `funding_collector.py` | 266 | Implemented | `test_position_funding.py` |
| Exit Coordinator | `thoat_vi_the/exit_coordinator.py` | 475 | Implemented | `test_exit_coordinator.py` (18 tests) |
| Emergency Flattener | `watchdog/emergency_flattener.py` | 438 | Implemented | — |
| Position State | `thoat_vi_the/position_state.py` | 91 | Implemented | — |
| Position Sizer | `thoat_vi_the/position_sizer.py` | 65 | Implemented | `test_position_sizer.py` (14 tests) |
| Exit Strategies (×6) | `thoat_vi_the/strategies/` | 381 | Implemented | `test_strategies.py` (32 tests) |
| **Layer 2 Research Spec** | `prompt/layer2_research/` (11 R modules) | spec | Spec complete (2026-06-03); audit lần 2 (2026-06-11); consistency audit (2026-06-14): 3 mâu thuẫn (1 HIGH OLS window, 2 MED correlation/trigger actions) resolved/documented; còn M-R1 chưa build (viết code từ spec) | 149 test defs |
| Research / Replay Engine | `nghien_cuu/` | — | Build Phase 0–1 (spec ready) | — |
| ML / ONNX Inference | `hoc_may/suy_luan/` | — | Build Phase 2 | — |
| Stack Launchers | `kich_ban/` | 181 | Implemented | — |

<img width="1920" height="1080" alt="image" src="https://github.com/user-attachments/assets/7208720b-877d-4960-8e0d-1f991cbd9ca1" />

**Nguyên tắc thiết kế hiệu năng:** hot-path loại bỏ GC hoàn toàn (`gc.disable()`), pre-allocated ctypes struct thay Python objects, lock-free SPSC ring buffer, tránh lock trên critical path khi SPSC assumption còn giữ được.

---

### 3.1. Cấu Hình & Môi Trường (Config & Runtime)

Module điều phối cấp cao nhất, kết hợp giữa quản lý tham số tĩnh và kiến trúc giả lập Paper Trading.

#### Cấu hình tĩnh (`cau_hinh/`)

Tách bạch hoàn toàn logic và tham số. Không hardcode bất kỳ giá trị nào trong source code:

- `universe.yaml` — Exchange adapters (timeout, Token Bucket rate/capacity cho Binance/Bybit/OKX) + Symbol master: ánh xạ cặp tiền thành số nguyên `symbol_id` để truy xuất mảng O(1) trong hot-path, định nghĩa `qty_step`, `price_step`, `max_pos_usdt`.
- `trading.yaml` — Toàn bộ tham số giao dịch: Risk Gate (`max_daily_loss_usdt`, `max_drawdown_pct`), OMS (`segment_rotation_count: 60000`, `archive_max_size: 50000`), PnL (`scale_factor: 1e8`, `checkpoint_every: 1000`), Position Sync, Funding, Session rotation, Strategy (`vector_size`).
- `infra.yaml` — Hạ tầng vận hành: Flow Control (CRITICAL queue `100k`, drain budget `200µs`, EWMA Adaptive Shedder), Execution Protection (Rate Limiter, Circuit Breaker, Retry — dưới key `execution:`), Alerting Telegram + rules (dưới key `alerting:`), System Metrics observability (dưới key `observability:`).
- `adapter_loader.py` — Factory tự động nạp cấu hình kết hợp biến môi trường `.env`:

```python
# cau_hinh/adapter_loader.py
def build_adapters(env_path, exchanges_yaml, active_exchanges):
    """Tự động khởi tạo Exchange Adapters từ config files."""
    active = list({cfg.exchange for cfg in configs.values()})
    adapters = build_adapters(env_path, exchanges_yaml, active_exchanges=active)
```

#### Môi trường Paper Trading (`moi_truong_chay/paper/`)

Không dùng "mock" đơn giản. Đây là một sàn giao dịch nội bộ mô phỏng vi cấu trúc thị trường (Market Microstructure) với **Pipeline Khớp Lệnh 19 Bước**:

- **Seeded & Replay-friendly**: Mọi lệnh được gán `SHA256-seeded RNG` để giảm sai khác giữa các lần chạy paper/replay. Đây là tính chất determinism trong phạm vi simulator; nó không đồng nghĩa với live determinism hay external consistency với exchange thật.
- **Latency Jitter**: Phân phối Lognormal đuôi dài (heavy-tail) mô phỏng network jitter thực tế. Không khớp lệnh "ngay lập tức" như các thư viện thông thường.
- **Nhiễu loạn Orderbook (Spoofing)**: Tự động bào mòn sổ lệnh, mô phỏng ảo ảnh thanh khoản (Liquidity Mirage) và lệnh tảng băng (Iceberg).
- **Trượt giá (Kyle's Alpha)**: Ứng dụng mô hình Kyle's Alpha tính toán self-impact. Tự động sinh tỷ lệ độc hại (Toxicity / Adverse Selection) liên tục.
- `microstructure_model.py` — Process nền nghe ZMQ để bám sát Volatility, Order Flow Imbalance, và tính toán hệ số lây lan rủi ro chéo (Contagion: BTC sập → ALT đổ) trong cửa sổ 500ms.
- `shock_simulator.py` — Giả lập các cú sốc đột ngột (Flash Crash, Liquidation Cascade) để stress-test thuật toán quản lý rủi ro.

#### Môi trường Live Trading (`moi_truong_chay/live/`) — Live Orchestrator (production-oriented)

File `live_runner.py` — **1038 dòng**, là module khởi động toàn bộ pipeline giao dịch thật (real money). Tuân thủ kiến trúc **SRE-grade 12-Step Fail-Fast Startup**:

```
Step  1: system.KILLED check           ← Abort ngay nếu sentinel tồn tại
Step  2: Load live_config.yaml         ← SRE tuning parameters
Step  3: Load universe.yaml + secrets  ← Symbols + API keys
Step  4: Init Exchange Adapters        ← Binance/Bybit/OKX với httpx.AsyncClient riêng
Step  5: Balance check                 ← Fail-fast nếu < min_startup_balance_usdt
Step  6: Init KairosState (WAL-first)  ← Crash-recovery từ WAL → Snapshot
Step  7: Init ExecutionGateway         ← Ring buffer + 5 threads
Step  8: Startup Reconciliation        ← Cancel sweep + orphan fill detection
Step  9: Flush ZMQ stale signals       ← Drain messages tồn đọng
Step 10: Launch Health Daemon          ← 401/403/5xx/429 monitoring
Step 11: Launch Reconcile Daemon       ← Periodic state sync
Step 12: Launch KillSwitch Monitor     ← system.KILLED file polling
```

**3 nguyên tắc vận hành cốt lõi:**

- **Absolute Physical Rate Isolation**: Mỗi daemon (Health, Reconcile, Hot-Path) khởi tạo `httpx.AsyncClient` và `TokenBucket` **hoàn toàn độc lập**. Không bao giờ chia sẻ connection pool hay rate quota giữa background monitoring và execution path — tránh tình huống health check chiếm hết rate limit của lệnh giao dịch.

- **Crash-Consistency (WAL First)**: Khi khôi phục sau crash, thứ tự bắt buộc là: Đọc WAL nội bộ (lấy lệnh in-flight) → Fetch sàn (lấy lệnh đã khớp) → So sánh. Nếu phát hiện orphan fill (sàn có position mà WAL không ghi nhận), symbol đó bị **HALT** ngay lập tức. Đây là contract mong muốn; nó chỉ đáng tin khi không có mutation path nào bypass WAL hoặc side effect external chưa được reconcile lại.

- **CoalescingQueue**: Queue đặc biệt cho reconciliation snapshots — khi đầy, tự động drop snapshot cũ nhất thay vì block producer. Cơ chế này chỉ nên áp cho snapshot/telemetry có thể thay thế; không nên hiểu là mọi event causal đều an toàn để drop.

> `Fail-fast`, `drop`, và `coalescing` ở lớp vận hành là trade-off latency. Nếu event bị bỏ có thể làm sai portfolio/order state, engine nên tự đánh dấu `degraded`, chạy recovery, hoặc `halt` thay vì tiếp tục coi state là authoritative.

**Cấu hình SRE** (`live_config.yaml`):

```yaml
live:
  health_check:
    interval_s: 60.0           # Mỗi 60s kiểm tra balance + latency
    max_failures: 3            # 3 lần 5xx liên tiếp → safe_hard_kill
    latency_threshold_ms: 1000 # Cảnh báo nếu exchange response > 1s
  reconciliation:
    interval_s: 120.0          # Mỗi 2 phút đối soát state
    max_drift_s: 600.0         # Nếu > 10 phút không sync → force sync ngay
    max_pressure: 0.70         # Skip sync nếu gateway backpressure > 70%
  cancel_policy:
    mode: "ALL"                # Cancel tất cả open orders khi startup
    cooldown_s: 10.0           # Chờ 10s sau cancel để exchange settle
```

---

### 3.2. Hồ Dữ Liệu (Data Lake)

Kiến trúc Cold-Storage, tối ưu cho Backtest Vectorized hàng tỷ rows.

#### Dữ liệu thô (`ho_du_lieu/tho/`) — M-D1 Output

Không bao giờ chỉnh sửa (Immutable). Lưu dưới định dạng Parquet nén Snappy theo chuẩn **Year/Month Partitioning**. Mọi Parquet file sau write PHẢI đăng ký với M-D0 `LineageRegistry`.

- `{exchange}/{symbol}/year={Y}/month={M}/data.parquet` — **1h OHLCV Bar** (17 columns): event_time_ns, OHLCV, funding_rate, oi_usdt, liquidation_buy/sell_usdt, dq_flags, is_settlement_bar, is_backfill.
- `{exchange}/{symbol}/long_short/year={Y}/month={M}/data.parquet` — **Long/Short Ratio** (5min, 7 columns): timestamp_ns, long_short_ratio, long/short_account_ratio.
- `onchain/{asset}/{metric}/year={Y}/month={M}/data.parquet` — **On-chain Data** (hourly, 6 columns): exchange_reserve, exchange_netflow, stablecoin_supply từ CryptoQuant.

#### Kho Đặc Trưng Online (`ho_du_lieu/kho_dac_trung/online/memory_store.py`)

File **291 dòng**, là bộ nhớ cache siêu tốc trên RAM phục vụ inference live. Thiết kế đạt chuẩn **Zero-allocation trên hot-path**:

```python
# ho_du_lieu/kho_dac_trung/online/memory_store.py
class OnlineFeatureStore:
    """Pre-allocated, cache-line-aligned feature store for MAX_SYMBOLS symbols."""

    def __init__(self, max_symbols: int = 256) -> None:
        # ── Main store: NumPy structured array, 192 bytes/symbol ──
        self._store = np.zeros(max_symbols, dtype=FEATURE_EVENT_DTYPE)

        # ── Pre-compute destination pointers (startup cost, zero hot-path alloc) ──
        self._feat_ptrs: list[int] = [
            int(self._feat_col[i].ctypes.data) for i in range(max_symbols)
        ]
```

**Thiết kế chống False-sharing**: `FEATURE_EVENT_DTYPE.itemsize == 192 bytes` (vừa khít 3 cache lines 64B). Symbols ở index `i` và `i+1` bắt đầu ở offset `192*i` và `192*(i+1)` — header 64B đầu tiên không bao giờ trùng cache line.

**Reorder Buffer (ROB)**: Trong thị trường crypto, do độ trễ mạng, các luồng WebSocket thường gửi gói tin bị lộn xộn thứ tự (out-of-order). Tính toán OFI hoặc EMA dựa trên dữ liệu lộn xộn sẽ cho kết quả sai. ROB là một "phòng chờ" chứa các tick đến sớm trong cửa sổ 5ms. Khi đến hạn, hệ thống dọn dẹp bằng thuật toán **In-place Insertion Sort** siêu nhẹ, vì mảng thường chỉ có 1-3 phần tử bị lộn xộn:

> ROB chỉ là heuristic reorder window, không phải "timeline sự thật" tuyệt đối. Nếu exchange timestamp thiếu nhất quán hoặc event đến trễ ngoài cửa sổ, feature nên phân biệt rõ `event_time` với `processing_time`, và cần policy rõ cho late event thay vì giả định reorder xong là đúng.

```python
# In-place insertion sort — n ≤ 64, thường chỉ 1-3 phần tử
for i in range(1, n):
    key_ts   = int(buf[i, 0])
    key_slot = int(buf[i, 1])
    j = i - 1
    while j >= 0 and int(buf[j, 0]) > key_ts:
        buf[j + 1, 0] = buf[j, 0]
        buf[j + 1, 1] = buf[j, 1]
        j -= 1
    buf[j + 1, 0] = key_ts
    buf[j + 1, 1] = key_slot
```

**Zero-Allocation Write** — `_write_to_store()` ghi trực tiếp vào NumPy structured array mà không tạo bất kỳ Python object nào:

```python
# memory_store.py — _write_to_store()
def _write_to_store(self, idx, raw, effective_mask):
    """Zero-allocation write to main store."""
    s = self._store   # single attribute lookup, not a copy
    # Column-first access: ghi trực tiếp vào mảng int64/uint64
    # KHÔNG tạo numpy.void intermediate object
    s["exchange_ts"][idx]            = raw.exchange_ts
    s["receive_ts"][idx]             = raw.receive_ts
    s["processed_ts"][idx]           = raw.processed_ts
    s["source_latency_ns"][idx]      = raw.source_latency_ns
    s["computation_latency_ns"][idx] = raw.computation_latency_ns
    s["feature_mask"][idx]           = effective_mask
    # ctypes.memmove: copy 128 bytes feature block ở tốc độ C
    ctypes.memmove(self._feat_ptrs[idx], ctypes.addressof(raw.features), _FEAT_BYTES)
```

> **Tại sao dùng Column-first thay vì Row indexing?** Nếu viết `self._store[idx]["exchange_ts"] = ...` (row-first), NumPy sẽ tạo một `numpy.void` object tạm thời — tức là 1 lần allocation cho mỗi tick. Column-first `self._store["exchange_ts"][idx] = ...` ghi thẳng vào mảng, bypass hoàn toàn Python object layer.

**Lockless `_drop_mask` Read** — Khi `signal_congestion()` thay đổi `_drop_mask` từ monitor thread, hot-path đọc giá trị mới **không cần lock**. CPython GIL đảm bảo `LOAD_ATTR` (đọc integer) là atomic ở tầng bytecode. Thêm lock sẽ tốn ~100ns/tick — vô nghĩa vì giá trị chỉ thay đổi mỗi vài giây.

```python
# commit_raw() — hot-path, KHÔNG lock
effective_mask = raw.feature_mask & ~self._drop_mask   # GIL-atomic int read
if effective_mask == 0:
    return False   # Mọi features bị drop → bỏ qua tick này
```

**Priority Degradation API** — 3 cấp độ Backpressure:

| Level | Hành vi | Mục đích |
|-------|---------|----------|
| 0 | Normal — ghi tất cả features | Hoạt động bình thường |
| 1 | Drop Trade-derived (Welford, EMA, OFI) | Giảm tải khi hệ thống bắt đầu nghẽn |
| 2 | Drop ALL market features | Chỉ giữ Risk/Liquidation sentinel |

---

### 3.3. Động Cơ Dữ Liệu (Data Engine)

**Hợp đồng hiệu năng:** latency budget **< 10–50µs/tick** từ WebSocket receive đến feature ready trong `OnlineFeatureStore`.

<img width="1920" height="1080" alt="image" src="https://github.com/user-attachments/assets/e35efc49-1df3-4ce2-8247-ea6420478ac5" />

#### M-D0: Data Lineage Registry (`dong_co_du_lieu/quan_ly_phien_ban/` — 8 files)

Immutable dataset registry — mọi Parquet artifact đều phải đăng ký trước khi dùng trong backtest.

- `dataset_record.py` — `DatasetRecord` + `VerificationRecord`, canonical Arrow IPC hash, `write_and_register()` atomic
- `lineage_registry.py` — Append-only JSONL, `as_of()` PIT query, orphan sweep
- `pit_verifier.py` — `verify_pit_production()` stratified 500+ rows, timestamp unit heuristic (s/ms/ns)
- `symbol_lifecycle_poller.py` — Daemon daily poll → state-based interval tracking
- **Tests:** 25 unit tests (`test/test_d0_lineage.py`)

#### M-D1: Raw Data Ingestion (`dong_co_du_lieu/thu_thap/` — 10 new modules)

Nhận bar từ WS/REST → QC → buffer settlement → write Parquet → register M-D0.

- **WebSocket Gateway** (Binance, OKX, Bybit): `binance_ws.py`, `okx_ws.py`, `bybit_ws.py`
- **REST API Pollers**: OI (5m), Funding (1h), L/S Ratio (5m), Klines (1m), On-chain (CryptoQuant)
- `bar_processor.py` — `compute_dq_flags()` 4-bit bitmask, `attach_oi_to_bar()` (INV-D1.11 >= boundary), `write_parquet_atomic()`
- `settlement_buffer.py` — T+5min funding buffer + persistent spool (int64 serialized as string, INV-D1.24)
- `dead_letter_queue.py` — Rolling 1h window per-exchange, alert khi rate > 1% và > 100 msgs
- `funding_interval_cache.py` — Settlement minutes cache (minutes not hours, INV-D1.5), Bybit stale detection
- `liquidation_aggregator.py` — Thread-safe per-bar aggregation của `LiquidationEvent`
- `aux_parquet_writer.py` — Long/Short ratio + On-chain Parquet riêng biệt
- `schema_validator.py` — 3 schemas: OHLCV (17 cols), Long/Short (7 cols), On-chain (6 cols)
- `startup_routines.py` — Orphan scan + SIGTERM handler

**Output:** `ho_du_lieu/tho/{exchange}/{symbol}/` (OHLCV+liquidation) | `.../long_short/` (5min) | `.../onchain/` (hourly)
- **Tests:** 31 unit tests (`test/test_d1_ingestion.py`)

#### M-D2: Gap Reconciliation (`dong_co_du_lieu/xu_ly_lo/` — 8 files)

Nightly batch chạy lúc 02:00 UTC, nhận raw bars từ M-D1 → phát hiện gaps → fill từ REST → gán `data_quality` → merge funding rates → ghi clean Parquet.

- `reconcile.py` — Orchestrator 7 bước: daemon heartbeat check → lifecycle → maintenance windows → gap manifest → fill/classify → funding merge → atomic write + coverage report
- `schema_validator.py` — `MD2_SCHEMA` explicit PyArrow (26 columns), `write_with_schema_version()` atomic, schema_version=1 mandatory
- `maintenance_event_logger.py` — Daemon poll exchange status mỗi 5min → `maintenance_log_{date}.parquet` + `daemon_heartbeat.json`; stale > 15min → quality=3
- `gap_detector.py` — Missing bar scan + zombie volume detection (MAD robust_zscore, two-tail, baseline từ history only — không look-ahead); gap manifest immutable detection fields
- `rest_filler.py` — `fill_gap() → (DataFrame, quality, reason)` với reason authoritative cho fill_outcome; intra-gap continuity (4 checks) + boundary validation symmetric; HTTP 418/429 backoff
- `quality_tagger.py` — `data_quality` 0–4; daemon stale → mark bars với quality=3 (không skip); `compute_coverage_report()` aggregate từ batch runner
- `reconcile_funding_rates.py` — `funding_rate_raw` + anti-lookahead (`funding_published_ns ≤ close_time_ns`); `received_ns` set sau response; `_check_funding_cache_freshness()` trong `load_funding_schedule()`; FAIL LOUD nếu cache miss

**Output:** `ho_du_lieu/da_xu_ly/` (clean bars, 26 cols) | `ho_du_lieu/gap_manifest/` (audit) | `ho_du_lieu/bao_cao_phu_song/` (daily coverage)

**`data_quality` enum:** 0=WS native | 1=REST filled | 2=suspect (zombie/boundary) | 3=missing | 4=scheduled_maintenance
- **Tests:** 37 unit tests (`test/test_d2_gap_reconciliation.py`) — T-D2.1 → T-D2.32

#### M-D3: Point-in-Time Universe Manager (`dong_co_du_lieu/xu_ly_lo/` — 3 files)

**Anti-survivorship-bias module quan trọng nhất.** Quản lý universe theo `known_at` semantics: `get_universe(date)` chỉ include assets mà KAIROS biết trước 00:00 UTC của ngày đó.

- `pit_universe.py` — `make_asset_id()` UUID v5 per listing event (deterministic, tránh ticker collision LUNA/LUNA2); `AssetRegistry` SQLite (6 tables, WAL mode, write-once trigger); `PiTUniverseManager.get_universe()` / `get_universe_history()` / `get_asset_id()`; `build_daily_snapshot()` eligibility formula (ADV, OI, coverage, funding dysfunction, stablecoin); `process_exchange_poll()` listing/delist detection; circuit breaker >50% universe missing; `pit_audit()` verify INV-D3.1/D3.2/D3.3/D3.20
- `symbol_remapper.py` — `apply_rename()` RENAME: asset_id GIỮNGUYÊN; `apply_fork()` FORK: new asset_id; `flag_ambiguous_rename()` → review queue JSONL cho manual review
- `seed_sector_assignments.py` — One-time seeder: `cau_hinh/sector_map.yaml` → `sector_assignments` table; explicit override cho LUNA-style ticker collision

**Lifecycle (Phase 0):** `ACTIVE` → `SUSPECTED_DELIST` → `DELISTED` (terminal). False alarm: `SUSPECTED_DELIST` → `ACTIVE`. Dual condition confirm: ≥3 polls + ≥12h wall-clock.

**Output:** `ho_du_lieu/lich_su_vu_tru/{date}__{built_at}.parquet` (versioned immutable) + `manifest.json` (append-only)
- **Tests:** 32 unit tests (`test/test_d3_pit_universe.py`) — T-D3.1 → T-D3.30

#### M-D4: Feature Cache (`xu_ly_lo/` + `nghien_cuu/khung_alpha/` — 5 files)

Pre-compute và cache feature matrices cho research. Cache invalidated by feature logic hash khi logic thay đổi.

- `khung_alpha/feature_spec.py` — `FeatureSpec` frozen dataclass; `FEATURE_SPECS` gồm các feature thuộc nhiều nhóm (momentum / funding / open-interest / cross-sectional / microstructure) — *danh sách feature cụ thể là nội bộ, đã lược bỏ khỏi bản công khai*; DAG cycle validation tại import time (INV-D4.24)
- `khung_alpha/feature_registry.py` — `FEATURE_REGISTRY` tập các `FeatureFn` module-level; kỹ thuật xử lý gồm winsorize/outlier-flag, Welford expanding z-score (numerically stable, PiT-safe), rolling-OLS beta neutralization kèm coverage check, warmup → NaN khi thiếu lịch sử; `IncrementalFeatureEngine` topo-sort + sync-emit đảm bảo batch/live parity; `FeatureSpecRegistry` — cầu nối M-D0 PIT Verifier. *(Tham số cửa sổ & công thức chi tiết là nội bộ, đã lược bỏ)*
- `xu_ly_lo/feature_cache.py` — `compute_feature_logic_hash()` SHA-256(AST bundle); 12-char `cache_hash`; `write_cache()` atomic + provenance metadata (INV-D4.21/23); `FeatureCache.get_or_compute()` idempotent, BTC-first ordering, stale context fix
- `xu_ly_lo/pre_aggregate_l2.py` — `L2Snapshot` binary encode/decode; `store_raw_snapshot()` append daily `.bin` (**KHÔNG BAO GIỜ delete**); order-flow imbalance + book-pressure features từ L2 *(công thức & tham số cửa sổ là nội bộ, đã lược bỏ)*, thiếu dữ liệu → NaN (INV-D4.11b). **M-H1 Nghĩa vụ 2 (spec — mở rộng):** lưu kèm receive timestamp + lastUpdateId/sequence + depth coverage metadata, và raw trade stream (per-trade, không aggregate) — L2 diffs bị netted, trade stream là nguồn duy nhất decompose cancel-vs-trade cho HFT queue model sau này

**Cache path:** `ho_du_lieu/kho_dac_trung/offline/{hash_12}/{asset_id}__{start}_{end}.parquet`

#### Xử lý dòng (`dong_co_du_lieu/xu_ly_dong/bo_loc/`)

- `orderbook_engine.py` — Engine đồng bộ L2 Orderbook realtime cho cả 3 sàn. Xử lý snapshot + incremental update.
- `ohlc_engine.py` — Aggregator tổng hợp OHLCV candles từ raw trades.

#### Feature Registry (`dong_co_du_lieu/ong_dan_dac_trung/online/feature_registry.py`)

File **293 dòng**. Trong hệ thống ML, tính năng A có thể phụ thuộc tính năng B và C. Thay vì duyệt qua cây phụ thuộc (dependency graph) mỗi khi có tick mới — việc này tốn CPU — Kairos **biên dịch trước đồ thị (Precompiled DAG)** thành một danh sách phẳng ngay lúc khởi động. Lúc chạy, hệ thống chỉ việc loop qua danh sách này:

```python
# feature_registry.py — Chạy MỘT LẦN khi khởi động
def compile_dag() -> dict[int, CompiledPlan]:
    """Converts the DAG into flat CompiledPlan tuples keyed by event_type_id.
    Runtime cost per tick: one dict lookup + iteration over ≤5 CompiledSteps.
    No graph analysis, no conditional branching."""
    plans = {}
    for evt_id, fds in DEPENDENCY_TRIGGER_MAP.items():
        plans[evt_id] = tuple(
            CompiledStep(mask_bit=fd.mask_bit, update_fn=fd.update_fn)
            for fd in fds
        )
    return plans
```

**6 Alpha Features được tính toán realtime**:

| Index | Feature | Công thức toán học | Trigger |
|-------|---------|-------------------|---------|
| 0 | `MICRO_PRICE` | `(bid_vol × ask + ask_vol × bid) / (bid_vol + ask_vol)` | Orderbook |
| 1 | `BOOK_PRESSURE` | `bid_vol / (bid_vol + ask_vol)` → Range [0, 1] | Orderbook |
| 2 | `WELFORD_VAR` | Welford online variance O(1) — `M2 / (n-1)` | Trade |
| 3 | `EMA_FAST` | `α=2/(10+1)` — Recursive EMA 10-period | Trade |
| 4 | `EMA_SLOW` | `α=2/(50+1)` — Recursive EMA 50-period | Trade |
| 5 | `OFI` | Cont, Kukanov & Stoikov (2014) eq.(1) — `cumsum(e_n^B - e_n^A)` | Orderbook |

**OFI (Order Flow Imbalance)** — Thuật toán tiên tiến theo paper của Cont et al. 2014. Tính toán áp lực mua/bán thuần túy dựa trên biến động của mức giá tốt nhất (best bid/ask), thay vì chỉ nhìn vào khối lượng giao dịch:

```python
# feature_registry.py — OFI implementation
def _update_ofi(features, state, payload):
    """Order Flow Imbalance per Cont, Kukanov & Stoikov (2014), eq. (1)."""
    if best_bid > prev_bid:
        e_bid = bid_vol                         # Giá tăng: toàn bộ volume là cầu mới
    elif best_bid == prev_bid:
        e_bid = bid_vol - state.ofi_prev_bid_vol  # Giá đi ngang: chênh lệch volume
    else:
        e_bid = -state.ofi_prev_bid_vol           # Giá giảm: mức giá cũ bị hủy bỏ/khớp hết

    features[FEATURE_IDX_OFI] = state.ofi_cum_bid_delta - state.ofi_cum_ask_delta
```

#### Incremental Engine (`dong_co_du_lieu/ong_dan_dac_trung/online/incremental_engine.py`)

File **353 dòng**. Động cơ thực thi Hot-Path chính. Mọi thiết kế đều hướng đến mục tiêu: **không tạo bất kỳ Python object nào trong vòng lặp**.

**Dispatch Table O(1)** — Thông thường, để rẽ nhánh xử lý loại dữ liệu (Trade, Orderbook, Liquidation), lập trình viên dùng chuỗi `if/elif/else`. Trong HFT, việc này tốn chu kỳ CPU cho Branch Prediction. Kairos giải quyết bằng mảng con trỏ hàm:

```python
# incremental_engine.py — Zero-branch routing
self._dispatch = [
    self._handle_trade,        # 0: EVT_TRADE
    self._handle_orderbook,    # 1: EVT_ORDERBOOK
    self._handle_liquidation,  # 2: EVT_LIQUIDATION
    self._handle_mark_price,   # 3: EVT_MARK_PRICE
    self._handle_rest,         # 4: EVT_REST
]
# Hot-path: một lần dereference mảng → gọi hàm
return self._dispatch[event_type_id](symbol_id, ...)
```

**Object Pool (Ring Buffer)** — Pre-allocate 16,384 slots × 192 bytes = ~3.1 MB trong L3 cache:

```python
# Pool tĩnh — KHÔNG allocation sau __init__
POOL_SIZE = 16_384   # power-of-2
self._pool = [make_empty_raw() for _ in range(POOL_SIZE)]
# Pre-compute numpy views — np.frombuffer chỉ gọi 1 lần
self._pool_views = [
    np.frombuffer(self._pool[i].features, dtype=np.float64)
    for i in range(POOL_SIZE)
]
```

**ScratchPayload** — Thay thế `dict` bằng `@dataclass(slots=True)` tái sử dụng:

```python
@dataclass(slots=True)
class ScratchPayload:
    """Reusable payload buffer; replaces per-tick dict creation."""
    price:    float = 0.0
    qty:      float = 0.0
    bid_vol:  float = 0.0
    ask_vol:  float = 0.0
    best_bid: float = 0.0
    best_ask: float = 0.0

    def __getitem__(self, key: str) -> float:
        return getattr(self, key)   # Dict-compatible interface
```

**Transactional State Update** — Mô hình giao dịch ngăn data corruption:

```python
def _run_plan(self, symbol_id, event_type_id, exchange_ts, receive_ts):
    # 1. Borrow pool slot; seed features từ store qua memmove
    ctypes.memmove(
        ctypes.addressof(raw.features),
        self._sym_src_ptrs[symbol_id],   # Pre-computed pointer
        _FEAT_BYTES,                      # 128 bytes, ~0.05µs
    )

    # 2. Snapshot SymbolState vào scratch (no allocation)
    _copy_state(state, scratch)

    # 3. Execute update_fns trên draft_view (NOT live store)
    for step in plan:
        step.update_fn(draft_view, state, payload)

    # 4. Gate: commit to store — nếu bị reject, rollback
    if not self._store.commit_raw(raw):
        _copy_state(scratch, state)   # Rollback — zero allocation
        return None

    return raw
```

---

### 3.4. Phòng Nghiên Cứu & Kiểm Thử — Layer 2 Research Core

> **Status 2026-06-04:** Layer 2 spec đã hoàn thiện và audited (M-R1 → M-R11). Cross-audit Layer 1+2 phát hiện và fix **15 bugs** (3 critical, 7 significant, 5 minor) — xem chi tiết bên dưới. Sẵn sàng bắt đầu build Phase 0.
>
> **Status 2026-06-11 (audit lần 2):** Phát hiện **4 xung đột spec mức HIGH** giữa M-R1/R3/R4/R5/R6/R9 — **ĐÃ RESOLVED trong prompt** (cùng ngày, uncommitted lúc đó): (1) double-invert M-R3↔M-R4 → T1 là owner duy nhất của inversion, caller pass signal RAW; (2) WF windows vs holdout → n_windows 6→5 (`floor((in_sample−12)/3)`), splitter raise nếu test window chạm holdout; (3) n_trials → counter = line-count per data_window (create dedup config_hash, increment_trial append); (4) signal_cache → +asset_id + canonical path + overlap-guard (no-overlap 90d → DEFERRED, không PASS im lặng). **Còn lại:** M-R1 (`hoc_may/huan_luyen/`) **CHƯA được build** — spec sẵn sàng, cần viết code từ spec trước khi build tiếp Phase 0. (Không phải "mất source" — M-R1 chưa từng được tạo.)
>
> **Status 2026-06-14 (Đồng bộ toàn diện Layer 1/2/3):** Thực hiện kiểm tra tính đồng bộ logic liên module trên cả 3 Layer (Data, Research, Live):
> - **Layer 1 (Data Core) — Audit ban đầu:** 3 mâu thuẫn HIGH đã được giải quyết: (1) `funding_rate_raw` thống nhất để NaN ở non-settlement bars nhằm tránh lỗi tính toán z-score của M-D4; (2) đồng bộ enum `data_quality` (`1=rest_filled, 2=suspect`); (3) làm rõ `FeatureSpecRegistry` thuộc M-D4.
> - **Layer 1 (Data Core) — Deep audit:** Thêm 3 HIGH + 4 MEDIUM đã fix triệt để: **(H1)** M-D2 thêm cột `funding_rate_applied` (= `funding_rate_raw × mark_price × position_size`, non-null tại settlement bars) vào MD2_SCHEMA; **(H2)** M-D4 sửa logic settlement bar detection: dùng `exchange_metadata.yaml` thay vì hardcode `is_settlement_bar`, thêm fallback `bar_hour in [0,8,16]`; **(H3)** M-D3 bổ sung `Depends on: M-D0` (LineageRegistry) thay vì poll trực tiếp — lifecycle state xác định qua M-D0 registry; **(M1)** M-D2 sửa path `symbol_lifecycle_raw` thành `ho_du_lieu/tho/lifecycle/symbol_lifecycle_raw.parquet` (khớp M-D0 output); **(M2)** M-D3 `asset_id` divergence: xác nhận UUID v5 = SSOT, M-D2 dùng `(exchange, symbol)` tuple chỉ là lookup key; **(M3)** M-D3 rename `funding_rate_8h` → `funding_rate_raw`, `annualized_rate` → `funding_rate_annual`, sửa APR comment `×3×365` → `×(365/funding_interval_days)`; **(M4)** M-D3 OI sourcing: ghi rõ `oi_usdt` lấy từ M-D1 bar (M-D2 cleaned), không tự REST poll.
> - **Layer 2 (Research Core):** 3 mâu thuẫn chính đã được xử lý: (1) đồng bộ `window` tính beta của M-R8 thành 504 bars (21 ngày) để khớp với M-R1; (2) xác nhận M-R10 SCAN-4 tự tính độc lập; (3) chỉnh sửa TRIGGER-F3 của M-R9 thành HALT + giữ nguyên vị thế để khớp với M-R11/M-R6.
> - **Layer 3 (Live Core):** 4 phát hiện đã đồng bộ: (1) sửa window của M-L1 thành 60 calendar market days; (2) bổ sung kênh `family_edge_exhaustion` từ M-L4 vào M-L3; (3) chốt định nghĩa `feature_drift_flag` (M-L2 status ∈ {ALERT, CRITICAL}); (4) bổ sung dependency M-L1 → M-L5.
>
> **Status 2026-06-13 (consistency pass toàn hệ + docs sync):** Cross-check logic 6 module layer3_live (11 mâu thuẫn + 1 deadlock RG-6 đã fix) rồi sync `docs/` theo `prompt/` (source of truth) cho toàn bộ layer 1/2/3/4. Layer 2 lệch docs: **M-R3** docs cũ 2-state → đúng là **3-state PASS/BORDERLINE/KILL** (hard floor 0.015, cost params 16/30); **M-R5** thêm **WARN tier + sharpe_tier**. Layer 1: M-D4 feature `funding_rate_8h` → `funding_rate_raw` (cấm rename), winsorization 5×IQR, M-D3/M-D4 status BUILT. **Xung đột cross-layer M-R9↔M-L1 RESOLVED**: shadow gate baseline = `backtest_ic_sim` (execution-adjusted, không phải t1_cv_ic_mean frictionless); đếm ngày = calendar market days (không phải signal-log).
>
> **Bug fixes 2026-06-04 (cross-layer audit):**
>
> - **[CRITICAL]** K3 formula: `×10000` → `×1000` (per-mille) — gate T0 vô hiệu trước đây
> - **[CRITICAL]** M-R2 Leakage Audit: 10 → 14 checks (thêm AUDIT-11/12/13/14)
> - **[CRITICAL]** effective_n formula: `T×(1-ρ)²/halflife` → `T×(1-|ρ|)/(1+|ρ|)` (Cochrane 2001)
> - **[HIGH]** M-D3 ADV: `bar_count` giờ loại cả `quality=4` (maintenance), không chỉ `quality=3`
> - **[HIGH]** M-D4: winsorization contract — feature neutralization không đọc pre-computed return từ bar
> - **[HIGH]** cross-day funding blackout: bar 23:30 UTC phải check settlement ngày hôm sau
> - **[HIGH]** M-R4 T1: nhận `t0_result` và tự invert signal nếu `signal_direction=-1`
> - **[HIGH]** M-R2 AUDIT-9: None-guard cho `research_yaml_hash` Phase 0
> - **[HIGH]** `risk_gate.py`: `ConfirmedState` constructor missing `unrealized_pnl=0.0` (unblocks test suite)
> - **[MEDIUM]** M-R7: `settlement_hours` → parse từ key thực `settlement_times_utc` trong YAML

**Layer 2 Research Pipeline (3-tier screening):**

| Module | File | Phase | Mô tả | Trạng thái |
|--------|------|-------|-------|------------|
| **M-R1** | `hoc_may/huan_luyen/` | Phase 0 | Model Training — Ridge default, ExperimentRecord, PurgedKFold, DSR. | Spec-only / Chưa build |
| **M-R2** | `nghien_cuu/danh_gia/leakage_audit.py` | Phase 0 | Leakage Audit — 14 checks, hard gate trước T1 | Spec-only / Chưa build |
| **M-R3** | `nghien_cuu/nha_may_alpha/t0_screen.py` | Phase 0 | T0 Screen — IC + cost + stability, ≤15 min. 3-state PASS/BORDERLINE/KILL (hard floor 0.015) | Spec-only / Chưa build |
| **M-R4** | `nghien_cuu/nha_may_alpha/t1_validate.py` | Phase 0 | T1 Validate — PurgedKFold + DSR + 9 checks (2-state PASS/KILL) | Spec-only / Chưa build |
| **M-R5** | `nghien_cuu/nha_may_alpha/t2_diligence.py` | Phase 1 | T2 Full Diligence — 11 checks. 3-state PASS/WARN/KILL + sharpe_tier | Spec-only / Chưa build |
| **M-R6** | `nghien_cuu/dong_co_phat_lai/vectorized_backtest.py` | Phase 0 | Polars vectorized backtest | Spec-only / Chưa build |
| **M-R7** | `nghien_cuu/kiem_thu_qua_khu/ma_tran_sie_toc/` | Phase 0 | Cost Model — slippage + regime multiplier | Spec-only / Chưa build |
| **M-R8** | `nghien_cuu/nha_may_alpha/factor_neutralizer.py` | Phase 1 | Factor Neutralization — joint BTC+ETH OLS | Spec-only / Chưa build |
| **M-R9** | `nghien_cuu/nha_may_alpha/alpha_registry.py` | Phase 1 | Alpha Registry + Cemetery | Spec-only / Chưa build |
| **M-R10** | `nghien_cuu/kham_pha_dac_trung/anomaly_miner.py` | Phase 1 (Tier B) | Anomaly Miner | Spec-only / Chưa build |
| **M-R11** | `giam_sat/trang_thai_thi_truong/market_state_engine.py` | Phase 1 | Market State — 8-dim state vector | Spec-only / Chưa build |

### Đặc Tả Chi Tiết Layer 2 (Research Core) — Trạng thái: Spec-only / Chưa build

Dưới đây là đặc tả chi tiết của Layer 2 (Research Core) bao gồm các tệp tin đặc tả, mã nguồn đích, bất biến kỹ thuật và kịch bản phục hồi lỗi:

---

#### M-R1: Model Training Pipeline
-   **Trạng thái:** Spec-only / Chưa build
-   **Tệp tin đặc tả:** M-R1_model_training.md
-   **Tệp mã nguồn đích:**
    -   trainer.py — Bộ điều khiển huấn luyện mô hình (Ridge default)
    -   labeler.py — forward return labels với funding blackout + BTC-neutral
    -   experiment_record.py — Pre-registration, config_hash auto-compute, n_trials governance
    -   reproducibility.py — SHA-256 dataset lock, seed pin, env snapshot
    -   dataset_splitter.py — PiT-safe train/val split, no future leak
    -   purged_kfold.py — PurgedKFold CV (5-fold, 21-day embargo, pre-test purge only)
    -   dsr_calculator.py — Deflated Sharpe Ratio
    -   model_registry.py — Versioned model artifact storage (append-only)
-   **Bất biến kỹ thuật (Invariants):**
    -   `[INV-R1.5]` `Trainer.fit()` bị BLOCK nếu `experiment_record.config_hash` rỗng.
    -   `[INV-REPRO.1]` Dataset SHA-256 phải khớp với DatasetRecord trong M-D0 trước khi gọi `fit()`.
    -   `[INV-REPRO.2]` `Trainer.fit()` bị BLOCK nếu `lock_dataset()` chưa được gọi.
    -   `[INV-REPRO.3]` Seed phải là kiểu số nguyên (int), raise TypeError nếu None hoặc "random".
    -   `[INV-REPRO.4]` `snapshot_env()` phải được gọi trước `fit()`.
    -   `[INV-SPLIT.1]` Không có bar nào trong val tồn tại trong train (set disjoint).
    -   `[INV-SPLIT.2]` Chỉ loại bỏ pre-val bars: bar $t$ bị loại nếu $t < val\_start$ AND $t + h > val\_start$.
    -   `[INV-TRAIN.6]` Cổng LightGBM: `Trainer(model_type="lightgbm")` yêu cầu `ridge_baseline_ic` và OOS IC cải thiện > 15% so với baseline.
    -   `[INV-R1.8]` Nhất quán nhãn (Consistency): Tín hiệu BTC-neutral yêu cầu nhãn forward return cũng bắt buộc khử BTC-neutral.
-   **Kịch bản lỗi & Phục hồi:**
    -   *Lệch dữ liệu / Corrupt Dataset:* So sánh SHA-256 của dataset thực tế và DatasetRecord đã đăng ký. Nếu phát hiện lệch, raise exception và ngắt quá trình huấn luyện để tránh training trên dữ liệu hỏng.
    -   *Data snooping / p-hacking âm thầm:* Ghi nhận chéo `n_trials_total` dựa trên file `research_trials_counter.jsonl`. Block fit() nếu `n_trials_total < 1`. Tính toán DSR hiệu chỉnh theo số lần thử thực tế.
    -   *Look-ahead bias trong labels:* Giới hạn beta BTC chỉ tính trên dữ liệu lịch sử cửa sổ $[t-504, t)$ (không bao gồm bar $t$ hiện tại). Funding blackout kiểm tra giao dịch quyết toán cho cả ngày hiện hành và ngày kế tiếp.

---

#### M-R2: Leakage Audit
-   **Trạng thái:** Spec-only / Chưa build
-   **Tệp tin đặc tả:** M-R2_leakage_audit.md
-   **Tệp mã nguồn đích:**
    -   leakage_audit.py — 14 checks kiểm toán rò rỉ dữ liệu trước T1
-   **Bất biến kỹ thuật (Invariants):**
    -   `[AUDIT-1]` Future Probe Test (Không được override). FAIL nếu: (1) max(|IC_future_k|) > 0.0125, (2) max(|IC_future_k|) > 2 * max(|IC_past_k|), hoặc (3) p_value < 0.01 qua Block Bootstrap (block_size = halflife).
    -   `[AUDIT-2]` Normalization PiT Check (Không được override). Đảm bảo $|f(D_{\le T})[T] - f(D_{full})[T]| \le 10^{-6}$ trên 100 timestamps ngẫu nhiên.
    -   `[AUDIT-3]` Universe PiT Check (Không được override). Khớp tín hiệu qua `asset_id` với lịch sử universe.
    -   `[AUDIT-6]` Delisted Symbols Check (Không được override). Tránh survivorship bias.
    -   `[AUDIT-8 / 8b]` Sector PiT Check (Không được override). Demeaning dựa trên sector assignments và universe tại thời điểm $t$.
    -   `[AUDIT-11]` Feature DAG Lineage Audit (Không được override). Kiểm tra `pit_safe=True` trên toàn bộ DAG ancestors và logic hash khớp FeatureSpec.
    -   `[AUDIT-12]` Vendor Timestamp Audit (Không được override). Phát hiện backfill và clock skew ($event\_time > recv\_time$).
    -   `[AUDIT-13]` Data Revision Audit (Không được override). Đảm bảo content_hash của raw datasets không thay đổi.
    -   `[AUDIT-14]` Cache Contamination Audit (Không được override). Kiểm tra cache_hash, data_version, compute_ts_utc.
-   **Kịch bản lỗi & Phục hồi:**
    -   *Rò rỉ dữ liệu hoặc lỗi cấu trúc (Audit Fail):* Nếu bất kỳ check không overridable nào bị fail, dừng pipeline và ném exception (FAIL LOUD). Researcher phải debug logic đặc trưng/nhãn.
    -   *Overrides quá mức cho phép:* Giới hạn tối đa 2 lần override mỗi năm cho các kiểm toán vận hành (AUDIT-4, AUDIT-5, AUDIT-10). Nếu vượt quá, khóa tự động và cảnh báo Telegram.

---

#### M-R3: T0 Screen (Quick Kill)
-   **Trạng thái:** Spec-only / Chưa build
-   **Tệp tin đặc tả:** M-R3_t0_screen.md
-   **Tệp mã nguồn đích:**
    -   t0_screen.py — Bộ lọc nhanh hypotheses dưới 15 phút
-   **Bất biến kỹ thuật (Invariants):**
    -   Quyết định T0: PASS ($|IC| \ge ic\_threshold$ AND K2 pass AND K3 pass); BORDERLINE ($|IC| \in [0.015, ic\_threshold)$ AND STABLE); KILL ($|IC| < 0.015$ hoặc K2/K3 fail hoặc BORDERLINE + SIGN_FLIP).
    -   `[INV-R3.1]` Cochrane 2001 AR(1) cho effective_n: $T_{eff} = T \times (1 - |\rho_1|) / (1 + |\rho_1|)$.
    -   `[INV-R3.2]` K3: $cost\_bps / (|raw\_ic| \times 1000) \le 0.40$ (Dimensionless ratio).
    -   `[INV-R3.4]` T0 input returns bắt buộc phải là GROSS returns (chưa trừ cost), để tránh trừ chi phí hai lần (false KILL ở K2/K3).
    -   `[INV-R3.5]` Hướng tín hiệu: `raw_ic < 0` -> `signal_direction = -1` -> T1 tự động invert signal.
-   **Kịch bản lỗi & Phục hồi:**
    -   *Lỗi thiếu dữ liệu quan sát:* Nếu $n\_valid\_obs < 20$, auto-KILL với lý do "Insufficient obs".
    -   *Lỗi dấu hiệu IC không ổn định (Sign Flip):* Nếu $ic\_h1 \times ic\_h2 < 0$ và ở trạng thái BORDERLINE, tự động chuyển thành KILL để dọn dẹp review queue.
    -   *Lỗi cost model offline:* Tự động fallback về cấu hình mặc định trong `research.yaml` và đánh dấu `using_hardcoded_cost = True`.

---

#### M-R4: T1 Validate
-   **Trạng thái:** Spec-only / Chưa build
-   **Tệp tin đặc tả:** M-R4_t1_validate.md
-   **Tệp mã nguồn đích:**
    -   t1_validate.py — Trình thẩm định cross-validation nâng cao
    -   regime_split.py — Tách và đánh giá chất lượng tín hiệu theo regime
-   **Bất biến kỹ thuật (Invariants):**
    -   `[INV-R4.1]` Backtest nội bộ tại T1 bắt buộc dùng `execution_price = "open_next"` (chống look-ahead bias khi dùng "close").
    -   `[INV-R4.2]` T1 sở hữu duy nhất quyền đảo ngược tín hiệu. Nếu `t0_result.signal_direction == -1`, tự động nhân `signal_value` với -1. Cấm caller đảo ngược thủ công.
    -   `[T1.1]` PurgedKFold CV (5-fold, 21-day embargo) -> `mean_cv_ic > 0.020`.
    -   `[T1.1b]` `ICIR >= 1.0` (với `ICIR = mean(IC_series) / std(IC_series)` trên các folds).
    -   `[T1.2]` DSR (Deflated Sharpe Ratio) $> 0.50$ (dùng `n_trials_total` từ ExperimentRecord).
    -   `[T1.3]` Regime Split IC: trending, ranging, stressed. Kill nếu bất kỳ chế độ nào có $IC < -0.010$. Enforce `research_yaml_hash` để phát hiện sửa đổi tham số sau-hoc.
    -   `[T1.4]` Capacity Estimate (rough) — alpha phải vượt ngưỡng capacity tối thiểu *(giá trị vốn cụ thể là nội bộ, đã lược bỏ)*.
-   **Kịch bản lỗi & Phục hồi:**
    -   *Lỗi trôi tham số cấu hình (Config drift):* So sánh `current research.yaml hash` vs `experiment_record.research_yaml_hash`. Nếu lệch, FAIL T1.3 và dừng pipeline.
    -   *Lỗi std = 0:* Nếu tất cả folds có cùng IC, std = 0 -> ICIR = 0 -> KILL lập tức (lỗi tính toán).

---

#### M-R5: T2 Full Diligence
-   **Trạng thái:** Spec-only / Chưa build
-   **Tệp tin đặc tả:** M-R5_t2_diligence.md
-   **Tệp mã nguồn đích:**
    -   t2_diligence.py — Trình thẩm định cuối cùng của alpha
    -   walk_forward.py — Phân tích Walk-Forward (5 windows)
    -   stress_tester.py — Chạy stress test Luna/FTX + 20 random periods
    -   capacity_estimator.py — Ước lượng capacity dựa trên L2 depth thực
    -   reveal_holdout.py — Kịch bản mở holdout data (IRREVERSIBLE)
-   **Bất biến kỹ thuật (Invariants):**
    -   `[T2.1]` Sharpe $\ge 0.80$, Max Drawdown $< 0.25$, Information Ratio vs Benchmark $\ge 0.20$.
    -   `[T2.2]` Walk-forward: 5 windows (expanding train, fixed test), $train + test \le in\_sample$ (không chạm holdout). $wf\_ic\_std < 0.50 \times wf\_ic\_mean$.
    -   `[T2.3]` Stress test: Luna crash & FTX collapse (phải dương ít nhất 1 trong 2). Random stress pass rate $\ge 70\%$.
    -   `[T2.5]` Drawdown recovery time $\le 120$ ngày.
    -   `[T2.8]` Edge Half-Life by Regime: $halflife\_regime[stressed] \ge 2$ bars.
    -   `[T2.9]` Cascade Exit: cascade impact $\le 0.30$.
    -   `[T2.10]` Regime Sharpe positive count $\ge 3$.
    -   `[T2.11]` Feature Drift KS check: Kill nếu $p\_value < 0.01$ AND $ks\_stat > 0.20$.
    -   Dual Scenario fill rate (Scenario A: optimistic, Scenario B: conservative). Cả hai Scenario Sharpe đều phải đạt chuẩn: $Sharpe[A] \ge 0.80$ và $Sharpe[B] \ge 0.75$. Kill nếu $Sharpe[A] \ge 0.80$ nhưng $Sharpe[B] < 0.50$.
-   **Kịch bản lỗi & Phục hồi:**
    -   *Khuyết dữ liệu M-R11 thực tế:* Tự động chuyển sang `cascade_mode = "PROXY"`, tính toán proxy dựa trên range > 3% và volume spike > 2x. Gắn cờ `revalidate_t29 = True` trong registry để cập nhật sau khi có dữ liệu thật.
    -   *Lỗi reveal holdout sai thời điểm:* Khóa cứng quá trình reveal bằng script `reveal_holdout.py` yêu cầu confirm thủ công, kiểm tra hash, kiểm tra git tag trên remote. Nếu fail, bắt buộc thu thập dữ liệu mới.

---

#### M-R6: Vectorized Backtest Engine
-   **Trạng thái:** Spec-only / Chưa build
-   **Tệp tin đặc tả:** M-R6_vectorized_backtest.md
-   **Tệp mã nguồn đích:**
    -   vectorized_backtest.py — Engine kiểm thử Polars-native
-   **Bất biến kỹ thuật (Invariants):**
    -   `[INV-D3.7]` Mọi phép join downstream bắt buộc phải join qua `asset_id` (PiT-safe), cấm sử dụng ticker symbol string để tránh collision.
    -   `[INV-R6.1]` Mặc định `execution_price = "open_next"`. Sharpe của `open_next` bắt buộc phải nhỏ hơn `close`. Nếu bằng nhau -> bug trong PnL timeline.
    -   `[INV-R6.2]` PnL Timeline bắt buộc:
        1. Nhận bar t close data.
        2. Tính features[0..t] (expanding, PiT-safe).
        3. Tính signal[t].
        4. Tính target position[t].
        5. Ghi nhận delta position.
        6. Bar t+1 (open): fill_price = open[t+1].
        7. Ghi nhận position bắt đầu active.
        8. Tính transaction cost.
        9. PnL: `bar_pnl[t] = position[t] * (open[t+2] - open[t+1]) / open[t+1]`.
    -   `[INV-R6.4]` Maker fill blended cost (Scenario A vs Scenario B): `blended_cost = maker_full_cost * fill_rate + taker_full_cost * (1 - fill_rate)`.
-   **Kịch bản lỗi & Phục hồi:**
    -   *Lỗi bias do execution price:* Nếu phát hiện dùng close price, ném exception `ResearchYamlModifiedAfterExperimentError` và gắn cờ cảnh báo trong `ExperimentRecord`.
    -   *Lỗi over-optimistic Sharpe:* Nếu Sharpe[A] >= 0.8 nhưng Sharpe[B] < 0.5 (lệch quá lớn do execution quality), tự động KILL.

---

#### M-R7: Cost Model
-   **Trạng thái:** Spec-only / Chưa build
-   **Tệp tin đặc tả:** M-R7_cost_model.md
-   **Tệp mã nguồn đích:**
    -   slippage_matrix.py — Ma trận slippage theo size × venue × regime
    -   venue_profiler.py — Profile thanh khoản per exchange
    -   regime_cost_adjuster.py — Bộ điều chỉnh chi phí theo market regime
-   **Bất biến kỹ thuật (Invariants):**
    -   `[INV-EXC.1]` Bắt buộc load fee từ `exchange_metadata.yaml` (cấm hardcode).
    -   `regime_multiplier(state_vector)` trả về hệ số điều chỉnh cost (từ 1.0 đến 5.0 tùy trạng thái thị trường).
    -   Blended cost: adverse selection được điều chỉnh theo regime, taker adverse selection bằng 2x maker.
    -   Hold days: $mean\_holding\_bars \times bar\_duration\_hours / 24$.
-   **Kịch bản lỗi & Phục hồi:**
    -   *Khuyết dữ liệu state_vector:* Tự động fallback về hệ số 1.0 (Normal regime) để không làm sập pipeline (graceful degradation), nhưng log cảnh báo.

---

#### M-R8: Factor Neutralization
-   **Trạng thái:** Spec-only / Chưa build
-   **Tệp tin đặc tả:** M-R8_factor_neutralization.md
-   **Tệp mã nguồn đích:**
    -   factor_neutralizer.py — Bộ khử beta của hệ thống
-   **Bất biến kỹ thuật (Invariants):**
    -   `[INV-R8.1]` Beta computation chỉ dùng dữ liệu $\le t$ (rolling OLS 504 bars cho BTC/ETH, expanding window OLS cho funding). Cấm tuyệt đối OLS trên toàn bộ dữ liệu in-sample.
    -   `[INV-R8.2]` Khử BTC+ETH: dùng joint multivariate OLS (cấm sequential OLS để tránh re-introducing beta).
    -   `[INV-R8.3]` $R^2 > 0.60$ -> warn + document (không auto-kill để hỗ trợ peg assets như WBTC/renBTC).
    -   `[INV-R8.4]` Sector demeaning: mean phải tính PER TIMESTAMP (cross-sectional), cấm dùng global mean.
    -   `[INV-R8.5]` Sector universe join bắt buộc dùng `asset_id` từ M-D3 asset registry.
-   **Kịch bản lỗi & Phục hồi:**
    -   *Lỗi ma trận suy biến (det < threshold) khi joint OLS:* Tự động fallback về chỉ neutralize BTC, đặt `beta_eth = 0` và log warning (graceful degradation).
    -   *Khuyết thông tin sector:* Fallback gán sector "unknown" và không thực hiện demeaning cho các asset đó.

---

#### M-R9: Alpha Registry + Cemetery
-   **Trạng thái:** Spec-only / Chưa build
-   **Tệp tin đặc tả:** M-R9_alpha_registry.md
-   **Tệp mã nguồn đích:**
    -   alpha_registry.py — Quản lý vòng đời và lưu trữ AlphaRecord
    -   alpha_cemetery.py — Lưu trữ AlphaAutopsy và đối sánh độ tương tự
    -   lifecycle.py — Thẩm định chuyển stage (hard gates)
    -   alpha_retirement.py — Bộ điều phối nghỉ hưu alpha khi decay
-   **Bất biến kỹ thuật (Invariants):**
    -   `[INV-R9.1]` Immutability: Các trường cơ bản của AlphaRecord (`alpha_id`, `hypothesis`, `registered_at`, `config_hash_at_registration`, `n_trials_at_registration`) không thể thay đổi sau khi ghi nhận.
    -   `[INV-R9.2]` Chuyển giai đoạn PAPER -> SHADOW yêu cầu: (1) thời gian chạy $\ge 60$ ngày calendar market days, (2) đã log event PAPER_INTERIM tại ngày 30, (3) $|paper\_ic\_live| \ge 0.025$, (4) $paper\_ic\_live \ge 0.70 \times backtest\_ic\_sim$, (5) $paper\_sharpe \ge 0.60$.
    -   `[INV-R9.11]` `n_trials_at_registration` phải phản ánh đúng tổng thực nghiệm đã chạy tại thời điểm đó của toàn bộ nghiên cứu.
-   **Kịch bản lỗi & Phục hồi:**
    -   *Tranh chấp ghi file registry:* Sử dụng cơ chế file lock hệ thống khi ghi `alpha_registry.jsonl` và `stage_change_events.jsonl`.
    -   *Khuyết dữ liệu M-R11 khi kiểm tra funding extension:* Tự động fallback coi như không có funding dispersion cao (`has_high_funding = False`), kéo dài paper thêm 30 ngày để đảm bảo an toàn.

---

#### M-R10: Anomaly Miner
-   **Trạng thái:** Spec-only / Chưa build
-   **Tệp tin đặc tả:** M-R10_anomaly_miner.md
-   **Tệp mã nguồn đích:**
    -   observation_log.py — Ghi nhận các bất thường của thị trường
    -   anomaly_miner.py — Quét nightly các bất thường
    -   feature_cache.py — Lưu trữ baselines cho anomaly miner (rolling stats: mean/std/MAD/median)
-   **Bất biến kỹ thuật (Invariants):**
    -   `[SCAN-1]` robust Z-score dùng MAD_30d: $Z\_robust = (x - median\_30d) / (1.4826 \times MAD\_30d)$ để chống nhiễu từ outlier cũ.
    -   `[SCAN-2]` Phân loại hướng đi dựa trên ΔOI và Δprice: long_build, short_build, short_covering, long_liquidation, sideways_accumulation. Ngưỡng Δprice phân biệt sideways là 0.5%.
    -   `[SCAN-3]` Basis anomaly: Binance vs Bybit, cả $Z > 2\sigma$ AND $|diff| > 2$ bps.
    -   `[SCAN-4]` Cross-asset correlation: EWMA correlation BTC-ETH $\lambda=0.94$, $min\_obs=72$. Chỉ dùng dữ liệu có `data_quality <= 1`.
    -   `[SCAN-8]` Correlation vs M-R11 OOD: Tự động corroborative check với M-R11 OOD score.
-   **Kịch bản lỗi & Phục hồi:**
    -   *Stale baselines cache:* Lưu baselines dưới file `feature_cache.py` with metadata versioning `{date, window, method}`. Nếu cache bị hỏng hoặc thiếu, tự động recompute online từ raw data (tốn thời gian hơn nhưng an toàn).

---

#### M-R11: Market State Engine
-   **Trạng thái:** Spec-only / Chưa build
-   **Tệp tin đặc tả:** M-R11_market_state_engine.md
-   **Tệp mã nguồn đích:**
    -   market_state_engine.py — Trình tính toán state vector 8 chiều per bar
    -   state_store.py — Lưu trữ Parquet và query interface
-   **Bất biến kỹ thuật (Invariants):**
    -   8 Dimensions: Spread Regime, Liquidity Regime, Funding Dispersion Regime, Liquidation Intensity, Exchange Fragmentation, BTC-Alt Correlation, Stablecoin Depeg Index, Out-Of-Distribution (OOD) Score.
    -   `[INV-R11.2]` Mọi thresholds định nghĩa các regime phải load từ `research.yaml` (key `regime:`).
    -   `[STATE-3]` Forward-fill funding rate chỉ dùng trong M-R11 context. Nếu last known > 8h+1bar cũ -> set `funding_dispersion_stale=True`, regime = "unknown".
    -   `[STATE-5]` Chỉ compute basis_dispersion khi tất cả exchanges có funding_rate không null trong cùng settlement window ±15 phút.
-   **Kịch bản lỗi & Phục hồi:**
    -   *Khuyết dữ liệu sàn / lệch timing:* Nếu một sàn mất dữ liệu, hoặc lệch timing, các trường tương ứng gán `NaN` và `regime = "unknown"`, fallback về trạng thái bình thường (không raise error làm sập real-time flow).

---


**Key design decisions (từ spec audit):**
- T0→T1→T2: 3-tier gate, không skip. T0 kills 85–95%, T1 kills 60–80%.
- ExperimentRecord: 23 fields, config_hash auto-computed, n_trials_total = unique config_hash per data_window
- AlphaRecord: 37 fields, frozen=True; SCREENING→PAPER honor 3-state (T0 PASS/override, T2 PASS/WARN); PAPER→SHADOW cần **6 điều kiện** (INV-R9.2)
- **Shadow gate baseline = `backtest_ic_sim`** (T2 chạy QUA chính M-L1 fill simulator — execution-adjusted), KHÔNG phải t1_cv_ic_mean frictionless: paper_ic_live tính trên filled subset (adverse-selected cơ học) → so với frictionless là apples-to-oranges. Đếm ngày gate = **calendar market days** (M-D2 SSOT, không phải signal-log). [Resolved M-R9↔M-L1 2026-06-13]
- M-R3 T0: 3-state với hard floor |IC| < 0.015 (0.025 = ranh giới PASS/BORDERLINE standard mode); half-sample stability SIGN_FLIP; signal_direction ownership (T1 là nơi DUY NHẤT invert). Cost params round-trip maker 16 / taker 30 bps (khớp bảng M-R7)
- M-R5 T2: WARN tier khi backtest_sharpe ∈ [0.80, 1.20) → max_paper_position 50%; stage sau T2 PASS vẫn SCREENING đến human sign-off
- LIVE→HIBERNATING: `btc_alt_corr > 0.8 AND alt_xs_dispersion thấp` — liquidity vacuum KHÔNG trigger HIBERNATE (chỉ scale down 50%)
- M-R6 backtest: execution_price="open_next", taker_adv_sel = 2× maker (8 bps vs 4 bps)
- T1.6 BTC+ETH: joint OLS (không sequential) — implement trực tiếp trong T1.6, không phụ thuộc M-R8 Phase 1

Bất kỳ quỹ giao dịch định lượng nào cũng đối mặt với **Simulation-to-Reality Gap**. Kairos giải quyết bằng kiến trúc đồng nhất — cùng feature_registry.py cho cả batch research và live inference.

- **Động cơ phát lại (`nghien_cuu/dong_co_phat_lai/`)** — M-R6 vectorized backtest (Polars-native, single file)
- **Nhà máy Alpha (`nghien_cuu/nha_may_alpha/`)** — T0/T1/T2 + registry + cost model + factor neutral
- **Kiểm thử quá khứ (`nghien_cuu/kiem_thu_qua_khu/`)** — vectorized (Polars) + event-driven (dành cho dự án HFT tương lai — tách riêng khỏi Kairos; M-H1 là STRATEGIC GATE doc với 5 gates chưa trigger — KHÔNG build, chỉ "Nghĩa vụ lưu data" hiệu lực ngay: own-order lifecycle logs + raw L2/trade stream)
- **Đánh giá (`nghien_cuu/danh_gia/`)** — leakage_audit, purged_kfold, dsr_calculator, walk_forward, stress_tester

---

### 3.5. Trí Tuệ Nhân Tạo & MLOps (Machine Learning) `[WIP]`

Bộ não của hệ thống. Trong khi các quy tắc rủi ro và thực thi lệnh được viết bằng code tĩnh, logic dự đoán giá được giao hoàn toàn cho các mô hình Machine Learning.

- **Tối ưu suy luận (ONNX/TensorRT)**: Thư viện PyTorch rất tốt để huấn luyện (`hoc_may/huan_luyen/`), nhưng nó chứa quá nhiều overhead, không đủ nhanh để đưa ra quyết định trong vài chục microsecond. Do đó, sau khi train xong, model được xuất ra định dạng ONNX và chạy suy luận (`hoc_may/suy_luan/`) thông qua ONNX Runtime (viết bằng C++) hoặc TensorRT (tối ưu phần cứng GPU/NPU), giúp giảm độ trễ xuống mức giới hạn vật lý.
- **Tổ hợp Alpha (`hoc_may/to_hop_alpha/`)**: Một mô hình duy nhất hiếm khi chiến thắng thị trường trong dài hạn. Kairos sử dụng kỹ thuật Ensemble — kết hợp hàng chục/trăm tín hiệu nhỏ lẻ (ví dụ: 1 mô hình soi funding rate, 1 mô hình soi orderbook, 1 mô hình soi on-chain) thành một quyết định giao dịch thống nhất.
- **Giám sát mô hình (MLOps)**: Thị trường Crypto có tính thay đổi chế độ (regime shift) rất cao. Một mô hình đang lãi đậm hôm nay có thể lỗ sấp mặt vào ngày mai vì "khẩu vị" của thị trường đã đổi. Do đó, hệ thống MLOps (`giam_sat_mo_hinh/`) phải hoạt động liên tục 24/7:
  - `sai_lech_dac_trung/` (Feature Drift): Phát hiện ngay lập tức khi phân phối của dữ liệu đầu vào khác biệt so với lúc train.
  - `sai_lech_du_doan/` (Prediction Drift): Theo dõi độ chính xác. Nếu model bắt đầu đoán sai liên tục, hệ thống sẽ tự động kích hoạt Circuit Breaker cắt quyền giao dịch của model đó trước khi nó gây lỗ nặng.

---

### 3.6. Thực Thi Chiến Dịch (Execution Core)

**Trách nhiệm:** Nhận `SignalEvent` từ ZMQ bus, qua 7 pre-trade gates, đẩy vào SPSC ring buffer, worker sizing lệnh và submit qua EMS. File chính `vong_lap_su_kien.py` — 1251 dòng.

**Hợp đồng hiệu năng:** signal-to-ring-write < 10µs (Thread 1 hot-path, GC disabled). ring-read-to-HTTP-submit latency không bị bounded (network dependent).


#### ExitCoordinator — Quản Lý Thoát Vị Thế

`ExitCoordinator` (`thoat_vi_the/exit_coordinator.py`, 475 dòng) là state machine per-symbol vận hành trên Thread 2 event loop, chịu trách nhiệm hoàn toàn cho việc đóng vị thế. Khi Thread 3 nhận MarkPrice mới, nó gọi `call_soon_threadsafe(ec.on_price_tick, sym, price, ts)` để dispatch vào Thread 2 — không bao giờ block hot-path.

Mỗi symbol có trạng thái `FLAT → OPEN → CLOSING → PARTIAL → CLOSED → QUARANTINED/ERROR`. Khi một lệnh mở vị thế fill thành công (trong `_fire()`), `on_position_opened()` được gọi để chuyển sang `OPEN`. Mỗi price tick, coordinator đánh giá tất cả exit strategies (FixedPercent, ATR-Based, TrailingStop, Breakeven, PartialExit, TimeBased) và đặt lệnh `MARKET reduce_only=True` trực tiếp qua adapter — **bypassing EMS hoàn toàn** vì lệnh đóng không cần pre-trade risk gate.

`EmergencyFlattener` (`watchdog/emergency_flattener.py`, 438 dòng) là fallback cuối cùng: được Watchdog gọi khi kích hoạt kill switch, đóng tất cả vị thế đang mở trên mọi sàn và ghi `FLATTEN_LOCK` để chặn vĩnh viễn mọi lệnh mới cho đến khi con người can thiệp.

#### Kiến trúc 5-Thread (`ExecutionGateway`)

Python GIL được giải phóng tại I/O boundaries (ZMQ recv, HTTP, file write). Mỗi thread chiếm một loại I/O riêng biệt để tối đa hóa parallelism trong giới hạn GIL:

```
Thread 1  HOT PATH     ZMQ SUB(5557) → versioned ring buffer + risk commit
Thread 2  EXEC WORKER  asyncio.run() → spin-wait ring → sizing → EMS
Thread 3  PRICE ORACLE ZMQ SUB(5555, topic=MARK_PRICE) → _price_cache dict
Thread 4  BOUND LOGGER queue.Queue(5000) drain → Python logging
Thread 5  WATCHDOG HB  ZMQ PUB(5559) + file mtime touch → dual-channel heartbeat
```

- **Thread 1 (Hot-Path)**: Nhận tín hiệu từ ZMQ, kiểm tra xem tín hiệu có còn "tươi" không, qua 7 cửa chặn an toàn, rồi đẩy vào Ring Buffer. Thread này tuyệt đối KHÔNG được tạo bất kỳ Python object nào — vì chỉ cần 1 lần GC pause là tín hiệu bị cũ.
- **Thread 2 (Worker)**: Chờ data từ Ring Buffer, tính kích thước lệnh, rồi gửi lên sàn qua HTTP API. Chạy asyncio event loop để xử lý tối đa 50 HTTP request song song.
- **Thread 3 (Price Oracle)**: Liên tục cập nhật bảng giá MarkPrice cho tất cả symbol. Thread 1 tra cứu bảng giá này để tính giá trị USD của lệnh.
- **Thread 4 (Logger)**: Thread 1 và Thread 2 không bao giờ gọi `logging` trực tiếp (vì logging acquire lock). Thay vào đó, chúng đẩy message vào `queue.Queue(5000)`, Thread 4 lấy ra và ghi log.
- **Thread 5 (Watchdog Heartbeat)**: Gửi tín hiệu "tôi còn sống" định kỳ qua 2 kênh. Nếu Watchdog không nhận được → giết process.

#### Thread 1: Hot-Path — Zero-Allocation Receive

**Invariants:** `gc.disable()` trước khi vào loop. Tất cả object (recv buffer, ctypes overlay, ring slots) pre-allocated tại `__init__`. Zero allocation trong hot loop.

**Pre-allocated ZMQ receive buffer** — 64-byte `bytearray` với ctypes overlay, tái sử dụng mỗi signal:

```python
# thuc_thi_lenh/vong_lap_su_kien.py — Thread 1
gc.disable()   # ← CRITICAL: Tắt GC trên hot-path

# Pre-allocate recv buffer + ctypes overlay — zero-alloc recv
self._recv_buf    = bytearray(64)
self._recv_ctypes = (ctypes.c_char * 64).from_buffer(self._recv_buf)
self._recv_sig    = _SignalEventRaw.from_buffer(self._recv_buf)

while not stop_event.is_set():
    # ── 1. Zero-alloc recv ──
    msg = sub.recv(copy=False)           # Frame buffer protocol
    ctypes.memmove(recv_ctypes, msg.bytes, 64)  # Copy trực tiếp qua pointer

    # ── 2. Staleness check (<5ms) ──
    if time.perf_counter_ns() - recv_sig.signal_ts > 5_000_000:
        self._drop("stale"); continue

    # ── 3. Circuit breaker ──
    if cb_event.is_set():
        if time.perf_counter() > self._cb_reset_at:
            cb_event.clear(); self._cb_errors = 0
        else:
            self._drop("cb_open"); continue

    # ── 5. Backpressure: ring buffer lag ──
    lag = write_idx - self._read_idx.value
    if lag >= ring_size // 2:
        self._drop("ring_lag"); continue

    # ── 5b. Flow backpressure gate ──
    if flow_channel.pressure > 0.70:
        self._drop("flow_bp"); continue
```

Mỗi signal được copy 64 bytes vào recv buffer bằng `ctypes.memmove` (zero allocation), sau đó qua 7 gates theo thứ tự. Gate đầu tiên fail → drop ngay, không evaluate tiếp.

> Đây là trade-off `correctness vs latency` cần đọc thật kỹ. Cơ chế drop chỉ phù hợp khi việc bỏ event không làm sai quyết định giao dịch hoặc hệ thống có sequencing/gap detection để phát hiện trạng thái đã degraded.

**7 cửa chặn (Gate)** — Triết lý "Fail Fast": vứt bỏ càng sớm càng tốt, chỉ xử lý tín hiệu thực sự hợp lệ. Mỗi tín hiệu phải vượt qua TẤT CẢ 7 cửa mới được chuyển thành lệnh:

| Gate | Mục đích | Giải thích | Hành vi khi thất bại |
|------|----------|-----------|---------------------|
| Staleness | Signal cũ hơn 5ms | Thị trường crypto biến động cực nhanh. Tín hiệu 5ms tuổi đã mất giá trị vì giá có thể đã thay đổi đáng kể. | Drop + log |
| Circuit Breaker | ≥5 lỗi adapter liên tiếp | Nếu sàn giao dịch đang gặp sự cố (API timeout, rate limit), tiếp tục gửi lệnh chỉ làm tình hình tệ hơn. Tạm dừng 5 giây để sàn hồi phục. | Block 5 giây |
| Symbol Lookup | Symbol không tồn tại | Tín hiệu cho cặp tiền chưa được đăng ký → không thể giao dịch. | Drop |
| Price Check | Chưa có MarkPrice | Không thể tính kích thước lệnh (USDT → quantity) nếu chưa biết giá hiện tại. | Drop |
| Ring Lag | Worker chậm ≥50% ring | Thread 2 đang xử lý chậm (sàn lag?). Nếu tiếp tục nhồi data vào ring buffer sẽ tràn → mất data. | Drop |
| Flow Backpressure | Pressure > 70% | Toàn hệ thống đang quá tải (queues gần đầy). Giảm lượng input để tránh sập. | Drop |
| Risk Gate | Pre-trade risk check | Kiểm tra rủi ro cuối cùng: vượt giới hạn lỗ? Quá nhiều lệnh mở? Lệnh trùng? | Rollback + drop |

#### Thread 1 → Thread 2: SPSC Versioned Ring Buffer

Sau khi tín hiệu vượt qua 7 cửa chặn, nó cần được "chuyển giao" từ Thread 1 sang Thread 2 để thực thi. Vấn đề: làm sao 2 thread giao tiếp mà không dùng Lock (vì Lock tốn ~200ns mỗi lần acquire/release)?

Giải pháp là **Versioned Ring Buffer** — một mảng vòng tròn 256 slots. Thread 1 ghi data vào slot, rồi tăng số version lên 1. Thread 2 liên tục kiểm tra version — khi thấy version tăng, biết có data mới. Không cần Lock vì chỉ có 1 writer (Thread 1) và 1 reader (Thread 2):

> Assumption `SPSC` này phải được giữ thật chặt. Nếu sau này có thêm producer khác (retry path, reconciler injector, daemon phụ) cùng ghi vào ring, cấu trúc hiện tại không còn an toàn và cần runtime assert hoặc chuyển sang `MPSC/MPMC`.

```python
# Slot definition — SPSC (Single Producer Single Consumer)
@dataclass
class _RingSlot:
    data:    _SignalEventRaw
    version: ctypes.c_int64    # Writer += 1 sau khi ghi xong

# Thread 1: Ghi vào ring — 0 allocation
ring_pos = write_idx & ring_mask         # Bit-mask thay modulo (nhanh hơn)
slot     = ring[ring_pos]
ctypes.memmove(ctypes.addressof(slot.data), recv_ctypes, 64)  # Copy 64B
slot.version.value += 1   # "Xuất bản" cho Thread 2: data đã sẵn sàng
```

#### Thread 2: Exec Worker — Hybrid Spin-Wait

Thread 2 là nơi lệnh thực sự được gửi lên sàn. Nó chờ data từ Ring Buffer bằng kỹ thuật **Hybrid Spin-Wait**: quay vòng kiểm tra 100 lần (spin) → nếu chưa có data → nhường quyền cho event loop (`await asyncio.sleep(0)`) → lặp lại. Kỹ thuật này cân bằng giữa độ trễ thấp (spin nhanh khi có data) và tiết kiệm CPU (nhường khi không có gì).

> Với mô hình `thread + asyncio`, thứ tự event đi vào ring không tự động đồng nghĩa với thứ tự request hoàn tất hoặc side effect external xuất hiện. Nếu downstream logic phụ thuộc ordering chặt, production flow cần thêm `sequence ownership`, execution queue ordering rõ ràng, hoặc idempotent reconciliation thay vì giả định `arrival order ~= execution order`.

Sau khi nhận được tín hiệu, Thread 2 tính kích thước lệnh bằng `Decimal` (không dùng `float` vì sai số thập phân có thể khiến lệnh bị sàn từ chối — ví dụ BTC lot_size = 0.001, nếu float tính ra 0.0019999999 sẽ bị reject):

```python
# thuc_thi_lenh/vong_lap_su_kien.py — Thread 2
async def _worker_loop(self):
    exec_sem = asyncio.Semaphore(50)   # max 50 HTTP calls in-flight

    while not stop_event.is_set():
        slot = ring[read_idx & ring_mask]

        # ── Hybrid spin-wait: spin 100 lần → yield → lặp lại ──
        spin = 0
        while slot.version.value != expected:
            spin += 1
            if spin >= 100:
                spin = 0
                await asyncio.sleep(0)   # yield event loop
            if stop_event.is_set(): return

        # ── Sizing: dùng Decimal để tránh float epsilon ──
        qty_step_d = Decimal(str(cfg.qty_step))
        steps      = int((order_usdt / price) / cfg.qty_step)
        qty        = float(qty_step_d * steps)
```

#### Thread 5: Watchdog Heartbeat — Dual-Channel

Thread 5 giải quyết câu hỏi: "Làm sao biết bot còn sống?". Mỗi vài giây, nó gửi tín hiệu "heartbeat" qua **2 kênh hoàn toàn độc lập**: ZMQ publish và file system touch. Tại sao 2 kênh? Vì nếu chỉ dùng ZMQ mà ZMQ bị lỗi → Watchdog tưởng bot chết → giết nhầm. Ngược lại nếu chỉ dùng file mà filesystem bị treo → cũng giết nhầm. Cần cả 2 kênh đều mất tín hiệu mới kết luận bot thực sự chết:

```python
# thuc_thi_lenh/vong_lap_su_kien.py — Thread 5
def _heartbeat_loop(self):
    while not stop_event.is_set():
        # Kênh 1: ZMQ PUB — NOBLOCK để không bị treo nếu Watchdog lag
        hb_pub.send_multipart([b"HB", b""], flags=zmq.NOBLOCK)

        # Kênh 2: Chạm file — chỉ cập nhật timestamp, không ghi nội dung
        os.utime(alive_path, None)

        stop_event.wait(interval_s)   # Sleep nhưng có thể bị đánh thức
```

#### Durable WAL (`durable_wal.py`) — 354 dòng

**Trách nhiệm:** Ghi nhật ký mutation intent lên đĩa trước khi HTTP request được gửi. Cung cấp `best-known state` sau crash/restart trong phạm vi mutations đã được WAL bao phủ.

**Nguyên lý Write-Ahead:** `ORDER_SENT` entry được append vào WAL trước khi HTTP call được initiated. Sau crash, replay WAL phân biệt `in-flight` (sent, không có fill response) vs `confirmed` vs `unknown`. Ghost-position risk giảm xuống còn trong cửa sổ giữa WAL write và exchange confirmation.

File WAL có kích thước cố định ~4MB, được ánh xạ vào RAM qua `mmap` — ghi vào WAL nhanh như ghi vào RAM, nhưng dữ liệu vẫn an toàn trên đĩa. Mỗi entry WAL có cấu trúc cố định 64 bytes (vừa khít 1 cache line CPU, tối ưu tốc độ đọc/ghi):

```python
class _WALEntry(ctypes.LittleEndianStructure):
    _pack_ = 1
    _fields_ = [
        ("seq_id",       ctypes.c_uint64),    # Số thứ tự tăng dần, không lặp
        ("timestamp_ns", ctypes.c_int64),      # Thời điểm ghi (nanosecond)
        ("entry_type",   ctypes.c_uint32),     # Loại sự kiện (enum bên dưới)
        ("flags",        ctypes.c_uint32),      # Cờ phụ trợ
        ("payload",      ctypes.c_char * 32),  # Order ID (tối đa 32 ký tự)
        ("crc32",        ctypes.c_uint32),     # Mã kiểm tra — phát hiện hỏng
        ("_align",       ctypes.c_uint32),      # Đệm để đạt chính xác 64B
    ]   # 8+8+4+4+32+4+4 = 64 bytes
```

Mỗi loại sự kiện được đánh dấu rõ ràng. Lưu ý `ORDER_SENT` được ghi **TRƯỚC** khi gửi lệnh — đây chính là "Write-Ahead" (ghi trước):

```python
class WALEntryType(IntEnum):
    ORDER_SENT      = 1   # Ghi TRƯỚC khi gửi → biết lệnh đang "in-flight"
    ORDER_FILLED    = 2   # Ghi SAU khi sàn xác nhận khớp
    ORDER_CANCELLED = 3   # Lệnh đã hủy thành công
    ORDER_REJECTED  = 4   # Sàn từ chối lệnh
    ORDER_UNKNOWN   = 5   # Hủy lệnh timeout → trạng thái không xác định
    POSITION_SNAP   = 6   # Ảnh chụp vị thế định kỳ (backup)
    PNL_CHECKPOINT  = 7   # Điểm kiểm tra lãi/lỗ
    RISK_OVERRIDE   = 8   # Can thiệp thủ công vào trạng thái rủi ro
```

**Cơ chế bảo vệ tính toàn vẹn**: Mỗi entry có trường `crc32` — mã kiểm tra tính toàn vẹn. Khi đọc lại, hệ thống tính lại CRC và so sánh. Nếu khác nhau = dữ liệu bị hỏng (do mất điện giữa chừng ghi). Ngoài ra, `fsync()` được gọi mỗi 64 entries, nhưng các sự kiện quan trọng (`ORDER_SENT`, `RISK_OVERRIDE`) luôn `fsync()` ngay lập tức — đảm bảo dữ liệu đã xuống đĩa vật lý.

**Thuật toán Recovery** khi khởi động lại sau sự cố:
1. **Verify header**: Kiểm tra magic bytes (`b"KAIROS_W"`) — đảm bảo đúng file WAL
2. **Scan tuần tự**: Đọc từng entry, kiểm tra CRC. Entry đầu tiên có CRC sai = ranh giới hỏng → cắt bỏ phần đuôi corrupt
3. **Đặt con trỏ**: `seq_next = last_valid_entry.seq_id + 1` — bắt đầu ghi từ sau entry hợp lệ cuối cùng
4. **Rebuild state**: Replay các entry hợp lệ để khôi phục lại trạng thái in-memory (vị thế, lệnh đang mở, PnL)

<img width="1920" height="1080" alt="image" src="https://github.com/user-attachments/assets/850d2a61-790b-412a-831d-9751e3c78451" />

> Muốn gọi WAL là `truth source` đúng nghĩa, cần thêm 3 điều kiện: mọi mutation state phải đi qua WAL, sequencing phải nhất quán, và replay phải cho ra cùng logical state sau khi đối soát với exchange.

#### Order Management System (`quan_ly_lenh/`) — 1,114 dòng

Hệ thống Quản lý Lệnh (OMS) là sổ cái trung tâm: mọi lệnh giao dịch phải được đăng ký, theo dõi, và đóng sổ tại đây trước khi rời khỏi hệ thống. OMS gồm 3 thành phần:

- **`order_book.py` (763 dòng)**: Sổ lệnh in-memory với **Per-Symbol Locking**. Mỗi symbol (BTC, ETH, ...) có lock riêng — lệnh BTC không block lệnh ETH. Tài liệu này xem WAL là persistence contract của order state; muốn crash-recovery đáng tin cho production thì mọi transition quan trọng phải thực sự không có đường bypass.
- **`reconciler.py` (400 dòng)**: Chạy định kỳ 60s, đối chiếu trạng thái OMS local với sàn. Phát hiện "missed fills" (lệnh đã khớp trên sàn nhưng OMS chưa biết do mất kết nối) và inject ngược vào pipeline.
- **`oms_serializer.py` (135 dòng)**: Binary codec cho WAL payload — pack/unpack `OrderEntry` vào đúng 32 bytes bằng `ctypes.LittleEndianStructure`.

> Nếu dùng Kairos làm reference cho OMS production, state machine tối thiểu nên được enforce rõ: `NEW -> ACK -> PARTIAL -> FILLED / CANCELED / REJECTED`. Bất kỳ transition nhảy cóc hoặc đi ngược phải bị xem là mismatch để reconcile hoặc halt.

**Durability Contract**: Mọi lệnh được ghi WAL theo protocol 2-phase:
1. `ORDER_SENT`: Payload = `client_order_id[:32]` (identity record), `flags = 0`.
2. `ORDER_FILLED / CANCELLED / REJECTED`: Payload = `_OrderPayload` 32 bytes, `flags = wal_seq_id` của entry ORDER_SENT tương ứng — liên kết update về gốc.

**Backpressure Gates**: OrderBook từ chối lệnh mới khi hệ thống quá tải, kiểm tra **trước khi acquire lock** (zero contention):

```python
# thuc_thi_lenh/quan_ly_lenh/order_book.py
def _gate_reject_reason(self) -> Optional[str]:
    if self._time_validator.capital_multiplier() == 0.0:
        return "capital_zero"          # TimeValidator đã cắt vốn
    if self._cb_registry.get_state("place") == CBState.OPEN:
        return "circuit_open"          # Circuit breaker đang mở
    util = self._wal.utilization()
    if util >= 0.95:                   # WAL đạt 95% capacity
        return f"wal_full:{util:.0%}"  # Bảo lưu headroom cho updates
    return None
```

**Object Pool** (`OrderPool`): Pre-allocate 2,048 `OrderEntry` instances khi khởi tạo. Hot-path `register()` lấy từ pool thay vì `__init__` mới — giảm ~3× RAM per instance (nhờ `slots=True`) và loại bỏ hoàn toàn GC pressure. Pool cạn → fallback sang fresh allocation (không panic, không block).

**Lock Ordering** (bất biến chống deadlock): `_sym_locks[symbol]` **TRƯỚC** → `_wal_lock` **SAU**. Không bao giờ giữ `_wal_lock` mà không giữ symbol lock trước. Hold time dưới lock: chỉ dict ops + ctypes encode (<1µs).

**WAL Replay** khi startup (single-threaded, trước concurrent access):
1. **Pass 1** (`ORDER_SENT`): Xây dựng `seq_to_coid` map và stub `OrderEntry`.
2. **Pass 2** (update entries): Dùng `entry.flags` → lookup coid → apply `_OrderPayload`.
3. **Partition**: Non-terminal → `_active` dict. Terminal → `_archive` deque(maxlen=50,000).
4. **Verification**: FILLED orders phải có `filled_qty > 0`, qty không âm — fail-stop nếu vi phạm.

**WAL Rotation**: Khi `entry_count >= segment_rotation_count` (mặc định 60,000), `rotate_wal()` snapshot toàn bộ active orders vào WAL mới rồi atomic swap reference. Old WAL được close nhưng không xóa — caller chịu trách nhiệm archive.

#### PnL Accounting Engine (`ke_toan_pnl/`) — 1,297 dòng

**Ba rủi ro kế toán và cơ chế xử lý:**

| Rủi ro | Biểu hiện | Cơ chế xử lý |
|--------|-----------|--------------|
| Floating-point accumulation | `0.1 + 0.2 ≠ 0.3` tích lũy qua nghìn giao dịch | Integer-scaled arithmetic: mọi giá trị × `scale_factor=1e8` → `int64` |
| Concurrent mutation (torn read) | Thread 1 đọc PnL khi Thread 2 đang cập nhật | OCC với `scale_ref.version` check trong `sym_lock` |
| Crash mid-update | Mất điện giữa fee update → PnL/fee diverge vĩnh viễn | WAL dual-entry checkpoint: `CheckpointA` + `CheckpointB` liên kết qua `txn_nonce` |

**Kiến trúc:** Integer-Scaled Arithmetic + WAL-Backed Checkpoint + OCC:

- **`pnl_aggregator.py` (734 dòng)**: Root orchestrator — điều phối `RealizedPnLTracker`, `FeeLedger`, và `MarkToMarket`. Sở hữu WAL checkpoint protocol, authority reconciliation, và crash-forensics dump.
- **`pnl_tracker.py` (389 dòng)**: Tính realized PnL bằng thuật toán FIFO (First-In-First-Out). Mỗi fill được ghi WAL `TRADE_RECORD` 32 bytes.
- **`fee_ledger.py` (299 dòng)**: Ghi nhận trade fees và funding payments. Phát hiện anomaly (fee > 1% notional → cảnh báo).
- **`mark_to_market.py` (120 dòng)**: Theo dõi mark price real-time và tính unrealized PnL.

**Integer Arithmetic — Precision Contract:**

Mọi giá trị tài chính được nhân với `scale_factor = 1e8` (100,000,000) và lưu dưới dạng `int64`. Python integer arithmetic là exact — không có epsilon error. Khi cần hiển thị, chia ngược cho `scale_factor`:

```python
# Ví dụ: 0.00012345 BTC → 12345 (scaled)
# Phép cộng: 12345 + 67890 = 80235 (chính xác tuyệt đối)
# Nếu dùng float: 0.00012345 + 0.00067890 = 0.0008023499999... (sai!)
```

> Integer arithmetic chỉ giải quyết vấn đề precision. Với multi-asset hoặc multi-collateral, vẫn cần một lớp chuẩn hóa về `base currency`, `funding sign`, `fee semantics`, và loại sản phẩm (`linear`, `inverse`, `coin-margined`), nếu không PnL có thể đúng về số học nhưng sai về kinh tế.

**OCC (Optimistic Concurrency Control)**: `RealizedPnLTracker` sử dụng OCC thay vì global lock. Mỗi fill được xử lý qua vòng lặp retry tối đa 5 lần:

```python
# thuc_thi_lenh/quan_ly_danh_muc/ke_toan_pnl/pnl_tracker.py
for _attempt in range(MAX_OCC_RETRIES):
    ver = self._scale_ref.version    # Snapshot version
    sf  = self._scale_ref.factor
    qty_s   = round(fill_qty * sf)   # Scale to integer
    price_s = round(avg_price * sf)

    with sym_lock:
        if self._scale_ref.version != ver:
            continue   # Scale đã thay đổi → retry
        gross_pnl_s = _apply_fill(state, side, qty_s, price_s, sf)
        # WAL append dưới sym_lock → wal_lock (lock ordering)
        with self._wal_lock:
            self._wal.append(TRADE_RECORD, payload, flags=fill_ts_ns & 0xFFFF_FFFF)
        break
else:
    raise ScaleVersionConflictError(f"OCC starvation after 5 retries")
```

> OCC chỉ an toàn khi operation là `idempotent` và `retry-safe`. Nếu business logic phụ thuộc side effect bên ngoài hoặc trạng thái cũ không thể tái áp dụng thuần túy, critical path nên dùng ownership/locking chặt hơn thay vì trông chờ vào retry.

**Dual-Key Fill Idempotency**: Mỗi fill được deduplicate bằng 2 key: `(client_order_id, cum_qty_scaled)` + `fill_ts_ns`. OrderedDict FIFO eviction tại 10,000 entries. Sau WAL replay, `seed_idempotency_cache()` rebuild cache từ OMS OrderBook — ngăn WebSocket reconnect gửi lại fill cũ gây double PnL.

**WAL Checkpoint Protocol** (★FIX-1+7) — Atomic dual-entry:

```
CheckpointA (type=7):  txn_nonce(4B) + total_realized(8B) + total_fees(8B) + net_equity(8B)
CheckpointB (type=70): txn_nonce(4B) + peak_equity(8B) + max_drawdown(8B) + total_funding(8B)
                       flags = A.seq_id (liên kết B → A)
```

3-layer replay verification: `B.flags == A.seq_id` ∧ `B.txn_nonce == A.txn_nonce` ∧ CRC32 per entry. Nếu bất kỳ layer nào fail → checkpoint bị bỏ qua, replay từ đầu.

**Scale Downgrade** (★FIX-2): Khi `int64` gần tràn (do scale_factor quá lớn), hệ thống thực hiện **epoch barrier** dưới `_global_lock`: WAL `SCALE_CHANGE` → `fsync()` → rescale tất cả sub-modules → `version++`. OCC trong các thread khác sẽ phát hiện version change và retry.

**Integrity Invariants** — bộ contract tối thiểu cần được enforce bằng runtime checks hoặc sampling khi hướng production:

| # | Invariant | Hành vi khi vi phạm |
|---|-----------|---------------------|
| 1 | `net_pnl == realized - fees - funding` | CRITICAL log + crash dump |
| 2 | `equity >= -max_drawdown_threshold` | CRITICAL log + crash dump |
| 3 | `unrealized == mark_to_market sum` | WARNING only (cross-module) |

**Authority Reconciliation** (★FIX-5): Cho phép exchange override PnL local — nhưng **CHỈ AN TOÀN khi `position.size == 0`**. Nếu vị thế đang mở, override bị **DEFERRED** với `DIVERGENCE_WARNING` để tránh equity corruption.

#### Session Manager (`session_manager.py`) — 662 dòng

**Trách nhiệm:** Đóng sổ phiên giao dịch hàng ngày — snapshot PnL aggregator, tính Sharpe ratio, ghi record vào NDJSON archive, reset session state. Đảm bảo idempotency: nếu rotation bị interrupt giữa chừng, không mất record và không duplicate.

**Cơ chế:** atomic rotation + verified write loop (read-back sau write) + hot/cold storage tiering (NDJSON active → Parquet cold):

**Rotate Session** — Quy trình đóng sổ ngày:

```python
# thuc_thi_lenh/quan_ly_danh_muc/session_manager.py
def rotate_session(self) -> dict:
    with self._global_lock:
        # 1. Snapshot PnL state TRƯỚC khi reset
        dump = self._pnl_agg.snapshot()
        old_session = self._pnl_agg.reset_session()

        # 2. Tính Sharpe Ratio = mean(returns) / std(returns)
        sharpe = self._calculate_sharpe(old_session)

        # 3. Kiểm tra equity guardrail
        if dump["net_equity"] < self._equity_floor:
            logger.critical("EQUITY_FLOOR_BREACH equity=%.2f floor=%.2f",
                          dump["net_equity"], self._equity_floor)

        # 4. Ghi vào NDJSON archive theo kiểu append-only
        record = {
            "session_id": self._session_counter,
            "start_ns": old_session.session_start_ns,
            "end_ns": time.time_ns(),
            "realized_pnl": dump["total_realized_pnl"],
            "sharpe_ratio": sharpe,
            ...
        }
        self._append_verified(record)
        self._session_counter += 1
    return record
```

**Crash-Resilient Append** — Mỗi record được ghi vào file NDJSON (1 JSON object / dòng) bằng quy trình đảm bảo durability:

1. **`portalocker.lock()`**: Khóa file cross-platform (Windows `LockFileEx` / POSIX `fcntl.flock`) — ngăn 2 process ghi đồng thời.
2. **`os.write()` + `os.fsync()`**: Ghi trực tiếp qua file descriptor (bypass Python buffer) rồi flush xuống đĩa vật lý.
3. **Verified write loop**: Sau khi ghi, đọc lại dòng cuối và so sánh — nếu không khớp → retry tối đa 3 lần.

```python
# Verified write — đọc lại sau khi ghi để xác nhận
for attempt in range(self._max_write_retries):
    fd = os.open(str(self._archive_path), os.O_WRONLY | os.O_APPEND | os.O_CREAT)
    try:
        os.write(fd, line_bytes)
        os.fsync(fd)                    # Flush xuống đĩa vật lý
    finally:
        os.close(fd)

    # Verify: đọc lại dòng cuối → so sánh
    last_line = self._read_last_line()
    if last_line.strip() == line.strip():
        return   # ✓ Ghi thành công
    logger.warning("write_verify_failed attempt=%d", attempt)
```

**O(1) Tail Read** — Tại sao không đọc toàn bộ file?

Khi khởi động, SessionManager cần đọc session cuối để khôi phục equity. File archive có thể chứa hàng nghìn sessions. Thay vì đọc toàn bộ (O(n)), hệ thống seek đến cuối file và đọc ngược để tìm ký tự `\n` cuối cùng — chỉ parse 1 dòng JSON:

```python
def _read_last_line(self) -> str:
    with open(self._archive_path, "rb") as f:
        f.seek(0, 2)              # Seek đến EOF
        pos = f.tell()
        buf = bytearray()
        while pos > 0:
            pos -= 1
            f.seek(pos)
            char = f.read(1)
            if char == b'\n' and buf:
                break             # Tìm thấy \n → dòng cuối hoàn chỉnh
            buf.append(char[0])
    return bytes(buf[::-1]).decode("utf-8")
```

**NDJSON → Parquet Compaction**: Sau mỗi `compaction_interval` sessions, file NDJSON (hot data) được compact thành Parquet với Snappy compression (cold storage) bằng Polars. Parquet cho phép query phân tích (Sharpe trung bình, drawdown xu hướng) với tốc độ columnar, trong khi NDJSON giữ vai trò append-only WAL cho durability.

**Truncate Corrupt Tail**: Nếu máy crash giữa lúc ghi NDJSON, dòng cuối có thể bị cắt ngang (partial JSON). Khi startup, `_truncate_corrupt_last_line()` phát hiện và loại bỏ dòng lỗi — chỉ mất tối đa 1 session record (acceptable loss so với corruption toàn file).

#### Position Synchronizer (`position_sync.py`) — 441 dòng

PositionSynchronizer là lớp đối soát giúp kéo local state về gần thực tế trên sàn và phát hiện divergence sớm. Nó không nên được hiểu là bằng chứng rằng trạng thái local `luôn` khớp exchange trong mọi failure mode. Nó hoạt động theo 3 chế độ:

1. **Startup Full Sync**: Chạy **MỘT LẦN** sau WAL replay, TRƯỚC khi accept fills. Exchange được xem là authority cho các field có thể kiểm chứng trực tiếp ở startup; nếu xuất hiện drift chưa giải thích được hoặc side effect chưa reconcile xong, hướng an toàn hơn là `degraded` / `halt` thay vì force-overwrite mù `KairosState`.
2. **Periodic Drift Detection** (60s): So sánh local vs exchange. **CHỈ LOG, KHÔNG auto-fix**. 3 violations liên tiếp → **kill-switch** (dừng toàn bộ giao dịch).
3. **Priority Self-Healing Queue**: External callers push symbols qua `request_heal()`. Worker pop theo priority (notional cao nhất trước) và re-query exchange tại max 2 REST/s.

**Clock Skew EMA**: Mỗi response từ exchange mang theo timestamp. Hệ thống tính EMA của `(local_recv_ms - exchange_ts_ms)` — nếu skew > 500ms → WARNING, > 2000ms → CRITICAL. Phát hiện sớm network issues hoặc system clock drift.

**Kill-Switch Cooldown**: Trước khi trigger kill-switch, kiểm tra `ho_du_lieu/he_thong/system.KILLED` file mtime — nếu đã có kill trong `kill_cooldown_s` giây gần đây → suppress để tránh flapping.

#### Funding Collector (`funding_collector.py`) — 266 dòng

FundingCollector thu thập định kỳ các khoản thanh toán funding rate từ tất cả sàn giao dịch. Đặc biệt quan trọng: **Sign Normalization (★FIX-10)** — Binance/Bybit trả `positive = received` (đảo dấu so với Kairos), trong khi OKX trả `positive = paid` (giữ nguyên). Nếu không chuẩn hóa, PnL sẽ sai hoàn toàn.

**NDJSON Dedup WAL** (`funding_dedup.jsonl`): Mỗi funding payment được hash và lưu vào LRU OrderedDict (50,000 entries). File NDJSON đóng vai trò WAL — crash mid-write chỉ corrupt dòng cuối. Daily compaction prune entries cũ hơn `dedup_keep_days` bằng atomic `write → fsync → os.replace`.

#### Protection Stack (`chien_luoc_thu_lai/`) — 994 dòng

**Trách nhiệm:** Xử lý 4 loại lỗi REST API (429 rate-limit, 5xx server error, timeout, connection reset) qua 3 tầng xếp chồng. **Invariant quan trọng nhất:** `PLACE + TIMEOUT = UnconfirmedOrder` — tuyệt đối không retry, phải reconcile qua OMS.

**Kiến trúc 3 tầng:**

**Tầng 1: Adaptive Rate Limiter** (`rate_limiter.py`) — Token Bucket per-endpoint. PLACE, CANCEL, và INFO có bucket **hoàn toàn độc lập** — khi PLACE cạn token (thị trường sôi động), CANCEL vẫn đi được (quan trọng khi cần emergency cancel). Rate tự điều chỉnh qua EWMA latency tracking:

```
R_new = R_old × (target_latency / ewma_latency)  — proportional throttle
```

Death Spiral Prevention: Nếu không có request trong `recovery_window_s` (30s), EWMA latency bị halve dần và rate recover +10% base_rate mỗi chu kỳ — tránh tình trạng rate bị giảm vĩnh viễn.

**Tầng 2: Weighted Circuit Breaker** (`circuit_breaker.py`) — State machine 3 trạng thái:

```
CLOSED ──[weight_accum ≥ threshold]──► OPEN ──[recovery_timeout]──► HALF_OPEN
                                                                          │
              ◄──────────[probe success]──────────────────────────────────┘
              ──────────[probe failure]──────────────────────────────► OPEN
```

Error weights cộng dồn: `timeout=1`, `ratelimit=3`, `server_error=5`. HALF_OPEN chỉ cho phép **đúng 1 probe request** tại một thời điểm (ngăn Thundering Herd). Alert debouncing: CRITICAL log tối đa 1 lần / `alert_cooldown_ms` / breaker.

**Tầng 3: Smart Retry Policy** (`retry_policy.py`) — Decorrelated Jitter (AWS best practice):

```
delay_i = min(max_delay, uniform(base_delay, prev_delay × 3))
```

Tốt hơn exponential backoff vì dàn đều tải, tránh synchronized retry storms. **Deadline-aware**: nếu `elapsed + next_delay > deadline_ms` → abort sớm. Quy tắc quan trọng nhất: **PLACE + TIMEOUT = UnconfirmedOrder** — lệnh có thể đã vào matching engine, **TUYỆT ĐỐI KHÔNG retry** (Ghost Position risk). EMS phải reconcile.

**`ExecutionWrapper`** (`execution_wrapper.py`) — Điểm tích hợp duy nhất, gói cả 3 tầng vào 1 method call:

```python
# Trước: adapter._execute_with_retry(...)
# Sau:   wrapper.protected_call(coro_factory, EndpointKind.PLACE, ...)
#
# Luồng nội bộ:
#   RateLimiter.acquire() → CircuitBreaker.check() → SmartRetryPolicy.execute()
#   → CB feedback → Latency record → Intent resolve → Eviction
```

**Intent State Machine**: Mỗi lệnh đi qua lifecycle `PENDING → SENT → ACKNOWLEDGED / UNCONFIRMED → RESOLVED`. Intent stale (>300s chưa RESOLVED) bị evict vào `deque(maxlen=10,000)` — chống OOM.

#### Execution Configuration (`cau_hinh/`)

Toàn bộ runtime parameters được hợp nhất vào 3 file YAML phẳng, cho phép thay đổi hành vi mà không cần sửa code:

| File | Scope | Top-level keys |
|------|-------|---------------|
| `trading.yaml` | Risk, OMS, PnL, Position Sync, Session, Strategy | `risk`, `order_book`, `wal`, `reconciliation`, `backpressure`, `cancel_all`, `pnl`, `position_sync`, `funding`, `persistence`, `session`, `strategy` |
| `infra.yaml` | Flow Control, Execution Protection, Alerting, Observability | `flow_control`, `execution` (`rate_limiter`, `circuit_breaker`, `retry`), `alerting`, `observability` |
| `universe.yaml` | Exchange adapters, Symbol master | `exchanges`, `symbols` |

---

### 3.7. Lưới Bảo Vệ (Risk System)

**Trách nhiệm:** Cửa chặn cuối cùng trước khi order rời hệ thống. Quyền từ chối tuyệt đối bất kể Signal Engine và Portfolio Engine đã approve. Fail-fast: evaluate rules theo thứ tự chi phí tính toán tăng dần.

**Hợp đồng hiệu năng:** `risk_gate.check()` hoàn thành trong **< 50µs** trên hot-path. File `risk_gate.py` — 646 dòng.

<img width="1920" height="1080" alt="image" src="https://github.com/user-attachments/assets/5f9c5cce-16f7-4580-920c-394daa564500" />

#### Pre-trade Risk Gate — `check()` < 50µs

Hàm `check()` là cửa chặn cuối cùng trước khi lệnh được gửi. Nó nhận vào thông tin lệnh (symbol, hướng mua/bán, giá trị USD, order ID) và trả về 0 nếu cho phép, hoặc mã lỗi cụ thể nếu từ chối.

Điểm quan trọng: biến `self._state` là một tham chiếu Python — khi monitor thread cập nhật state mới, nó tạo một state object mới và gán vào `self._state`. Thread 1 đọc `self._state` luôn nhận được một bản state hoàn chỉnh (nhờ GIL đảm bảo assignment là atomic). Không cần lock, không có tình trạng đọc state "nửa cũ nửa mới":

```python
# quan_tri_rui_ro/kiem_tra_truoc_lenh/risk_gate.py
def check(self, symbol, side, order_usdt, order_id) -> int:
    state = self._state   # Đọc tham chiếu — GIL-atomic, không lock

    # KIỂM TRA ĐẦU TIÊN: State có còn "tươi" không?
    # Nếu state đã cũ 500ms → có thể vị thế/PnL đã thay đổi mà ta chưa biết
    age_ns = now_ns - state.updated_ns
    if age_ns > _STALE_HARD_NS:      # > 500ms → NGỪNG hoàn toàn
        return RiskCode.STATE_STALE_HARD
    if age_ns > _STALE_REDUCE_NS:    # > 200ms → chỉ cho đóng vị thế
        if not is_reduce:
            return RiskCode.STATE_REDUCE_ONLY

    # KIỂM TRA TUẦN TỰ: chạy qua từng rule
    # Dừng NGAY khi gặp rule đầu tiên fail (fail-fast)
    for rule in self._rules:
        code = rule.check(symbol, side, order_usdt, state, self, now_ns)
        if code: return code          # Trả mã lỗi → lệnh bị từ chối

    return RiskCode.OK                # Tất cả rules pass → cho phép
```

**6 Rule kiểm tra tuần tự** — Mỗi rule giải quyết một loại rủi ro cụ thể. Thứ tự quan trọng: rule "rẻ" (tính toán ít) đặt trước để reject nhanh, rule "đắt" đặt sau:

| # | Rule | Chức năng chi tiết | Ngưỡng ví dụ |
|---|------|-------------------|-------------|
| 1 | `MaxDailyLossRule` | Nếu tổng lỗ trong ngày vượt ngưỡng → ngừng giao dịch. Tránh "revenge trading" (cố gỡ gạc sau khi thua). | `max_daily_loss_usdt` |
| 2 | `MaxDrawdownRule` | Nếu equity giảm quá nhiều so với đỉnh → ngừng. Bảo vệ vốn khi thị trường đi ngược chiến lược. | `max_drawdown_pct` |
| 3 | `MaxOrderRateRule` | Giới hạn số lệnh/giây và lệnh/phút. Ngăn chặn vòng lặp vô hạn gửi lệnh do bug. | `per_sec`, `per_min` |
| 4 | `DuplicateOrderGuardRule` | Nếu lệnh trùng symbol+side+size trong khoảng thời gian ngắn → chặn. Ngăn gửi lệnh trùng do retry logic lỗi. | `duplicate_cooldown_ms` |
| 5 | `MaxOpenOrdersRule` | Giới hạn số lệnh đang mở trên mỗi symbol. Tránh "lệnh treo" quá nhiều ăn margin. | `max_open_orders_per_symbol` |
| 6 | `MaxPositionConcentrationRule` | Không cho phép tập trung quá nhiều vốn vào 1 cặp coin. Đa dạng hóa bắt buộc. | `max_concentration_pct` |

> **Built vs spec target:** 6 rules trên là phần ĐÃ BUILT (~70%). Spec đầy đủ M-L5 (`prompt/layer3_live/M-L5_risk_gate.md`) định nghĩa **RG-0 → RG-24** trong kiến trúc 2 tầng **GATE** (hot-path, pure function < 5ms) / **MONITORS** (background) + **WATCHDOG** (separate OS process, tiered MODE 1/2/3). Các check spec chưa build: RG-18 margin/liquidation, RG-19 VaR/ES incremental, RG-21 correlation shock, RG-22 depeg, RG-23 ADV, RG-24 PnL attribution, reduce-only exemption (4 điều kiện), pending notional reservation (TOCTOU), persistent halt state. Xem M-L5 còn thiếu (70%→100%) trong spec.

**Rate Bucket O(1)** — 1000-bucket per-second window + 60-bucket per-minute window:

```python
# Generation-safe full-cycle reset — tránh negative count khi time jump >1000ms
self._sec_buckets    = array.array("q", [0] * 1000)
self._sec_bucket_gen = array.array("q", [0] * 1000)   # generation tag
```

#### Watchdog Kill Switch (`nguoi_gac_cong/watchdog/`)

Out-of-band Watchdog ping hệ thống qua 2 kênh song song:
1. **ZMQ PUB/SUB** (port 5559): Watchdog SUB nhận HB để đo miss count.
2. **File mtime** (`/tmp/kairos.alive`): Kênh dự phòng độc lập với ZMQ.

Watchdog chỉ trigger khi **CẢ HAI** kênh miss ≥ threshold liên tiếp → tránh false-positive. Khi trigger:
- Hủy mọi kết nối REST API
- Market Close toàn bộ vị thế
- Đặt file cờ `ho_du_lieu/he_thong/system.KILLED` → chặn khởi động cho đến khi điều tra xong

> Watchdog chỉ thực sự an toàn nếu có `global halt state` dùng chung cho toàn hệ thống. Nếu OMS dừng nhưng một execution path khác vẫn còn quyền phát lệnh, kill switch mới chỉ là "cảnh báo mạnh", chưa phải system-wide stop tuyệt đối.

---

### 3.8. Hạ Tầng & Giao Tiếp (Infrastructure)

Cấu trúc xương sống kết nối các module rời rạc thành một thể thống nhất.

#### Event Bus (`ha_tang/bus_su_kien/zmq_bus.py`)

Hệ thần kinh trung ương sử dụng ZeroMQ PUB/SUB:

```python
# ha_tang/bus_su_kien/zmq_bus.py
class AsyncEventBus:
    """Mọi module đều cắm vào đây để Đọc (SUB) hoặc Ghi (PUB) Sự kiện."""
    def __init__(self,
        pub_url: str = "inproc://kairos-event-bus",   # 10-50µs/msg
        sub_url: str = "inproc://kairos-event-bus",
    ):
        self.ctx = zmq.asyncio.Context()

    async def publish(self, topic: str, payload_dict):
        message_bytes = orjson.dumps(payload_dict)    # orjson: siêu tốc
        await self.publisher.send_multipart([
            topic.encode('utf-8'), message_bytes
        ])
```

- HWM = 10,000 — Chặn tràn RAM, message thừa có thể bị drop thay vì block. Điều này phù hợp cho `latency mode`; với luồng causal quan trọng cần kết hợp sequencing + gap detection hoặc chuyển sang `integrity mode`.
- `inproc://` — Cùng process: loại bỏ TCP kernel stack, chỉ mất ~10-50µs/msg.
- `orjson` — Serialize JSON nhanh gấp ~10× so với `json` stdlib.

#### Adaptive Shedder (`shedder.py`) — 209 dòng

Trong thời điểm thị trường biến động mạnh (Flash Crash), lượng tin nhắn từ sàn có thể tăng gấp 10-50 lần bình thường. Nếu bot cố gắng xử lý tất cả, độ trễ sẽ tăng từ microsecond lên hàng giây → giao dịch bằng dữ liệu cũ → lỗ nặng.
Adaptive Shedder giải quyết bài toán: **Khi nào nên vứt bỏ data cũ để bảo vệ hệ thống?** Nó sử dụng động cơ áp suất không khóa (Lock-free) với **4-factor multi-dimensional pressure model**:

> Shedder là cơ chế bảo vệ hiệu năng, không phải chứng minh correctness. Nếu data bị drop thuộc luồng causal, production-safe flow cần thêm sequencing, degraded-state marking, và khả năng recovery/halt rõ ràng.

**Công thức Áp Suất Đa Nhân Tố**:

```
P = w1 × drop_rate_norm     (35% — tỷ lệ vứt data/giây)
  + w2 × depth_ratio         (30% — mức đầy của queues)
  + w3 × latency_ratio       (20% — độ trễ vượt ngưỡng target)
  + w4 × growth_ratio         (15% — tốc độ tăng queue depth)
  → clamped to [0.0, 1.0]
```

**Continuous-time EWMA** — Thay vì dùng discrete IIR filter (bị ảnh hưởng bởi sample rate không đều), Kairos sử dụng hàm mũ liên tục `e^(-dt/τ)` để smooth:

```python
# ha_tang/kiem_soat_luu_luong/drop_policy/shedder.py
def update(self, latency_ns: float = 0.0) -> float:
    """Recompute multi-factor pressure. Returns pressure ∈ [0.0, 1.0]."""
    dt    = max(1e-9, now - self._last_ts)
    decay = math.exp(-dt / self._tau)   # τ = 5.0s — physics-like decay

    # Drop rate: sum 1-second ring window (10 slots × 100ms)
    total_drops_1s = sum(
        self._buckets[i] for i in range(10)
        if now_ms - self._bucket_ts_ms[i] <= 1000
    )
    raw_drop_norm   = min(1.0, total_drops_1s / self._max_drop_rate_norm)
    self._drop_rate = self._drop_rate * decay + raw_drop_norm * (1.0 - decay)

    # Composite pressure
    p = (self._w1 * self._drop_rate
       + self._w2 * self._depth_ratio
       + self._w3 * latency_ratio
       + self._w4 * growth_ratio)
    self._pressure = max(0.0, min(1.0, p))
```

**Hysteresis State Machine (Schmitt Trigger)** — Chống dao động trạng thái khi pressure giao động quanh ngưỡng:

```python
# Schmitt Trigger: 2 ngưỡng riêng biệt
if p > self._high_thresh:      # 0.70 → bật shedding
    self._state = PressureLevel.HIGH
elif p < self._low_thresh:     # 0.40 → tắt shedding
    self._state = PressureLevel.LOW
# Dải [0.40, 0.70]: giữ nguyên state → KHÔNG ping-pong
```

> **Tại sao cần Hysteresis?** Nếu chỉ dùng 1 ngưỡng (ví dụ 0.5), khi pressure dao động 0.49 → 0.51 → 0.49 liên tục, hệ thống sẽ bật/tắt shedding hàng chục lần/giây → hiệu ứng "giật" rất xấu. Với 2 ngưỡng, phải vượt qua 0.70 mới bật, và phải dưới 0.40 mới tắt.

**Head-Drop O(1)** — Khi queue đầy, vứt phần tử cũ nhất (`deque.popleft()` — C-level atomic):

```python
def maybe_drop(self, q: deque, priority_id: int) -> bool:
    if len(q) >= self._max_sizes[priority_id]:
        q.popleft()          # C-level atomic head drop — O(1)
        self._dropped += 1   # INPLACE_ADD bytecode — GIL-atomic
        return True
    return False
```

#### Channel Manager (`channel_manager.py`)

Quản lý 3 hàng đợi SPSC theo ưu tiên:

| Priority | Kênh | Nội dung |
|----------|------|----------|
| CRITICAL | Kill Switch, Risk Alert | Luôn được drain trước |
| SIGNAL | Ring buffer events | Ưu tiên cao |
| DATA | Market data updates | Drain sau cùng, có fairness guarantee |

> SPSC queues chỉ an toàn khi có đúng một writer và một reader. Đây là assumption thiết kế — nếu codebase mở rộng và multiple producers/consumers được thêm, cần runtime assertion tại điểm produce/consume (ví dụ: `threading.get_ident()` guard hoặc `assert writer_id == current_thread`) để tránh data race. Latency đẹp không chứng minh được correctness khi assumption bị vi phạm.

#### Event Schema (`luoc_do_du_lieu/v1/`)

Chuẩn hóa cấu trúc toàn hệ thống bằng ctypes struct. Mọi event được đóng gói ở kích thước cố định để fit vừa CPU cache line:

- `_FeatureEventRaw` — **192 bytes** (3 cache lines 64B): chứa `symbol_id` (uint32), `exchange_ts` (int64), `receive_ts` (int64), `processed_ts` (int64), `source_latency_ns` (int64), `computation_latency_ns` (int64), `feature_mask` (uint32), và `features[16]` (float64 × 16 = 128B).
- `_SignalEventRaw` — **64 bytes** (1 cache line): chứa `symbol_id` (uint32), `direction` (int8), `signal_ts` (int64), `exchange_ts` (int64), `receive_ts` (int64), `feature_ts` (int64).

> **Tại sao 192 bytes?** 192 = 3 × 64B cache lines. Khi CPU nạp features của symbol `i`, nó kéo đúng 3 cache lines vào L1 cache. Header (symbol_id, timestamps) nằm trên cache line thứ 1, features nằm trên cache lines 2-3. Symbol `i+1` bắt đầu ở cache line thứ 4 → không bao giờ bị False-sharing.

#### Time Validator (`dong_ho_thoi_gian/time_validator.py`) — 253 dòng

Đảm bảo tính chính xác thời gian ở cấp sub-microsecond. Sử dụng **ctypes truy xuất trực tiếp `librt.so.1`** để đọc POSIX clock:

```python
# ha_tang/dong_ho_thoi_gian/time_validator.py
class _Timespec(ctypes.Structure):
    _fields_ = [("tv_sec", ctypes.c_long), ("tv_nsec", ctypes.c_long)]

_librt = ctypes.CDLL("librt.so.1", use_errno=True)
_librt.clock_gettime.argtypes = [ctypes.c_int, ctypes.POINTER(_Timespec)]

def _clock_ns(clock_id: int) -> int:
    """Read a POSIX clock in nanoseconds — sub-µs precision."""
    ts = _Timespec()
    if _librt.clock_gettime(clock_id, ctypes.byref(ts)) == 0:
        return ts.tv_sec * 1_000_000_000 + ts.tv_nsec
```

**3 nguồn thời gian theo thứ tự ưu tiên**:

| Nguồn | Độ chính xác | Điều kiện |
|-------|-------------|----------|
| PTP (IEEE 1588) | Sub-microsecond | `phc2sys` + `ptp4l` đang chạy |
| NTP / chrony | ~1-10ms | Fallback khi PTP crash |
| CLOCK_MONOTONIC | Chỉ thứ tự | Last resort — chỉ đảm bảo event ordering |

**Mô hình Graceful Degradation**:

| Trạng thái | Điều kiện | `capital_multiplier` | Hành vi |
|-----------|-----------|---------------------|--------|
| NOMINAL | Skew < 100µs | 1.0 | Giao dịch bình thường |
| DEGRADED | Skew > 100µs hoặc PTP crash | 0.2 | Giảm 80% kích thước lệnh |
| CRITICAL | Skew > 1ms hoặc clock diverge | 0.0 | Đóng băng toàn bộ giao dịch |

> **Tại sao Clock Skew nguy hiểm?** OFI (Order Flow Imbalance) so sánh `best_bid` hiện tại với `prev_best_bid` dựa trên timestamp ordering. Nếu clock sai 1ms, hệ thống có thể xử lý tick cũ trước tick mới → OFI tính ra giá trị ngược dấu → signal sai → lệnh sai hướng.

---

### 3.9. Giám Sát & Kiểm Thử (Monitoring & Testing)

#### Latency Tracker (`giam_sat/theo_doi_do_tre/tracker.py`) — 93 dòng

Đo lường 4 phân đoạn độ trễ. Tất cả `record_*()` đều **O(1), zero-allocation**:

```python
# giam_sat/theo_doi_do_tre/tracker.py
class LatencyTracker:
    """Hot-path safe. All record_*() methods are O(1), zero-allocation."""
    def __init__(self):
        self.tick_to_signal  = HdrHistogram()  # exchange_ts → signal_ts
        self.tick_to_feature = HdrHistogram()  # exchange_ts → feature_ts
        self.signal_to_order = HdrHistogram()  # signal_ts  → order_submit
        self.order_to_fill   = HdrHistogram()  # order_submit → fill

    def record_signal(self, exchange_ts, receive_ts, feature_ts, signal_ts):
        """O(1). Call from Thread 1 (hot loop)."""
        self.tick_to_signal.record(max(0, signal_ts - exchange_ts))
        self.tick_to_feature.record(max(0, feature_ts - exchange_ts))
```

| Phân đoạn | Clock Domain | Thread |
|-----------|-------------|--------|
| Tick → Feature | wall clock (cross-process) | Data Engine |
| Tick → Signal | wall clock (cross-process) | Signal Engine |
| Signal → Order | wall clock (same process) | Thread 1 |
| Order → Fill | monotonic (in-process) | Thread 2 |

#### HdrHistogram (`giam_sat/theo_doi_do_tre/histogram.py`) — 116 dòng

Việc đo đạc độ trễ (latency) không được phép làm chậm chính hệ thống đang được đo. Nếu lưu mọi điểm dữ liệu độ trễ vào danh sách (list), RAM sẽ bị ăn mòn rất nhanh và gây GC pause. Kairos sử dụng **HdrHistogram** — cấu trúc dữ liệu đếm độ trễ siêu nhẹ, **O(1) record, zero-allocation**:

**Bucket Layout (7-bit mantissa, 128 sub-buckets per exponent)**: Thay vì lưu giá trị chính xác, độ trễ được phân loại vào các "rổ" (buckets). Độ trễ càng lớn, "rổ" càng rộng (sai số 1% không đáng kể đối với 10 giây, nhưng 1µs rất quan trọng đối với độ trễ 10µs).

```python
# giam_sat/theo_doi_do_tre/histogram.py
MANTISSA_BITS    = 7
SUB_BUCKET_COUNT = 128        # 2^7
_MIN_NS          = 1_000      # 1 µs — clamp floor
_MAX_NS          = 10_000_000_000  # 10 s — clamp ceiling
ARRAY_SIZE       = 3_584      # 28 × 128 slots = ~28 kB total

def record(self, value_ns: int) -> None:
    """O(1). Zero allocation. GIL-atomic +=1."""
    bit_len = value_ns.bit_length()
    if bit_len <= MANTISSA_BITS:
        idx = value_ns                    # bucket 0: direct index
    else:
        shift = bit_len - MANTISSA_BITS
        idx   = shift * 128 + ((value_ns >> shift) & 0x7F)
    self._cur[idx] += 1
```

> **Tại sao không dùng list append?** `list.append(latency)` tạo Python int object mới → GC pressure. HdrHistogram chỉ tăng bộ đếm (`+=1`) tại một vị trí cố định trong mảng `array.array("L")` đã cấp phát từ trước — **hoàn toàn không sinh ra object rác**.

**Double-Buffering** — Vấn đề: làm sao để luồng đo đạc (Reporter) có thể đọc kết quả hiển thị lên màn hình mà không khóa (lock) luồng ghi dữ liệu (Hot-path)? Giải pháp: Dùng 2 mảng (buffers). Trong vòng 30 giây, Hot-path ghi vào mảng A. Sau 30s, Reporter đảo ngược con trỏ (mảng A thành mảng đóng băng để đọc, mảng B làm mới để Hot-path ghi). Việc tráo đổi này là atomic dưới GIL:

```python
def swap(self) -> "HdrHistogram":
    """Off-path. Rotate buffers atomically under GIL."""
    old_cur, old_frozen = self._cur, self._frozen
    for i in range(ARRAY_SIZE):
        old_frozen[i] = 0          # Xóa sạch mảng cũ
    self._cur    = old_frozen      # Tráo mảng: mảng cũ thành mảng ghi mới
    self._frozen = old_cur         # Mảng vừa ghi xong mang ra đọc
    return self
```

#### System Metrics Collector (`giam_sat/chi_so_hieu_suat/collector.py`) — 382 dòng

Hệ thống cần theo dõi RAM, CPU, và mức sử dụng GC liên tục. Nhưng các lệnh như lấy CPU (`psutil.cpu_percent()`) hay quét RAM (`tracemalloc.take_snapshot()`) là I/O-blocking — chúng có thể treo chương trình hàng chục mili-giây.

Giải pháp: Chạy Collector trên **Dedicated OS Thread** riêng biệt (Thread của hệ điều hành, không phải asyncio). Kiến trúc 3 tầng:

```
Collector thread  ──push()──►  _buffer  ──drain()──►  Reporter (asyncio)
TM daemon thread  ──────────►  ctypes.c_double       ──►  Collector reads
```

**Tracemalloc Safety Gates** — Tính năng "chụp ảnh RAM" (`take_snapshot`) rất đắt đỏ (stop-the-world). Do đó, nó bị khóa sau 3 "cửa an toàn":

| Gate | Điều kiện | Mục đích |
|------|-----------|----------|
| RSS Threshold | `RSS > tracemalloc_rss_threshold_mb` | Chỉ chụp ảnh RAM nếu máy đang tốn quá nhiều RAM |
| Cooldown | `≥ tracemalloc_cooldown_s` giữa 2 lần | Tránh việc chụp liên tục gây nghẽn máy |
| Single-flight Lock | `threading.Lock` non-blocking | Đảm bảo chỉ có 1 tiến trình chụp ảnh RAM tại một thời điểm |

**GC Metrics** — Tín hiệu hạng nhất cho hệ low-latency. Việc theo dõi xem Garbage Collector có bị tạm ngưng hay không là rất quan trọng. Bằng cách lấy delta của `gc.get_stats()["collections"]`, ta biết ngay trong giây vừa rồi hệ thống có bị khựng vì dọn rác hay không.

```python
# collector.py — GC pause detection
gc_collections_delta: tuple[int, int, int]  # GC runs per generation
# delta > 0 ⟹ GC pause occurred during sample interval
```

#### Alert System (`giam_sat/canh_bao/`)

- `alert_manager.py` — Orchestrator quản lý Alert với Deduplication (chống spam).
- `alert_rules.py` — Rule Engine đánh giá cảnh báo dựa trên PnL, Latency, Error Rate.
- `telegram_sender.py` — Gửi tin nhắn Alert qua Telegram Bot API.
- Spec mở rộng (chưa build — xem `prompt/layer3_live/alert_management.md`): 4 severity tiers (CRITICAL 15min / ALERT 2h SLA / WARNING digest / INFO) + HFT data-retention alerts (M-H1 Nghĩa vụ): own-order log thiếu join key > 1%/ngày hoặc L2/trade collector down > 30 phút → ALERT (data mất là mất vĩnh viễn).

<img width="1920" height="1080" alt="image" src="https://github.com/user-attachments/assets/d39e89ec-f3a2-4eb0-8b89-b194853be762" />

#### Test Suite (`test/`)

| File | Mục đích |
|------|----------|
| `test_chaos_risk.py` | 24 tests — Chaos Test, cố tình gây lỗi để kiểm tra phục hồi |
| `test_d0_lineage.py` | 25 tests — [M-D0] Data Lineage Registry |
| `test_d1_ingestion.py` | 31 tests — [M-D1] Raw Data Ingestion |
| `test_d2_gap_reconciliation.py` | 37 tests — [M-D2] Gap Reconciliation |
| `test_d3_pit_universe.py` | 32 tests — [M-D3] PiT Universe Manager |
| `test_d4_feature_cache.py` | 29 tests — [M-D4] Feature Cache |
| `test_execution_pipeline.py` | 40 tests — End-to-end pipeline thực thi lệnh |
| `test_exit_coordinator.py` | 18 tests — ExitCoordinator state machine, cooldown, quarantine |
| `test_exit_e2e.py` | 12 tests — Exit system end-to-end integration |
| `test_feature_layer.py` | 48 tests — Kiểm thử lớp tính năng |
| `test_live_runner.py` | 74 tests — Live Runner, 14 components |
| `test_paper_adapter_queries.py` | 10 tests cho `PaperExchangeAdapter.get_positions/get_balances/get_funding_history` |
| `test_position_funding.py` | 13 tests cho `PositionSynchronizer` (full_sync, drift detection, kill-switch) và `FundingCollector` |
| `test_position_sizer.py` | 14 tests — PositionSizer (qty, anti-flip, lot rounding) |
| `test_profiler.py` | Đo hiệu năng hệ thống |
| `test_rest_api.py` | 4 tests — REST API calls tới exchanges |
| `test_session_pnl.py` | 20 tests cho `SessionManager` (checksum, rotate, crash-resilient write) và `PnLAggregator` (on_fill, checkpoint, scale downgrade, integrity, replay) |
| `test_signal_engine.py` | 10 tests — Engine tín hiệu |
| `test_state.py` | Kiểm thử WAL + state recovery |
| `test_strategies.py` | 32 tests — 6 exit strategies + composite |
| `xem_du_lieu_binance.py` | Raw data stream inspector — Binance |
| `xem_du_lieu_bybit.py` | Raw data stream inspector — Bybit |
| `xem_du_lieu_okx.py` | Raw data stream inspector — OKX |

---

## 4. Ràng Buộc & Quyết Định Thiết Kế (Design Constraints & Engineering Decisions)

Phần này ghi lại các quyết định thiết kế có trade-off rõ ràng — tại sao chọn thiết kế A thay vì B, và invariant nào phải giữ để thiết kế còn đúng. Mỗi mục có evidence requirement để chứng minh claim còn đúng sau thay đổi.

---

### 4.1. Zero-Allocation Design Constraint (Chống GC Pauses)

**Ràng buộc thiết kế:** Python GC Gen-0 collection gây latency spike **1-5ms**, Gen-2 lên đến **50ms**. Trên hot-path (Thread 1), bất kỳ allocation nào đều là rủi ro GC pause.

**Quy tắc:** Pre-allocation 100% tại `__init__`. Hot-path chỉ ghi đè lên vùng nhớ đã cấp phát — không tạo `dict`, `list`, hay `bytes` mới.


<img width="1920" height="1080" alt="image" src="https://github.com/user-attachments/assets/c7223510-0a99-4729-9998-20c4128f6f92" />


```python
# ANTI-PATTERN: Tạo dict mới MỖI tick → GC pressure
payload = {"price": 100.0, "qty": 1.0}   # 1 dict + 2 str keys + 2 float = 5 objects

# KAIROS: Ghi đè field trên pre-allocated dataclass → zero objects
self._scratch_payload.price = 100.0       # STORE_ATTR bytecode — 0 objects
self._scratch_payload.qty   = 1.0
```

**Danh sách đối tượng Pre-allocated trong Kairos**:

| Object | Vị trí | Kích thước | Mục đích |
|--------|--------|-----------|----------|
| `_RingSlot[256]` | `vong_lap_su_kien.py` | 256 × 72B | SPSC Ring Buffer |
| `_recv_buf` | `vong_lap_su_kien.py` | 64 bytes | ZMQ recv overlay |
| `ScratchPayload` | `incremental_engine.py` | 48 bytes | Dict replacement |
| `SymbolState[N]` | `incremental_engine.py` | Per-symbol | Rollback snapshot |
| `_pool[16384]` | `incremental_engine.py` | 16K × 192B = 3.1MB | Object Pool |
| `_pool_views[16384]` | `incremental_engine.py` | NumPy views | frombuffer cache |
| `OnlineFeatureStore._store` | `memory_store.py` | 256 × 192B = 48KB | Main feature array |
| `ROB arrays` | `memory_store.py` | 256 × 64 × 16 × 8B | Reorder Buffer |
| `HdrHistogram._cur` | `histogram.py` | 3584 × 8B = 28KB | Latency buckets |
| `_WALEntry[65536]` | `durable_wal.py` | 65K × 64B = 4MB | WAL mmap |
| `OrderPool[2048]` | `order_book.py` | 2048 × ~120B = 240KB | OMS Order Pool |
| `_fill_id_cache` | `pnl_tracker.py` | 10K entries OrderedDict | Fill Idempotency |

> **Kết quả**: Thread 1 (Hot-Path) gọi `gc.disable()` hoàn toàn. Không có GC nào chạy trên thread xử lý tín hiệu.

<img width="1920" height="1080" alt="image" src="https://github.com/user-attachments/assets/677f87c2-4f93-473c-b5d6-39f9bb66df8e" />

---

### 4.2. Triết lý Memory Alignment (Chống False-Sharing)

**Thiết kế:** Padding mọi struct quan trọng vào bội số 64 bytes để đảm bảo mỗi struct chiếm đúng 1 cache line, loại bỏ false-sharing giữa các thread:

```python
# Struct 50 bytes — 2 symbols có thể nằm chung cache line
class BadStruct(ctypes.Structure):
    _fields_ = [("data", ctypes.c_char * 50)]  # 50B → False-sharing risk

# Struct CHÍNH XÁC 64 bytes — 1 struct = 1 cache line = 0 False-sharing
class _WALEntry(ctypes.LittleEndianStructure):
    _pack_ = 1
    _fields_ = [
        ("seq_id",   ctypes.c_uint64),    # 8B
        ("ts_ns",    ctypes.c_int64),     # 8B
        ("type",     ctypes.c_uint32),    # 4B
        ("flags",    ctypes.c_uint32),    # 4B
        ("payload",  ctypes.c_char * 32), # 32B
        ("crc32",    ctypes.c_uint32),    # 4B
        ("_align",   ctypes.c_uint32),    # 4B ← PADDING để đạt 64B
    ]   # Total: 8+8+4+4+32+4+4 = 64 bytes ✓
```

<img width="1920" height="1080" alt="image" src="https://github.com/user-attachments/assets/e74ac464-d0ec-4364-96e6-406d7a41e735" />


---

### 4.3. ctypes.memmove — Bypass Python Object Layer

**Context:** Copy 16 float64 bằng Python for-loop tạo 16 temporary Python float objects (~1-2µs).

**Thiết kế:** `ctypes.memmove` copy trực tiếp 128 bytes ở tốc độ C (~0.05µs, nhanh gấp **20-40×**):

<img width="1920" height="1080" alt="image" src="https://github.com/user-attachments/assets/d24c3875-231a-450d-bde9-33841a69d72e" />

```python
# Python loop: 16 objects tạm, ~1.5µs
for i in range(16):
    draft[i] = store_features[i]   # Mỗi lần = PyFloat_FromDouble + GC track

# ctypes.memmove: 0 objects, ~0.05µs
ctypes.memmove(
    ctypes.addressof(raw.features),       # dest: pool slot
    self._sym_src_ptrs[symbol_id],        # src: pre-computed pointer
    128,                                   # 16 × 8 bytes
)
# Pointer pre-computed tại register_symbol() → hot-path không gọi .ctypes.data
```

---

### 4.4. Transactional Update Protocol (Chống Data Corruption)

**Invariant:** Nếu `commit_raw()` bị reject (backpressure/stale), `SymbolState` phải rollback về trạng thái trước. State và store không bao giờ được bất đồng bộ.

**Protocol 4 bước — compute trên draft, commit hoặc rollback:**

```
                    ┌─ commit_raw() PASS ──► Ghi vào Live Store ✓
Snapshot → Draft ─► │
                    └─ commit_raw() FAIL ──► Rollback SymbolState từ Scratch ✓
```

Cả accept path và reject path đều **zero allocation** nhờ pre-allocated scratch buffer.

---

### 4.5. Microstructure Simulation (Paper Trading Realism)

**Mục tiêu:** Loại bỏ Simulation-to-Reality Gap — Paper Engine phải tái hiện đủ 5 yếu tố market microstructure để backtest có thể dùng làm evidence cho live deployment:

| Yếu tố | Cách mô phỏng | Tác động |
|--------|--------------|---------|
| Network Latency | Lognormal Jitter (heavy-tail) | Lệnh đến sàn bị trễ 5-500ms |
| Slippage | Kyle's Alpha — self-impact | Lệnh lớn đẩy giá bất lợi |
| Liquidity Mirage | Spoofing simulation | Sổ lệnh "dày" nhưng rút trước khi khớp |
| Adverse Selection | Toxicity ratio | Lệnh bị front-run bởi informed trader |
| Flash Crash | `shock_simulator.py` | Giá lao dốc 5-20% trong vài giây |

---

### 4.6. Dual-Channel Failsafe (Chống Single Point of Failure)

**Thiết kế:** Watchdog dùng 2 kênh hoàn toàn độc lập. Trigger chỉ khi **CẢ HAI** miss ≥ threshold — loại bỏ false-positive từ network glitch đơn lẻ:

```
Thread 5 ──► ZMQ PUB (port 5559)  ──► Watchdog SUB  ──┐
             │                                        ├─ BOTH miss ≥ N → KILL                                   |
Thread 5 ──► os.utime(alive_path) ──► Watchdog stat ──┘
```

Watchdog chỉ kích hoạt Kill Switch khi **CẢ HAI** kênh mất tín hiệu liên tiếp → loại bỏ false-positive từ network glitch. Khi trigger: Market Close toàn bộ vị thế → đặt `ho_du_lieu/he_thong/system.KILLED` → chặn khởi động cho đến khi điều tra.

---

### 4.7. Clock Integrity — TimeValidator

**Invariant:** Clock skew > 1ms khiến OFI/EMA nhận tick lộn thứ tự → signal có thể đảo dấu. Hệ thống phải giảm exposure trước khi tín hiệu sai gây lệnh ngược chiều.

**`TimeValidator` 3 tầng — PTP → NTP → Monotonic, graceful degradation:**

```python
# Degradation chain trong Thread 1 (hot-path):
_tv_mult = time_validator.capital_multiplier()

if _tv_mult == 0.0:               # CRITICAL: clock diverge
    risk.rollback(sid, order_usdt)
    self._drop("time_critical")    # Đóng băng 100% giao dịch

if _tv_mult < 1.0:                # DEGRADED: PTP crash
    # Cho qua 1/5 lệnh → giảm exposure 80%
    if write_idx % round(1.0 / _tv_mult) != 0:
        risk.rollback(sid, order_usdt)
        self._drop("time_degraded")
```

> **Thiết kế:** Không hard-stop ngay — degrade gracefully theo bậc để tránh cascade liquidation. `capital_multiplier = 0.2` khi DEGRADED (giảm 80% size); `= 0.0` khi CRITICAL (đóng băng hoàn toàn).

---

## 5. Công Nghệ & Thư Viện (Tech Stack)

| Thành phần | Công nghệ | Vai trò |
|-----------|-----------|---------|
| Ngôn ngữ chính | Python 3.13+ | Core logic, async I/O |
| Messaging | ZeroMQ (pyzmq) | Event Bus PUB/SUB, inproc/tcp |
| Serialization | orjson | JSON serialize ~10× nhanh hơn stdlib |
| Numerical | NumPy, ctypes | Structured arrays, memmove, zero-alloc |
| Data Format | Apache Parquet | Columnar storage, Hive partitioning |
| Data Processing | Polars | Vectorized backtest + Session compaction |
| ML Framework | PyTorch, ONNX | Model training & inference |
| HTTP Client | httpx | Async REST API calls |
| File Locking | portalocker | Cross-platform file locking (WAL/Session) |
| Monitoring | psutil, tracemalloc | CPU/RAM/GC metrics |
| Alerting | Telegram Bot API | Real-time alert notifications |
| Containerization | Docker | Infrastructure packaging |
| Configuration | YAML, .env | Decoupled config management |
| Crypto Exchanges | Binance, OKX, Bybit | REST + WebSocket APIs |
| On-chain Data | CryptoQuant API | BTC/ETH reserve, netflow |

---

## 6. Thống Kê Codebase

| Metric | Giá trị |
|--------|---------|
| Tổng số module Python | 194 files (git-tracked) |
| File lớn nhất | `vong_lap_su_kien.py` — 1251 dòng |
| **Live Runner** | `live_runner.py` — 1038 dòng (12-Step SRE Startup) |
| Paper EMS Adapter | `paper_ems_adapter.py` — 832 dòng (19-Step Fill Pipeline) |
| OMS OrderBook | `order_book.py` — 763 dòng |
| PnL Aggregator | `pnl_aggregator.py` — 734 dòng |
| Session Manager | `session_manager.py` — 662 dòng |
| File quan trọng nhất | `risk_gate.py` — 646 dòng, 6 rules |
| Position Synchronizer | `position_sync.py` — 441 dòng |
| Execution Wrapper | `execution_wrapper.py` — 435 dòng |
| Feature Engine | `incremental_engine.py` — 422 dòng |
| Reconciler | `reconciler.py` — 400 dòng |
| PnL Tracker | `pnl_tracker.py` — 389 dòng |
| Feature Store | `memory_store.py` — 342 dòng |
| Durable WAL | `durable_wal.py` — 354 dòng |
| Fee Ledger | `fee_ledger.py` — 299 dòng |
| Funding Collector | `funding_collector.py` — 266 dòng |
| Circuit Breaker | `circuit_breaker.py` — 261 dòng |
| Rate Limiter | `rate_limiter.py` — 257 dòng |
| Retry Policy | `retry_policy.py` — 221 dòng |
| Mark-to-Market | `mark_to_market.py` — 120 dòng |
| OMS Serializer | `oms_serializer.py` — 135 dòng |
| Latency Tracker | `tracker.py` — 93 dòng (tinh gọn) |
| **Test: Live Runner** | `test_live_runner.py` — 966 dòng, 74 tests, 14 components |
| Test: Execution | `test_execution_pipeline.py` — 534 dòng, 40 tests |
| Test: Chaos Risk | `test_chaos_risk.py` — 441 dòng, 24 tests |
| Số Alpha Features | 6 (MicroPrice, BookPressure, WelfordVar, EMA Fast/Slow, OFI) |
| Số Thread song song | 5 (Hot-Path, Worker, Oracle, Logger, Watchdog) |
| Số Live Daemons | 3 (HealthDaemon, ReconcilerDaemon, KillSwitchMonitor) |
| Số Exchange hỗ trợ | 3 (Binance, OKX, Bybit) |
| Số Risk Rules | 6 (DailyLoss, Drawdown, Rate, Duplicate, OpenOrders, Concentration) |
| PnL Scale Factor | 1e8 (100,000,000) — integer arithmetic |
| WAL File Size | ~4MB (32 + 65536 × 64 bytes) |
| OMS Order Pool | 2,048 slots (slots=True, ~3× RAM savings) |
| Feature Pool Size | 16,384 slots × 192 bytes = ~3.1 MB |
| Ring Buffer | 256 slots (power-of-2) |
| ROB Window | 5ms, 64 slots |
| Target Latency | < 10–50µs per tick |

---

## 7. Hướng Phát Triển Tiếp Theo

1. **Formal Event Model & Sequencing** — Chuẩn hóa `sequence_id`, tách rõ `event_time` / `ingest_time`, thêm gap detection và policy rõ cho out-of-order / dropped events.
2. **Strict WAL Coverage & Deterministic Replay** — Xác nhận mọi mutation trọng yếu đều đi qua WAL, replay từ snapshot + WAL rebuild ra cùng logical state, và không phụ thuộc thread scheduling.
3. **Global State Machine & Kill Consistency** — Formalize trạng thái `INIT -> SYNC -> RUNNING -> HALT`, đồng bộ kill switch trên toàn bộ module có khả năng phát lệnh.
4. **Invariant & Reconciliation System** — Bổ sung hoặc siết chặt runtime invariants cho order, position, balance, PnL; tăng tần suất đối soát với exchange khi phát hiện divergence.
5. **Replay / Chaos / Fault Injection** — Mở rộng `test_chaos_risk.py`, replay historical streams, test restart giữa chừng, test race condition và stress load để kiểm tra correctness trước khi tối ưu thêm latency.
6. **AI & Model Development** — Triển khai ONNX inference engine trong `hoc_may/suy_luan/`. Tích hợp Transformer-based Alpha model.
7. **Grafana Integration** — Kết nối `SystemMetricsReporter`, `LatencyReporter`, `ExecutionWrapper.snapshot()`, và `PnLAggregator.dump_crash_state()` vào Grafana qua Prometheus exporter.
8. **Multi-Strategy Execution & MLOps** — Kích hoạt `danh_ba_chien_luoc/` để chạy nhiều Alpha strategies song song, đồng thời tích hợp ML Monitoring (`sai_lech_dac_trung/`, `sai_lech_du_doan/`) để auto-disable model khi prediction drift vượt ngưỡng.

---

## 8. Kiểm Tra Sẵn Sàng Production (Production Readiness Checklist)

Phần này liệt kê 20 điểm kiểm tra tối thiểu trước khi triển khai Kairos hoặc bất kỳ hệ thống tham khảo từ Kairos cho môi trường real-money. Mỗi mục là một **gap đã xác định** trong kiến trúc hiện tại — không phải lý thuyết mà là điểm cụ thể cần bổ sung trước khi xem là production-safe.

### Nhóm 1: Tính Đúng Đắn Sự Kiện (Event Correctness)

| # | Điểm kiểm tra | Trạng thái hiện tại | Hành động cần thiết |
|---|---------------|---------------------|---------------------|
| 1 | Mọi event mang `sequence_id`, `event_time`, và `ingest_time` tách biệt | Thiếu `sequence_id` chuẩn hóa trên toàn pipeline | Thêm field `seq` tăng dần monotonic vào mọi event schema (`_FeatureEventRaw`, `_SignalEventRaw`) |
| 2 | Gap detection trên mọi ZMQ SUB socket | ZMQ HWM = 10,000 drop messages silently | Detect gap từ `sequence_id`; policy rõ cho gap: `degraded` hoặc `halt`, không tiếp tục im lặng |
| 3 | Drop event trên luồng causal (fill, risk update) không bị xử lý như coalescing | Shedder và HWM có thể drop fill/risk events | Phân loại event trước khi drop: luồng causal không được drop; nếu overflow → `halt` thay vì `popleft` |
| 4 | Chính sách out-of-order được định nghĩa và test per stream | ROB window xử lý out-of-order nhưng không có policy rõ sau khi window hết hạn | Định nghĩa: event ngoài cửa sổ ROB bị accept muộn, reject, hay trigger `degraded`; thêm test cho từng trường hợp |

### Nhóm 2: Tính Bền Vững State (Persistence & Replay)

| # | Điểm kiểm tra | Trạng thái hiện tại | Hành động cần thiết |
|---|---------------|---------------------|---------------------|
| 5 | Mọi mutation state trọng yếu (position, order, PnL, balance) phải đi qua WAL trước side effect | Một số mutation path có thể bypass WAL (ví dụ: reconciler direct-write vào state) | Audit toàn bộ write path; đảm bảo không có `state.update_*()` nào thiếu WAL entry tương ứng |
| 6 | Replay từ snapshot + WAL luôn rebuild ra cùng logical state | Replay được implement nhưng chưa có invariant test xác nhận | Viết test: inject fault sau mỗi WAL entry type → restart → assert state == pre-fault state |
| 7 | WAL rotation đảm bảo không mất entry khi swap | `rotate_wal()` atomic swap nhưng caller chịu trách nhiệm archive WAL cũ | Thêm bước verify trong rotate: new WAL có đủ active order snapshot trước khi swap; test crash-during-rotate |

### Nhóm 3: Tính Đồng Thời (Concurrency)

| # | Điểm kiểm tra | Trạng thái hiện tại | Hành động cần thiết |
|---|---------------|---------------------|---------------------|
| 8 | SPSC ring buffer có runtime enforcement single writer / single reader | SPSC là assumption thiết kế, không có runtime guard | Thêm `assert` / `threading.get_ident()` guard tại `produce()` và `consume()` entry points; fail-fast nếu vi phạm |
| 9 | OCC trong `pnl_tracker.py` đảm bảo operation idempotent và retry-safe | ĐÃ FIX: Đọc version/factor dưới scale_lock và PnLAggregator giữ scale_lock khi downgrade | Verify: mọi retry trong OCC loop có thể áp lại mà không có side effect; stress test với concurrent scale change |
| 10 | Thread + asyncio ordering được document và stress test | Thread 2 nhận từ ring buffer; event arrival order ≠ request completion order | Document rõ: thread 1 push → thread 2 pop là FIFO; nhưng HTTP response ordering không đảm bảo; test race condition |

### Nhóm 4: Kế Toán (Accounting)

| # | Điểm kiểm tra | Trạng thái hiện tại | Hành động cần thiết |
|---|---------------|---------------------|---------------------|
| 11 | PnL multi-asset được normalize về base currency trước khi cộng dồn | `PnLAggregator` hiện dùng integer-scaled arithmetic nhưng không có lớp currency normalization | Thêm `CurrencyNormalizer`: convert mọi PnL về USDT bằng mark price; test với portfolio có BTC-margined + USDT-margined |
| 12 | Funding sign convention được verify và test per exchange | `BINANCE/BYBIT` negate, `OKX` pass-through đã implement trong `FundingCollector` | Thêm exchange mock test với known funding payment: assert sign sau normalization; document trong `universe.yaml` |
| 13 | Fee model (maker/taker/rebate/inverse product) được test so với exchange statement | Fee anomaly detection có (fee > 1% notional → warning) nhưng không có cross-check với exchange API | Viết reconciliation test: sum fees trong WAL vs sum fees từ exchange `/api/v1/income`; alert khi diverge > threshold |

### Nhóm 5: Kill Switch & State Machine (Risk Formalization)

| # | Điểm kiểm tra | Trạng thái hiện tại | Hành động cần thiết |
|---|---------------|---------------------|---------------------|
| 14 | Global halt flag được kiểm tra atomic bởi mọi code path có thể phát lệnh | ĐÃ FIX: Bổ sung cơ chế fallback tự ghi system.KILLED từ event loop gateway khi ZMQ gửi lỗi | Thêm `threading.Event` / `multiprocessing.Event` `_halt_flag`; mọi `protected_call()` check flag trước khi acquire RateLimiter |
| 15 | Kill switch trigger → toàn bộ execution path dừng, không có lệnh mới | Watchdog trigger market-close nhưng không đảm bảo ExecutionGateway ring buffer đã drain | Test: trigger kill → assert không có `ORDER_SENT` WAL entry nào sau thời điểm kill |
| 16 | State machine `INIT → SYNC → RUNNING → HALT` được enforce, không thể skip | State transitions không được formalize; ví dụ có thể `RUNNING` khi WAL replay chưa xong | Implement `EngineState` enum; mọi transition phải qua method rõ ràng với pre/post condition check |
| 17 | Kill consistency: không có execution path nào có thể phát lệnh khi engine ở state `HALT` | Risk Gate check state nhưng các path khác (reconciler, self-heal) không kiểm tra `HALT` | Audit mọi `adapter.place_order()` call site; bọc trong `_guard_halt()` trước khi gọi |

### Nhóm 6: Bất Biến & Kiểm Thử (Integrity & Testing)

| # | Điểm kiểm tra | Trạng thái hiện tại | Hành động cần thiết |
|---|---------------|---------------------|---------------------|
| 18 | Runtime invariant checks cho order, position, balance, PnL chạy ở mỗi checkpoint | `verify_integrity()` implement nhưng không được schedule chạy định kỳ | Gọi `verify_integrity()` sau mỗi WAL checkpoint và sau mỗi reconciler cycle; log violation với severity |
| 19 | Tần suất reconciliation với exchange tăng tự động khi phát hiện divergence | Reconciler chạy cố định 60s; drift detection chạy 60s | Implement adaptive schedule: 1 violation → 30s; 2 violations → 10s; 3 violations → kill switch |
| 20 | Chaos/fault injection và replay stress test đã pass | `test_chaos_risk.py` có 4 adversarial scenarios; chưa có restart-mid-replay test | Thêm: kill-during-WAL-write, restart-during-reconciliation, clock-jump test, 24h stream replay, concurrent-fills race stress |

> Danh sách này không phải "nice to have" — mỗi mục đại diện cho một failure mode đã được xác định qua code review. Triển khai Kairos cho tiền thật trước khi tất cả 20 điểm được kiểm tra và pass là rủi ro có thể định lượng được.

---

> **KAIROS v3.2** — *"Thời điểm quyết định"* (Καιρός)
>
> Mọi microsecond đều có giá trị. Mọi dòng code đều có mục đích.
>
> **Tác giả:** P Vinh ([ppvinh1513@gmail.com](mailto:ppvinh1513@gmail.com))