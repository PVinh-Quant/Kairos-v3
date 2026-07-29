> 📖 **[Phiên bản Tiếng Việt / Vietnamese Version](muc_tieu_he_thong.md)**

# KAIROS v3.8 — System Objectives

**Version:** 1.5  
**Date:** 2026-07-01  
**Status:** Deployed — Internal

---

## 1. Vision

KAIROS is a mid-frequency algorithmic trading system built to:

- Operate **24/7 without manual intervention** under normal market conditions
- Execute **signals from ML models (ONNX)** with internal latency under 1ms from signal reception to order submission
- Protect capital through **multiple independent risk control layers**, with no single point of failure

KAIROS is not an HFT system. The objective is alpha from an edge in **price direction prediction over minute-to-hour timeframes**, not market-making or latency arbitrage.

---

## 2. Trading Objectives

### 2.1 Performance
| Metric | Target | Stop Threshold |
|--------|--------|----------------|
| Sharpe Ratio (annualized) | ≥ 1.5 | < 0.5 for 30 consecutive days |
| Max Drawdown | ≤ 15% equity | > 15% → halt (RG-4); > 20% → emergency flatten (watchdog) |
| Win Rate | ≥ 52% | < 45% over 200 trades |
| PnL / Commission | ≥ 3:1 | < 1.5:1 over 7 days |
| Daily Loss Limit | ≤ 2% equity | > 3% → halt for the day (RG-4); weekly > 10% → halt |

### 2.2 Execution Quality
| Metric | Target |
|--------|--------|
| Average slippage | ≤ 0.05% per order |
| Fill rate — taker orders | ≥ 95% fill within 500ms |
| Fill rate — maker orders (BTC/ETH) | ≥ 55% target (conservative scenario, per M-R7 Scenario B) |
| Order rejection rate | < 1% (from exchange, not risk gate) |
| Reconciliation gap | 0 (all fills recorded within 60s) |

### 2.3 Operational Scope
- **Exchanges:** Binance Futures, Bybit, OKX
- **Instruments:** Crypto perpetual futures (USDT-margined)
- **Concurrent symbols:** 5–15 (up to 30 when scaling)
- **Order size:** $50–$5,000 USDT per order
- **Target capacity per alpha:** $500K–$2M (research viability minimum $100K — M-R4 T1.4; binding gate actual L2 depth — M-R5 T2.6)
- **Leverage:** 1x–5x (default 1x, per symbol config)

---

## 3. Technical Objectives

### 3.1 Latency
| Stage | Target | Alert Threshold |
|-------|--------|-----------------|
| Signal received → order sent (hot path) | ≤ 500µs | > 2ms |
| Network latency (signal → exchange ACK) | ≤ 50ms | > 200ms |
| Tick received → exit coordinator | ≤ 5ms | > 20ms |
| Reconciliation cycle | ≤ 30s | > 120s |

### 3.2 Reliability
| Metric | Target |
|--------|--------|
| Uptime (excl. planned maintenance) | ≥ 99.5% per month |
| Mean Time To Recovery (MTTR) | ≤ 5 minutes for auto-healable failures |
| Zero position divergence | No gap > $10 USDT between KAIROS and exchange for > 60s |
| WAL durability | No fill data lost after crash and restart |
| Startup data integrity | 100% — no trading when state is unconfirmed |

### 3.3 Capital Safety
**Protection layers in priority order:**

1. **Pre-trade Risk Gate** — pre-order checks: max position, max daily loss, drawdown, order rate
2. **ExecutionRiskEngine** — manages pending USDT exposure, prevents over-commit
3. **Backpressure pipeline** — ring buffer lag, flow pressure, circuit breaker
4. **Watchdog (out-of-process)** — independent kill switch, not dependent on main process
5. **Exchange-level stops** — stop loss orders, exchange circuit breakers

No layer may be disabled or bypassed in production.

### 3.4 Scalability
| Dimension | Current | 12-Month Target |
|-----------|---------|-----------------|
| Symbols | 10 | 30 |
| Exchanges | 3 | 5 |
| Orders/day | ~200 | ~1,000 |
| WAL lifespan | ~36 min @ 10 orders/s | Unlimited (auto-rotation) |
| WS connections | 20 | Max 10 (multiplexed) |

---

## 4. Operational Objectives

### 4.1 Monitoring & Alerting
| Event | Channel | Response Time |
|-------|---------|---------------|
| Kill switch triggered | Telegram CRITICAL + email | Immediate |
| Position drift > $10 USDT | Telegram HIGH | < 60s |
| WAL utilization > 80% | Telegram MEDIUM | < 5 min |
| Exchange connection loss > 10s | Telegram HIGH | Immediate |
| PnL integrity violation | Telegram CRITICAL | Immediate |
| Daily loss > 1.5% | Telegram HIGH | Immediate |

**Principle:** Every abnormal state must generate an alert; no "silent degraded mode".

### 4.2 Startup & Shutdown
**Startup checklist (auto-verified):**
- [ ] All WAL files exist and are not corrupt (CRC32 valid)
- [ ] State replay successful — no integrity violations
- [ ] RiskGate received state update from exchange (sync_state has been called)
- [ ] All exchange connections established and authenticated
- [ ] Watchdog process running and receiving heartbeat
- [ ] No unacknowledged `system.KILLED` file

If any check fails → abort startup, no trading.

**Graceful shutdown:**
1. Stop accepting new signals
2. Wait for all pending orders to be filled or cancelled (timeout: 30s)
3. Archive session data (PnL, trade history, performance metrics)
4. Write WAL checkpoint
5. Notify watchdog to stop monitoring
6. Exit clean

### 4.3 Disaster Recovery

The system follows strict fail-safe incident handling under independent Watchdog supervision and the Live Runner's 12-step startup procedure.

| Incident / Disaster Type | Recovery Action | Recovery SLA |
|--------------------------|-----------------|--------------|
| **Main process crash** | Process manager (supervisor/systemd) auto-restarts, triggering the 12-step SRE-grade check sequence (verify system.KILLED flag, reconcile wallet and orders). | < 2 min |
| **WAL corruption** | Immediate system halt. Requires manual engineer intervention to scan WAL CRC32, identify corruption point, and truncate the corrupted tail. No automatic recovery to prevent state divergence. | N/A (Manual Review) |
| **Exchange disconnection** | Freeze all new order placement. Auto-reconnect WebSocket/REST using Exponential Backoff with random Jitter. | < 30s |
| **Position divergence > $10** | Detected by 120s reconciliation cycle. Flag reduce-only state, send CRITICAL Telegram alert. Auto-repair via `force_override_state()`. | < 120s |
| **Clock skew > 1ms** | `time_validator.py` low-level CLOCK_REALTIME read detects skew exceeding threshold → sets `capital_multiplier = 0.0` immediately to block Risk Gate from placing new orders. | < 1ms (Instant) |
| **Wide-area network partition** | Watchdog detects dual miss → writes `system.KILLED` flag at 0ms → emergency calls `emergency_flattener.py` via isolated REST connection pool to cancel all orders, MARKET close open positions, and write `FLATTEN_LOCK` flag to hard-block Gateway & OrderBook from placing new orders. | < 5s |
| **Exchange solvency run / freeze** | RG-17 detects consecutive withdrawal failure rate ≥ 3 AND exchange token price drops > 30% vs BTC → Send CRITICAL alert → Pause all positions and lock trading. | < 10s |

---

## 5. Risk Objectives & Control Mechanisms

### 5.1 Hard Limits (Enforced by Pre-trade Risk Gate RG-1 through RG-17)

All pre-trade risk control rules are enforced in [risk_gate.py](file:///d:/The%20V/Kairos-v3/quan_tri_rui_ro/kiem_tra_truoc_lenh/risk_gate.py) as ultra-low-latency logic gates (< 50µs) that cannot be overridden at runtime:

| Rule | Description & Hard Limit | Spec Code |
|------|--------------------------|-----------|
| **Order Rate Burst** | Maximum 60 orders per minute (burst) and 600 orders per hour (sustained avg ≤ 10/min). | RG-1 |
| **Max Gross Leverage** | Total gross leverage must not exceed 200% Equity (2.0x). | RG-2 |
| **Single Position Max Exposure** | Maximum position for a single symbol must not exceed 20% Net Asset Value (NAV). | RG-3 |
| **Daily Loss Limit** | Realized + unrealized daily PnL exceeds 3.0% Equity → Halt trading for the day. | RG-4 |
| **Weekly Loss Limit** | Realized + unrealized weekly PnL exceeds 10.0% Equity → Halt trading. | RG-4 |
| **Auto-Halt Drawdown** | Drawdown from peak equity reaches 15.0% → Halt trading, cancel orders. | RG-4 |
| **Emergency Drawdown** | Drawdown from peak equity reaches 20.0% → Watchdog triggers emergency flatten of all positions. | RG-4 |
| **Stale State Guard** | Block orders if Risk Gate state is older than 500ms or update timestamp is 0. | RG-12 |
| **Dead-Man Switch** | Auto-cancel orders if main thread connection lost > 120s, reduce positions 50% at 300s, flatten at 600s. | RG-15 |
| **ADL Rank Monitor** | Auto-deleverage position if account enters top 10% of exchange ADL queue. | RG-16 |
| **Exchange Solvency Monitor** | Halt exchange trading if dual withdrawal failure or exchange token depeg (> 30% vs BTC) detected. | RG-17 |

### 5.2 Soft Limits (Alert + Review)
| Parameter | Threshold |
|-----------|-----------|
| Single position > 10% equity | Alert, continue |
| Gross leverage > 150% equity | Alert, continue |
| Daily loss > 1.5% | Alert + reduce size 50% |
| Drawdown > 10% | Alert + review strategy |

### 5.3 Risk Principles
- **Asymmetric risk:** A single losing trade must not erase more than what 3 winning trades earn
- **No martingale:** Never increase size after a loss
- **Planned exits:** Every position must have an exit strategy defined before opening
- **No overnight naked:** Unhedged overnight positions must have hard stops

---

## 6. Development Objectives

### 6.1 Before Go-Live (P0 — BLOCKER)
The following issues must be fixed before any live trading:

1. [x] **CR-01** — Ring buffer SPSC version protocol: fixed so system actually processes signals (FIXED)
2. [x] **CR-02** — WAL auto-rotation: implemented trigger so WAL never fills during runtime (FIXED)
3. [x] **CR-03** — RiskGate sync_state(): wired `ReconciliationAgent` into live flow or call `sync_state()` from startup and periodically (FIXED)

### 6.2 Within 30 Days (P1)
- [x] HR-01: Implement `get_pressure()` and `submit_reconciliation_task()` on ExecutionGateway (FIXED)
- [x] HR-02: Fix OrderBook dict iteration race (per-symbol index or global reader lock) (FIXED - Vulnerability 1)
- [x] HR-03: Fix RiskGate `_pending_by_sym` race (FIXED - Protected by threading.Lock)
- [x] HR-04: Fix Emergency Kill NOBLOCK drop — write `system.KILLED` from `_publish_emergency_kill()` in gateway (FIXED - Vulnerability 3)
- [x] HR-05: Fix OrderPool leak when WAL full (FIXED)
- [x] HR-06: Implement startup cross-validation between DurableWAL and KairosState JSONL WAL — detect divergence on restart before trading (FIXED)

### 6.3 Within 90 Days (P2)
- Unified WAL architecture (Theme 2 from audit report)
- RiskGate push model — KairosState → RiskGate automatic sync (Theme 3)
- WebSocket connection multiplexing for Binance (Theme 5)
- Full observability: ring lag, WAL utilization trend, RiskGate state age (Theme 6)

### 6.4 12-Month Objectives
- System has been running live ≥ 6 months continuously without critical incidents
- Scaled to 30 symbols with current architecture (after fixing scalability issues)
- Positive P&L, Sharpe ≥ 1.5 on rolling 6 months
- Accumulated sufficient capital to fund the HFT phase (separate project)

---

## 7. Definition of Success

The system is considered **successful** when:

1. **Technical:** Operating at 99.5%+ uptime, no critical bugs, all fills recorded correctly, no position divergence
2. **Risk:** Never exceeded max daily loss limit, drawdown always under control, kill switch functions correctly when needed
3. **Alpha:** Sharpe ≥ 1.5, win rate ≥ 52%, PnL/commission ≥ 3:1 on rolling 30 days
4. **Operational:** No manual intervention needed outside business hours; all anomalies detected and alerted within < 60s

The system is **NOT successful** if:

- Position divergence > $50 USDT persists > 5 minutes without being detected
- Kill switch fails when triggered
- Any fill is permanently lost (not reconciled after restart)
- System trades when RiskGate has stale state (updated_ns == 0 or state > 500ms old)

---

## 8. What KAIROS Does NOT Do

To avoid scope creep and maintain stability:

- **Not HFT:** No market-making, no latency arbitrage, no co-location. When capital is sufficient, HFT is a separate project funded by KAIROS profits — not a Phase upgrade of KAIROS. Only exception (data storage, not building): M-H1 "Mid-freq obligation" — store raw L2 + trade stream + own-order lifecycle logs from now, since calibration data cannot be collected retroactively.
- **No sub-30-minute timeframe trading:** Sub-30min = HFT territory. Minimum bar horizon = 30min (INV-STRATEGY.1). Reducing horizon is not optimization — it is scope creep into another project.
- **No manual position override:** All positions must be opened and closed by algorithm. Manual override only via emergency flatten.
- **No high leverage:** Maximum gross 2.0× (200% equity), default 1x. KAIROS makes money from alpha, not leverage.
- **No trading when uncertain:** If state is unconfirmed, if risk gate is stale, if exchange is disconnected → DO NOT trade. Missing an opportunity is better than a wrong position.

---

*This document is a living document — updated when the system reaches milestones or when objectives change based on actual performance.*
