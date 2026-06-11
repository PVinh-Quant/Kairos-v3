# KAIROS v3 — Mục Tiêu Hệ Thống

**Phiên bản:** 1.0  
**Ngày:** 2026-05-24  
**Trạng thái:** Draft — Nội bộ

---

## 1. Tầm Nhìn

KAIROS là hệ thống giao dịch thuật toán tần suất trung bình (mid-frequency) được xây dựng để:

- Vận hành **24/7 không cần can thiệp thủ công** trong điều kiện thị trường bình thường
- Thực thi **tín hiệu từ mô hình ML (ONNX)** với độ trễ nội bộ dưới 1ms từ nhận tín hiệu đến gửi lệnh
- Bảo vệ vốn thông qua **nhiều lớp kiểm soát rủi ro độc lập**, không có single point of failure

KAIROS không phải hệ thống HFT. Mục tiêu là alpha từ edge về **dự đoán chiều giá trong khung thời gian phút đến giờ**, không phải market-making hay latency arbitrage.

---

## 2. Mục Tiêu Giao Dịch

### 2.1 Hiệu Suất
| Chỉ Số | Mục Tiêu | Ngưỡng Dừng |
|--------|----------|-------------|
| Sharpe Ratio (annualized) | ≥ 1.5 | < 0.5 trong 30 ngày liên tiếp |
| Max Drawdown | ≤ 15% equity | > 15% → halt (RG-4); > 20% → emergency flatten (watchdog) |
| Win Rate | ≥ 52% | < 45% trong 200 giao dịch |
| PnL / Commission | ≥ 3:1 | < 1.5:1 trong 7 ngày |
| Daily Loss Limit | ≤ 2% equity | > 3% → dừng ngày đó (RG-4); weekly > 10% → halt |

### 2.2 Chất Lượng Thực Thi
| Chỉ Số | Mục Tiêu |
|--------|----------|
| Slippage trung bình | ≤ 0.05% per order |
| Fill rate — taker orders | ≥ 95% fill trong 500ms |
| Fill rate — maker orders (BTC/ETH) | ≥ 55% target (conservative scenario, per M-R7 Scenario B) |
| Order rejection rate | < 1% (từ exchange, không phải risk gate) |
| Reconciliation gap | 0 (mọi fill được ghi nhận trong 60s) |

### 2.3 Phạm Vi Hoạt Động
- **Sàn giao dịch:** Binance Futures, Bybit, OKX
- **Công cụ:** Crypto perpetual futures (USDT-margined)
- **Số symbols đồng thời:** 5–15 (tối đa 30 khi scale)
- **Order size:** $50–$5,000 USDT per order
- **Capacity mục tiêu mỗi alpha:** $500K–$2M (research viability tối thiểu $100K — M-R4 T1.4; binding gate L2 depth thực — M-R5 T2.6)
- **Leverage:** 1x–5x (mặc định 1x, tùy symbol config)

---

## 3. Mục Tiêu Kỹ Thuật

### 3.1 Độ Trễ (Latency)
| Giai Đoạn | Mục Tiêu | Ngưỡng Cảnh Báo |
|-----------|----------|-----------------|
| Nhận tín hiệu → gửi lệnh (hot path) | ≤ 500µs | > 2ms |
| Network latency (signal → exchange ACK) | ≤ 50ms | > 200ms |
| Tick nhận → exit coordinator | ≤ 5ms | > 20ms |
| Reconciliation cycle | ≤ 30s | > 120s |

### 3.2 Độ Tin Cậy
| Chỉ Số | Mục Tiêu |
|--------|----------|
| Uptime (excl. planned maintenance) | ≥ 99.5% per month |
| Mean Time To Recovery (MTTR) | ≤ 5 phút cho lỗi auto-healable |
| Zero position divergence | Không có gap > $10 USDT giữa KAIROS và sàn quá 60s |
| WAL durability | Không mất fill data sau crash và restart |
| Startup data integrity | 100% — không trade khi state không confirmed |

### 3.3 Bảo Mật Vốn (Capital Safety)
**Các lớp bảo vệ theo thứ tự ưu tiên:**

1. **Pre-trade Risk Gate** — kiểm tra trước mỗi lệnh: max position, max daily loss, drawdown, order rate
2. **ExecutionRiskEngine** — quản lý exposure USDT đang pending, ngăn over-commit
3. **Backpressure pipeline** — ring buffer lag, flow pressure, circuit breaker
4. **Watchdog (out-of-process)** — kill switch độc lập, không phụ thuộc vào tiến trình chính
5. **Exchange-level stops** — stop loss orders, exchange circuit breakers

Không có lớp nào được tắt hoặc bypass trong production.

### 3.4 Khả Năng Mở Rộng (Scalability)
| Chiều | Hiện tại | Mục Tiêu 12 tháng |
|-------|----------|-------------------|
| Symbols | 10 | 30 |
| Exchanges | 3 | 5 |
| Orders/ngày | ~200 | ~1,000 |
| WAL lifespan | ~36 phút @ 10 orders/s | Không giới hạn (auto-rotation) |
| WS connections | 20 | Tối đa 10 (multiplexed) |

---

## 4. Mục Tiêu Vận Hành

### 4.1 Monitoring và Alerting
| Event | Kênh | Thời Gian Phản Hồi |
|-------|------|-------------------|
| Kill switch triggered | Telegram CRITICAL + email | Ngay lập tức |
| Position drift > $10 USDT | Telegram HIGH | < 60s |
| WAL utilization > 80% | Telegram MEDIUM | < 5 phút |
| Exchange connection loss > 10s | Telegram HIGH | Ngay lập tức |
| PnL integrity violation | Telegram CRITICAL | Ngay lập tức |
| Daily loss > 1.5% | Telegram HIGH | Ngay lập tức |

**Nguyên tắc:** Mọi trạng thái bất thường phải sinh ra alert; không có "silent degraded mode".

### 4.2 Khởi Động và Dừng
**Startup checklist (auto-verified):**
- [ ] Tất cả WAL files tồn tại và không bị corrupt (CRC32 valid)
- [ ] State replay thành công — không có integrity violations
- [ ] RiskGate nhận state update từ exchange (sync_state đã được gọi)
- [ ] Tất cả exchange connections established và authenticated
- [ ] Watchdog process đang chạy và đang nhận heartbeat
- [ ] Không có `system.KILLED` file chưa được acknowledged

Nếu bất kỳ check nào fail → abort startup, không trade.

**Graceful shutdown:**
1. Dừng nhận tín hiệu mới
2. Chờ tất cả pending orders được filled hoặc cancelled (timeout: 30s)
3. Archive session data (PnL, trade history, performance metrics)
4. Ghi WAL checkpoint
5. Notify watchdog dừng monitoring
6. Exit clean

### 4.3 Khôi Phục Sau Sự Cố
| Loại Sự Cố | Hành Động | SLA Khôi Phục |
|------------|-----------|---------------|
| Process crash (unexpected) | Auto-restart qua supervisor | < 2 phút |
| WAL corruption | Halt + manual review | N/A (không auto-recover) |
| Exchange disconnection | Auto-reconnect với exponential backoff + jitter | < 30s |
| Position divergence confirmed | force_override_state() + alert | < reconciliation_interval |
| Kill switch triggered | Flatten tất cả positions, halt | < 5 phút |

---

## 5. Mục Tiêu Rủi Ro

### 5.1 Hard Limits (Không Thể Override)
| Tham Số | Giới Hạn |
|---------|----------|
| Max single position size (per symbol) | 20% portfolio (M-L5 RG-3) |
| Max gross leverage | 200% equity = 2.0× (M-L5 RG-2, M-L4) |
| Max daily loss | 3% equity → RG-4 halt |
| Max weekly loss | 10% equity → RG-4 halt |
| Max drawdown từ peak (auto-halt) | 15% equity → RG-4 halt |
| Max drawdown từ peak (emergency flatten) | 20% equity → watchdog kill |
| Max orders/phút (burst) | 60 |
| Max orders/giờ (sustained avg ≤ 10/phút) | 600 |

### 5.2 Soft Limits (Alert + Review)
| Tham Số | Ngưỡng |
|---------|--------|
| Single position > 10% equity | Alert, tiếp tục |
| Gross leverage > 150% equity | Alert, tiếp tục |
| Daily loss > 1.5% | Alert + giảm size 50% |
| Drawdown > 10% | Alert + review strategy |

### 5.3 Nguyên Tắc Rủi Ro
- **Rủi ro không đối xứng:** Một giao dịch thua không được xóa nhiều hơn những gì 3 giao dịch thắng kiếm được
- **Không martingale:** Không tăng size sau khi thua
- **Thoát có kế hoạch:** Mọi vị thế phải có exit strategy được xác định trước khi mở
- **Không overnight naked:** Các vị thế không hedge qua đêm phải có hard stop

---

## 6. Mục Tiêu Phát Triển

### 6.1 Trước Khi Live (P0 — BLOCKER)
Những vấn đề sau phải được fix trước khi bất kỳ live trading nào:

1. **CR-01** — Ring buffer SPSC version protocol: sửa để system thực sự xử lý signals
2. **CR-02** — WAL auto-rotation: implement trigger để WAL không bao giờ full trong runtime
3. **CR-03** — RiskGate sync_state(): wire `ReconciliationAgent` vào live flow hoặc call `sync_state()` từ startup và định kỳ

### 6.2 Trong Vòng 30 Ngày (P1)
- HR-01: Implement `get_pressure()` và `submit_reconciliation_task()` trên ExecutionGateway
- HR-02: Fix OrderBook dict iteration race (per-symbol index hoặc global reader lock)
- HR-03: Fix RiskGate `_pending_by_sym` race (asyncio.Lock)
- HR-04: Fix Emergency Kill NOBLOCK drop — write `system.KILLED` từ `_publish_emergency_kill()` trong gateway (hiện chỉ được written bởi `safe_hard_kill()` trong live_runner; nếu NOBLOCK send bị drop khi SNDHWM đầy, không có fallback durability)
- HR-05: Fix OrderPool leak khi WAL full
- HR-06: Implement startup cross-validation giữa DurableWAL và KairosState JSONL WAL — detect divergence khi restart trước khi trade (full unified WAL architecture là P2; đây là detection safeguard cho P1)

### 6.3 Trong Vòng 90 Ngày (P2)
- Unified WAL architecture (Theme 2 trong audit report)
- RiskGate push model — KairosState → RiskGate automatic sync (Theme 3)
- WebSocket connection multiplexing cho Binance (Theme 5)
- Full observability: ring lag, WAL utilization trend, RiskGate state age (Theme 6)

### 6.4 Mục Tiêu 12 Tháng
- Hệ thống đã chạy live ≥ 6 tháng liên tục không có critical incident
- Scale lên 30 symbols với kiến trúc hiện tại (sau fix scalability issues)
- P&L dương, Sharpe ≥ 1.5 trên rolling 6 tháng
- Tích lũy đủ vốn để fund giai đoạn HFT (dự án riêng biệt)

---

## 7. Định Nghĩa Thành Công

Hệ thống được coi là **thành công** khi:

1. **Kỹ thuật:** Vận hành 99.5%+ uptime, không có critical bugs, mọi fill được ghi nhận đúng, không có position divergence
2. **Rủi ro:** Chưa bao giờ vượt max daily loss limit, drawdown luôn trong kiểm soát, kill switch hoạt động đúng khi cần
3. **Alpha:** Sharpe ≥ 1.5, win rate ≥ 52%, PnL/commission ≥ 3:1 trên rolling 30 ngày
4. **Vận hành:** Không cần manual intervention ngoài business hours; tất cả anomalies được detect và alert trong < 60s

Hệ thống **KHÔNG thành công** nếu:

- Có position divergence > $50 USDT kéo dài > 5 phút mà không được detect
- Kill switch fail khi được trigger
- Bất kỳ fill nào bị mất vĩnh viễn (không được reconcile sau restart)
- System trade khi RiskGate có stale state (updated_ns == 0 hoặc state > 500ms cũ)

---

## 8. Những Điều KAIROS KHÔNG Làm

Để tránh scope creep và duy trì tính ổn định:

- **Không HFT:** Không market-making, không latency arbitrage, không co-location. Khi đủ vốn, HFT là project riêng biệt được fund bởi KAIROS profits — không phải một Phase upgrade của KAIROS.
- **Không trade dưới khung thời gian 30 phút:** Sub-30min = HFT territory. Minimum bar horizon = 30min (INV-STRATEGY.1). Giảm horizon không phải optimization — là scope creep sang project khác.
- **Không manual override positions:** Tất cả positions phải được mở và đóng bởi thuật toán. Manual override chỉ qua emergency flatten.
- **Không leverage cao:** Tối đa gross 2.0× (200% equity), mặc định 1x. KAIROS kiếm tiền từ alpha, không từ leverage.
- **Không trade khi không chắc:** Nếu state không confirmed, nếu risk gate stale, nếu exchange disconnected → KHÔNG trade. Bỏ lỡ cơ hội tốt hơn sai position.

---

*Tài liệu này là living document — cập nhật khi hệ thống đạt milestone hoặc khi objectives thay đổi dựa trên performance thực tế.*
