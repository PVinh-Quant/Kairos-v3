# KAIROS v3.8 — Mục Tiêu Hệ Thống

**Phiên bản:** 1.5  
**Ngày:** 2026-07-01  
**Trạng thái:** Tài liệu tham khảo — Tổng quan công khai *(các tham số vận hành/vốn cụ thể đã được lược bỏ)*

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
- **Số symbols đồng thời:** một rổ nhỏ, mở rộng dần khi scale
- **Order size:** *(giá trị vốn/lệnh cụ thể — nội bộ, đã lược bỏ)*
- **Capacity mục tiêu mỗi alpha:** *(quy mô vốn cụ thể — nội bộ, đã lược bỏ; ràng buộc bằng L2 depth thực — M-R4 T1.4 / M-R5 T2.6)*
- **Leverage:** thấp, mặc định 1x (tùy symbol config)

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

### 4.3 Khôi Phục Sau Sự Cố & Kế Hoạch Xử Lý Thảm Họa (Disaster Recovery)

Hệ thống tuân thủ quy chuẩn xử lý sự cố fail-safe nghiêm ngặt dưới sự giám sát độc lập của Watchdog và quy trình khởi động 12 bước của Live Runner.

| Loại Sự Cố / Thảm Họa | Hành Động Phục Hồi | SLA Khôi Phục |
|-----------------------|--------------------|--------------|
| **Tiến trình chính bị sập (Process Crash)** | Trình quản lý tiến trình (supervisor/systemd) tự động khởi động lại, kích hoạt chuỗi 12 bước SRE-grade check (xác minh cờ system.KILLED, đối soát ví và lệnh). | < 2 phút |
| **Hỏng Nhật ký WAL (WAL Corruption)** | Dừng hệ thống lập tức (Halt). Yêu cầu kỹ sư can thiệp thủ công quét CRC32 của WAL, xác định điểm corrupt, và cắt bỏ (truncate) phần đuôi hỏng. Không tự phục hồi tự động để tránh sai lệch trạng thái. | N/A (Manual Review) |
| **Mất kết nối Sàn (Exchange Disconnection)** | Đóng băng toàn bộ hoạt động đặt lệnh mới. Tự động kết nối lại WebSocket/REST bằng thuật toán Exponential Backoff với Jitter ngẫu nhiên. | < 30s |
| **Bất đồng bộ Vị thế (Position Divergence > $10)** | Phát hiện bởi chu kỳ đối soát 120s. Đánh dấu trạng thái giảm vị thế (reduce-only), gửi cảnh báo CRITICAL Telegram. Tự động sửa chữa thông qua hàm `force_override_state()`. | < 120s |
| **Mất đồng bộ thời gian (Clock Skew > 1ms)** | `time_validator.py` đọc CLOCK_REALTIME mức thấp phát hiện skew vượt ngưỡng -> đặt `capital_multiplier = 0.0` ngay lập tức để chặn Risk Gate đặt lệnh mới. | < 1ms (Tức thời) |
| **Nghẽn mạng mạng diện rộng (Network Partition)** | Watchdog phát hiện mất kết nối kép (Dual Miss) -> ghi cờ `system.KILLED` ở 0ms -> gọi khẩn cấp `emergency_flattener.py` qua connection pool REST biệt lập để hủy toàn bộ lệnh và đóng MARKET các vị thế mở. | < 5s |
| **Sàn giao dịch đóng băng rút tiền / sập (Solvency Run)** | RG-17 phát hiện tỷ lệ rút tiền lỗi liên tiếp >= 3 lần VÀ giá token sàn giảm > 30% so với BTC -> Gửi cảnh báo CRITICAL -> Tạm dừng mọi vị thế và khóa giao dịch. | < 10s |

---

## 5. Mục Tiêu Rủi Ro & Cơ Chế Kiểm Soát

### 5.1 Hard Limits (Kiểm soát cứng bởi Pre-trade Risk Gate RG-1 đến RG-17)

Toàn bộ các quy tắc kiểm soát rủi ro pre-trade được thực thi trong `risk_gate.py` dưới dạng các cửa chặn logic có độ trễ cực thấp (< 50µs) và cấm override ở runtime:

| Quy Tắc | Mô Tả & Giới Hạn Cứng | Mã Spec |
|---------|-----------------------|---------|
| **Hạn mức Lệnh (Order Rate Burst)** | Tối đa 60 lệnh mỗi phút (burst) và 600 lệnh mỗi giờ (sustained avg ≤ 10/phút). | RG-1 |
| **Đòn bẩy gộp tối đa (Gross Leverage)** | Tổng đòn bẩy gộp (gross leverage) không vượt quá 200% Equity (2.0x). | RG-2 |
| **Tỷ trọng Vị thế Đơn lẻ (Max Exposure)** | Vị thế tối đa cho một symbol đơn lẻ không vượt quá 20% Net Asset Value (NAV). | RG-3 |
| **Giới hạn Thua lỗ Ngày (Daily Loss Limit)** | Thua lỗ thực tế + unrealized PnL ngày vượt quá 3.0% Equity -> Halt giao dịch ngày đó. | RG-4 |
| **Giới hạn Thua lỗ Tuần (Weekly Loss Limit)** | Thua lỗ thực tế + unrealized PnL tuần vượt quá 10.0% Equity -> Halt giao dịch. | RG-4 |
| **Ngưỡng Drawdown Dừng (Auto-Halt Drawdown)** | Drawdown từ đỉnh tài sản (peak equity) đạt 15.0% -> Halt giao dịch, hủy lệnh. | RG-4 |
| **Ngưỡng Drawdown Khẩn Cấp (Emergency Drawdown)** | Drawdown từ đỉnh tài sản đạt 20.0% -> Watchdog kích hoạt xả khẩn cấp toàn bộ vị thế. | RG-4 |
| **Giám sát Stale State (Stale Guard)** | Cấm đặt lệnh nếu trạng thái Risk Gate cũ quá 500ms hoặc thời gian cập nhật bằng 0. | RG-12 |
| **Công tắc Dead-Man Switch** | Tự động hủy lệnh nếu mất kết nối luồng chính quá 120s, giảm 50% vị thế ở 300s, flatten ở 600s. | RG-15 |
| **Giám sát ADL Rank (Auto-Deleveraging)** | Tự động hạ vị thế nếu tài khoản lọt vào top 10% ADL queue của sàn. | RG-16 |
| **Giám sát Khả năng Thanh khoản Sàn** | Halt giao dịch sàn nếu phát hiện lỗi rút tiền kép hoặc depeg token sàn (>30% vs BTC). | RG-17 |

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

1. [x] **CR-01** — Ring buffer SPSC version protocol: sửa để system thực sự xử lý signals (ĐÃ FIX)
2. [x] **CR-02** — WAL auto-rotation: implement trigger để WAL không bao giờ full trong runtime (ĐÃ FIX)
3. [x] **CR-03** — RiskGate sync_state(): wire `ReconciliationAgent` vào live flow hoặc call `sync_state()` từ startup và định kỳ (ĐÃ FIX)

### 6.2 Trong Vòng 30 Ngày (P1)
- [x] HR-01: Implement `get_pressure()` và `submit_reconciliation_task()` trên ExecutionGateway (ĐÃ FIX)
- [x] HR-02: Fix OrderBook dict iteration race (per-symbol index hoặc global reader lock) (ĐÃ FIX - Lỗ hổng 1)
- [x] HR-03: Fix RiskGate `_pending_by_sym` race (ĐÃ FIX - Bảo vệ bởi threading.Lock)
- [x] HR-04: Fix Emergency Kill NOBLOCK drop — write `system.KILLED` từ `_publish_emergency_kill()` trong gateway (ĐÃ FIX - Lỗ hổng 3)
- [x] HR-05: Fix OrderPool leak khi WAL full (ĐÃ FIX)
- [x] HR-06: Implement startup cross-validation giữa DurableWAL và KairosState JSONL WAL — detect divergence khi restart trước khi trade (ĐÃ FIX)

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

- **Không HFT:** Không market-making, không latency arbitrage, không co-location. Khi đủ vốn, HFT là project riêng biệt được fund bởi KAIROS profits — không phải một Phase upgrade của KAIROS. Ngoại lệ duy nhất (lưu data, không phải build): M-H1 "Nghĩa vụ mid-freq" — lưu raw L2 + trade stream + own-order lifecycle logs từ bây giờ, vì calibration data không thu lại retroactively được.
- **Không trade dưới khung thời gian 30 phút:** Sub-30min = HFT territory. Minimum bar horizon = 30min (INV-STRATEGY.1). Giảm horizon không phải optimization — là scope creep sang project khác.
- **Không manual override positions:** Tất cả positions phải được mở và đóng bởi thuật toán. Manual override chỉ qua emergency flatten.
- **Không leverage cao:** Tối đa gross 2.0× (200% equity), mặc định 1x. KAIROS kiếm tiền từ alpha, không từ leverage.
- **Không trade khi không chắc:** Nếu state không confirmed, nếu risk gate stale, nếu exchange disconnected → KHÔNG trade. Bỏ lỡ cơ hội tốt hơn sai position.

---

*Tài liệu này là living document — cập nhật khi hệ thống đạt milestone hoặc khi objectives thay đổi dựa trên performance thực tế.*
