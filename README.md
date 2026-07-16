# PJM Sensor

[![hacs_badge](https://img.shields.io/badge/HACS-Default-orange.svg?style=for-the-badge)](https://github.com/custom-components/hacs)

A Home Assistant integration providing real-time PJM sensor data for monitoring zonal wholesale energy loads, forecasts, and predicting coincident system peaks. This integration leverages PJM's DataMiner 2 API for up-to-date insights.

**Version 2.2.0** upgrades coincident-peak curtailment control to **Cover5CP** (prefer on-hour arming with 5CP cover safety). Integration build tag: `grok_cover5cp_rc1`.

---

## Features

This integration uses PJM’s DataMiner 2 API to provide live and forecasted insight into grid conditions. It supports both basic monitoring and advanced predictive capabilities.

### 🔄 Real-Time & Forecast Load Sensors

- **System Load**: PJM-wide real-time load (updated every 5 minutes)
- **System Forecasts**:
  - 2-hour forecast (updated every 5 minutes)
  - Daily forecast (updated hourly)
- **Zonal Load & Forecasts**: Instantaneous and forecasted values for your selected PJM zone
- **Zonal LMP**: Hourly average Locational Marginal Price (LMP) for your zone

### 🔮 Coincident Peak Prediction (System & Zone)

- Predicts daily system or zonal peak load time and magnitude using:
  - Real-time derivative analysis (rate of change, acceleration)
  - 2-hour and daily forecasts
  - Kinematic modeling with adaptive time and magnitude biasing
- Tracks and stores the **top five daily peaks** across the season
- Designed to support **5CP Capacity PLC and Network Service PLC management** or load curtailment strategies

#### Cover5CP peak-hour control (2.2.0)

On **high-risk days**, `peak_hour_active` is driven by **Cover5CP**:

- Prefer first arm on the true peak hour (official “good” / armOK)
- Cover safety: first arm aimed at `[actual−2, actual]` peak hours (avoid late/miss)
- Sticky high-risk once seen (forecast drop mid-day does not un-arm cover)
- Single-hour active after first arm (limits hard false-positive multi-hour arms)
- Operational lock (hr90 path-A) can re-hit the true peak hour after lock engages
- Post-peak freeze: stop refine/arm when afternoon peak reference is more than **2 hours** past (reboot-safe; no fixed 8 PM cutoff)

High-risk latch (tighter than 2.1.x):

- `predicted_peak ≥ 0.98 × max(configured threshold, 5th-highest stored peak)` when five peaks are stored  
- Was **0.95** in 2.1.x (more false high-risk days)

#### Exposed Attributes

| Attribute | Description |
|----------|-------------|
| `predicted_peak` | Forecasted load in MW |
| `predicted_peak_time` | Timestamp of predicted peak |
| `observed_peak` | Highest observed real-time load today |
| `observed_peak_time` | Timestamp of observed peak |
| `peak_hour_active` | `true` during Cover5CP active curtailment window (high-risk days) |
| `high_risk_day` | `true` if day meets the high-risk latch (see above) |
| `saw_high_risk` | `true` if high-risk was seen earlier today (sticky) |
| `cover5cp_armed` | `true` after first Cover5CP arm today |
| `cover5cp_first_arm_h` | Local hour of first arm (diagnostics) |
| `cover5cp_peak_hint_max` | Running latest peak-hour hint (diagnostics) |
| `prediction_time_locked` | Operational lock engaged |
| `integration_version` | Build tag (e.g. `grok_cover5cp_rc1`) |
| `observed_roc` / `observed_acc` | Real-time load rate-of-change and acceleration |
| `forecasted_roc` / `forecasted_acc` | Forecasted load rate-of-change and acceleration |
| `time_bias`, `magnitude_bias` | Adaptive corrections from snapshot vs actual learning |
| `top_five_peaks` | List of the top five daily peaks observed this season |

> All timestamps are in ISO 8601 format and localized to your Home Assistant instance's time zone.

---

### ⚙️ Configuration Settings

- `peak_threshold_zone` / `peak_threshold_system`  
  Minimum load (in MW) for the high-risk bar (combined with 5th historical peak).  
  Defaults (2.2.0): **18,000** (zone), **150,000** (system) — aligned with 2026 summer peak levels (was 17k / 138k).

- `accuracy_threshold`  
  *(Reserved for future use.)* Intended for evaluating forecast confidence before triggering peak alerts.

---

### 🔑 API Key Behavior

- **With API Key**:
  - Full access to all sensors
  - Non-members may not exceed 6 data connections per minute
- **Without API Key**:
  - Limited to 3 sensor entities
  - For evaluation or testing only
  - Risk of IP ban

---

## Installation

1. In HACS, go to **HACS > Integrations**.
2. Click on **+ Explore & add custom repository**.
3. Add the repository:
   - **URL:** `https://github.com/jabbauer/PJM_Sensor`
   - **Category:** Integration
4. Search for **PJM Sensor** and install it (or update to **2.2.0**).
5. Restart Home Assistant.
6. Navigate to **Settings > Devices & Services** and add the **PJM Sensor** integration.
7. In the configuration flow:
   - **Select your utility zone.**
   - **Enter your API key** (optional). Without an API key, you may become IP banned by PJM. Utilize for initial testing only.
   - **Choose the sensor entities** you want to enable.
     - By default, **instantaneous_total_load**, **total_short_forecast**, and **total_load_forecast** are selected.
     - Without an API key, only up to 3 sensors can be selected.
     - Per PJM, Non-members may not exceed 6 data connections per minute.

Manual install: copy `custom_components/PJM_sensor` into your HA `config/custom_components/` folder and restart.

---

## Upgrading from 2.1.6 → 2.2.0

### What stays the same
- Same integration domain, sensors, and PJM API usage
- Curtailment automations can keep using attribute **`peak_hour_active`** (still `true` / `false`)
- Top-five peak history, forecasts, and config entry structure are unchanged
- Your existing **peak threshold** values are **kept** until you reconfigure the integration

### What changes (behavior)

| Topic | **2.1.6** | **2.2.0** |
|--------|-----------|-----------|
| When is `peak_hour_active` true? | On a high-risk day, during the **clock hour of the live predicted peak** (forecast can walk the hour later in the day) | On a high-risk day, via **Cover5CP**: prefers the true peak hour, waits rather than arming too early, aims not to arm **after** the peak, then usually **one hour** of active |
| High-risk day | Predicted peak ≥ **95%** of max(threshold, 5th stored peak); can turn off mid-day if the forecast drops | ≥ **98%** of that bar; **sticky** once seen (stays high-risk for the rest of the day) |
| Default thresholds (new installs only) | ~17k zone / 138k system (config flow sometimes used 16.5k / 140k) | **18,000** zone / **150,000** system |
| After the peak / late reboot | Forecast could keep walking; reboot late in the evening could invent odd peaks | **Freeze** refine/arm when the afternoon peak reference is **more than 2 hours** past |
| Diagnostics | Basic peak / ROC / bias attrs | Adds `saw_high_risk`, `cover5cp_armed`, `cover5cp_first_arm_h`, `prediction_time_locked`, `integration_version` (`grok_cover5cp_rc1`) |

### What you should expect after updating
1. **Fewer “high risk” days** that are only loosely near the 5th peak (stricter 98% latch + higher defaults for new installs).
2. **`peak_hour_active` may not match the predicted-peak hour** when the forecast walks later — Cover5CP can wait, then arm on-hour, or arm once and stay only that hour.
3. **Fewer full misses** of the peak hour on hard days (offline board: stock late/miss ~29% vs Cover5CP **0%** on its HR set); possible **more single-hour early** arms (hard FP tradeoff).
4. After a true peak, the entity should **stop chasing** a new evening prediction as aggressively (post-peak freeze).

### What you should do after updating
1. Install **2.2.0**, restart Home Assistant.
2. Confirm the coincident-peak entity shows `integration_version`: **`grok_cover5cp_rc1`**.
3. Optionally open the integration options and set thresholds for your zone/system (recommended if you still have legacy 17k / 138k and want the 2026-oriented floors).
4. Keep existing automations on `peak_hour_active`; re-test on the next hot day.
5. No need to delete `.storage` peak history unless you are fixing a known bad top-five entry.

### Plain-English: old vs new active window
- **2.1.6:** “If today looks high-risk, turn active for whatever hour the **prediction currently points at**.”
- **2.2.0:** “If today looks high-risk, turn active when Cover5CP thinks it’s the **right curtailment hour** — prefer on-hour, avoid late, limit multi-hour false runs.”

---

## 💡 Example: Load Curtailment Automation

```yaml
- alias: Curtail HVAC During PJM Peak
  trigger:
    - platform: state
      entity_id: sensor.coincident_peak_prediction_system
      attribute: peak_hour_active
      to: "true"
  action:
    - service: climate.set_temperature
      target:
        entity_id: climate.house
      data:
        temperature: 78
```

---

## Historic board (controller grade, offline)

Replay on the 2026 summer development board (same forecast Sim path for both policies):

| Policy | HR days | Cover `[act−2,act]` | On-hour | Late+miss | Early&gt;2h |
|--------|--------:|--------------------:|--------:|----------:|-----------:|
| **2.1.6 stock** (pred-hour active, HR @ 0.95) | 14 | 64% | 50% | 29% | 7% |
| **2.2.0 Cover5CP** (HR @ 0.98 sticky) | 12 | **100%** | **83%** | **0%** | **0%** |
| Morning DA (on Cover HR set) | 12 | 83% | 58% | 8% | 8% |

Head-to-head on shared HR days: Cover better **4**, stock better **0**, ties **8**.

Official-style (score_day on HR rows): Cover good **~67%** vs stock **~50%** (Cover trades some hard-FP for far fewer misses).

---

## Changelog

### 2.2.0
- **Cover5CP** `peak_hour_active` policy (prefer on-hour + cover safety; sticky HR; single-hour active)
- High-risk fraction **0.98** (was 0.95)
- Default peak thresholds **18,000 / 150,000 MW** (was 17k / 138k)
- Post-peak freeze at **peak reference + 2 hours** (reboot hygiene; no fixed 20:00)
- hr90-style operational lock (path-A, short-not-later)
- Diagnostics: `cover5cp_*`, `saw_high_risk`, `integration_version`
- Improved bias learning from decision-hour snapshots (not post-lock frozen pred)

### 2.1.6
- Fixed: Resolved AttributeError: `_last_reset_date` crash on startup by initializing peak history dates immediately.

---

## Disclaimer

This integration is not affiliated with, nor supported by, PJM or any PJM member. Use at your own risk. Data from PJM's DataMiner 2 API is for internal use only. Redistribution of this data or any derivative information is prohibited unless you are a PJM member.
