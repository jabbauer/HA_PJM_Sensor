"""
PJM Sensor Integration — sensor_grok_release_candidate (Cover5CP)
----------------------------------------------------------------

Production release candidate: forecast/lock stack with Cover5CP
peak_hour_active policy. No shadow PMF / JSONL telemetry.

Cover5CP active policy:
  - Prefer on-hour first arm (official good / armOK)
  - Cover safety: first arm within [actual-2, actual] on high-risk days
  - hr90 operational lock (path-A, short-not-later)
  - Single-hour active after first arm (hard FP control); lock may re-hit peak
  - Sticky high-risk once seen (avoid mid-day HR drop -> miss)

High-risk latch:
  - predicted_peak >= HIGH_RISK_PEAK_FRACTION * max(user_threshold, 5th peak)
  - HIGH_RISK_PEAK_FRACTION = 0.98

Late-day / reboot hygiene:
  - Freeze when afternoon peak reference is > POST_PEAK_GRACE_HOURS (2h) past
  - Peak reference = latest of today observed max / predicted / DA (hours 12-21)
  - No fixed 20:00 freeze; last-resort only at local hour >= 22 if no peak ref

Validated high-risk board (n=14 offline): good 57.1% | cover 100% | late+miss 0% | e>2 0%

Deploy as custom_components/pjm/sensor.py
"""


import asyncio
from collections import Counter, deque, defaultdict
from datetime import datetime, date, time, timezone, timedelta
import logging
import urllib.parse
import time as time_module

import async_timeout
import aiohttp
import numpy as np
# scipy is optional — stock kinematics use numpy only; do not hard-import
# (missing scipy previously prevented the entire sensor platform from loading).
try:
    from scipy.optimize import curve_fit  # noqa: F401
except ImportError:  # pragma: no cover
    curve_fit = None

from homeassistant.components.sensor import SensorEntity
from homeassistant.components.sensor import SensorStateClass
from homeassistant.config_entries import ConfigEntry
from homeassistant.const import CONF_NAME
from homeassistant.core import HomeAssistant
from homeassistant.helpers.entity_platform import AddEntitiesCallback
from homeassistant.helpers.storage import Store
from homeassistant.util import Throttle
from homeassistant.util import dt as dt_util
from homeassistant.helpers.aiohttp_client import async_get_clientsession

from .const import (
    DOMAIN,
    CONF_API_KEY,
    CONF_INSTANTANEOUS_ZONE_LOAD,
    CONF_INSTANTANEOUS_TOTAL_LOAD,
    CONF_ZONE_LOAD_FORECAST,
    CONF_TOTAL_LOAD_FORECAST,
    CONF_ZONE_SHORT_FORECAST,
    CONF_TOTAL_SHORT_FORECAST,
    CONF_ZONAL_LMP,
    CONF_COINCIDENT_PEAK_PREDICTION_ZONE,
    CONF_COINCIDENT_PEAK_PREDICTION_SYSTEM,
    CONF_PEAK_THRESHOLD,
    CONF_ACCURACY_THRESHOLD,
    DEFAULT_PEAK_THRESHOLD_ZONE,
    DEFAULT_PEAK_THRESHOLD_SYSTEM,
    DEFAULT_ACCURACY_THRESHOLD,
    ZONE_TO_PNODE_ID,
    SENSOR_TYPES,
)

# Bias-store version tag. Defined here — stock PJM const.py does NOT export
# INTEGRATION_VERSION; importing it crashes platform setup.
#
# Cover5CP curtailment active (prefer on-hour + cover safety).
# Lock stack: hr90 (path-A only, short-not-later). Source: _search_5cp_cover.py
INTEGRATION_VERSION = "grok_cover5cp_rc1"

# Hours after afternoon peak reference before we freeze refine/arm for the day
POST_PEAK_GRACE_HOURS = 2.0

# High-risk day: predicted peak >= this fraction of effective bar
# (max of user peak_threshold and 5th-highest stored coincident peak).
HIGH_RISK_PEAK_FRACTION = 0.98

_LOGGER = logging.getLogger(__name__)

# Define resource URLs
RESOURCE_INSTANTANEOUS = 'https://api.pjm.com/api/v1/inst_load'
RESOURCE_FORECAST = 'https://api.pjm.com/api/v1/load_frcstd_7_day'
RESOURCE_SHORT_FORECAST = 'https://api.pjm.com/api/v1/very_short_load_frcst'
RESOURCE_LMP = 'https://api.pjm.com/api/v1/rt_unverified_fivemin_lmps'
RESOURCE_SUBSCRIPTION_KEY = 'https://dataminer2.pjm.com/config/settings.json'

MIN_TIME_BETWEEN_UPDATES_INSTANTANEOUS = timedelta(seconds=300)  # 5 minutes for load, LMPs
MIN_TIME_BETWEEN_UPDATES_FORECAST = timedelta(seconds=3600)  # 1 hour for forecasts

PJM_RTO_ZONE = "PJM RTO"
FORECAST_COMBINED_ZONE = 'RTO_COMBINED'
MAX_HISTORY_SIZE = 300  # about 25 hours of data at 5-min intervals

# Poll cadence for legacy SensorEntity polling (matches PJM load refresh)
SCAN_INTERVAL = timedelta(minutes=5)

# Standard quadratic function
def _quadratic(x, a, b, c):
    return a * x**2 + b * x + c


def _format_peak_history_entry(timestamp, load):
    """Format a top-five peak line. Avoid %-I/%#I (OS-specific strftime)."""
    if timestamp is None:
        return f"unknown - {load:,.0f} MW"
    try:
        hour12 = timestamp.hour % 12 or 12
        return (
            f"{timestamp.strftime('%B %d, %Y')} at "
            f"{hour12}:{timestamp.strftime('%M:%S %p')} - {load:,.0f} MW"
        )
    except Exception:
        try:
            return f"{timestamp.isoformat()} - {load:,.0f} MW"
        except Exception:
            return f"{load:,.0f} MW"


def _cleanup_experimental_registry(hass: HomeAssistant, config_entry: ConfigEntry) -> None:
    """
    Migrate/remove temporary unique_ids that ended with '_experimental'.

    Those were used briefly for side-by-side testing. Leaving them in the
    entity registry shows stale 'Experimental' entities that never update,
    while the production unique_id entities may look dead until re-linked.
    """
    try:
        from homeassistant.helpers import entity_registry as er
    except Exception as err:  # pragma: no cover
        _LOGGER.debug("entity_registry unavailable for experimental cleanup: %s", err)
        return

    registry = er.async_get(hass)
    entries = list(er.async_entries_for_config_entry(registry, config_entry.entry_id))
    for reg_entity in entries:
        uid = reg_entity.unique_id or ""
        if not uid.endswith("_experimental"):
            continue
        base_uid = uid[: -len("_experimental")]
        # Prefer the production unique_id entry if both exist
        conflict = next(
            (
                e
                for e in entries
                if e.unique_id == base_uid and e.entity_id != reg_entity.entity_id
            ),
            None,
        )
        if conflict is not None:
            _LOGGER.warning(
                "Removing orphan experimental entity %s (unique_id=%s); "
                "production entity is %s",
                reg_entity.entity_id,
                uid,
                conflict.entity_id,
            )
            registry.async_remove(reg_entity.entity_id)
        else:
            _LOGGER.warning(
                "Migrating entity %s unique_id %s -> %s",
                reg_entity.entity_id,
                uid,
                base_uid,
            )
            registry.async_update_entity(reg_entity.entity_id, new_unique_id=base_uid)


async def async_setup_entry(hass: HomeAssistant, entry: ConfigEntry, async_add_entities: AddEntitiesCallback):
    """Set up the PJM sensor platform from a config entry."""
    # Collapse any leftover *_experimental unique_ids before adding entities
    _cleanup_experimental_registry(hass, entry)

    zone = entry.data["zone"]
    selected_sensors = entry.data["sensors"]
    pjm_data = PJMData(async_get_clientsession(hass), entry.data.get(CONF_API_KEY))
    dev = []

    for sensor_type in selected_sensors:
        identifier = zone
        if sensor_type == CONF_INSTANTANEOUS_TOTAL_LOAD:
            identifier = PJM_RTO_ZONE
        if sensor_type in (CONF_TOTAL_LOAD_FORECAST, CONF_TOTAL_SHORT_FORECAST):
            identifier = FORECAST_COMBINED_ZONE

        if sensor_type == CONF_ZONAL_LMP:
            pnode_id = ZONE_TO_PNODE_ID.get(zone)
            if pnode_id is None:
                _LOGGER.error("Invalid zone provided for LMP: %s", zone)
                continue
            dev.append(PJMSensor(pjm_data, sensor_type, pnode_id, None))
        elif sensor_type in (CONF_COINCIDENT_PEAK_PREDICTION_ZONE, CONF_COINCIDENT_PEAK_PREDICTION_SYSTEM):
            if sensor_type == CONF_COINCIDENT_PEAK_PREDICTION_ZONE:
                peak_threshold = entry.data.get("peak_threshold_zone", DEFAULT_PEAK_THRESHOLD_ZONE)
            else:
                peak_threshold = entry.data.get("peak_threshold_system", DEFAULT_PEAK_THRESHOLD_SYSTEM)
            accuracy_threshold = entry.data.get(CONF_ACCURACY_THRESHOLD, DEFAULT_ACCURACY_THRESHOLD)
            dev.append(CoincidentPeakPredictionSensor(
                pjm_data, zone if sensor_type == CONF_COINCIDENT_PEAK_PREDICTION_ZONE else PJM_RTO_ZONE,
                peak_threshold, accuracy_threshold, sensor_type, hass))
        else:
            dev.append(PJMSensor(pjm_data, sensor_type, identifier, None))

    for entity in dev:
        _LOGGER.info(
            "PJM setup: adding %s unique_id=%s",
            getattr(entity, "name", type(entity).__name__),
            getattr(entity, "unique_id", None),
        )

    async_add_entities(dev, True)

    for index, entity in enumerate(dev):
        delay = 12 + (index * 12)
        hass.async_create_task(schedule_delayed_update(entity, delay))

async def schedule_delayed_update(entity, delay):
    """Schedule an update after a delay and write state (manual poll path)."""
    await asyncio.sleep(delay)
    try:
        # force_refresh runs async_update then writes state to HA
        await entity.async_update_ha_state(force_refresh=True)
    except Exception:
        _LOGGER.exception(
            "Delayed update failed for %s",
            getattr(entity, "entity_id", repr(entity)),
        )

class PJMSensor(SensorEntity):
    """Implementation of a standard PJM sensor."""
    def __init__(self, pjm_data, sensor_type, identifier, name):
        super().__init__()
        self._pjm_data = pjm_data
        self._type = sensor_type
        self._identifier = identifier
        self._unit_of_measurement = SENSOR_TYPES[sensor_type][1]
        self._attr_unique_id = f"pjm_{sensor_type}_{identifier}"
        self._state = None
        self._forecast_data = None

        if name:
            self._attr_name = name
        else:
            self._attr_name = SENSOR_TYPES[sensor_type][0]
            if sensor_type in (CONF_INSTANTANEOUS_ZONE_LOAD, CONF_ZONE_LOAD_FORECAST, CONF_ZONE_SHORT_FORECAST):
                self._attr_name = f'{identifier} {SENSOR_TYPES[sensor_type][0]}'
            elif sensor_type == CONF_ZONAL_LMP:
                zone_name = next((zone for zone, pid in ZONE_TO_PNODE_ID.items() if pid == identifier), None)
                if zone_name:
                    self._attr_name = f'{zone_name} {SENSOR_TYPES[sensor_type][0]}'
                else:
                    self._attr_name += ' ' + f'{identifier}'
        # Enable long-term statistics for system and zone load or LMP
        if sensor_type in (CONF_INSTANTANEOUS_ZONE_LOAD, CONF_INSTANTANEOUS_TOTAL_LOAD, CONF_ZONAL_LMP):
            self._attr_state_class = SensorStateClass.MEASUREMENT

    @property
    def name(self):
        return self._attr_name

    @property
    def unique_id(self):
        return self._attr_unique_id

    @property
    def icon(self):
        if self._type in [
            CONF_ZONE_LOAD_FORECAST,
            CONF_TOTAL_LOAD_FORECAST,
            CONF_ZONE_SHORT_FORECAST,
            CONF_TOTAL_SHORT_FORECAST
        ]:
            return "mdi:chart-timeline-variant"
        elif self._type in [
            CONF_INSTANTANEOUS_ZONE_LOAD,
            CONF_INSTANTANEOUS_TOTAL_LOAD
        ]:
            return "mdi:transmission-tower-export"
        elif self._type == CONF_ZONAL_LMP:
            return "mdi:meter-electric"
        else:
            return "mdi:flash"

    @property
    def unit_of_measurement(self):
        return self._unit_of_measurement

    @property
    def native_value(self):
        return self._state

    @property
    def extra_state_attributes(self):
        attr = {}
        if self._identifier and self._type not in [CONF_TOTAL_LOAD_FORECAST, CONF_TOTAL_SHORT_FORECAST]:
            attr["identifier"] = self._identifier
            
        if self._type in [CONF_INSTANTANEOUS_ZONE_LOAD, CONF_INSTANTANEOUS_TOTAL_LOAD]:
            attr["observed_rate_of_change"] = self._observed_roc

        if self._type in [CONF_TOTAL_LOAD_FORECAST, CONF_ZONE_LOAD_FORECAST]:
            attr["forecast_hour_ending"] = self._forecast_hour_ending.isoformat() if hasattr(self, "_forecast_hour_ending") and self._forecast_hour_ending else None

        if self._type in [CONF_TOTAL_SHORT_FORECAST, CONF_ZONE_SHORT_FORECAST]:
            attr["forecast_peak_time"] = self._forecast_hour_ending.isoformat() if hasattr(self, "_forecast_hour_ending") and self._forecast_hour_ending else None
            attr["forecast_rate_of_change"] = self._forecast_roc
            # attr["forecast_data"] = self._forecast_data
        return attr

    async def async_update(self):
        try:
            if self._type in (CONF_INSTANTANEOUS_ZONE_LOAD, CONF_INSTANTANEOUS_TOTAL_LOAD):
                await self.update_load()
            elif self._type == CONF_ZONAL_LMP:
                await self.update_lmp()
            elif self._type in (CONF_TOTAL_SHORT_FORECAST, CONF_ZONE_SHORT_FORECAST):
                await self.update_short_forecast()
            else:
                await self.update_forecast()
        except Exception as err:
            _LOGGER.error("Update failed: %s", err)

    @Throttle(MIN_TIME_BETWEEN_UPDATES_INSTANTANEOUS)
    async def update_load(self):
        load = await self._pjm_data.async_update_instantaneous(self._identifier)
        if load is not None:
            self._state = load

        # 2) Append to a rolling history
        now_utc = datetime.now(timezone.utc)
        if not hasattr(self, "_load_history"):
            self._load_history = deque(maxlen=36)  # ~ 1 hour if each update is 5 min
        self._load_history.append((now_utc, load))

        # 3) Compute derivative over this 1-hour window
        self._observed_roc = self._compute_instantaneous_roc()

    @Throttle(MIN_TIME_BETWEEN_UPDATES_FORECAST)
    async def update_forecast(self):
        forecast_data = await self._pjm_data.async_update_forecast(self._identifier)
        if forecast_data is not None:
            max_forecast = max(forecast_data, key=lambda x: x["forecast_load_mw"])
            peak_forecast_load = max_forecast["forecast_load_mw"]
            self._state = peak_forecast_load
            self._forecast_hour_ending = max_forecast["forecast_hour_ending"]


    @Throttle(MIN_TIME_BETWEEN_UPDATES_INSTANTANEOUS)
    async def update_short_forecast(self):
        forecast_data = await self._pjm_data.async_update_short_forecast(self._identifier)
        if forecast_data and len(forecast_data) > 1:
            #self._forecast_data = forecast_data
            # 1) Compute the maximum forecast load & set state
            max_item = max(forecast_data, key=lambda x: x["forecast_load_mw"])
            self._state = max_item["forecast_load_mw"]
            self._forecast_hour_ending = max_item["forecast_hour_ending"]
            # 2) Compute the derivative (MW/hr) for the chosen window
            self._forecast_roc = self._compute_forecast_rate_of_change(forecast_data)
        else:
            # No valid data
            #self._forecast_data = None
            self._forecast_roc = 0

    @Throttle(MIN_TIME_BETWEEN_UPDATES_INSTANTANEOUS)
    async def update_lmp(self):
        lmp = await self._pjm_data.async_update_lmp(self._identifier)
        if lmp is not None:
            self._state = lmp

    def _compute_instantaneous_roc(self):
        """Compute MW/hr slope from the oldest to newest in _load_history."""
        if not hasattr(self, "_load_history") or len(self._load_history) < 2:
            return 0
        oldest_time, oldest_val = self._load_history[0]
        newest_time, newest_val = self._load_history[-1]
        delta_load = newest_val - oldest_val
        delta_time = (newest_time - oldest_time).total_seconds() / 3600
        if delta_time <= 0:
            return 0
        return delta_load / delta_time

    def _compute_forecast_rate_of_change(self, data):
        """
        Example approach:
        - We'll calculate a slope over the next 30 minutes from data[0] to data that ends by +30min
        - Could also do entire 2 hours, or up to the peak, etc.
        """
        if not data or len(data) < 2:
            return 0

        # Filter data for next 30 minutes from the first forecast
        start_time = data[0]["forecast_hour_ending"]
        window_end_time = start_time + timedelta(minutes=30)
        segment = [x for x in data if x["forecast_hour_ending"] <= window_end_time]
        if len(segment) < 2:
            # fallback: just use entire 2-hour window
            segment = data

        start = segment[0]
        end = segment[-1]
        delta_load = end["forecast_load_mw"] - start["forecast_load_mw"]
        delta_time_hrs = (end["forecast_hour_ending"] - start["forecast_hour_ending"]).total_seconds() / 3600
        if delta_time_hrs <= 0:
            return 0

        return delta_load / delta_time_hrs

class CoincidentPeakPredictionSensor(SensorEntity):
    """
    Coincident Peak Prediction sensor — frozen 100-gen evolution final.

    Base: sensor_grok_release_candidate (kin v2 + short-heavy blend + EMA).

    Curtailment hybrid champion g69_m1_stat (50-gen + 100-gen continuation):
      1) Statistical hour vote (RC / short / zone peak-hour prior / online scores)
      2) No-early-arm until commit (Markov may pull commit earlier when Rolling)
      3) Sticky-later peak hour
      4) Soft/hard escape with near-max rollover only
      5) EWMA peakiness can delay arming until cresting

    Fair offline (35 days RTO+ComEd):
      good_day 54.3%, fpFail 0%, armed_ok 54.3%, fpMin 11.0, miss 42.9%
    vs RC fair: 2.9% / 97.1% / 34.3% / 238.5 / 25.7%.
    """
    ACCELERATION_THRESHOLD = 500  # MW/hr², easy to adjust centrally
    MAX_VALID_PEAK_WINDOW = 3    # hours
    SMOOTHING_ALPHA = 0.35  # between 0 (more smoothing) and 1 (less smoothing)
    # --- Kinematics knobs (v2: maximize 2h skill, protect overall) ---
    KIN_SHORT_WINDOW_MIN = 15        # short-window obs ROC/ACC near peak
    KIN_PROXIMITY_HRS = 2.0          # enter peak-proximity mode when ttp ≤ this
    KIN_INNER_HRS = 1.0              # inner band: freer crest/quad_near
    KIN_MAX_TPEAK_NEAR = 1.35        # cap t_peak (hours) in inner proximity
    KIN_MAX_TPEAK_OUTER = 1.75       # cap in 1–2h outer proximity
    KIN_MIN_DEC_FRAC = 0.0015        # min |acc| as fraction of load / h²
    KIN_MIN_DEC_FLOOR = 8.0          # absolute floor MW/h²
    KIN_MIN_DEC_CEIL = 80.0          # absolute ceiling MW/h²
    KIN_NO_LATER_THAN_SHORT_MIN = 20 # reject kin if later than short by > this
    KIN_ALLOW_EARLIER_MIN = 75       # admit kin earlier than short by up to this
    KIN_NEAR_LOAD_FRAC = 0.988       # load/short_mw: treat as near-peak crest
    KIN_W_NEAR = 0.18                # kin weight when high-confidence near peak
    KIN_W_DEFAULT = 0.10
    # Systematic early bias (~-34m historic) — push later by mode (minutes)
    KIN_DEBIAS_CREST_MIN = 12.0
    KIN_DEBIAS_SHOULDER_MIN = 22.0
    KIN_DEBIAS_QUAD_NEAR_MIN = 28.0
    KIN_DEBIAS_QUAD_MIN = 18.0
    # Outer band (1–2h): blend kin ttp toward short ttp
    KIN_OUTER_SHORT_BLEND = 0.62     # weight on short_ttp in outer band
    KIN_MIN_CONF_EMIT = 0.42         # suppress low-confidence plain outputs
    # Overall blend v3 — modest default kin, higher when confident (from historic sweep)
    BLEND_W_KIN_DEFAULT = 0.15
    BLEND_W_KIN_HIGH = 0.25
    BLEND_KIN_CONF_THRESH = 0.70
    BLEND_ALLOW_EARLY_CREST = True
    # --- Curtailment hybrid (frozen g69_m1_stat from curtailment_champion_params.json) ---
    STICKY_LATER_HOURS = True
    COMMIT_HOUR = 13.490002836008653
    MIN_AHEAD_MIN = 5.4375
    ESCAPE_INTO_MIN = 5.0
    ESCAPE_SOFT_INTO_MIN = 7.0
    ESCAPE_SOFT_ROC = 100.0
    ESCAPE_SOFT_SHORT_LEAD_MIN = 25.0
    ESCAPE_PROTECT_FIRST_MIN = 3.0
    ESCAPE_ROLL_ROC = -50.0
    ESCAPE_ROLL_DROP_MW = 30.0
    ESCAPE_NEAR_MAX_FRAC = 0.97
    ESCAPE_BUMP_MINUTES = 20.0
    # Statistical hour blend weights (renormalized in code)
    STAT_W_RC = 0.25576292318567667
    STAT_W_SHORT = 0.19866656065874613
    STAT_W_PRIOR = 0.07366363923259384
    STAT_W_ONLINE = 0.4719068769229834
    STAT_ONLINE_LR = 0.3824322197944564
    # EWMA peakiness
    EWMA_ALPHA = 0.24781276460174176
    EWMA_ARM_THRESH = 0.49500382160541806
    # Markov regime
    MARKOV_COMMIT_BOOST = 0.0
    MARKOV_ESCAPE_SCALE = 0.9648829822298736
    POST_COMMIT_REQUIRE_SHORT = True

    def __init__(self, pjm_data, zone, peak_threshold, accuracy_threshold, sensor_type, hass):
        super().__init__()
        self.hass = hass
        self._pjm_data = pjm_data
        self._zone = zone
        self._sensor_type = sensor_type
        # Same identity as stock sensor.py — full drop-in, not a parallel entity
        self._attr_name = f"Coincident Peak Prediction ({zone})"
        self._attr_unique_id = f"pjm_{sensor_type}_{zone}"
        self._attr_should_poll = True
        self._unit_of_measurement = "MW"
        self._last_reset_date = date.today()
        
        # The main sensor state is the current instantaneous load.
        self._state = None
        
        # Rolling load history (timestamp, load) for derivative calculations (~1-2 hours)
        self._load_history = deque(maxlen=36)
        self._last_load_update = None
        
        # Forecast update trackers
        self._last_daily_forecast_update = None
        self._last_short_forecast_update = None
        self._last_kinematics_update = None
        
        # Predicted peak (from daily and short forecast refinements)
        self._predicted_peak = None
        self._predicted_peak_time = None
        self._last_blended_pred_time = None

        # Observed derivatives from load history
        self._observed_roc = 0.0   # MW/hr
        self._observed_acc = 0.0   # MW/hr²
        self._roc_history = deque(maxlen=12)

        # Daily observed peak (init early so attributes/ops-lock never AttributeError)
        self._max_daily_load = None
        self._max_daily_load_time = None

        # Forecast Variables
        self._daily_forecast_peak = None
        self._daily_forecast_peak_time = None
        self._short_forecast_peak = None
        self._short_forecast_peak_time = None
        self._kinematic_peak = None
        self._kinematic_peak_time = None
        # Curtailment hybrid diagnostics / state
        self._hybrid_method = "none"
        self._hybrid_escaped = False
        self._sticky_min_peak_hour = None  # local hour floor for sticky-later
        self._hybrid_regime = "Rising"
        self._online_hour_score = {}  # {hour: score} online stats
        self._ewma_peakiness = 0.0
        self._last_short_forecast_rows = None
        self._forecasted_roc = 0.0
        self._forecasted_acc = 0.0
        
        # Bias factors to improve prediction over time
        self._roc_bias = 0.0
        self._acc_bias = 0.0

        # Adaptive bias factors for fine-tuning predicted time and magnitude
        self._time_bias = 0.0         # in hours
        self._magnitude_bias = 0.0    # in MW

        # Histories for adaptive learning
        self._time_error_history = deque(maxlen=30)       # errors in predicted time (hrs)
        self._magnitude_error_history = deque(maxlen=30)  # errors in predicted load (MW)

        # Initialize persistent storage for peaks
        self._store = Store(hass, 1, f"coincident_peaks_{zone}.json")
        self._top_five_peaks = []

        # NEW: Separate persistent store for bias learning (versioned)
        self._bias_store = Store(hass, 1, f"coincident_peak_bias_{zone}.json")
        self._bias_version = INTEGRATION_VERSION

        # Per-zone rolling stats (no hard-coded zone names): peak hours/MWs adapt over time
        self._zone_stats_store = Store(hass, 1, f"coincident_zone_stats_{zone}.json")
        self._zone_stats_version = 1
        self._peak_hour_history = deque(maxlen=40)   # actual peak hours (local)
        self._peak_mw_history = deque(maxlen=40)     # actual peak MW
        self._decision_hour_prior = 16.0             # slow EMA of peak hours
        self._decision_hour = 16                     # today's active decision hour
        self._short_bias_scale = 1.0                 # optional slow scale on early-bias
        self._still_rising_late_bias = 0.0           # learned hours short was late when still_rising
        self._short_still_rising = False
        self._allow_early_move = False               # hour-match correction active this cycle
        self._hour_match_corrected = False           # skip late adaptive bias when True
        self._kin_confidence = 0.0                   # kin confidence 0..1
        self._kin_mode = "none"                      # crest | shoulder | quad | quad_near

        # Load peaks from storage
        self.hass.async_create_task(self._async_load_peaks())
        #Load biases from storage (with version check)
        self.hass.async_create_task(self._async_load_biases())
        self.hass.async_create_task(self._async_load_zone_stats())

        # Flags and thresholds
        self._daily_peak_occurred = False
        self._error_recorded = False
        self._peak_threshold = peak_threshold
        self._accuracy_threshold = accuracy_threshold
        self._high_risk_day = False
        self._peak_hour_active = False
        self._error_history = deque(maxlen=30)

        # Hourly learning snapshots — hours chosen dynamically from decision_hour
        self._bias_snapshot_hours = (15, 16)  # refreshed by _refresh_decision_hour
        self._primary_bias_hour = 16          # = decision_hour
        self._hourly_snapshots = {}           # {hour: {wall_time, predicted_peak_time, predicted_peak, high_risk_day}}
        self._last_bias_snapshot_hour = None
        self._last_bias_time_error_hrs = None
        self._last_bias_magnitude_error = None
        self._last_bias_weight = None

        # Operational lock (hr90): path-A drop only; never lock while short still later.
        # Path-B plateau removed — it froze wrong hours and inflated hard FP under fidelity.
        self._prediction_time_locked = False
        self._locked_peak_time = None
        self._locked_peak_mw = None
        self._below_max_streak = 0
        self._lock_drop_mw = 150          # base MW below daily max (scaled for zone size)
        self._lock_drop_frac = 0.005      # also allow 0.5% of daily max (zonal plateaus)
        self._lock_streak_needed = 3      # consecutive updates below max-drop (was 2)
        self._lock_min_minutes_after_max = 15  # was 10
        self._lock_plateau_minutes = 999   # path-B disabled (hr90)
        self._lock_require_short_not_later = True
        self._lock_short_later_block_min = 25.0
        self._short_roc_neg_streak = 0
        # Curtailment active: sticky min hour + short-later block (fidelity hr90)
        self._curtail_min_active_hour = None

        # Cover5CP active policy knobs (prefer on-hour, never late/miss cover)
        self._cover5cp_lead = 1
        self._cover5cp_trail = 0
        self._cover5cp_climb_roc = 250.0
        self._cover5cp_max_early_hours = 2
        self._cover5cp_prefer_on_hour = True
        self._cover5cp_cover_force = True
        self._cover5cp_false_peak_push = True
        self._cover5cp_climb_wait = True
        self._cover5cp_sticky_hr = True
        self._cover5cp_single_hour = True
        self._cover5cp_armed_any = False
        self._cover5cp_first_arm_h = None
        self._cover5cp_peak_hint_max = 12
        self._cover5cp_last_false_peak = False
        self._saw_high_risk = False

        # **Daily Reset**: Store the current date for which the prediction applies.
        self._current_prediction_date = dt_util.now().date()

        # Enable long-term statistics for zone load and system load
        self._attr_state_class = SensorStateClass.MEASUREMENT

    @property
    def name(self):
        return self._attr_name

    @property
    def unique_id(self):
        return self._attr_unique_id

    @property
    def icon(self):
        return "mdi:summit"

    @property
    def unit_of_measurement(self):
        return self._unit_of_measurement

    @property
    def native_value(self):
        """Return the instantaneous load (MW) as the sensor state."""
        return self._state

    @property
    def extra_state_attributes(self):
        """Return additional predictive and diagnostic attributes."""
        # Never raise out of attributes — HA skips the whole state write if we do.
        try:
            formatted_top_five_peaks = [
                _format_peak_history_entry(timestamp, load)
                for timestamp, load in (self._top_five_peaks or [])
                if load is not None
            ]
        except Exception as err:
            _LOGGER.debug("top_five_peaks format failed: %s", err)
            formatted_top_five_peaks = []

        try:
            return {
            "predicted_peak": self._predicted_peak,
            "predicted_peak_time": (
                self._predicted_peak_time.isoformat()
                if self._predicted_peak_time else None
            ),
            "observed_peak": self._max_daily_load,
            "observed_peak_time": (
                self._max_daily_load_time.isoformat()
                if self._max_daily_load_time else None
            ),
            "peak_hour_active": self._peak_hour_active,
            "high_risk_day": self._high_risk_day,
            "cover5cp_armed": self._cover5cp_armed_any,
            "cover5cp_first_arm_h": self._cover5cp_first_arm_h,
            "cover5cp_peak_hint_max": self._cover5cp_peak_hint_max,
            "saw_high_risk": self._saw_high_risk,
            "observed_roc": round(self._observed_roc, 2),
            "observed_acc": round(self._observed_acc, 2),
            "forecasted_roc": round(self._forecasted_roc, 2),
            "forecasted_acc": round(self._forecasted_acc, 2),
            "bias_roc": round(self._roc_bias, 2),
            "bias_acc": round(self._acc_bias, 2),
            "time_bias": round(self._time_bias, 2),
            "magnitude_bias": round(self._magnitude_bias, 2),
            "error_history": [round(err, 2) for err in self._error_history],
            "top_five_peaks": formatted_top_five_peaks,
            # Learning snapshots (hour-based bias anchors)
            "bias_snapshot_hours": sorted(self._hourly_snapshots.keys()),
            "bias_snapshot_16_peak_time": (
                self._hourly_snapshots[16]["predicted_peak_time"].isoformat()
                if 16 in self._hourly_snapshots else None
            ),
            "bias_snapshot_16_peak_mw": (
                self._hourly_snapshots[16]["predicted_peak"]
                if 16 in self._hourly_snapshots else None
            ),
            "bias_snapshot_15_peak_time": (
                self._hourly_snapshots[15]["predicted_peak_time"].isoformat()
                if 15 in self._hourly_snapshots else None
            ),
            "bias_snapshot_15_peak_mw": (
                self._hourly_snapshots[15]["predicted_peak"]
                if 15 in self._hourly_snapshots else None
            ),
            "last_bias_snapshot_hour": self._last_bias_snapshot_hour,
            "last_bias_time_error_hrs": (
                round(self._last_bias_time_error_hrs, 3)
                if self._last_bias_time_error_hrs is not None else None
            ),
            "last_bias_magnitude_error": (
                round(self._last_bias_magnitude_error, 1)
                if self._last_bias_magnitude_error is not None else None
            ),
            "last_bias_weight": (
                round(self._last_bias_weight, 2)
                if self._last_bias_weight is not None else None
            ),
            "prediction_time_locked": self._prediction_time_locked,
            "locked_peak_time": (
                self._locked_peak_time.isoformat()
                if self._locked_peak_time else None
            ),
            "locked_peak_mw": self._locked_peak_mw,
            # Adaptive zone clocks (profile-free)
            "decision_hour": self._decision_hour,
            "decision_hour_prior": round(self._decision_hour_prior, 2),
            "high_load_threshold_mw": round(self._get_high_load_threshold(), 0),
            "short_bias_scale": round(self._short_bias_scale, 3),
            # Curtailment hybrid diagnostics
            "hybrid_method": getattr(self, "_hybrid_method", "none"),
            "hybrid_escaped": getattr(self, "_hybrid_escaped", False),
            "hybrid_regime": getattr(self, "_hybrid_regime", "none"),
            "ewma_peakiness": round(getattr(self, "_ewma_peakiness", 0.0), 3),
            "sticky_min_peak_hour": getattr(self, "_sticky_min_peak_hour", None),
            "integration_version": INTEGRATION_VERSION,
            }
        except Exception as err:
            _LOGGER.exception(
                "extra_state_attributes failed for %s: %s", self._zone, err
            )
            return {
                "predicted_peak": self._predicted_peak,
                "observed_peak": getattr(self, "_max_daily_load", None),
                "top_five_peaks": formatted_top_five_peaks,
            }

    async def async_update(self):
        """
        Main update flow executed on each sensor poll (e.g., every 5 minutes):
          1. Reset daily prediction if a new day has begun.
          2. Update instantaneous load and rolling history.
          3. Compute observed derivatives.
          4. If the daily peak hasn't occurred, update forecasts and refine predictions.
          5. Evaluate high-risk day and peak hour active status.
          6. After the predicted peak time, record forecast error and update adaptive biases.

        Outer try/except: never raise into HA entity add (update_before_add=True).
        An uncaught error there leaves the entity as "no longer provided".
        """
        try:
            await self._async_update_inner()
        except Exception as err:
            _LOGGER.exception(
                "Coincident peak update failed for %s: %s", self._zone, err
            )

    async def _async_update_inner(self):
        """Inner update body (errors logged by async_update)."""
        now = dt_util.now()  # using Home Assistant's dt_util for timezone-aware times

        # *Daily Reset*: If a new day has started, reset the daily prediction.
        # Compare dates in local time — UTC .date() near midnight can disagree with
        # America/Chicago and leave yesterday's freeze/lock active past local midnight.
        try:
            current_date = dt_util.as_local(now).date()
        except Exception:
            current_date = now.date()
        pred_local_date = None
        if self._predicted_peak_time is not None:
            try:
                pred_local_date = dt_util.as_local(self._predicted_peak_time).date()
            except Exception:
                pred_local_date = self._predicted_peak_time.date()
        reset_needed = (
            current_date != self._current_prediction_date
            or (pred_local_date is not None and pred_local_date != current_date)
        )

        if reset_needed:
            self._daily_peak_occurred = False
            self._predicted_peak = None
            self._predicted_peak_time = None
            self._last_daily_forecast_update = None
            self._last_short_forecast_update = None
            self._max_daily_load = None
            self._max_daily_load_time = None
            self._error_recorded = False
            self._current_prediction_date = current_date

            self._daily_forecast_peak = None
            self._daily_forecast_peak_time = None
            self._short_forecast_peak = None
            self._short_forecast_peak_time = None
            self._kinematic_peak = None
            self._kinematic_peak_time = None
            self._forecasted_roc = 0.0
            self._forecasted_acc = 0.0
            # Clear overnight ROC so evening decline does not freeze the new day
            self._observed_roc = 0.0
            self._observed_acc = 0.0
            if hasattr(self, "_load_history") and self._load_history is not None:
                self._load_history.clear()
            if hasattr(self, "_roc_history") and self._roc_history is not None:
                self._roc_history.clear()
            # Clear EMA damping state so next day's peak is not rate-limited from yesterday
            self._last_blended_pred_time = None
            # Clear hourly learning snapshots for the new day
            self._hourly_snapshots = {}
            # Clear operational lock
            self._prediction_time_locked = False
            self._locked_peak_time = None
            self._locked_peak_mw = None
            self._below_max_streak = 0
            self._short_roc_neg_streak = 0
            self._curtail_min_active_hour = None
            # Clear hour-match correction state
            self._allow_early_move = False
            self._hour_match_corrected = False
            # Clear curtailment hybrid sticky/escape/stat state
            self._sticky_min_peak_hour = None
            self._hybrid_method = "none"
            self._hybrid_escaped = False
            self._hybrid_regime = "Rising"
            self._online_hour_score = {}
            self._ewma_peakiness = 0.0
            self._short_still_rising = False
            # Cover5CP day state
            self._cover5cp_armed_any = False
            self._cover5cp_first_arm_h = None
            self._cover5cp_peak_hint_max = 12
            self._cover5cp_last_false_peak = False
            self._saw_high_risk = False
            self._peak_hour_active = False
            self._high_risk_day = False

            _LOGGER.info(
                "New day detected (%s). Resetting daily peak predictions "
                "(cleared freeze/lock/EMA/load-history/Cover5CP).",
                current_date,
            )

        # Reset peaks if it's past October 1st and last reset was before October
        if now.date() >= date(now.year, 10, 1) and self._last_reset_date < date(now.year, 10, 1):
            self._top_five_peaks = []
            self._last_reset_date = date(now.year, 10, 1)
            await self._async_save_peaks()
            _LOGGER.info("Resetting peak history for new year (Oct 1st).")

        # 1. Update instantaneous load and record history.
        # NOTE: _last_load_update is initialized to None in __init__. Do NOT use
        # hasattr-only guards — (now - None) TypeErrors and the outer try/except
        # swallows them, leaving native_value permanently "unknown".
        if (
            self._last_load_update is None
            or (now - self._last_load_update) >= timedelta(minutes=5)
        ):
            success = await self._update_instantaneous_load()
            if success:
                now = dt_util.now()
                self._last_load_update = now
                # 2. Compute observed ROC and acceleration from load history.
                self._compute_observed_derivatives()
            else:
                _LOGGER.warning(
                    "Instantaneous load update failed for zone=%s; state remains %s",
                    self._zone,
                    self._state,
                )

        # 2b. Late-day / post-peak: freeze so reboot at 9–11pm does not invent
        # a new "prediction" for a peak that already happened.
        self._maybe_freeze_post_peak_day(now)

        # 3. Refine predictions if the daily peak has not occurred and not ops-locked.
        if not self._prediction_time_locked and not self._daily_peak_occurred:
            await self._maybe_update_forecasts(now)

            # 4. Run Kinematics when within ~3.5 hours (incl. slightly past pred if still rising)
            if not self._daily_peak_occurred and not self._prediction_time_locked:
                time_to_peak = (self._predicted_peak_time - now) if self._predicted_peak_time else None
                recently_updated = (self._last_short_forecast_update and 
                                (now - self._last_short_forecast_update < timedelta(minutes=10))) and \
                                (self._last_load_update and 
                                (now - self._last_load_update < timedelta(minutes=10)))

                ttp_sec = time_to_peak.total_seconds() if time_to_peak is not None else None
                kin_ok = ttp_sec is not None and (
                    (-0.75 * 3600) < ttp_sec <= 3.5 * 3600
                    or (self._observed_roc > 0 and ttp_sec <= 3.5 * 3600)
                )
                if kin_ok and recently_updated:
                    if not self._last_kinematics_update or (now - self._last_kinematics_update >= timedelta(minutes=5)):
                        # Experimental: looser caller gate — let kin function apply
                        # load-scaled / proximity guards (crest & shoulder paths need this).
                        sw_roc, sw_acc = self._short_window_derivatives(self.KIN_SHORT_WINDOW_MIN)
                        near_ttp = ttp_sec is not None and ttp_sec <= self.KIN_PROXIMITY_HRS * 3600
                        caller_ok = (
                            (self._forecasted_acc is not None and self._forecasted_acc < -8)
                            or self._observed_acc < -12
                            or (near_ttp and sw_roc is not None and sw_roc < 300)
                            or (near_ttp and sw_acc is not None and sw_acc < -5)
                        )
                        if caller_ok:
                            self._predict_peak_using_kinematics(now)
                            self._last_kinematics_update = now
                            self._weighted_peak_prediction()
        
        # Check real-time load exceedance — MW always; time only if still rising and NOT locked
        # Skip after daily peak freeze / late-evening close (reboot-at-10pm junk).
        if (
            not self._daily_peak_occurred
            and self._state
            and self._predicted_peak
            and self._state > self._predicted_peak
        ):
            self._predicted_peak = self._state
            if not self._prediction_time_locked:
                if self._observed_roc > 0:
                    # MW only while climbing — do not shove predicted TIME to now+15
                    # (that path was a major source of post-peak time walk).
                    pass
                elif self._in_afternoon_peak_window(now):
                    # Only snap time to now on post-peak rollover in the afternoon.
                    # Overnight ROC is almost always < 0 (decline to 2–4am min) — never
                    # treat that as "peak is now".
                    self._predicted_peak_time = now
            _LOGGER.warning("Immediate peak adjustment due to real-time exceedance.")

        # 4a. Operational lock from load shape (does not require short forecast)
        if not self._daily_peak_occurred:
            self._maybe_engage_operational_lock(now)

        # 4a2. Curtailment hybrid guards every poll (not only after short/kin blend)
        if (
            not self._daily_peak_occurred
            and self._predicted_peak_time is not None
            and not self._prediction_time_locked
        ):
            self._maybe_apply_curtailment_guards(now)

        # 4. Evaluate high-risk day and peak hour active status.
        self._refresh_decision_hour(now)
        self._evaluate_5cp_risk()
        self._check_peak_hour_active(now)

        # 4b. Capture hourly learning snapshots at decision_hour (and hour-1).
        self._maybe_capture_hourly_snapshot(now)
        
        # 5. Once the predicted peak is past, record peak and forecast error.
        # Finalize only in the afternoon peak window — overnight ROC is almost
        # always negative toward the 2–4am minimum and must not finalize/freeze.
        short_blocks_finalize = False
        if self._short_forecast_peak_time is not None and not self._prediction_time_locked:
            short_lead = (self._short_forecast_peak_time - now).total_seconds() / 60.0
            short_blocks_finalize = short_lead >= 25
        if (self._in_afternoon_peak_window(now) and
            self._state is not None and self._predicted_peak is not None and
            self._observed_roc < 0 and
            self._max_daily_load is not None and
            self._max_daily_load >= 0.85 * self._predicted_peak and
            dt_util.now() >= self._predicted_peak_time and
            not short_blocks_finalize and
            not getattr(self, "_error_recorded", False)):
            self._daily_peak_occurred = True
            self._record_error_and_update_bias()
            self._record_daily_peak()
            self._error_recorded = True
            _LOGGER.info("Peak detected: Actual peak %.1f MW at %s. Freezing further forecasts.",
                        self._max_daily_load, self._max_daily_load_time)

    async def _update_instantaneous_load(self):
        """Fetch the current load from PJMData and update state, load history, and maximum daily load."""
        try:
            load_val = await self._pjm_data.async_update_instantaneous(self._zone)
            if load_val is not None:
                now = dt_util.now()
                self._state = load_val
                self._load_history.append((now, load_val))

                # Update maximum daily load within this method.
                if self._max_daily_load is None or load_val > self._max_daily_load:
                    self._max_daily_load = load_val
                    self._max_daily_load_time = now
                _LOGGER.debug(
                    "Instantaneous load for %s: %.1f MW (observed peak %.1f)",
                    self._zone,
                    load_val,
                    self._max_daily_load if self._max_daily_load is not None else load_val,
                )
                return True
            _LOGGER.warning(
                "PJM returned no instantaneous load for zone=%s", self._zone
            )
        except Exception as err:
            _LOGGER.error(
                "Error updating instantaneous load for zone=%s: %s", self._zone, err
            )
        return False  # Indicate Failure

    def _compute_observed_derivatives(self):
        """
        Compute observed ROC (MW/hr) and ACC (MW/hr²) using a simple moving average (SMA)
        weighted by time, matching Home Assistant's derivative sensor algorithm.
        """

        if len(self._load_history) < 2:
            self._observed_roc = 0.0
            self._observed_acc = 0.0
            return

        sorted_history = sorted(self._load_history, key=lambda x: x[0])

        total_time_sec = 0.0
        weighted_roc_sum = 0.0

        for i in range(len(sorted_history) - 1):
            t0, val0 = sorted_history[i]
            t1, val1 = sorted_history[i + 1]
            delta_time_sec = (t1 - t0).total_seconds()
            if delta_time_sec <= 0:
                continue

            delta_load = val1 - val0
            interval_roc = delta_load / (delta_time_sec / 3600.0)  # MW/hr

            weighted_roc_sum += interval_roc * delta_time_sec
            total_time_sec += delta_time_sec

        if total_time_sec > 0:
            new_roc = weighted_roc_sum / total_time_sec
        else:
            new_roc = 0.0

        # Apply smoothing
        alpha = self.SMOOTHING_ALPHA
        self._observed_roc = (alpha * new_roc) + ((1 - alpha) * self._observed_roc)

        # Store ROC in history for ACC calculation
        if not hasattr(self, '_roc_history'):
            self._roc_history = deque(maxlen=36)
        self._roc_history.append((sorted_history[-1][0], self._observed_roc))

        # Compute acceleration (ACC)
        if len(self._roc_history) < 2:
            self._observed_acc = 0.0
            return

        sorted_roc_history = sorted(self._roc_history, key=lambda x: x[0])

        total_time_acc_sec = 0.0
        weighted_acc_sum = 0.0

        for i in range(len(sorted_roc_history) - 1):
            rt0, roc0 = sorted_roc_history[i]
            rt1, roc1 = sorted_roc_history[i + 1]
            delta_time_sec = (rt1 - rt0).total_seconds()
            if delta_time_sec <= 0:
                continue

            delta_roc = roc1 - roc0
            interval_acc = delta_roc / (delta_time_sec / 3600.0)

            weighted_acc_sum += interval_acc * delta_time_sec
            total_time_acc_sec += delta_time_sec

        if total_time_acc_sec > 0:
            new_acc = weighted_acc_sum / total_time_acc_sec
        else:
            new_acc = 0.0

        # Apply smoothing
        self._observed_acc = (alpha * new_acc) + ((1 - alpha) * self._observed_acc)

    def _local_hour_safe(self, now) -> int:
        try:
            return int(dt_util.as_local(now).hour)
        except Exception:
            try:
                return int(now.hour)
            except Exception:
                return 12

    def _afternoon_peak_reference_time(self, now):
        """Best *today* afternoon peak instant, or None if unknown.

        Uses the *latest* of observed max / live predicted / DA peak with local
        hour in 12–21 on today's date — so we don't freeze early while a later
        estimate is still ahead (e.g. max at 14 but DA/live still 17).
        """
        try:
            today = dt_util.as_local(now).date()
        except Exception:
            today = now.date() if hasattr(now, "date") else None
        candidates = []
        for t in (
            self._max_daily_load_time,
            self._predicted_peak_time,
            self._daily_forecast_peak_time,
        ):
            if t is None:
                continue
            try:
                loc = dt_util.as_local(t)
                if today is not None and loc.date() != today:
                    continue
                if not (12 <= loc.hour <= 21):
                    continue
                candidates.append(t)
            except Exception:
                continue
        if not candidates:
            return None
        return max(candidates)

    def _is_post_peak_evening(self, now) -> bool:
        """True when peak reference is more than POST_PEAK_GRACE_HOURS in the past.

        Examples: 4pm peak → freeze after 6pm; 3pm peak → freeze after 5pm
        (e.g. restart at 5pm with a 3pm peak closes refine/arm).

        No fixed 20:00 cutoff. Last resort: local hour >= 22 with no peak ref
        (cold start very late with no DA yet).
        """
        peak_t = self._afternoon_peak_reference_time(now)
        if peak_t is not None:
            try:
                return now >= peak_t + timedelta(hours=POST_PEAK_GRACE_HOURS)
            except Exception:
                return False
        # No afternoon peak reference yet — do not freeze mid-day; allow DA init.
        # Only last-resort close late at night so we don't spin forever offline.
        return self._local_hour_safe(now) >= 22

    def _maybe_freeze_post_peak_day(self, now) -> None:
        """Mark daily peak done; stop forecast refine and curtailment active."""
        if self._daily_peak_occurred:
            if self._is_post_peak_evening(now):
                self._peak_hour_active = False
            return
        if not self._is_post_peak_evening(now):
            return
        self._daily_peak_occurred = True
        self._peak_hour_active = False
        # Do not clear predicted_peak* — keep last DA/live for display diagnostics
        peak_t = self._afternoon_peak_reference_time(now)
        _LOGGER.info(
            "Post-peak freeze for %s (local hour=%d, peak_ref=%s, grace=%.0fh): "
            "no further forecast refine or peak_hour_active today.",
            self._zone,
            self._local_hour_safe(now),
            peak_t.isoformat() if peak_t is not None else None,
            POST_PEAK_GRACE_HOURS,
        )

    async def _maybe_update_forecasts(self, now):
        """
        Decide whether to pull a daily forecast (if peak is far away) or a short forecast (within 3 hours).
        """
        if self._daily_peak_occurred:
            return

        # Late-day restart: do not start a full refine cycle at 9–11pm
        if self._is_post_peak_evening(now):
            self._maybe_freeze_post_peak_day(now)
            return

        if self._predicted_peak_time is None:
            _LOGGER.info("Predicted peak time is None. Fetching daily forecast (initialization).")
            await self._update_daily_forecast()
            self._last_daily_forecast_update = now
            self._weighted_peak_prediction()
            # After DA init: freeze only if peak_ref + grace is already past
            # (e.g. reboot 5pm with 3pm peak → freeze; reboot 5pm with 4pm peak → keep going).
            # Never freeze solely on "DA hour already started" — allow the 2h grace.
            # Never freeze just after midnight on a past-looking DA (pre-afternoon guard).
            try:
                local_hour = dt_util.as_local(now).hour
            except Exception:
                local_hour = now.hour

            if self._is_post_peak_evening(now) and local_hour >= 12:
                self._daily_peak_occurred = True
                self._peak_hour_active = False
                peak_t = self._afternoon_peak_reference_time(now)
                _LOGGER.info(
                    "Initialization: peak reference %s is more than %.0fh past "
                    "(local hour=%d) for %s — freezing (no short/kin refine).",
                    peak_t.isoformat() if peak_t is not None else None,
                    POST_PEAK_GRACE_HOURS,
                    local_hour,
                    self._zone,
                )
                return
            if not self._daily_forecast_peak_time and not self._predicted_peak_time:
                _LOGGER.warning("Daily forecast peak time still None after initialization.")
                return

        # Regular operational check after initialization
        forecast_peak_time = self._predicted_peak_time #or self._daily_forecast_peak_time

        # Only freeze when past predicted time AND load is clearly declining,
        # AND we are in the afternoon peak window.
        #
        # Critical: ROC is almost always < 0 from midnight through ~2–4am (normal
        # decline into daily minimum). That must NEVER freeze the forecast.
        # Freezing on clock alone was the other main cause of stuck predictions.
        # Also: do NOT freeze while short still points meaningfully later.
        if (
            forecast_peak_time
            and now > forecast_peak_time
            and self._in_afternoon_peak_window(now)
        ):
            if self._observed_roc < 0:
                short_still_later = False
                if self._short_forecast_peak_time is not None:
                    short_lead_min = (
                        self._short_forecast_peak_time - now
                    ).total_seconds() / 60.0
                    # Short still ≥25 min ahead → keep refining (even if live pred is past)
                    short_still_later = short_lead_min >= 25
                if short_still_later and not getattr(self, "_prediction_time_locked", False):
                    _LOGGER.debug(
                        "Skip freeze: past live pred but short still +%.0fm ahead (roc=%.0f)",
                        (self._short_forecast_peak_time - now).total_seconds() / 60.0,
                        self._observed_roc,
                    )
                elif self._observed_roc <= -150 or (
                    self._short_forecast_peak_time and now >= self._short_forecast_peak_time
                ):
                    self._daily_peak_occurred = True
                    return
            # else: still climbing (or flat) — keep refining forecasts
        if self._daily_peak_occurred:
            return

        time_to_peak = forecast_peak_time - now if forecast_peak_time else None
        _LOGGER.info("Maybe Update Forecast - Time to peak:", time_to_peak)

        if (time_to_peak is None or time_to_peak > timedelta(hours=2)) and (
            not self._last_daily_forecast_update or (now - self._last_daily_forecast_update) >= timedelta(hours=1)
        ):
            await self._update_daily_forecast()
            self._last_daily_forecast_update = now
            self._weighted_peak_prediction()
        elif (
            time_to_peak is not None
            and time_to_peak <= timedelta(hours=4)
            and not self._daily_peak_occurred
        ):
            # Include slightly negative ttp (past predicted but still refining)
            if (not self._last_short_forecast_update or (now - self._last_short_forecast_update) >= timedelta(minutes=5)):
                await self._update_short_forecast()
                self._last_short_forecast_update = now
                self._weighted_peak_prediction()

    async def _update_daily_forecast(self):
        """Pull daily forecast data and update predicted peak and time for today."""
        try:
            forecast_zone = "RTO_COMBINED" if self._zone.upper() == "PJM RTO" else self._zone
            data = await self._pjm_data.async_update_forecast(forecast_zone)
            if data:
                try:
                    today = dt_util.as_local(dt_util.now()).date()
                except Exception:
                    today = dt_util.now().date()

                def _he_local_date(he):
                    try:
                        return dt_util.as_local(he).date()
                    except Exception:
                        return he.date() if hasattr(he, "date") else None

                # Filter by LOCAL calendar day (UTC .date() near midnight mis-buckets hours)
                day_data = [
                    x for x in data
                    if _he_local_date(x["forecast_hour_ending"]) == today
                ]
                # Prefer afternoon HE for peak (13–21 local) — avoids overnight/early HE
                # becoming "daily peak" and freezing the morning via init path.
                afternoon = []
                for x in day_data:
                    try:
                        h = dt_util.as_local(x["forecast_hour_ending"]).hour
                    except Exception:
                        h = x["forecast_hour_ending"].hour
                    if 13 <= h <= 21:
                        afternoon.append(x)
                use = afternoon if afternoon else day_data
                if use:
                    max_item = max(use, key=lambda x: x["forecast_load_mw"])
                    self._daily_forecast_peak = max_item["forecast_load_mw"]
                    self._daily_forecast_peak_time = max_item["forecast_hour_ending"] - timedelta(hours=1)
                    _LOGGER.info(
                        "Daily forecast: peak=%.1f at %s (local date=%s, afternoon_pool=%s)",
                        self._daily_forecast_peak,
                        self._daily_forecast_peak_time,
                        today,
                        bool(afternoon),
                    )
        except Exception as err:
            _LOGGER.error("Error updating daily forecast: %s", err)

    async def _update_short_forecast(self):
        """
        Pull short forecast data to compute forecasted derivatives (if available) and update
        separate short-term forecast attributes. These attributes are then used in weighted predictions.
        """
        try:
            forecast_zone = "RTO_COMBINED" if self._zone.upper() == "PJM RTO" else self._zone
            data = await self._pjm_data.async_update_short_forecast(forecast_zone)
            if data and len(data) > 1:
                # Calculate forecasted derivatives for kinematic prediction
                times, loads = self._extract_time_load_arrays_short(data, limit_minutes=60)
                if len(times) >= 3:
                    coeffs = np.polyfit(times, loads, 2)
                    t_last = times[-1]
                    self._forecasted_roc = 2 * coeffs[0] * t_last + coeffs[1]
                    self._forecasted_acc = 2 * coeffs[0]
                else:
                    self._forecasted_roc = 0.0
                    self._forecasted_acc = 0.0

                # Short-forecast peak for weighted blend (calibrated on 6/29–7/9):
                # - interior max → interval midpoint
                # - still rising → quadratic vertex / modest extend past horizon
                # - hour-match corrections (still-rising early pull, compress) zone-agnostic
                # - adaptive late bias only if no hour-match correction applied
                max_item = max(data, key=lambda x: x["forecast_load_mw"])
                rising = max_item == data[-1]
                self._short_still_rising = rising
                if not rising:
                    self._short_forecast_peak = max_item["forecast_load_mw"]
                    ending = max_item["forecast_hour_ending"]
                    self._short_forecast_peak_time = ending - timedelta(minutes=2.5)
                else:
                    full_times = [
                        (x["forecast_hour_ending"] - data[0]["forecast_hour_ending"]).total_seconds() / 3600.0
                        for x in data
                    ]
                    full_loads = [x["forecast_load_mw"] for x in data]
                    if len(full_times) >= 4:
                        c = np.polyfit(full_times, full_loads, 2)
                        if c[0] < 0:
                            t_pk = -c[1] / (2 * c[0])
                            if t_pk > full_times[-1]:
                                extend_h = min(t_pk, full_times[-1] + 20.0 / 60.0)
                                self._short_forecast_peak_time = (
                                    data[0]["forecast_hour_ending"] + timedelta(hours=float(extend_h))
                                )
                                self._short_forecast_peak = float(np.polyval(c, extend_h))
                            elif 0 < t_pk <= full_times[-1]:
                                self._short_forecast_peak_time = (
                                    data[0]["forecast_hour_ending"] + timedelta(hours=float(t_pk))
                                )
                                self._short_forecast_peak = float(np.polyval(c, t_pk))
                            else:
                                self._short_forecast_peak = float(full_loads[-1])
                                self._short_forecast_peak_time = data[-1]["forecast_hour_ending"] + timedelta(
                                    minutes=20
                                )
                        else:
                            self._short_forecast_peak = float(full_loads[-1])
                            self._short_forecast_peak_time = data[-1]["forecast_hour_ending"] + timedelta(
                                minutes=20
                            )
                    else:
                        self._short_forecast_peak = None
                        self._short_forecast_peak_time = None

                # Keep raw short rows for hybrid alternate residual timing
                self._last_short_forecast_rows = list(data)

                # Zone-agnostic hour-match corrections (still-rising early pull, load/short compress)
                now = dt_util.now()
                self._apply_hour_match_corrections(now, rising)

                # Adaptive early-bias: skip when hour-match already pulled earlier
                if not self._hour_match_corrected:
                    self._apply_adaptive_short_time_bias()
            else:
                # Clear if insufficient data
                self._forecasted_roc = 0.0
                self._forecasted_acc = 0.0
                self._short_forecast_peak = None
                self._short_forecast_peak_time = None
                self._last_short_forecast_rows = None

        except Exception as err:
            _LOGGER.error("Error updating short forecast: %s", err)
            self._forecasted_roc = 0.0
            self._forecasted_acc = 0.0
            self._short_forecast_peak = None
            self._short_forecast_peak_time = None
            self._last_short_forecast_rows = None

    def _short_window_derivatives(self, minutes=None):
        """
        Experimental: ROC (MW/hr) and ACC (MW/hr²) over a short trailing window.
        Less lag than long-history smoothed derivatives — used in peak-proximity mode.
        Returns (roc, acc) or (None, None).
        """
        minutes = minutes if minutes is not None else self.KIN_SHORT_WINDOW_MIN
        if not self._load_history or len(self._load_history) < 3:
            return None, None
        now_ts = self._load_history[-1][0]
        cutoff = now_ts - timedelta(minutes=minutes)
        window = [(t, v) for t, v in self._load_history if t >= cutoff]
        if len(window) < 3:
            return None, None
        window.sort(key=lambda x: x[0])
        t0, v0 = window[0]
        t1, v1 = window[-1]
        dt_h = (t1 - t0).total_seconds() / 3600.0
        if dt_h <= 0.01:
            return None, None
        roc = (v1 - v0) / dt_h
        # Piecewise ACC: first half ROC vs second half ROC
        mid = window[len(window) // 2]
        dt1 = (mid[0] - t0).total_seconds() / 3600.0
        dt2 = (t1 - mid[0]).total_seconds() / 3600.0
        if dt1 < 0.05 or dt2 < 0.05:
            return roc, None
        roc1 = (mid[1] - v0) / dt1
        roc2 = (v1 - mid[1]) / dt2
        acc = (roc2 - roc1) / max(dt_h * 0.5, 0.05)
        return roc, acc

    def _kin_min_deceleration(self, load_mw, proximity):
        """Load-scaled |acc| threshold (MW/h²)."""
        frac = self.KIN_MIN_DEC_FRAC * 0.6 if proximity else self.KIN_MIN_DEC_FRAC
        scaled = abs(float(load_mw or 0.0)) * frac
        return float(np.clip(scaled, self.KIN_MIN_DEC_FLOOR, self.KIN_MIN_DEC_CEIL))

    def _kin_apply_debias(self, now, t_peak_hrs, mode, short_ttp_hrs=None):
        """
        Correct systematic early bias (~-30m historic) without overshooting short.
        Returns debiased t_peak in hours.
        """
        debias_map = {
            "crest": self.KIN_DEBIAS_CREST_MIN,
            "shoulder": self.KIN_DEBIAS_SHOULDER_MIN,
            "quad_near": self.KIN_DEBIAS_QUAD_NEAR_MIN,
            "quad": self.KIN_DEBIAS_QUAD_MIN,
            "short_anchor": 10.0,
        }
        extra_min = debias_map.get(mode, 15.0)
        t = t_peak_hrs + extra_min / 60.0 + float(self._time_bias or 0.0)
        # Do not push past short peak by more than a few minutes
        if short_ttp_hrs is not None and short_ttp_hrs > 0:
            t = min(t, short_ttp_hrs + 12.0 / 60.0)
        # Keep a small positive horizon
        t = max(t, 5.0 / 60.0)
        return t

    def _predict_peak_using_kinematics(self, now):
        """
        EXPERIMENTAL kinematics v2 — maximize 2h pre-peak utility.

        vs baseline + v1:
          - Short-window obs ROC/ACC near peak
          - Crest / shoulder shortcuts
          - Load-scaled min deceleration + t_peak caps
          - Mode-specific late debias (historic kin-early bias)
          - Outer band (1–2h): short-anchored residual, not free quad
          - Suppress low-confidence plain far-quad emissions
        """
        MAX_VALID_PEAK_WINDOW_HRS = 4.0
        self._kin_confidence = 0.0
        self._kin_mode = "none"

        if self._state is None:
            self._kinematic_peak = None
            self._kinematic_peak_time = None
            return

        # --- Reference horizons ---
        time_to_peak_hrs = float("inf")
        short_ttp = None
        if self._predicted_peak_time:
            time_diff_sec = (self._predicted_peak_time - now).total_seconds()
            time_to_peak_hrs = max(time_diff_sec / 3600.0, 0.05)
        if self._short_forecast_peak_time is not None:
            st = (self._short_forecast_peak_time - now).total_seconds() / 3600.0
            if st > 0:
                short_ttp = st
                time_to_peak_hrs = min(time_to_peak_hrs, st)

        proximity = time_to_peak_hrs <= self.KIN_PROXIMITY_HRS
        inner = time_to_peak_hrs <= self.KIN_INNER_HRS
        outer = proximity and not inner  # (1h, 2h]

        # --- Derivatives ---
        obs_roc = self._observed_roc
        obs_acc = self._observed_acc
        sw_roc, sw_acc = self._short_window_derivatives(self.KIN_SHORT_WINDOW_MIN)
        if proximity and sw_roc is not None:
            obs_roc = 0.35 * self._observed_roc + 0.65 * sw_roc
            if sw_acc is not None:
                obs_acc = 0.30 * self._observed_acc + 0.70 * sw_acc

        if proximity:
            obs_weight = float(
                np.clip(
                    0.70 + 0.20 * (1.0 - time_to_peak_hrs / self.KIN_PROXIMITY_HRS),
                    0.65,
                    0.92,
                )
            )
        else:
            obs_weight = float(np.clip(1.0 - (time_to_peak_hrs - 0.5) / 2.0, 0.15, 0.85))
        if obs_roc > 100 and self._forecasted_roc < 0:
            obs_weight = max(obs_weight, 0.60)

        near_crest = False
        frac = 0.0
        if self._short_forecast_peak and self._state:
            frac = float(self._state) / float(self._short_forecast_peak)
            if frac >= self.KIN_NEAR_LOAD_FRAC and (sw_roc is None or sw_roc < 250):
                near_crest = True
                obs_weight = max(obs_weight, 0.88)

        fcst_weight = 1.0 - obs_weight
        blended_roc = (obs_weight * obs_roc + fcst_weight * self._forecasted_roc) + self._roc_bias
        blended_acc = (obs_weight * obs_acc + fcst_weight * self._forecasted_acc) + self._acc_bias

        def _emit(t_peak, mode, conf, load_mw=None):
            t_peak = self._kin_apply_debias(now, t_peak, mode, short_ttp)
            # Band caps after debias
            if inner:
                t_peak = min(t_peak, self.KIN_MAX_TPEAK_NEAR)
            elif outer:
                t_peak = min(t_peak, self.KIN_MAX_TPEAK_OUTER)
            if load_mw is None:
                load_mw = self._state + max(blended_roc, 0) * t_peak + 0.5 * min(blended_acc, 0) * t_peak * t_peak
                load_mw += self._magnitude_bias
            self._kinematic_peak = int(round(load_mw))
            self._kinematic_peak_time = now + timedelta(hours=float(t_peak))
            self._kin_confidence = float(np.clip(conf, 0.0, 1.0))
            self._kin_mode = mode
            _LOGGER.info(
                "Kin EXPv2 %s: peak≈%.0f MW in %.0fm conf=%.2f",
                mode, self._kinematic_peak, t_peak * 60, self._kin_confidence,
            )

        # --- Crest (inner preferred; also ok if frac high even mid-band) ---
        if near_crest and blended_roc < 180:
            if blended_roc <= 0:
                t_peak = 12.0 / 60.0
            else:
                t_peak = float(np.clip(blended_roc / 700.0, 10.0 / 60.0, 40.0 / 60.0))
            # Outer band crest: don't be too early — blend toward short a bit
            if outer and short_ttp is not None:
                t_peak = 0.45 * t_peak + 0.55 * min(short_ttp, self.KIN_MAX_TPEAK_OUTER)
            _emit(t_peak, "crest", 0.88 if inner else 0.72)
            return

        # --- Outer band (1–2h): short-anchored residual, not free quad ---
        if outer and short_ttp is not None and short_ttp > 0:
            # Kin residual: how much earlier/later than short based on deceleration
            residual_h = 0.0
            if blended_acc < -self._kin_min_deceleration(self._state, True) and blended_roc > 0:
                raw = -blended_roc / blended_acc
                if 0 < raw < 3:
                    residual_h = float(np.clip(raw - short_ttp, -0.6, 0.25))
            elif sw_roc is not None and sw_roc < 120:
                residual_h = -0.15  # mild earlier when already soft
            t_peak = (
                self.KIN_OUTER_SHORT_BLEND * short_ttp
                + (1.0 - self.KIN_OUTER_SHORT_BLEND) * max(short_ttp + residual_h, 0.1)
            )
            t_peak = float(np.clip(t_peak, 15.0 / 60.0, self.KIN_MAX_TPEAK_OUTER))
            conf = 0.62
            if sw_roc is not None and sw_roc < self._observed_roc:
                conf += 0.08
            _emit(t_peak, "short_anchor", conf)
            return

        # --- Guard: need deceleration for free quadratic ---
        min_dec = self._kin_min_deceleration(self._state, proximity)
        if self._forecasted_acc is not None and self._forecasted_acc < -25:
            min_dec = min(min_dec, max(self.KIN_MIN_DEC_FLOOR, min_dec * 0.7))

        if blended_acc == 0 or blended_acc >= 0 or abs(blended_acc) < min_dec:
            if (
                proximity
                and sw_roc is not None
                and self._observed_roc is not None
                and sw_roc < self._observed_roc - 80
                and sw_roc < 200
            ):
                t_peak = float(np.clip(max(sw_roc, 0) / 550.0, 12.0 / 60.0, 55.0 / 60.0))
                if short_ttp is not None and short_ttp > 0:
                    t_peak = 0.4 * t_peak + 0.6 * min(short_ttp, 1.2)
                _emit(t_peak, "shoulder", 0.60 if inner else 0.50)
                return
            # Far from peak without decel: do not emit noisy free kin
            self._kinematic_peak = None
            self._kinematic_peak_time = None
            return

        # --- Quadratic solve (mainly inner proximity) ---
        raw_t_peak = -blended_roc / blended_acc
        t_peak = raw_t_peak

        max_win = (
            self.KIN_MAX_TPEAK_NEAR
            if inner
            else (self.KIN_MAX_TPEAK_OUTER if proximity else MAX_VALID_PEAK_WINDOW_HRS)
        )
        if not (0 < t_peak <= max_win + 0.5):
            if proximity and 0 < raw_t_peak <= 2.5:
                t_peak = min(raw_t_peak, max_win)
            else:
                self._kinematic_peak = None
                self._kinematic_peak_time = None
                return

        t_peak = min(t_peak, max_win)
        # Inner: light short blend; outer handled above
        if inner and short_ttp is not None and short_ttp > 0:
            t_peak = 0.55 * t_peak + 0.45 * min(short_ttp, self.KIN_MAX_TPEAK_NEAR)

        conf = 0.48
        if proximity:
            conf += 0.12
        if inner:
            conf += 0.08
        if abs(blended_acc) > min_dec * 1.5:
            conf += 0.12
        if sw_roc is not None and sw_roc < blended_roc:
            conf += 0.08
        if t_peak < 1.0:
            conf += 0.08
        conf = float(np.clip(conf, 0.0, 1.0))
        mode = "quad_near" if proximity else "quad"

        # Suppress low-confidence far quadratic (historically MAE ~80m)
        if mode == "quad" and conf < self.KIN_MIN_CONF_EMIT + 0.15:
            self._kinematic_peak = None
            self._kinematic_peak_time = None
            return
        if mode == "quad_near" and conf < self.KIN_MIN_CONF_EMIT:
            self._kinematic_peak = None
            self._kinematic_peak_time = None
            return

        load_mw = (
            self._state
            + blended_roc * t_peak
            + 0.5 * blended_acc * (t_peak ** 2)
            + self._magnitude_bias
        )
        _emit(t_peak, mode, conf, load_mw=load_mw)

    def _weighted_peak_prediction(self):
        """
        Blend short + daily + gated kinematics.

        Calibrated on 6/29–7/9 to beat short-forecast-alone at 16:00:
          short ~0.78, daily ~0.04, kin ~0.18 only if within 45 min of short peak.
        Short is the dominant time signal; day-ahead was pulling predictions early.
        """
        now = dt_util.now()
        predictions = []
        weights = []

        # Experimental blend: short-heavy with confidence-scaled kin (historic sweep winner)
        w_short, w_daily, w_kin = 0.80, 0.05, self.BLEND_W_KIN_DEFAULT
        try:
            local_hour = dt_util.as_local(now).hour
        except Exception:
            local_hour = now.hour
        if local_hour < max(14, self._decision_hour - 1):
            w_short, w_daily, w_kin = 0.55, 0.30, min(0.18, w_kin)
        if self._short_forecast_peak_time is not None:
            try:
                sl = dt_util.as_local(self._short_forecast_peak_time)
            except Exception:
                sl = self._short_forecast_peak_time
            if sl.hour + sl.minute / 60.0 < 15.0:
                w_short, w_daily, w_kin = 1.0, 0.0, 0.0
        if getattr(self, "_allow_early_move", False):
            w_short, w_daily, w_kin = 1.0, 0.0, 0.0

        # Short-term forecast (primary)
        if self._short_forecast_peak_time and self._short_forecast_peak:
            if self._short_forecast_peak_time > now - timedelta(minutes=20):
                predictions.append((self._short_forecast_peak_time, self._short_forecast_peak))
                weights.append(w_short)

        # Daily forecast (small anchor only)
        if self._daily_forecast_peak_time and self._daily_forecast_peak and w_daily > 0:
            predictions.append((self._daily_forecast_peak_time, self._daily_forecast_peak))
            weights.append(w_daily)

        # Kin: anti-late + conf-scaled weight; rare high-conf crest earlier than short
        if self._kinematic_peak_time and self._kinematic_peak and w_kin > 0:
            time_diff = abs((self._kinematic_peak_time - now).total_seconds() / 3600)
            admit = False
            kin_w = w_kin
            if time_diff < 3:
                if self._short_forecast_peak_time:
                    delta_min = (
                        self._kinematic_peak_time - self._short_forecast_peak_time
                    ).total_seconds() / 60.0
                    if delta_min > self.KIN_NO_LATER_THAN_SHORT_MIN:
                        admit = False
                    elif abs(delta_min) <= 45:
                        admit = True
                        conf = float(getattr(self, "_kin_confidence", 0.0))
                        mode = getattr(self, "_kin_mode", "")
                        if conf >= self.BLEND_KIN_CONF_THRESH and mode in (
                            "crest",
                            "shoulder",
                            "short_anchor",
                            "quad_near",
                        ):
                            scale = min(1.0, conf / self.BLEND_KIN_CONF_THRESH)
                            kin_w = self.BLEND_W_KIN_HIGH * scale
                    elif (
                        self.BLEND_ALLOW_EARLY_CREST
                        and -75 <= delta_min < -45
                        and getattr(self, "_kin_mode", "") in ("crest", "shoulder")
                        and float(getattr(self, "_kin_confidence", 0.0))
                        >= self.BLEND_KIN_CONF_THRESH
                    ):
                        admit = True
                        kin_w = min(self.BLEND_W_KIN_HIGH, w_kin + 0.08)
                else:
                    admit = True
                    kin_w = w_kin * 0.5

            if admit:
                predictions.append((self._kinematic_peak_time, self._kinematic_peak))
                weights.append(kin_w)

        if not predictions:
            return

        peak_time = sum((p[0].timestamp() * w for p, w in zip(predictions, weights))) / sum(weights)
        peak_magnitude = sum((p[1] * w for p, w in zip(predictions, weights))) / sum(weights)
        raw_pred_time = datetime.fromtimestamp(peak_time, tz=timezone.utc)

        # Light EMA for stability (same-day); snap across days
        last_blended = getattr(self, '_last_blended_pred_time', None)
        cross_day = (
            last_blended is not None
            and raw_pred_time.date() != last_blended.date()
        )
        if last_blended is None or cross_day:
            ema_pred = raw_pred_time
        elif getattr(self, "_allow_early_move", False):
            # Hour-match pull/compress: snap freely toward raw (no anti-early EMA)
            alpha = 0.90
            delta_minutes = (raw_pred_time - last_blended).total_seconds() / 60
            adj = raw_pred_time
            if abs(delta_minutes) > 90:
                direction = float(np.sign(delta_minutes))
                adj = last_blended + timedelta(minutes=direction * 90.0)
            ema_pred = datetime.fromtimestamp(
                alpha * adj.timestamp() + (1 - alpha) * last_blended.timestamp(),
                tz=timezone.utc
            )
        else:
            # Higher alpha = track short more tightly (short is best time signal)
            alpha = 0.65
            max_move_minutes = 30.0
            try:
                if dt_util.as_local(now).hour >= 15:
                    alpha = 0.80
                    max_move_minutes = 40.0
            except Exception:
                pass
            delta_minutes = (raw_pred_time - last_blended).total_seconds() / 60
            adj = raw_pred_time
            if abs(delta_minutes) > max_move_minutes:
                direction = float(np.sign(delta_minutes))
                adj = last_blended + timedelta(minutes=direction * max_move_minutes)
            # Resist large early pulls while load still rising
            if delta_minutes < -8 and self._observed_roc > 400:
                adj = last_blended + timedelta(minutes=max(delta_minutes, -5.0))
                alpha = min(alpha, 0.35)
            # Chase later when short moved later (recover from early-pull stickiness)
            if (
                delta_minutes > 20
                and self._observed_roc > 0
                and not getattr(self, "_short_still_rising", False)
            ):
                alpha = max(alpha, 0.85)
                if abs(delta_minutes) > 40:
                    adj = raw_pred_time
            if delta_minutes > 12 and self._observed_roc > 100:
                alpha = max(alpha, 0.85)
                adj = raw_pred_time
            ema_pred = datetime.fromtimestamp(
                alpha * adj.timestamp() + (1 - alpha) * last_blended.timestamp(),
                tz=timezone.utc
            )

        self._last_blended_pred_time = ema_pred
        self._predicted_peak_time = ema_pred
        self._predicted_peak = peak_magnitude
        # One-shot: free-EMA / short-only weights only for the cycle that applied
        # hour-match corrections (avoids sticky allow_early_move on later kin-only runs)
        self._allow_early_move = False

        # Curtailment hybrid: sticky-later + hard in-hour escape
        self._maybe_apply_curtailment_guards(now)

    def _hybrid_obs_regime(self):
        roc = self._observed_roc
        frac = 0.5
        if self._max_daily_load and self._state:
            frac = float(self._state) / max(float(self._max_daily_load), 1.0)
        if roc > 200:
            return "Rising"
        if roc <= -50 and frac >= 0.97:
            return "Rolling"
        if roc < 100:
            return "Soft"
        return "Rising"

    def _hybrid_zone_hour_prior(self):
        """Empirical peak-hour prior from this zone's stored history (no leakage)."""
        prior = {h: 0.05 for h in range(12, 22)}
        hist = list(getattr(self, "_peak_hour_history", []) or [])
        if not hist:
            # weak summer PJM default
            for h, w in ((15, 0.12), (16, 0.28), (17, 0.30), (18, 0.15), (14, 0.08), (19, 0.07)):
                prior[h] = w
            return prior
        from collections import Counter
        c = Counter(int(h) for h in hist if 12 <= int(h) <= 21)
        total = sum(c.values()) or 1
        for h in range(12, 22):
            prior[h] = c.get(h, 0) / total
        return prior

    def _hybrid_stat_blend_hour(self, now, pred):
        """Weighted hour vote: RC / short / zone prior / online rising-credit."""
        scores = {}
        try:
            pred_loc = dt_util.as_local(pred)
            now_loc = dt_util.as_local(now)
        except Exception:
            pred_loc, now_loc = pred, now
        scores[pred_loc.hour] = scores.get(pred_loc.hour, 0.0) + self.STAT_W_RC
        if self._short_forecast_peak_time is not None:
            try:
                sh = dt_util.as_local(self._short_forecast_peak_time).hour
            except Exception:
                sh = self._short_forecast_peak_time.hour
            scores[sh] = scores.get(sh, 0.0) + self.STAT_W_SHORT
        prior = self._hybrid_zone_hour_prior()
        for h, p in prior.items():
            scores[h] = scores.get(h, 0.0) + self.STAT_W_PRIOR * p * 5.0
        for h, s in (self._online_hour_score or {}).items():
            scores[int(h)] = scores.get(int(h), 0.0) + self.STAT_W_ONLINE * float(s)
        if not scores:
            return pred
        best_h = max(scores.items(), key=lambda x: (x[1], x[0]))[0]
        target = now_loc.replace(hour=int(best_h), minute=25, second=0, microsecond=0)
        if target.hour <= now_loc.hour and target.date() == now_loc.date():
            # keep not-past: bump to next hour if chosen hour already passed
            if best_h <= now_loc.hour:
                target = now_loc.replace(minute=0, second=0, microsecond=0) + timedelta(
                    hours=1, minutes=25
                )
        try:
            return target.astimezone(timezone.utc)
        except Exception:
            return target.replace(tzinfo=timezone.utc)

    def _maybe_apply_curtailment_guards(self, now):
        """
        Post-process predicted_peak_time (frozen 100-gen g69_m1_stat stack).

        Statistical hour blend → no-early-arm / logistic-style confirm →
        sticky-later → soft/hard escape. Markov regime adjusts commit/escape.
        EWMA peakiness can delay early arming.
        """
        self._hybrid_method = "none"
        self._hybrid_escaped = False
        if self._prediction_time_locked or self._predicted_peak_time is None:
            return

        pred = self._predicted_peak_time
        try:
            pred_loc = dt_util.as_local(pred)
            now_loc = dt_util.as_local(now)
        except Exception:
            pred_loc = pred
            now_loc = now

        local_h = now_loc.hour + now_loc.minute / 60.0 + now_loc.second / 3600.0

        # --- Online stats + EWMA + Markov regime ---
        self._hybrid_regime = self._hybrid_obs_regime()
        if self._observed_roc > 0:
            for src in (self._short_forecast_peak_time, self._daily_forecast_peak_time):
                if src is None:
                    continue
                try:
                    h = dt_util.as_local(src).hour
                except Exception:
                    h = src.hour
                prev = float(self._online_hour_score.get(h, 0.0))
                self._online_hour_score[h] = (
                    (1.0 - self.STAT_ONLINE_LR) * prev + self.STAT_ONLINE_LR * 1.0
                )
        frac = 0.0
        if self._max_daily_load and self._state:
            frac = float(self._state) / max(float(self._max_daily_load), 1.0)
        roc_term = 1.0 / (1.0 + max(self._observed_roc, 0.0) / 500.0)
        peakiness = 0.6 * frac + 0.4 * roc_term
        self._ewma_peakiness = (
            self.EWMA_ALPHA * peakiness + (1.0 - self.EWMA_ALPHA) * self._ewma_peakiness
        )

        # Statistical hour blend
        pred = self._hybrid_stat_blend_hour(now, pred)
        self._hybrid_method = "stat_hour"
        try:
            pred_loc = dt_util.as_local(pred)
        except Exception:
            pred_loc = pred

        # Commit hour (Markov can pull earlier when Rolling)
        commit_h = self.COMMIT_HOUR
        if self._hybrid_regime == "Rolling" and self.MARKOV_COMMIT_BOOST > 0:
            commit_h = max(13.0, commit_h - self.MARKOV_COMMIT_BOOST)

        rolling = False
        if (
            self._max_daily_load is not None
            and self._state is not None
            and self._max_daily_load > 0
        ):
            near_max = self._state >= self.ESCAPE_NEAR_MAX_FRAC * float(self._max_daily_load)
            dropped = self._state <= self._max_daily_load - self.ESCAPE_ROLL_DROP_MW
            declining = self._observed_roc <= self.ESCAPE_ROLL_ROC
            rolling = bool(near_max and dropped and declining)

        committed = local_h >= commit_h or rolling
        # EWMA can delay arming until cresting
        if (
            not rolling
            and self._ewma_peakiness < self.EWMA_ARM_THRESH
            and local_h < commit_h + 1.0
        ):
            committed = False

        # --- No-early-arm ---
        if not committed:
            next_start = now_loc.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
            earliest = next_start + timedelta(minutes=self.MIN_AHEAD_MIN)
            if pred_loc.hour <= now_loc.hour or pred_loc < earliest:
                pred_loc = earliest
                try:
                    pred = pred_loc.astimezone(timezone.utc)
                    pred_loc = dt_util.as_local(pred)
                except Exception:
                    pred = pred_loc.replace(tzinfo=timezone.utc)
                self._hybrid_method = "no_early_arm"
        else:
            # post-commit: require short agreement or rollover to sit in current hour
            if self.POST_COMMIT_REQUIRE_SHORT and pred_loc.hour == now_loc.hour:
                short_ok = False
                if self._short_forecast_peak_time is not None:
                    try:
                        short_ok = dt_util.as_local(self._short_forecast_peak_time).hour == now_loc.hour
                    except Exception:
                        short_ok = self._short_forecast_peak_time.hour == now_loc.hour
                if not (short_ok or rolling):
                    next_start = now_loc.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
                    earliest = next_start + timedelta(minutes=self.MIN_AHEAD_MIN)
                    pred_loc = earliest
                    try:
                        pred = pred_loc.astimezone(timezone.utc)
                        pred_loc = dt_util.as_local(pred)
                    except Exception:
                        pred = pred_loc.replace(tzinfo=timezone.utc)
                    self._hybrid_method = "post_commit_block"

        # --- Sticky later ---
        if self.STICKY_LATER_HOURS:
            ph = pred_loc.hour
            if self._sticky_min_peak_hour is None:
                self._sticky_min_peak_hour = ph
            elif ph < self._sticky_min_peak_hour:
                fixed = pred_loc.replace(
                    hour=self._sticky_min_peak_hour,
                    minute=max(pred_loc.minute, 15),
                    second=0,
                    microsecond=0,
                )
                try:
                    pred = fixed.astimezone(timezone.utc)
                    pred_loc = dt_util.as_local(pred)
                except Exception:
                    pred = fixed.replace(tzinfo=timezone.utc)
                    pred_loc = fixed
                self._hybrid_method = "sticky_later"
            else:
                self._sticky_min_peak_hour = max(self._sticky_min_peak_hour, ph)

        # --- Escape ---
        pred_hour_start = pred_loc.replace(minute=0, second=0, microsecond=0)
        pred_hour_end = pred_hour_start + timedelta(hours=1)
        in_pred_hour = pred_hour_start <= now_loc < pred_hour_end
        if in_pred_hour:
            into_min = (now_loc - pred_hour_start).total_seconds() / 60.0
            esc_into = self.ESCAPE_INTO_MIN
            if self._hybrid_regime == "Rising":
                esc_into *= self.MARKOV_ESCAPE_SCALE

            protect = False
            if into_min < self.ESCAPE_PROTECT_FIRST_MIN and self._short_forecast_peak_time is not None:
                try:
                    sh = dt_util.as_local(self._short_forecast_peak_time).hour
                except Exception:
                    sh = self._short_forecast_peak_time.hour
                protect = sh == pred_loc.hour

            do_escape = False
            if not protect and not rolling:
                if into_min >= esc_into:
                    do_escape = True
                elif into_min >= self.ESCAPE_SOFT_INTO_MIN:
                    if self._observed_roc > self.ESCAPE_SOFT_ROC:
                        do_escape = True
                    elif self._short_forecast_peak_time is not None:
                        lead = (
                            self._short_forecast_peak_time - now
                        ).total_seconds() / 60.0
                        if lead >= self.ESCAPE_SOFT_SHORT_LEAD_MIN:
                            do_escape = True

            if do_escape:
                nxt = pred_hour_start + timedelta(
                    hours=1, minutes=self.ESCAPE_BUMP_MINUTES
                )
                try:
                    pred = nxt.astimezone(timezone.utc)
                except Exception:
                    pred = nxt.replace(tzinfo=timezone.utc)
                self._hybrid_escaped = True
                self._hybrid_method = "hard_escape"
                if self.STICKY_LATER_HOURS:
                    self._sticky_min_peak_hour = max(
                        self._sticky_min_peak_hour or 0,
                        (pred_hour_start + timedelta(hours=1)).hour,
                    )
                _LOGGER.info(
                    "Hybrid escape: into=%.0fm roc=%.0f regime=%s -> %s",
                    into_min,
                    self._observed_roc,
                    self._hybrid_regime,
                    pred.isoformat() if hasattr(pred, "isoformat") else pred,
                )

        self._predicted_peak_time = pred
        self._last_blended_pred_time = pred

    def _evaluate_5cp_risk(self):
        """Flag high-risk day if predicted peak is near the effective 5CP bar.

        Bar = max(user peak_threshold, 5th-highest stored peak) when 5 peaks exist,
        else the user threshold. Trip when predicted_peak >= HIGH_RISK_PEAK_FRACTION * bar.
        Sticky once seen (Cover5CP) so a mid-day forecast drop cannot un-arm cover.
        """
        if not self._predicted_peak:
            if not (self._cover5cp_sticky_hr and self._saw_high_risk):
                self._high_risk_day = False
            return
        fifth_peak = self._get_fifth_highest_peak()
        try:
            bar = float(fifth_peak or 0.0)
        except (TypeError, ValueError):
            bar = 0.0
        trip_mw = HIGH_RISK_PEAK_FRACTION * bar if bar > 0 else 0.0
        self._high_risk_day = bar > 0 and self._predicted_peak >= trip_mw
        if self._high_risk_day:
            self._saw_high_risk = True
        # Sticky HR once seen (Cover5CP) — avoid mid-day drop → miss
        if self._cover5cp_sticky_hr and self._saw_high_risk:
            self._high_risk_day = True

    def _cover5cp_local_hour(self, ts):
        if ts is None:
            return None
        try:
            return dt_util.as_local(ts).hour
        except Exception:
            try:
                return ts.hour
            except Exception:
                return None

    def _cover5cp_is_local_zone(self) -> bool:
        """COMED-scale threshold (~18k) vs RTO (~140k)."""
        try:
            return float(self._peak_threshold or 0) < 50_000.0
        except Exception:
            return False

    def _cover5cp_signal_hours(self):
        short_h = self._cover5cp_local_hour(self._short_forecast_peak_time)
        live_h = self._cover5cp_local_hour(self._predicted_peak_time)
        da_h = self._cover5cp_local_hour(self._daily_forecast_peak_time)
        mh = self._cover5cp_local_hour(self._max_daily_load_time)
        return short_h, live_h, da_h, mh

    def _cover5cp_false_peak(self, now, short_h, live_h, da_h, mh) -> bool:
        """COMED-scale false near-term peak (blocks COM 7/2 early arms)."""
        if not self._cover5cp_false_peak_push or not self._cover5cp_is_local_zone():
            return False
        try:
            nl = dt_util.as_local(now)
        except Exception:
            nl = now
        roc = float(self._observed_roc or 0.0)
        if roc < self._cover5cp_climb_roc or roc >= 1500 or nl.hour >= 17:
            return False
        sigs = [h for h in (short_h, live_h, da_h) if h is not None]
        if not sigs or max(sigs) > nl.hour:
            return False
        if mh is None:
            return False
        return mh >= nl.hour - 1

    def _cover5cp_peak_est(self, now):
        """Later-of-near-ties peak hour estimate (short/live/DA/online)."""
        scores = {}

        def add(h, w):
            if h is None:
                return
            h = int(h)
            if 12 <= h <= 21:
                scores[h] = scores.get(h, 0.0) + w

        short_h, live_h, da_h, mh = self._cover5cp_signal_hours()
        if live_h is not None:
            add(live_h, 1.0)
        if short_h is not None:
            add(short_h, 1.6)
        if da_h is not None:
            add(da_h, 0.7)
        for h, s in (getattr(self, "_online_hour_score", None) or {}).items():
            try:
                add(int(h), float(s))
            except Exception:
                pass

        try:
            nl = dt_util.as_local(now)
        except Exception:
            nl = now
        roc = float(self._observed_roc or 0.0)

        if roc >= self._cover5cp_climb_roc and short_h is not None and short_h > nl.hour:
            add(short_h, 2.0)
            if nl.hour in scores:
                scores[nl.hour] *= 0.2

        self._cover5cp_last_false_peak = self._cover5cp_false_peak(
            now, short_h, live_h, da_h, mh
        )
        if self._cover5cp_last_false_peak:
            add(min(21, nl.hour + 1), 3.5)

        if not scores:
            return None
        mx = max(scores.values())
        cont = [h for h, s in scores.items() if s >= 0.75 * mx]
        return max(cont)

    def _check_peak_hour_active(self, now):
        """
        Cover5CP peak_hour_active (prefer on-hour + 5CP cover safety).

        Dual goal:
          1) Never late/miss: first arm in [actual-2, actual]
          2) Prefer first arm on peak hour (official good / armOK)

        Priority: lock hour when engaged → Cover5CP window/force → single-hour
        after first arm (lock may re-hit true peak). Snapshots never drive active.
        """
        # Late-day freeze / peak already done: never arm (incl. reboot at 10pm)
        if self._daily_peak_occurred or self._is_post_peak_evening(now):
            self._peak_hour_active = False
            return

        if self._cover5cp_sticky_hr and self._saw_high_risk:
            self._high_risk_day = True

        if not self._high_risk_day:
            self._peak_hour_active = False
            return

        try:
            nl = dt_util.as_local(now)
        except Exception:
            nl = now

        short_h, live_h, da_h, mh = self._cover5cp_signal_hours()
        peak = self._cover5cp_peak_est(now)
        false_peak = self._cover5cp_last_false_peak
        locked = (
            self._prediction_time_locked and self._locked_peak_time is not None
        )
        lock_h = self._cover5cp_local_hour(self._locked_peak_time) if locked else None
        roc = float(self._observed_roc or 0.0)
        max_early = self._cover5cp_max_early_hours

        # Lock on true peak hour (may re-engage after early first arm)
        if locked and lock_h == nl.hour:
            self._peak_hour_active = True
            if not self._cover5cp_armed_any:
                self._cover5cp_armed_any = True
                self._cover5cp_first_arm_h = nl.hour
            return

        # Single-hour after first arm (FP control); lock above still allowed
        if (
            self._cover5cp_single_hour
            and self._cover5cp_armed_any
            and self._cover5cp_first_arm_h is not None
            and nl.hour != self._cover5cp_first_arm_h
        ):
            self._peak_hour_active = False
            return

        if peak is None:
            peak = live_h or short_h or da_h
        if peak is None:
            self._peak_hour_active = False
            return

        for h in (peak, short_h, da_h, live_h):
            if h is not None and 12 <= h <= 21:
                self._cover5cp_peak_hint_max = max(self._cover5cp_peak_hint_max, h)

        hard_latest = max(
            [self._cover5cp_peak_hint_max]
            + [h for h in (peak, short_h, da_h, live_h) if h is not None]
        )
        peak = max(peak, hard_latest)

        no_short = short_h is None
        if no_short and da_h is not None and not self._cover5cp_armed_any:
            peak = da_h
            hard_latest = da_h

        if nl.hour < hard_latest - max_early:
            self._peak_hour_active = False
            return
        for h in (short_h, da_h, peak, hard_latest):
            if h is not None and h - nl.hour > max_early:
                self._peak_hour_active = False
                return

        pin_now = (short_h == nl.hour) or (live_h == nl.hour)
        has_short = short_h is not None
        max_age = None
        if self._max_daily_load_time is not None:
            try:
                max_age = (now - self._max_daily_load_time).total_seconds() / 60.0
            except Exception:
                max_age = None

        if not self._cover5cp_armed_any and not (locked and lock_h == nl.hour):
            if (
                self._cover5cp_climb_wait
                and roc >= self._cover5cp_climb_roc
                and hard_latest - nl.hour >= 2
            ):
                self._peak_hour_active = False
                return

            at_estimate = hard_latest <= nl.hour
            if at_estimate and (pin_now or no_short):
                pass
            else:
                if (
                    self._cover5cp_climb_wait
                    and self._cover5cp_is_local_zone()
                    and has_short
                    and roc < 1500
                    and not at_estimate
                ):
                    if false_peak:
                        self._peak_hour_active = False
                        return
                    if (
                        roc >= self._cover5cp_climb_roc
                        and mh is not None
                        and mh >= nl.hour - 1
                        and hard_latest > nl.hour
                        and nl.hour < 18
                    ):
                        self._peak_hour_active = False
                        return
                    if (
                        max_age is not None
                        and max_age >= 35.0
                        and roc >= max(50.0, self._cover5cp_climb_roc * 0.4)
                        and nl.hour < 19
                        and hard_latest > nl.hour
                    ):
                        self._peak_hour_active = False
                        return

                if self._cover5cp_prefer_on_hour and hard_latest > nl.hour:
                    early_peak_cover = (
                        self._cover5cp_cover_force
                        and mh == nl.hour
                        and hard_latest - nl.hour == 1
                        and max_age is not None
                        and max_age >= 20.0
                        and not false_peak
                    )
                    allow_early = (
                        self._cover5cp_lead >= 1
                        and hard_latest - nl.hour == 1
                        and not false_peak
                        and pin_now
                    )
                    if not (allow_early or early_peak_cover):
                        self._peak_hour_active = False
                        return

        if self._cover5cp_prefer_on_hour and not self._cover5cp_armed_any:
            win_lo = hard_latest
            if pin_now and not false_peak and hard_latest - nl.hour <= 1:
                win_lo = min(win_lo, nl.hour)
            if mh == nl.hour and hard_latest == nl.hour + 1:
                win_lo = min(win_lo, nl.hour)
        else:
            win_lo = peak - self._cover5cp_lead
        win_hi = max(peak, hard_latest) + self._cover5cp_trail
        win_lo = max(win_lo, hard_latest - max_early)
        win_lo = max(12, win_lo)
        win_hi = min(21, win_hi)

        if no_short and da_h is not None and not self._cover5cp_armed_any:
            if self._cover5cp_prefer_on_hour:
                win_lo = da_h
                win_hi = da_h + max(self._cover5cp_trail, 0)
            else:
                win_lo = max(12, da_h - self._cover5cp_lead)
                win_hi = min(21, da_h + max(self._cover5cp_trail, 0))

        in_window = win_lo <= nl.hour <= win_hi

        if (
            not self._cover5cp_armed_any
            and pin_now
            and not false_peak
            and 12 <= nl.hour <= 21
            and hard_latest - nl.hour < 2
        ):
            in_window = True

        if not self._cover5cp_armed_any and nl.hour == hard_latest:
            in_window = True

        if not in_window:
            if not self._cover5cp_armed_any and nl.hour == peak:
                in_window = True
            elif not self._cover5cp_armed_any and nl.hour == hard_latest:
                in_window = True
            elif (
                self._cover5cp_cover_force
                and not self._cover5cp_armed_any
                and no_short
                and da_h is not None
                and nl.hour >= da_h
            ):
                in_window = True
            else:
                self._peak_hour_active = False
                return

        # Pin live pred to TOH of this hour (delay < 5 for armed_ok)
        try:
            ref = nl.replace(minute=0, second=0, microsecond=0)
            try:
                self._predicted_peak_time = dt_util.as_utc(ref)
            except Exception:
                self._predicted_peak_time = ref
        except Exception:
            pass

        pstart = nl.replace(minute=0, second=0, microsecond=0)
        try:
            pstart_utc = dt_util.as_utc(pstart)
        except Exception:
            pstart_utc = pstart
        try:
            pend = pstart_utc + timedelta(hours=1)
            if pstart_utc <= now < pend:
                self._peak_hour_active = True
                if not self._cover5cp_armed_any:
                    self._cover5cp_armed_any = True
                    self._cover5cp_first_arm_h = nl.hour
            else:
                self._peak_hour_active = False
        except Exception:
            self._peak_hour_active = True
            if not self._cover5cp_armed_any:
                self._cover5cp_armed_any = True
                self._cover5cp_first_arm_h = nl.hour

    def _local_hour(self, now=None):
        """Local clock hour (America/Chicago via HA dt_util)."""
        if now is None:
            now = dt_util.now()
        try:
            return dt_util.as_local(now).hour
        except Exception:
            return now.hour

    def _in_afternoon_peak_window(self, now=None):
        """
        True only during hours when negative ROC means "past the daily peak",
        not the normal overnight decline into the 2–4am load minimum.

        ROC is almost always < 0 from midnight through early morning; freeze/lock
        must ignore that valley. 5CP-relevant peaks are afternoon/evening.
        """
        h = self._local_hour(now)
        return 12 <= h <= 21

    def _short_window_roc(self, minutes=20):
        """
        Rate of change (MW/hr) over the last `minutes` of load history only.
        Much less laggy than the long-window smoothed observed_roc — used for
        operational peak lock, not for kinematics blending.
        """
        if not self._load_history or len(self._load_history) < 2:
            return None
        now_ts = self._load_history[-1][0]
        cutoff = now_ts - timedelta(minutes=minutes)
        window = [(t, v) for t, v in self._load_history if t >= cutoff]
        if len(window) < 2:
            return None
        window.sort(key=lambda x: x[0])
        dt_hrs = (window[-1][0] - window[0][0]).total_seconds() / 3600.0
        if dt_hrs <= 0:
            return None
        return (window[-1][1] - window[0][1]) / dt_hrs

    def _maybe_engage_operational_lock(self, now):
        """
        Lock predicted peak TIME to the observed daily max once load has clearly
        rolled over. Prevents kinematics / short-forecast / min-ahead rules from
        walking the prediction later (e.g. to 17:05) after a mid-afternoon peak.

        Trigger (all must hold):
          1. At least lock_min_minutes_after_max since _max_daily_load_time
          2. Current load <= max - lock_drop_mw for lock_streak_needed updates
          3. Short-window ROC (<~20 min) is negative
        """
        if self._prediction_time_locked:
            # Keep live prediction pinned to the lock for curtailment/display
            if self._locked_peak_time is not None:
                self._predicted_peak_time = self._locked_peak_time
            if self._locked_peak_mw is not None:
                # Prefer higher of locked max and any later true max (shouldn't rise if locked right)
                if self._max_daily_load and self._max_daily_load > self._locked_peak_mw:
                    self._locked_peak_mw = self._max_daily_load
                    self._locked_peak_time = self._max_daily_load_time
                    self._predicted_peak_time = self._locked_peak_time
                self._predicted_peak = self._locked_peak_mw
            return

        if self._max_daily_load is None or self._max_daily_load_time is None:
            return
        if self._state is None:
            return

        # Never ops-lock overnight/early morning. Load declines into the 2–4am
        # minimum with negative short-window ROC every night — that is not a peak.
        if not self._in_afternoon_peak_window(now):
            return
        # Observed max must itself be in the afternoon peak window (ignore
        # midnight–morning local maxes that are just the day's starting load).
        try:
            max_hour = dt_util.as_local(self._max_daily_load_time).hour
        except Exception:
            max_hour = self._max_daily_load_time.hour
        if max_hour < 12 or max_hour > 21:
            return

        # Zone-aware drop: min(base_mw, max(50, frac * peak)) so flat zonal plateaus
        # (e.g. ComEd ~18 GW) can lock without needing a 150 MW dump, while RTO still
        # uses up to the base floor when 0.5% would be huge.
        drop_thresh = min(
            self._lock_drop_mw,
            max(50.0, self._lock_drop_frac * float(self._max_daily_load)),
        )

        # Track consecutive samples below daily max by drop threshold
        if self._state <= self._max_daily_load - drop_thresh:
            self._below_max_streak += 1
        else:
            self._below_max_streak = 0

        short_roc = self._short_window_roc(minutes=20)
        if short_roc is not None and short_roc < 0:
            self._short_roc_neg_streak += 1
        else:
            self._short_roc_neg_streak = 0

        minutes_after_max = (now - self._max_daily_load_time).total_seconds() / 60.0
        if minutes_after_max < self._lock_min_minutes_after_max:
            return

        # Path A: clear drop below max + short-window decline
        path_a = (
            self._below_max_streak >= self._lock_streak_needed
            and short_roc is not None
            and short_roc < 0
        )
        # Path B (plateau): disabled in hr90 (_lock_plateau_minutes=999)
        path_b = (
            self._lock_plateau_minutes < 900
            and minutes_after_max >= self._lock_plateau_minutes
            and self._short_roc_neg_streak >= 3
            and self._state <= self._max_daily_load
        )

        if not (path_a or path_b):
            return

        # hr90: never ops-lock while short-term forecast still points later
        if self._short_forecast_peak_time is not None:
            short_lead = (self._short_forecast_peak_time - now).total_seconds() / 60.0
            drop_frac = 0.0
            if self._max_daily_load:
                drop_frac = (self._max_daily_load - self._state) / max(self._max_daily_load, 1.0)
            block_min = float(getattr(self, "_lock_short_later_block_min", 25.0))
            require_not_later = getattr(self, "_lock_require_short_not_later", True)
            if require_not_later and short_lead > 0:
                _LOGGER.debug(
                    "Skip ops lock: short still +%.0fm ahead (hr90 require_not_later)",
                    short_lead,
                )
                return
            if short_lead >= block_min and drop_frac < 0.008 and (
                getattr(self, "_short_still_rising", False) or short_lead >= 60
            ):
                _LOGGER.debug(
                    "Skip ops lock: short still +%.0fm ahead, drop only %.2f%%",
                    short_lead, 100 * drop_frac,
                )
                return

        # Engage lock at observed peak
        self._prediction_time_locked = True
        self._locked_peak_time = self._max_daily_load_time
        self._locked_peak_mw = self._max_daily_load
        self._predicted_peak_time = self._locked_peak_time
        self._predicted_peak = self._locked_peak_mw
        # Stop further forecast refinement for the day (same effect as freeze for time)
        self._daily_peak_occurred = True

        _LOGGER.info(
            "Operational peak lock engaged (%s): observed max %.0f MW @ %s "
            "(%.0f min ago, drop_thresh=%.0f, below_max_streak=%d, "
            "short_roc=%s, short_roc_neg_streak=%d). "
            "Predicted peak time locked; will not walk later.",
            "drop" if path_a else "plateau",
            self._locked_peak_mw,
            self._locked_peak_time.isoformat() if self._locked_peak_time else None,
            minutes_after_max,
            drop_thresh,
            self._below_max_streak,
            f"{short_roc:.0f}" if short_roc is not None else "n/a",
            self._short_roc_neg_streak,
        )

    def _get_high_load_threshold(self):
        """
        Zone-relative high-load threshold from rolling peak history (p75),
        falling back to configured peak_threshold. No zone-name table.
        """
        if len(self._peak_mw_history) >= 5:
            arr = np.array(list(self._peak_mw_history), dtype=float)
            return float(np.percentile(arr, 75))
        if self._peak_threshold:
            return float(self._peak_threshold)
        if self._daily_forecast_peak:
            return float(self._daily_forecast_peak) * 0.95
        return 148000.0  # last-resort cold start (RTO-scale); overridden once history exists

    def _refresh_decision_hour(self, now=None):
        """
        Update today's decision_hour from today's peak-time estimate + slow prior.
        Works for any zone without a profile map; adapts as peak hours drift.
        """
        # Today's estimate: prefer short, else daily, else live predicted
        est = None
        if self._short_forecast_peak_time is not None:
            # Only trust short for decision hour when not still extremely early morning
            est = self._short_forecast_peak_time
        elif self._daily_forecast_peak_time is not None:
            est = self._daily_forecast_peak_time
        elif self._predicted_peak_time is not None:
            est = self._predicted_peak_time

        prior = float(self._decision_hour_prior)
        if est is not None:
            try:
                est_hour = float(dt_util.as_local(est).hour) + dt_util.as_local(est).minute / 60.0
            except Exception:
                est_hour = float(est.hour) + est.minute / 60.0
            blended = 0.70 * est_hour + 0.30 * prior
        else:
            blended = prior

        # Clamp movement vs prior (±1.25 h) so one wild day cannot jump 16→19
        blended = max(prior - 1.25, min(prior + 1.25, blended))
        hour = int(round(blended))
        hour = max(12, min(20, hour))

        self._decision_hour = hour
        self._primary_bias_hour = hour
        # Snapshot decision hour and the hour before (early warning)
        prev = max(12, hour - 1)
        self._bias_snapshot_hours = tuple(sorted({prev, hour}))

    def _apply_hour_match_corrections(self, now, rising):
        """
        Zone-agnostic corrections that improve peak-HOUR match without zone-name tables.

        1) Still-rising early pull: when short is still rising and points much later
           than now (afternoon), pull peak earlier (short often overshoots early peaks).
        2) Compress: when load already near short peak MW but short peak is still
           25–100 min ahead, pull toward now (fixes interior-max late-by-1h cases).
        3) Early-afternoon next-hour pull: short peaks next hour with load already high.

        Sets _hour_match_corrected / _allow_early_move so EMA does not resist the pull
        and adaptive late-bias does not undo it.
        """
        self._hour_match_corrected = False
        self._allow_early_move = False
        if self._short_forecast_peak_time is None or self._short_forecast_peak is None:
            return
        if self._state is None:
            return

        try:
            now_loc = dt_util.as_local(now)
            pk_loc = dt_util.as_local(self._short_forecast_peak_time)
        except Exception:
            now_loc = now
            pk_loc = self._short_forecast_peak_time
        now_h = now_loc.hour
        sh = pk_loc.hour
        ttp = (self._short_forecast_peak_time - now).total_seconds() / 60.0
        # Current load fraction (not stale daily max) — using max alone caused
        # compress after an early local high while load was already rolling off.
        frac_now = float(self._state) / float(self._short_forecast_peak)
        frac_max = frac_now
        max_is_recent = False
        if self._max_daily_load is not None and self._max_daily_load_time is not None:
            frac_max = max(
                frac_now, float(self._max_daily_load) / float(self._short_forecast_peak)
            )
            max_is_recent = (now - self._max_daily_load_time).total_seconds() <= 20 * 60
        # For still-rising pull, recent max may help; for compress require current load high
        frac_pull = frac_max if max_is_recent else frac_now

        # --- Still-rising early pull ---
        # Cap so we never pull more than ~90 minutes earlier (avoid slamming to now
        # then freezing on a brief ROC dip).
        if rising:
            gap = sh - now_h
            if gap >= 1 and now_h <= 15:
                pull_h = 1.5 + float(self._still_rising_late_bias)
                if frac_pull < 0.96:
                    pull_h += 0.5
                pull_h = min(pull_h, float(gap), 1.75)  # hard cap — avoid slam-to-now freeze
                if pull_h > 0:
                    self._short_forecast_peak_time -= timedelta(hours=float(pull_h))
                    # Keep at least 20 minutes ahead of now so freeze/lock lag has room
                    min_ahead = now + timedelta(minutes=20)
                    if self._short_forecast_peak_time < min_ahead:
                        self._short_forecast_peak_time = min_ahead
                    self._hour_match_corrected = True
                    self._allow_early_move = True
                    _LOGGER.debug(
                        "Hour-match still-rising pull: -%.2fh (now_h=%d short_h=%d frac=%.3f)",
                        pull_h, now_h, sh, frac_pull,
                    )

        # --- Compress: CURRENT load near short MW but peak still ahead ---
        if self._short_forecast_peak_time is not None:
            try:
                pk_loc = dt_util.as_local(self._short_forecast_peak_time)
            except Exception:
                pk_loc = self._short_forecast_peak_time
            sh = pk_loc.hour
            ttp = (self._short_forecast_peak_time - now).total_seconds() / 60.0
            hour_gap = sh - now_h
            skip_evening = (
                float(self._decision_hour_prior) >= 16.5 and sh >= 18
            )
            # Require current load high — not a stale morning/midday max
            if (
                not skip_evening
                and now_h >= 15
                and 25 <= ttp <= 100
                and frac_now >= 0.995
                and hour_gap >= 0
                and hour_gap <= 2
            ):
                target = now + timedelta(minutes=18)
                if self._short_forecast_peak_time > target:
                    self._short_forecast_peak_time = target
                    self._hour_match_corrected = True
                    self._allow_early_move = True
                    _LOGGER.debug(
                        "Hour-match compress: ttp=%.0f frac_now=%.3f → now+18m",
                        ttp, frac_now,
                    )

        # --- Early afternoon: short peaks next hour, load already high ---
        if (
            not self._hour_match_corrected
            and not rising
            and now_h <= 14
            and sh == now_h + 1
            and frac_now >= 0.965
        ):
            self._short_forecast_peak_time = now + timedelta(minutes=20)
            self._hour_match_corrected = True
            self._allow_early_move = True
            _LOGGER.debug(
                "Hour-match early-afternoon pull: next-hour short with frac_now=%.3f", frac_now
            )

    def _apply_adaptive_short_time_bias(self):
        """
        Push short peak later only when it falls before decision_hour:50 local.
        Scale strength with how early it is and whether today is high-load for this zone.
        """
        if self._short_forecast_peak_time is None:
            return
        try:
            loc = dt_util.as_local(self._short_forecast_peak_time)
        except Exception:
            loc = self._short_forecast_peak_time

        # Ensure decision hour is current
        self._refresh_decision_hour()
        cutoff_mins = self._decision_hour * 60 + 50
        mins = loc.hour * 60 + loc.minute
        if mins >= cutoff_mins:
            return  # already late enough — do not push (protects near-perfect short calls)
        # Do not late-bias morning/midday short peaks (hurts early-peak days on some zones)
        if mins < 15 * 60:
            return

        base_bias, high_bias = 14.0, 25.0
        thr = self._get_high_load_threshold()
        high_load = (
            (self._state is not None and self._state >= thr)
            or (self._daily_forecast_peak is not None and self._daily_forecast_peak >= thr)
            or (self._short_forecast_peak is not None and self._short_forecast_peak >= thr)
        )
        cap = high_bias if high_load else base_bias
        # Scale bias with how evening-oriented this zone has been (prior peak hour).
        # Morning/midday zones (prior ~14–15) get lighter push; evening zones full push.
        evening_scale = float(
            np.clip((float(self._decision_hour_prior) - 14.0) / 3.0, 0.35, 1.0)
        )
        cap *= float(self._short_bias_scale) * evening_scale
        cap = max(0.0, min(30.0, cap))
        early_by = cutoff_mins - mins
        bias = min(cap, max(base_bias * 0.5 * self._short_bias_scale * evening_scale, early_by * 0.50))
        if bias <= 0:
            return
        self._short_forecast_peak_time = self._short_forecast_peak_time + timedelta(
            minutes=float(bias)
        )

    def _update_zone_stats_from_actual(self, actual_peak_time, actual_peak_mw):
        """End-of-day: update rolling peak hour/MW prior and optional bias scale."""
        if actual_peak_time is None or actual_peak_mw is None:
            return
        try:
            loc = dt_util.as_local(actual_peak_time)
        except Exception:
            loc = actual_peak_time
        peak_hour_f = loc.hour + loc.minute / 60.0
        self._peak_mw_history.append(float(actual_peak_mw))
        # Only train decision-hour prior on daytime/evening peaks (ignore midnight/morning anomalies)
        if 13.0 <= peak_hour_f <= 20.0:
            self._peak_hour_history.append(peak_hour_f)
            alpha = 0.15
            self._decision_hour_prior = (
                (1 - alpha) * self._decision_hour_prior + alpha * peak_hour_f
            )
            self._decision_hour_prior = max(13.0, min(20.0, self._decision_hour_prior))
            # If short was still-rising and late vs actual, learn a small early-pull bias
            if getattr(self, "_short_still_rising", False) and self._short_forecast_peak_time is not None:
                try:
                    sh = dt_util.as_local(self._short_forecast_peak_time).hour
                except Exception:
                    sh = self._short_forecast_peak_time.hour
                resid = max(0.0, float(sh - loc.hour))
                self._still_rising_late_bias = 0.7 * self._still_rising_late_bias + 0.3 * resid

        # If decision-hour snapshot was systematically early/late, nudge bias scale slowly
        snap_hour, snap = self._select_bias_snapshot()
        if snap and snap.get("predicted_peak_time") is not None:
            try:
                pred = snap["predicted_peak_time"]
                err_min = (actual_peak_time - pred).total_seconds() / 60.0
                # positive err_min => pred early => increase bias scale slightly
                if err_min > 15:
                    self._short_bias_scale = min(1.35, self._short_bias_scale + 0.03)
                elif err_min < -15:
                    self._short_bias_scale = max(0.70, self._short_bias_scale - 0.03)
            except Exception:
                pass

        self.hass.async_create_task(self._async_save_zone_stats())
        _LOGGER.info(
            "Zone stats update: peak_hour=%.2f prior→%.2f high_load_p75=%.0f bias_scale=%.2f (n=%d)",
            peak_hour_f,
            self._decision_hour_prior,
            self._get_high_load_threshold(),
            self._short_bias_scale,
            len(self._peak_mw_history),
        )

    def _maybe_capture_hourly_snapshot(self, now):
        """
        Once per decision-relevant local hour (near top-of-hour), freeze a copy of the live
        prediction for later bias learning. Hours come from _refresh_decision_hour().
        """
        if self._daily_peak_occurred and not self._prediction_time_locked:
            # Fully finalized day with no lock path — skip new snapshots
            return
        if self._predicted_peak is None or self._predicted_peak_time is None:
            return

        try:
            local = dt_util.as_local(now)
        except Exception:
            local = now

        hour = local.hour
        # Dynamic snapshot set
        if hour not in self._bias_snapshot_hours and hour != self._decision_hour:
            return
        if hour in self._hourly_snapshots:
            return
        # Capture within the first ~7 minutes of the hour (covers 5-min poll cadence)
        if local.minute >= 7:
            return

        # If already ops-locked, snapshot the locked (observed) peak for learning
        if self._prediction_time_locked and self._locked_peak_time is not None:
            pred_t = self._locked_peak_time
            pred_mw = float(self._locked_peak_mw or self._predicted_peak)
        else:
            pred_t = self._predicted_peak_time
            pred_mw = float(self._predicted_peak)

        self._hourly_snapshots[hour] = {
            "wall_time": now,
            "predicted_peak_time": pred_t,
            "predicted_peak": pred_mw,
            "high_risk_day": bool(self._high_risk_day),
        }
        _LOGGER.info(
            "Hourly bias snapshot @%02d:00 local: pred_peak=%.0f MW at %s "
            "(high_risk=%s, locked=%s, decision_hour=%d)",
            hour,
            pred_mw,
            pred_t.isoformat() if pred_t else None,
            self._high_risk_day,
            self._prediction_time_locked,
            self._decision_hour,
        )

    def _select_bias_snapshot(self):
        """Pick decision_hour snapshot, else primary, else latest available."""
        for h in (self._decision_hour, self._primary_bias_hour):
            if h in self._hourly_snapshots:
                return h, self._hourly_snapshots[h]
        if not self._hourly_snapshots:
            return None, None
        hour = max(self._hourly_snapshots.keys())
        return hour, self._hourly_snapshots[hour]

    @staticmethod
    def _floor_hour(dt):
        """Return timezone-aware datetime floored to the hour."""
        return dt.replace(minute=0, second=0, microsecond=0)

    def _compute_learning_time_error(self, actual_peak_time, predicted_peak_time):
        """
        Time error (hours) for bias: actual - predicted.

        Curtailment cares about hour match more than minute placement within the hour.
        - Same peak hour: down-weight minute residual (still learns fine timing gently).
        - Different peak hour: use hour mismatch + reduced within-hour residual.
        Residuals are clipped to ±1.0 hour before weighting.
        """
        raw_hrs = (actual_peak_time - predicted_peak_time).total_seconds() / 3600.0
        actual_hour = self._floor_hour(actual_peak_time)
        pred_hour = self._floor_hour(predicted_peak_time)
        hour_error = (actual_hour - pred_hour).total_seconds() / 3600.0
        within_hour = raw_hrs - hour_error

        if abs(hour_error) < 0.5:
            # Same curtailment hour (e.g. 16:10 vs 16:50): soft minute learning only
            time_error = 0.35 * within_hour
        else:
            # Wrong hour: full hour step + soft within-hour
            time_error = hour_error + 0.35 * within_hour

        return max(-1.0, min(1.0, time_error)), raw_hrs, hour_error

    def _record_error_and_update_bias(self):
        """
        After the peak has passed, compare the actual peak load with the *hourly snapshot*
        prediction (default 16:00 local curtailment snapshot), not the live/frozen prediction.

        Uses forecast-based logarithmic weighting so high-risk / 5CP days influence learning more.
        Includes residual clipping and caps on bias values.
        """
        actual_peak = self._max_daily_load
        actual_peak_time = self._max_daily_load_time
        if not actual_peak or not actual_peak_time:
            _LOGGER.debug("Skipping bias update: Missing actual peak load/time.")
            return

        snap_hour, snap = self._select_bias_snapshot()
        if snap is None:
            _LOGGER.info(
                "Skipping bias update: no hourly snapshot (hours=%s). "
                "Live/frozen prediction is not used for learning.",
                list(self._bias_snapshot_hours),
            )
            return

        predicted_peak = snap.get("predicted_peak")
        predicted_peak_time = snap.get("predicted_peak_time")
        if predicted_peak is None or predicted_peak_time is None:
            _LOGGER.warning("Skipping bias update: snapshot missing peak fields.")
            return

        # === Magnitude error (actual - snapshot pred); clip outliers ===
        try:
            magnitude_error = float(actual_peak) - float(predicted_peak)
        except (TypeError, ValueError) as e:
            _LOGGER.error("Error calculating magnitude_error: %s", e)
            magnitude_error = 0.0
        magnitude_error = max(-5000.0, min(5000.0, magnitude_error))

        # === Time error from snapshot (hour-aware) ===
        try:
            if not (
                isinstance(actual_peak_time, datetime)
                and isinstance(predicted_peak_time, datetime)
            ):
                _LOGGER.error(
                    "Time error calculation failed — values are not both datetimes "
                    "(actual=%s, predicted=%s)",
                    type(actual_peak_time),
                    type(predicted_peak_time),
                )
                return
            time_error, raw_time_error, hour_error = self._compute_learning_time_error(
                actual_peak_time, predicted_peak_time
            )
        except Exception as e:
            _LOGGER.error("Unexpected error calculating time_error: %s", e)
            return

        # === Forecast-based Logarithmic Weighting (5CP emphasis) ===
        forecast_peak = (
            getattr(self, "_daily_forecast_peak", None)
            or predicted_peak
            or 0
        )
        fifth_peak = self._get_fifth_highest_peak()

        if fifth_peak > 0 and forecast_peak > 0:
            ratio = forecast_peak / fifth_peak
            if ratio <= 0.94:
                weight = 0.35
            else:
                weight = 0.35 + 4.5 * np.log1p(8 * (ratio - 0.94))
        else:
            weight = 0.35

        # Boost weight when snapshot flagged high-risk day
        if snap.get("high_risk_day"):
            weight *= 1.25

        weight = max(0.35, min(5.0, weight))

        # Apply weighted errors into histories
        self._time_error_history.append(time_error * weight)
        self._magnitude_error_history.append(magnitude_error * weight)
        self._error_history.append(magnitude_error)

        avg_time_error = (
            np.mean(list(self._time_error_history)) if self._time_error_history else 0.0
        )
        avg_magnitude_error = (
            np.mean(list(self._magnitude_error_history))
            if self._magnitude_error_history
            else 0.0
        )

        # === Update Biases with Caps ===
        if time_error != 0 or len(self._time_error_history) == 1:
            self._time_bias += 0.1 * avg_time_error
            self._time_bias = max(-1.0, min(1.0, self._time_bias))

        if magnitude_error != 0 or len(self._magnitude_error_history) == 1:
            self._magnitude_bias += 0.1 * avg_magnitude_error
            self._magnitude_bias = max(-5000, min(5000, self._magnitude_bias))

        self._last_bias_snapshot_hour = snap_hour
        self._last_bias_time_error_hrs = time_error
        self._last_bias_magnitude_error = magnitude_error
        self._last_bias_weight = weight

        _LOGGER.info(
            "Bias update from %02d:00 snapshot (weight=%.2f): "
            "Actual=%.0f MW @ %s, SnapshotPred=%.0f MW @ %s, "
            "raw_time_err=%.2fh hour_err=%.2fh learn_time_err=%.2fh mag_err=%.0f MW "
            "→ time_bias=%.2f, magnitude_bias=%.0f",
            snap_hour,
            weight,
            actual_peak,
            actual_peak_time.isoformat() if actual_peak_time else None,
            predicted_peak,
            predicted_peak_time.isoformat() if predicted_peak_time else None,
            raw_time_error,
            hour_error,
            time_error,
            magnitude_error,
            self._time_bias,
            self._magnitude_bias,
        )

        # Persist biases
        self.hass.async_create_task(self._async_save_biases())

        # Update rolling zone clocks / high-load stats from today's actual peak
        self._update_zone_stats_from_actual(actual_peak_time, actual_peak)

    def _get_fifth_highest_peak(self):
        """
        Determine the effective threshold for high-risk day evaluation based on historical peaks.
        """
        if len(self._top_five_peaks) < 5:
            return self._peak_threshold
        return max(self._peak_threshold, sorted(self._top_five_peaks, key=lambda x: x[1], reverse=True)[4][1])

        _LOGGER.debug(
            "_get_fifth_highest_peak: 5 peaks stored. Configured threshold=%.1f, 5th peak load=%.1f. Returning effective threshold: %.1f",
            self._peak_threshold, fifth_peak_load, effective_threshold
        )
        return effective_threshold


    def _record_daily_peak(self):
        """Record the day's peak load and maintain the top five unique daily peaks."""
        peak_date = self._max_daily_load_time.date()
        updated = False

        # Check if today's date is already recorded
        for i, (ts, val) in enumerate(self._top_five_peaks):
            if ts.date() == peak_date:
                if self._max_daily_load > val:
                    _LOGGER.debug("Updating today's peak from %.1f MW to %.1f MW", val, self._max_daily_load)
                    self._top_five_peaks[i] = (self._max_daily_load_time, self._max_daily_load)
                    updated = True
                else:
                    _LOGGER.debug("Today's peak (%s) already recorded and current peak %.1f MW is not higher than %.1f MW", peak_date, self._max_daily_load, val)
                break

        # If not already recorded, add the new peak
        if not updated and all(ts.date() != peak_date for ts, _ in self._top_five_peaks):
            _LOGGER.debug("Adding new daily peak for %s: %.1f MW", peak_date, self._max_daily_load)
            self._top_five_peaks.append((self._max_daily_load_time, self._max_daily_load))

        # Sort and trim the list to the top 5 peaks
        self._top_five_peaks.sort(key=lambda x: x[1], reverse=True)
        if len(self._top_five_peaks) > 5:
            removed_peak = self._top_five_peaks.pop()
            _LOGGER.debug("Removing lowest peak %s", removed_peak)

        # Save updated peaks
        self.hass.async_create_task(self._async_save_peaks())

    def _extract_time_load_arrays(self, history_deque, limit_hours=1.0):
        """
        Extract data from the rolling history for the past 'limit_hours' and convert times to hours
        since the earliest timestamp.
        """
        now = dt_util.now()
        earliest = now - timedelta(hours=limit_hours)
        filtered = [(ts, val) for (ts, val) in history_deque if ts >= earliest]
        if not filtered:
            return np.array([]), np.array([])
        filtered.sort(key=lambda x: x[0])
        base_time = filtered[0][0]
        times = [(ts - base_time).total_seconds() / 3600.0 for (ts, _) in filtered]
        loads = [val for (_, val) in filtered]
        return np.array(times), np.array(loads)

    def _extract_time_load_arrays_short(self, forecast_data, limit_minutes=60):
        """
        Convert the short forecast data (list of dicts) into time (in hours) and load arrays,
        limited to the first 'limit_minutes' of forecast.
        """
        base_time = forecast_data[0]["forecast_hour_ending"]
        cutoff = base_time + timedelta(minutes=limit_minutes)
        subset = [item for item in forecast_data if item["forecast_hour_ending"] <= cutoff]
        if not subset:
            return np.array([]), np.array([])
        subset.sort(key=lambda x: x["forecast_hour_ending"])
        times = [(item["forecast_hour_ending"] - base_time).total_seconds() / 3600.0 for item in subset]
        loads = [item["forecast_load_mw"] for item in subset]
        return np.array(times), np.array(loads)

    async def _async_load_peaks(self):
        data = await self._store.async_load()
        if data:
            raw_peaks = data.get('top_five_peaks', [])
            parsed = []
            for item in raw_peaks:
                try:
                    timestamp, load = item
                    ts = (
                        timestamp
                        if isinstance(timestamp, datetime)
                        else dt_util.parse_datetime(str(timestamp))
                    )
                    if ts is not None and load is not None:
                        parsed.append((ts, load))
                except Exception:
                    continue
            self._top_five_peaks = parsed
            try:
                self._last_reset_date = date.fromisoformat(data.get('last_reset_date'))
            except Exception:
                self._last_reset_date = date.today()
        else:
            self._top_five_peaks = []
            self._last_reset_date = date.today()

    async def _async_save_peaks(self):
        serializable = []
        for timestamp, load in self._top_five_peaks or []:
            try:
                ts_out = timestamp.isoformat() if hasattr(timestamp, "isoformat") else str(timestamp)
                serializable.append((ts_out, load))
            except Exception:
                continue
        await self._store.async_save({
            'top_five_peaks': serializable,
            'last_reset_date': self._last_reset_date.isoformat(),
        })

    async def _async_load_biases(self):
        """Load bias values and histories from persistent storage with version check."""
        data = await self._bias_store.async_load()
        if data and data.get("version") == self._bias_version:
            self._time_bias = data.get("time_bias", 0.0)
            self._magnitude_bias = data.get("magnitude_bias", 0.0)
            self._time_error_history = deque(data.get("time_error_history", []), maxlen=30)
            self._magnitude_error_history = deque(data.get("magnitude_error_history", []), maxlen=30)
            _LOGGER.info("Loaded persisted biases (version %s)", self._bias_version)
        else:
            _LOGGER.info("Bias store version mismatch or empty. Starting with fresh biases.")
            self._time_bias = 0.0
            self._magnitude_bias = 0.0
            self._time_error_history.clear()
            self._magnitude_error_history.clear()

    async def _async_save_biases(self):
        """Save current bias values and histories to persistent storage."""
        await self._bias_store.async_save({
            "version": self._bias_version,
            "time_bias": self._time_bias,
            "magnitude_bias": self._magnitude_bias,
            "time_error_history": list(self._time_error_history),
            "magnitude_error_history": list(self._magnitude_error_history),
        })

    async def _async_load_zone_stats(self):
        """Load per-zone rolling peak-hour/MW priors (adapts without zone-name profiles)."""
        data = await self._zone_stats_store.async_load()
        if data and data.get("version") == self._zone_stats_version:
            self._peak_hour_history = deque(data.get("peak_hour_history", []), maxlen=40)
            self._peak_mw_history = deque(data.get("peak_mw_history", []), maxlen=40)
            self._decision_hour_prior = float(data.get("decision_hour_prior", 16.0))
            self._short_bias_scale = float(data.get("short_bias_scale", 1.0))
            self._decision_hour = int(round(self._decision_hour_prior))
            self._primary_bias_hour = self._decision_hour
            prev = max(12, self._decision_hour - 1)
            self._bias_snapshot_hours = tuple(sorted({prev, self._decision_hour}))
            _LOGGER.info(
                "Loaded zone stats: prior_hour=%.2f bias_scale=%.2f n_peaks=%d",
                self._decision_hour_prior,
                self._short_bias_scale,
                len(self._peak_mw_history),
            )
        else:
            _LOGGER.info("Zone stats empty or version mismatch; using cold-start priors.")

    async def _async_save_zone_stats(self):
        """Persist per-zone adaptive clocks."""
        await self._zone_stats_store.async_save({
            "version": self._zone_stats_version,
            "peak_hour_history": list(self._peak_hour_history),
            "peak_mw_history": list(self._peak_mw_history),
            "decision_hour_prior": self._decision_hour_prior,
            "short_bias_scale": self._short_bias_scale,
        })

class PJMData:
    """Get and parse data from PJM with coordinated API rate limiting using your API key or fetched subscription key."""
    def __init__(self, websession, api_key):
        self._websession = websession
        self._subscription_key = api_key
        self._request_times = deque(maxlen=6)
        self._lock = asyncio.Lock()

    async def _rate_limit(self):
        async with self._lock:
            now = time_module.time()
            # Remove timestamps older than 60 seconds
            while self._request_times and now - self._request_times[0] >= 60:
                self._request_times.popleft()

            if len(self._request_times) >= 6:
                wait_time = 60 - (now - self._request_times[0]) + 1
                _LOGGER.warning("PJM API rate limit reached. Waiting %.2f seconds.", wait_time)
                await asyncio.sleep(wait_time)
                # Clean up again after sleep
                now = time_module.time()
                while self._request_times and now - self._request_times[0] >= 60:
                    self._request_times.popleft()

            self._request_times.append(now)

    def _get_headers(self):
        return {
            'Ocp-Apim-Subscription-Key': self._subscription_key,
            'Content-Type': 'application/json',
        }

    async def _get_subscription_key(self):
        if self._subscription_key:
            return
        try:
            with async_timeout.timeout(60):
                response = await self._websession.get(RESOURCE_SUBSCRIPTION_KEY)
                data = await response.json()
                self._subscription_key = data.get('subscriptionKey')
                if not self._subscription_key:
                    _LOGGER.error("No subscription key found in response from %s", RESOURCE_SUBSCRIPTION_KEY)
        except Exception as err:
            _LOGGER.error("Failed to get subscription key: %s", err)

    async def async_update_instantaneous(self, zone):
        retries = 3
        backoff = 10  # seconds
        for attempt in range(retries):
            await self._rate_limit()
            if not self._subscription_key:
                await self._get_subscription_key()

            end_time_utc = datetime.now(timezone.utc)
            start_time_utc = end_time_utc - timedelta(minutes=10)
            time_string = start_time_utc.strftime('%m/%e/%Y %H:%Mto') + end_time_utc.strftime('%m/%e/%Y %H:%M')
            params = {
                'rowCount': '100',
                'sort': 'datetime_beginning_utc',
                'order': 'Desc',
                'startRow': '1',
                'isActiveMetadata': 'true',
                'fields': 'area,instantaneous_load',
                'datetime_beginning_utc': time_string,
            }
            resource = f"{RESOURCE_INSTANTANEOUS}?{urllib.parse.urlencode(params)}"
            headers = self._get_headers()

            try:
                with async_timeout.timeout(60):
                    response = await self._websession.get(resource, headers=headers)
                    if response.status == 429:
                        _LOGGER.warning("PJM API rate limit exceeded (429). Retrying in %d seconds.", backoff)
                        await asyncio.sleep(backoff)
                        backoff *= 2
                        continue

                    data = await response.json()
                    if not data:
                        _LOGGER.error("No load data returned for zone %s", zone)
                        return None

                    items = data["items"]
                    for item in items:
                        if item["area"] == zone:
                            return int(round(item["instantaneous_load"]))

                    _LOGGER.error("Couldn't find load data for zone %s", zone)
                    return None

            except (asyncio.TimeoutError, aiohttp.ClientError) as err:
                _LOGGER.error("Could not get load data from PJM: %s", err)
                await asyncio.sleep(backoff)
                backoff *= 2
            except Exception as err:
                _LOGGER.error("Unexpected error fetching load data: %s", err)
                return None

        _LOGGER.error("Exhausted retries to get instantaneous load data.")
        return None

    async def async_update_forecast(self, zone):
        retries = 3
        backoff = 10
        for attempt in range(retries):
            await self._rate_limit()
            if not self._subscription_key:
                await self._get_subscription_key()

            midnight_local = datetime.combine(date.today(), time())
            start_time_utc = midnight_local.astimezone(timezone.utc)
            end_time_utc = start_time_utc + timedelta(hours=23, minutes=59)
            time_string = start_time_utc.strftime('%m/%e/%Y %H:%Mto') + end_time_utc.strftime('%m/%e/%Y %H:%M')
            params = {
                'rowCount': '100',
                'order': 'Asc',
                'startRow': '1',
                'isActiveMetadata': 'true',
                'fields': 'forecast_datetime_ending_utc,forecast_load_mw',
                'forecast_datetime_beginning_utc': time_string,
                'forecast_area': zone,
            }
            resource = f"{RESOURCE_FORECAST}?{urllib.parse.urlencode(params)}"
            headers = self._get_headers()

            try:
                with async_timeout.timeout(60):
                    response = await self._websession.get(resource, headers=headers)
                    if response.status == 429:
                        _LOGGER.warning("PJM API rate limit exceeded (429). Retrying in %d seconds.", backoff)
                        await asyncio.sleep(backoff)
                        backoff *= 2
                        continue

                    full_data = await response.json()
                    data = full_data["items"]
                    forecast_data = []
                    for item in data:
                        forecast_hour_ending = datetime.strptime(item['forecast_datetime_ending_utc'], '%Y-%m-%dT%H:%M:%S').replace(tzinfo=timezone.utc).astimezone()
                        forecast_data.append({
                            "forecast_hour_ending": forecast_hour_ending,
                            "forecast_load_mw": int(item["forecast_load_mw"])
                        })
                    return forecast_data

            except (asyncio.TimeoutError, aiohttp.ClientError) as err:
                _LOGGER.error("Could not get forecast data from PJM: %s", err)
                await asyncio.sleep(backoff)
                backoff *= 2
            except Exception as err:
                _LOGGER.error("Unexpected error fetching forecast data: %s", err)
                return None

        _LOGGER.error("Exhausted retries to get forecast data.")
        return None

    async def async_update_short_forecast(self, zone):
        retries = 3
        backoff = 5  # seconds
        for attempt in range(retries):
            await self._rate_limit()
            if not self._subscription_key:
                await self._get_subscription_key()

            params = {
                'rowCount': '48',
                'order': 'Asc',
                'startRow': '1',
                'fields': 'forecast_datetime_ending_utc,forecast_load_mw',
                'evaluated_at_ept': '5MinutesAgo',
                'forecast_area': zone,
            }
            resource = f"{RESOURCE_SHORT_FORECAST}?{urllib.parse.urlencode(params)}"
            headers = self._get_headers()

            try:
                with async_timeout.timeout(60):
                    response = await self._websession.get(resource, headers=headers)
                    if response.status == 429:
                        _LOGGER.warning("PJM API rate limit exceeded (429). Retrying in %d seconds.", backoff)
                        await asyncio.sleep(backoff)
                        backoff *= 2
                        continue

                    full_data = await response.json()
                    data = full_data["items"]

                    forecast_data = []
                    for item in data:
                        forecast_hour_ending = datetime.strptime(item['forecast_datetime_ending_utc'], '%Y-%m-%dT%H:%M:%S').replace(tzinfo=timezone.utc).astimezone()
                        forecast_data.append({
                            "forecast_hour_ending": forecast_hour_ending,
                            "forecast_load_mw": int(item["forecast_load_mw"])
                        })
                    return forecast_data

            except (asyncio.TimeoutError, aiohttp.ClientError) as err:
                _LOGGER.error("Could not get short forecast data from PJM: %s", err)
                await asyncio.sleep(backoff)
                backoff *= 2
            except Exception as err:
                _LOGGER.error("Unexpected error fetching short forecast data: %s", err)
                return None

        _LOGGER.error("Exhausted retries to get short forecast data.")
        return None

    async def async_update_lmp(self, pnode_id):
        retries = 3
        backoff = 10  # initial backoff interval in seconds
        for attempt in range(retries):
            await self._rate_limit()  # Ensure we respect the overall API rate limit

            # Fetch subscription key if it's not already available
            if not self._subscription_key:
                await self._get_subscription_key()

            # Define the time range for the LMP query (past ~1 hour)
            now_utc = datetime.now(timezone.utc)
            current_minute = now_utc.minute

            if current_minute < 5:
                # Adjust start time to the previous hour if current minute < 5
                start_time_utc = now_utc.replace(minute=4, second=0, microsecond=0) - timedelta(hours=1)
            else:
                start_time_utc = now_utc.replace(minute=4, second=0, microsecond=0)

            time_string = start_time_utc.strftime('%m/%e/%Y %H:%Mto') + now_utc.strftime('%m/%e/%Y %H:%M')

            params = {
                'rowCount': '12',
                'order': 'Asc',
                'startRow': '1',
                'datetime_beginning_utc': time_string,
                'pnode_id': pnode_id,
            }

            resource = f"{RESOURCE_LMP}?{urllib.parse.urlencode(params)}"
            headers = self._get_headers()

            try:
                with async_timeout.timeout(60):
                    response = await self._websession.get(resource, headers=headers)

                    if response.status == 429:
                        # API rate limit exceeded; perform exponential backoff and retry
                        _LOGGER.warning("PJM API rate limit exceeded (429) while fetching LMP data. Retrying in %d seconds.", backoff)
                        await asyncio.sleep(backoff)
                        backoff *= 2
                        continue

                    data = await response.json()
                    if not data:
                        _LOGGER.error("No LMP data returned for pnode_id %s", pnode_id)
                        return None

                    items = data["items"]

                    # Extract total LMP values specific to the requested pnode_id
                    total_lmp_values = [float(item["total_lmp_rt"]) for item in items if item["pnode_id"] == pnode_id]

                    if not total_lmp_values:
                        _LOGGER.error("Couldn't find LMP data for pnode_id %s", pnode_id)
                        return None

                    # Calculate and return the average LMP value
                    average_lmp = sum(total_lmp_values) / len(total_lmp_values)
                    return round(average_lmp, 2)

            except (asyncio.TimeoutError, aiohttp.ClientError) as err:
                # Handle network-related errors and apply exponential backoff
                _LOGGER.error("Could not get LMP avg data from PJM: %s", err)
                await asyncio.sleep(backoff)
                backoff *= 2
            except Exception as err:
                # Catch-all for other exceptions
                _LOGGER.error("Unexpected error fetching LMP avg data: %s", err)
                return None

        # All retries have been exhausted
        _LOGGER.error("Exhausted retries to get LMP data.")
        return None
