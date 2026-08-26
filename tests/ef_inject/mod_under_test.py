"""
ef_inject: fast local meter injection for EcoFlow Stream Ultra.

Reads the LOCAL Shelly Pro 3EM (the one EcoFlow is actually bound to) over LAN RPC
and writes the true value straight into the device's `cloud_metter` field over BLE,
at a higher rate than the EcoFlow cloud manages (~2.5s).

This injects TRUTH, faster. It does not fabricate values. The cloud keeps writing the
same field with its own (older) value, so the device ends up seeing mostly-fresh data
with a periodic stale blip from the cloud.

SAFETY DESIGN
  - all six CloudMeter subfields are pinned from live device telemetry; only
    phase_c_power is replaced with the fresh local reading. A partial block could
    leave has_meter unset and unbind the meter, so we never send one.
  - refuses to start until it has observed has_meter=True and the expected meter SN.
  - if the local Shelly read fails or goes stale, we STOP injecting and let the cloud
    take over rather than repeating an old value.
  - writes are serialised through our own lock, because eflib has no send lock and
    other writers (the 15-min reconcile, UI controls) share this BLE connection.
  - a plausibility clamp rejects absurd readings before they are sent.
  - a rise slew limit refuses to chase a load spike shorter than our own response
    time. Only RISES are rationed, because a rise commands ramp-UP and its overshoot
    lands on the grid as export, while a fall's undershoot lands as import; only one
    of those is a compliance event. It can never command MORE output than the raw
    reading would, so a bug in it fails toward import. See SLEW_UP_W_PER_SEC.
  - the cloud writes this same field itself, and the Ultras stop feeding the house
    the moment their cloud link drops, so that path is not optional and cannot be
    blocked. Its frames arrive unbiased and unslewed, and measurement put 85% of the
    remaining reverse-flow magnitude within one tick of one. So an overwrite is
    answered with a counter-kick: a value deliberately BELOW the biased one for a
    couple of cycles, cancelling the ramp the cloud just commanded instead of waiting
    it out. Kicks only ever lower the injected value, so this too fails toward
    import, but the RELEASE of one is a rise and has to be rate limited like any
    other; dropping it in a single write made export measurably worse. See CLOUD_KICK.
  - kill switch: touch /config/ef_inject_stop  (or remove the integration)
    slew off:    touch /config/ef_inject_noslew  (presence DISABLES, so forgetting
                 the file leaves the protection ON)
    kick off:    touch /config/ef_inject_nokick  (presence DISABLES, same reasoning)

OBSERVABILITY
  Exposes sensor.ef_inject_status with counters so the effect can be measured
  against the recorder DB afterwards.

Config in const below. Start conservative at 1 Hz.
"""

import asyncio
import logging
import os
import struct
import time
from typing import NamedTuple

import aiohttp

from homeassistant.config_entries import SOURCE_IMPORT, ConfigEntry
from homeassistant.const import Platform
from homeassistant.core import HomeAssistant
from homeassistant.exceptions import ConfigEntryNotReady
from homeassistant.helpers import config_validation as cv
from homeassistant.helpers import device_registry as dr
from homeassistant.helpers.aiohttp_client import async_get_clientsession
from homeassistant.helpers.dispatcher import async_dispatcher_send

DOMAIN = "ef_inject"
_LOGGER = logging.getLogger(__name__)

# The YAML block (`ef_inject:` in configuration.yaml) is what owns the regulator and
# takes no options. Declaring it explicitly keeps that valid now that the integration
# also has a config flow, and silences the "no CONFIG_SCHEMA" deprecation path.
CONFIG_SCHEMA = cv.empty_config_schema(DOMAIN)

# Platforms carrying the knobs. Entities only; see async_setup_entry.
PLATFORMS = [Platform.BUTTON, Platform.NUMBER, Platform.SENSOR, Platform.SWITCH]
# Fired once per status tick (5s) so entities refresh without polling.
SIGNAL_UPDATE = f"{DOMAIN}_update"


class EfInjectRuntime(NamedTuple):
    """What the entity platforms are given.

    `device_entry` is the DEVICE REGISTRY row for the Ultra, a frozen snapshot whose
    only use is its `.id` at registration time. Never widen this to hold an ef_ble
    DeviceBase: that object is replaced whenever the ef_ble entry reloads and the
    abandoned one is permanently dead, so anything holding a reference regulates into
    the void. The address is re-resolved on every send.
    """

    address: str
    device_entry: dr.DeviceEntry

TARGET_TITLE = "EF-60434"          # main Ultra
# The config-entry TITLE is user-editable in the HA UI, so matching on it alone means
# a rename silently kills the regulator: _device() would return None for ever and the
# only symptom would be skipped_no_identity climbing. The BLE address is the entry's
# unique_id and cannot be edited, so it is the stable identity and the fallback.
TARGET_ADDRESS = "CC:BA:97:E3:3C:52"
EXPECTED_SN = "ECE334EA86F8"       # the meter EcoFlow is bound to
SHELLY_URL = "http://192.168.101.158/rpc/EM.GetStatus?id=0"

BASELINE_SEC = 15                  # observe before first write
STALE_AFTER_SEC = 3.0              # local reading older than this is not injected
HTTP_TIMEOUT_SEC = 0.8
MAX_ABS_W = 15000                  # plausibility clamp
STOP_FILE = "/config/ef_inject_stop"

# ---------------------------------------------------------------------------
# Meter transport. The HTTP RPC path costs ~110ms per read (p50 measured
# 2026-08-24); Modbus TCP against the same device costs ~24ms, so the swap is
# worth roughly 90ms of the ~1210ms end-to-end latency. It is NOT the dominant
# term: the Shelly aggregates internally at 1 Hz and that ~1000ms cannot be
# reduced from our side, which is why this is an 8% win and not a 5x one.
#
# Behind a touch file so the transport can be flipped, and rolled back, with no
# restart. Present -> Modbus, absent -> HTTP RPC. Same pattern as STOP_FILE.
MODBUS_FILE = "/config/ef_inject_modbus"
MODBUS_HOST = "192.168.101.158"
MODBUS_PORT = 502
MODBUS_UNIT = 1
# Register map established empirically 2026-08-24 by correlating 8 paired
# moving samples against the HTTP RPC on two meters (.158 and .149), agreeing
# to 0.02W. Notes that cost time and are worth keeping:
#   - FC3 (read holding registers) returns exception code 2. Only FC4 works.
#   - Values are float32 with the LOW word first, so the two 16-bit registers
#     must be swapped before unpacking as big-endian.
#   - Per-phase blocks sit 20 registers apart: A@1020, B@1040, C@1060, with
#     +0 voltage, +2 current, +4 active power. Hence phase C power at 1064.
#   - Reading a large span in one request returns nothing; keep counts small.
MODBUS_FC_READ_INPUT = 4
MODBUS_REG_C_ACT_POWER = 1064
MODBUS_TIMEOUT_SEC = 0.5

# ---------------------------------------------------------------------------
# Second meter. MEASURED 2026-08-24 at 20 Hz over 40s, both meters, 730 paired
# samples:
#   - Each meter's phase C value changes at exactly 1.00 Hz (median gap 0.999s
#     and 0.997s over 40 changes each). The 1 Hz aggregation is confirmed, not
#     assumed, and it is the dominant latency term.
#   - Their update instants are offset by 0.412s, i.e. 0.41 of the period, close
#     to the ideal 0.5. Interleaving them therefore roughly HALVES effective
#     data staleness, ~1000ms to ~500ms. This is the single biggest win
#     available, worth about 5x the Modbus transport swap.
#   - They AGREE on phase C: means +781.5W vs +779.2W, a 2.3W systematic
#     difference, 0.29% of reading. The wider per-sample spread (p05 -21W,
#     p95 +29W) is not disagreement, it IS the 0.41s window offset showing up
#     during load changes, which is exactly the signal being exploited.
SECOND_HOST = "192.168.101.149"     # main grid 3EM, all three phases clamped

# Two independent toggles, because observing is safe and acting is not.
#   shadow    -> poll the second meter and REPORT the comparison, use nothing.
#   interleave -> actually take the value from whichever meter updated last.
# Interleave implies shadow polling. Validate in shadow through a full surplus
# before enabling, because the 0.29% agreement above was measured at ~780W of
# steady import, NOT with kilowatts of PV flowing through the inverter.
SHADOW_FILE = "/config/ef_inject_shadow149"
INTERLEAVE_FILE = "/config/ef_inject_interleave"

# Divergence guard. Judged on a MEAN, never on a single sample: the 0.41s offset
# means the two meters legitimately disagree by a lot during a fast transient (a
# 2kW step inside one second shows up as ~800W of instantaneous difference), so
# an instantaneous threshold would false-trip constantly.
#
# THIS FALSE-TRIPPED IN PRODUCTION (2026-08-24 11:46:05, "+112W mean of 40
# samples"), and the design error was mine: I sized the threshold from the
# measured 0.29% steady-state agreement but sized the window for responsiveness.
# 40 samples at 0.25s is only TEN SECONDS, and on a moving signal the 0.412s
# offset needs just ~270 W/s of average drift to fake a 112W mean. Phase C does
# that routinely (89W -> 209W between consecutive 10s samples that afternoon).
# Averaging longer would only dilute the contamination, so instead the sample is
# now GATED ON QUIESCENCE: a difference only counts as evidence when the signal
# has been flat for longer than a full publication period, which is the only
# condition under which the two meters are comparable at all. During a ramp we
# simply have no opinion, which is the honest position.
#
# THAT GATE THEN FALSE-TRIPPED TOO (2026-08-25 07:07:56, "-128W mean of 40
# samples"), because it only tested the BOUND meter for flatness. A resistive load
# switches instantaneously, so the bound meter can sit flat at 400W across the
# whole window while the grid meter, having already published the edge, reads
# 1600W: flat by the gate's test, 1200W apart in fact. Measured diffs reached
# +/-1200W with SYMMETRIC sign while the same pair agreed to within 25W on every
# settled sample, which is the signature of sampling skew rather than drift. So
# the gate now requires BOTH series flat. Note what is NOT the fix: widening
# INTERLEAVE_DIVERGE_W. The artifact is the size of the switched load, so no
# threshold below that load survives it, and raising the limit would blind the
# guard to exactly the 100-200W drift it exists to catch.
#
# Verdicts are then taken on NON-OVERLAPPING windows and need INTERLEAVE_STRIKES
# consecutive bad ones, and the latch ALWAYS RECOVERS: it keeps judging while off
# and re-enables after consecutive good verdicts. There is deliberately NO
# permanent state and no recovery budget, because anything that can only be
# cleared by hand means someone has to be watching, and nobody is.
#
# Flapping is throttled by ESCALATING HYSTERESIS instead: each recovery inside
# INTERLEAVE_RECOVERY_DECAY_SEC doubles the number of consecutive good verdicts
# the next recovery must produce (2, 4, 8, 16 ... to a ceiling). A genuinely
# marginal pair therefore quiets itself down to almost nothing within an hour
# without ever being locked out, while a one-off glitch weeks apart still costs
# only the cheap 2 verdicts, because the escalation DECAYS. Note which way the
# risk points: a wrongly-locked guard costs latency (stale_avg ~146ms -> ~480ms),
# a wrongly-trusted diverging meter costs real export. So recovery is allowed to
# be slow, it just must never be impossible.
INTERLEAVE_DIVERGE_W = 100.0
INTERLEAVE_DIVERGE_WINDOW = 40      # QUIET samples per verdict, not wall time
INTERLEAVE_STRIKES = 2              # consecutive bad verdicts before locking off
INTERLEAVE_RECOVER_W = 40.0         # verdict must be this tight to count as good
INTERLEAVE_RECOVER_STRIKES = 2      # base consecutive good verdicts to re-enable
INTERLEAVE_RECOVER_MAX_STRIKES = 32     # ceiling on the doubling (~11min of quiet)
INTERLEAVE_RECOVERY_DECAY_SEC = 3600.0  # a recovery older than this stops escalating
INTERLEAVE_QUIET_W = 25.0           # "flat" means within this band ...
INTERLEAVE_QUIET_SEC = 1.5          # ... across at least this long (> 1Hz period)

# ---------------------------------------------------------------------------
# Poll rate is now DECOUPLED from write rate.
#
# The meter publishes a new value once per second. Sampling it at the write rate
# meant we could be a whole write period late in NOTICING a new value, on top of
# the aggregation delay. Measured: the loop was running at 1.6 Hz, not the 2 Hz
# configured, because _pace slept a flat period *on top of* the cycle
# work (500 + ~5 read + ~110 BLE = ~615ms). Mean detection delay was therefore
# ~310ms of pure waste.
#
# So: poll at POLL_PERIOD_SEC and write when the value actually CHANGES, with a
# keepalive floor so the field is still re-asserted against the cloud even when
# the reading is steady. Write rate stays ~2/s, which is the rate already proven
# safe, while mean detection delay drops to ~POLL_PERIOD_SEC/2.
#
# Not faster than this: at 20 Hz the read median improved but the TAIL got worse
# (max 316ms vs 71ms p95), because two meters on one 2.4GHz AP start contending.
# 4 Hz per meter is 8 reads/s, a fifth of the rate that misbehaved.
POLL_PERIOD_SEC = 0.25
KEEPALIVE_SEC = 0.5                 # re-assert at least this often regardless
WRITE_ON_CHANGE_W = 0.5             # treat as a new reading if it moved this much

# ---------------------------------------------------------------------------
# Rise slew limit: do not chase a load spike that is shorter than our own
# response time.
#
# Evidence, 2026-08-26. Two export events (-105W at 08:50:50 and -405W at
# 08:53:27) occurred with the meter reading steady at +38..+198W either side,
# with NO load switching off, and with injection age at 140-150ms, i.e. the
# transport was healthy and the loop was not late. The heat pump was cycling
# through the same minutes. So the excursions were not a missed load DROP, which
# is what a bigger cushion or a feed-forward addresses. They were the inverter
# faithfully chasing a sub-second load SPIKE: the reading jumps, we inject it, the
# inverter ramps up, and by the time that output arrives the spike is over and the
# surplus goes to the grid. The loop manufactured the export by being obedient.
#
# The fix is an asymmetry that costs nothing, because the two directions are not
# symmetric in consequence:
#   rising  reading -> commands ramp-UP   -> overshoot lands on the GRID (export)
#   falling reading -> commands ramp-DOWN -> undershoot lands on the GRID (import)
# Only one of those is a compliance event, so only the rising direction is
# limited. A fall passes through untouched at full speed.
#
# Deliberately a RATE limit, not a min-over-window filter. A min filter also
# suppresses spikes, but it sits below the true reading during ordinary meter
# noise, which is a permanent hidden cushion: it would quietly raise import all
# day and be indistinguishable from someone having moved the bias. A rate limit
# has no steady-state effect at all, because a settled reading has zero slew.
#
# SLEW_UP_MIN_W is what guarantees that: any rise up to that size passes
# instantly regardless of elapsed time, so ordinary noise never touches the
# limiter and a spike is attenuated in proportion to how brief it is. A genuine
# sustained step is still tracked, just over ~1s per 400W, and the deficit while
# it catches up is served by the grid, which is the compliant direction.
#
# The floor is a ONE-SHOT allowance, granted once per rise episode and not again
# until the reading stops rising. Granting it per poll instead (which is what the
# first version of this did) silently floors the tracking rate at
# SLEW_UP_MIN_W / POLL_PERIOD_SEC, i.e. 240W/s at a 0.25s poll, so every value of
# SLEW_UP_W_PER_SEC below 240 would have behaved identically to 240 and the
# constant would have been a decoration. At the shipped 400W/s the rate term is
# the binding one either way, so this changed no behaviour when it was fixed; it
# is what makes the rate tunable DOWNWARD at all.
SLEW_UP_W_PER_SEC = 400.0
SLEW_UP_MIN_W = 60.0                # any rise this small passes untouched, once
# A reference older than this is not a reference. After a read gap the load may
# legitimately be anywhere, and clamping against a stale anchor would hold the
# injected value down for no reason. Matches STALE_AFTER_SEC on purpose.
SLEW_RESET_SEC = 3.0
# Presence DISABLES the limiter, unlike every other flag file here, because this
# one is a safety default rather than an experiment: the failure mode of
# forgetting to create a file must be "protected", not "unprotected". It exists so
# the limiter can be A/B'd against its own absence without a restart.
NOSLEW_FILE = "/config/ef_inject_noslew"

# ---------------------------------------------------------------------------
# Cloud overwrite counter-kick.
#
# The Ultras stop feeding the house the moment their cloud link drops, so the
# cloud is not a telemetry channel: it is inside their control loop, and it writes
# phase C itself. Measured 2026-08-26: 2% of telemetry frames carry a value we did
# not send, ~51 an hour, exposed for ~146ms each before we re-assert. Correlating
# every reverse-flow sample that day against those events put 9 of 16 within one
# status tick, carrying 85% of the total reverse magnitude, against a 14% base
# rate. That is where the residual export is.
#
# The mechanism is specific. At equilibrium our injected value sits `cushion` watts
# BELOW truth, which is exactly what buys the import cushion. A cloud frame carries
# the cloud's own reading, unbiased and never slew limited, so the Ultra suddenly
# sees a house drawing more than it was regulating to and ramps output UP. Note the
# bias cannot help here at any setting: the applied bias EQUALS the cushion at
# equilibrium, so the jump and the headroom absorbing it scale together and cancel.
# What is left over, and what actually reaches the grid, is the cloud reading's own
# staleness plus the Ultra's ramp overshoot, and neither term depends on the bias.
#
# So this does not try to reduce the jump. It cancels it: on detecting an overwrite,
# re-assert a value BELOW the normal biased one for a couple of cycles, actively
# pulling output back down instead of waiting for the Ultra to settle.
#
# The magnitude is measured, not guessed. At detection we know the value the cloud
# wrote and our own local reading, and their difference IS how much extra output the
# cloud just commanded. Only a POSITIVE difference is kicked: a cloud value below
# ours commands less output, which lands as import, and needs no help.
#
# Direction of failure: a kick only ever lowers the injected value, so it only ever
# commands LESS output. Every failure mode here is import.
#
# THE RELEASE EDGE, and the bug that made the first version of this worse than
# nothing. `_slew_limit` governs the meter READING; the kick is subtracted after it,
# so the limiter never sees it and `_slew_ref_w` never moves when it changes. The
# first version therefore dropped the whole correction in ONE write when the hold
# expired, which is a step UP of up to CLOUD_KICK_MAX_W on the wire even with a
# dead-flat meter. That is a command to ramp output up hard, and its overshoot is
# export: exactly the failure the slew limiter exists to prevent, reintroduced once
# per kick. Measured on 2026-08-26, phase C export ran 0.29Wh/h in the 4.6h before
# the kick went live and 1.21Wh/h in the 69min after, the worst hour-14 in three days.
# The pull-down was never the problem; every intermediate value is below the biased
# one and lands as import. Only the recovery was.
#
# So the correction is now BLED OFF at SLEW_UP_W_PER_SEC rather than dropped. That
# rate is reused deliberately instead of getting its own constant: it makes the
# invariant true by construction, because the release can then never command a rise
# the limiter would have refused from a real load. A 300W kick takes 0.75s to clear
# at 400W/s, during all of which the value is still under truth.
CLOUD_KICK = True
CLOUD_KICK_GAIN = 1.0               # fraction of the measured delta to cancel
CLOUD_KICK_MAX_W = 300.0            # hard ceiling on a single kick, watts
# Writes to HOLD the correction at full size before the release ramp starts. Two at
# the 0.5s keepalive is ~0.5s of pull-down against a ~146ms exposure, so the
# correction outlasts the disturbance rather than racing it. Worst-case cost if every
# kick were full size: 300W held 0.5s plus 0.75s of ramp, ~1200 events/day, ~90Wh of
# import. That is the price of the mechanism, and it is import, not export.
CLOUD_KICK_CYCLES = 2
# Presence DISABLES, same reasoning as NOSLEW_FILE: forgetting the file must leave
# the protection on. Exists so the kick can be A/B'd without a restart.
NOKICK_FILE = "/config/ef_inject_nokick"

# ---------------------------------------------------------------------------
# Import cushion. The regulator drives what it SEES to zero, and it sees
# `truth + BIAS_W`, so it settles real grid at `-BIAS_W`. A NEGATIVE bias
# therefore parks the house at a slight IMPORT, which is the safe cushion:
# it keeps a margin against real export instead of hunting across zero.
#
# Gain is 1:1, so BIAS_W = -30 means roughly +30W of import cushion.
# Sign convention (confirmed by the causation test 2026-08-21): positive
# phase_c_power = importing, negative = exporting.
#
# Keep this NEGATIVE. A positive value tells the inverter it is importing when
# it is not, commanding it to ramp UP and manufacture genuine export.
# DEFAULT bias, used when no runtime override is present. Lowered from -150 to
# -68 on 2026-08-24 (settling cushion 115W -> 60W) on the strength of the latency
# work: the cushion covers export during the loop's response time, and that fell
# from 481ms to 142ms, so the same protection needs materially less margin.
BIAS_W = -68
# Hard guard: refuse any bias that could push real export. Positive values and
# oversized magnitudes are rejected at startup AND on every runtime override.
BIAS_MAX_ABS = 200

# Runtime override, so tuning the cushion does not cost an HA restart. Write a
# single number (watts, NEGATIVE) into this file:
#     echo -68 > /config/ef_inject_bias
# Re-read whenever the file's mtime or size changes, and validated EVERY time
# against exactly the same two rules as the startup guard, because a runtime knob
# that skips the safety check is worse than no knob at all. Anything unparseable,
# positive, or oversized is refused and the previous good value stays in force:
# an operator typo must not be able to command export. Delete the file to return
# to BIAS_W above.
BIAS_FILE = "/config/ef_inject_bias"
# Sentinel distinct from None, which legitimately means "the file is absent".
_BIAS_UNSET = object()
# The read runs in an EXECUTOR, not the event loop: HA's loop-blocking detector
# correctly flagged the first version of this for calling open() inline four times
# a second. Throttled as well, because a knob a human turns does not need 4 Hz and
# an executor hop per poll is real overhead on the write path. 2s is imperceptible
# when tuning by hand and cuts the I/O by 8x.
BIAS_REFRESH_SEC = 2.0


def _read_bias_file():
    """Blocking. Runs in the executor. Returns the raw text, or None if absent."""
    try:
        with open(BIAS_FILE) as f:
            return f.read().strip()
    except OSError:
        return None

# Arm the cushion by PROXIMITY TO EXPORT, not by battery direction.
#
# This used to gate on `batt_w <= -BIAS_MIN_DISCHARGE_W`, i.e. only while the
# battery was DISCHARGING. That is backwards for the case that actually matters:
# during a PV surplus the battery CHARGES (batt_w positive), so the cushion was
# suppressed for the whole surplus window. Measured on the morning of
# 2026-08-24: biased=232 against bias_idle=2709 with batt_w pinned near +80W,
# so raising BIAS_W on its own would have changed precisely nothing.
#
# Export risk is a function of how close real grid sits to zero, so gate on
# that. The bias fades in linearly: full strength at 0W and below (export
# territory), nothing at BIAS_FADE_W of import or above, where there is no
# export to prevent and a cushion would only buy grid for no reason.
#
# The fade is deliberately continuous. A hard threshold would step the injected
# value by the whole of BIAS_W as it crossed, and the regulator would hunt
# across that step instead of settling.
#
# Equilibrium: the regulator drives (w + bias) to zero, so real grid settles at
#   w = -BIAS_W * F / (F - BIAS_W)
# which for BIAS_W=-150 and F=500 is about +115W of real import cushion.
BIAS_FADE_W = 500.0
# Predicted settling point of REAL grid, from the fade equilibrium derived above.
# Logged at START so the expectation is on the record and can be checked against
# what the Shelly actually reports, rather than being asserted after the fact.
def bias_settle_w(bias):
    """Predicted resting point of REAL grid for a given bias, from the equilibrium
    derived above. Import is positive."""
    return (-bias * BIAS_FADE_W / (BIAS_FADE_W - bias)) if bias else 0.0


def bias_is_safe(bias):
    """The one rule that matters: never a positive bias, never an oversized one.
    A positive bias tells the inverter it is importing when it is not, which
    commands ramp-UP and manufactures the very export we exist to prevent."""
    return bias == bias and bias <= 0 and abs(bias) <= BIAS_MAX_ABS


BIAS_SETTLE_W = bias_settle_w(BIAS_W)

# Event-driven correction. The cloud writes cfg_cloud_metter over its own MQTT
# link, which we cannot intercept, so we cannot stop the overwrite. What we CAN
# do is cut how LONG it survives: the echo classifier already tells us when a
# value we did not send appears, so instead of waiting out the next timer tick we
# re-assert immediately. Exposure drops from up to a whole poll period to about one
# BLE round trip (~130ms measured).
REASSERT_ON_CLOUD = True
# Floor between consecutive writes, so a burst of cloud frames cannot make us
# hammer the BLE link.
MIN_SEND_GAP_SEC = 0.15

# Verbose per-send / per-echo logging. Chatty by design: ~1 line per send plus
# ~1 per received telemetry frame. Touch /config/ef_inject_quiet to silence.
VERBOSE = True
QUIET_FILE = "/config/ef_inject_quiet"
# How close an echo must be to a value we sent to be attributed to us (watts).
ECHO_MATCH_TOL = 2

# A/B mode: alternate INJ (we write) and CLOUD (we stay silent) blocks, scoring
# each from the LOCAL Shelly only. The local meter is the one channel the device
# cannot influence by echoing our own writes back at us, so it is the only valid
# judge of regulation tightness. Touch /config/ef_inject_ab to enable.
AB_FILE = "/config/ef_inject_ab"
AB_BLOCK_SEC = 180
# A grid excursion this large counts as a regulation miss.
EXCURSION_W = 100

# ---------------------------------------------------------------------------
# Causation test: does the regulator ACT on cfg_cloud_metter, or is it display
# only? Touch /config/ef_inject_causation to arm one run.
#
# Method: brief paired pulses. REAL trials add CAUS_OFFSET_W to the true reading;
# SHAM trials inject the truth exactly as normal operation does. The only
# difference between arms is the offset, so anything that shows up in REAL and
# not SHAM is attributable to the injected value.
#
# Judged ONLY from the local Shelly and battery power. Both are physical
# measurements the device cannot produce by echoing our own write back at us,
# which is the trap that invalidated the earlier single-spike attempt.
#
# Direction is deliberately NEGATIVE (fake export -> "back off"). The inverter
# reduces output, and the house briefly draws from the grid instead. The
# opposite sign would command it to ramp UP and could cause real export.
CAUSATION_FILE = "/config/ef_inject_causation"
CAUS_OFFSET_W = -600
CAUS_TRIALS = 8              # alternating REAL / SHAM
CAUS_PRE_SEC = 18
CAUS_PULSE_SEC = 4
CAUS_POST_SEC = 14
CAUS_PULSE_PERIOD = 0.5      # re-assert during pulse to outrun cloud overwrite
CAUS_MIN_DISCHARGE_W = 400   # need real output to modulate
CAUS_MAX_BASELINE_W = 150    # need a quiet grid to read the response against
# Difference in grid response between arms that we accept as real control.
CAUS_DECISION_W = 150

# Self-healing device resolution.
#
# ef_ble replaces the DeviceBase object when its config entry is reloaded: a
# disconnect schedules a reload, and async_unload_entry calls device.disconnect(),
# which sets self._conn = None on the object being torn down. A reference captured
# once at loop start therefore goes PERMANENTLY dead: every send raises
# "'NoneType' object has no attribute 'sendPacket'" while the live object served by
# entry.runtime_data is perfectly healthy. Observed 2026-08-24: 74928 consecutive
# write errors at the loop rate while ef_read and the 15-minute reconcile both
# wrote to the same physical device without trouble.
#
# So: re-resolve the device EVERY cycle, re-attach the message listener whenever the
# object identity changes, and never hold a long-lived reference.
NOT_READY_BACKOFF_START = 1.0   # first wait when the link cannot accept a write
NOT_READY_BACKOFF_MAX = 30.0    # cap, so a dead link cannot spin at the loop rate
ERR_LOG_MIN_GAP_SEC = 30.0      # rate limit for repeated write-error / not-ready logs
IDENTITY_POLL_SEC = 2.0         # how often to re-check for usable telemetry at startup
IDENTITY_LOG_GAP_SEC = 60.0     # how often to say we are still waiting for telemetry

# A CONNECTED link can still be dead. The teardown at 08:21:04 on 2026-08-24 was
# reported by eflib as "Ping response not received after 90.0 seconds": for up to
# 90s is_connected stayed True and every write reported success while nothing was
# reaching the device. DisplayPropertyUpload streams continuously (frame age runs
# ~0.4s in practice), so silence is the honest liveness signal and is_connected
# alone is not a sufficient readiness gate.
FRAME_MAX_AGE_SEC = 8.0

# Compact liveness line so the HA log stays usable. The per-send and per-echo
# lines are DEBUG (see `dbg`), which at ~2 lines/second had been evicting every
# other integration's records from the log buffer and made diagnosing the ef_ble
# disconnects need a 25000-line pull. Raise them live, without a restart, with:
#   service: logger.set_level
#   data: {custom_components.ef_inject: debug}
HEARTBEAT_SEC = 10.0

log = _LOGGER.warning
dbg = _LOGGER.debug


# ---------------------------------------------------------------------------
# Locating the ef_ble config entry we regulate.
#
# Title first, because that is what this has always matched and it keeps today's
# behaviour bit-for-bit. Address second, so a rename in the HA UI degrades to
# "still works" instead of "silently stops regulating". getattr() because the test
# harness's fake entry has no unique_id.
# ---------------------------------------------------------------------------
def find_ef_ble_entry(hass):
    entries = hass.config_entries.async_entries("ef_ble")
    for entry in entries:
        if entry.title == TARGET_TITLE:
            return entry
    for entry in entries:
        if getattr(entry, "unique_id", None) == TARGET_ADDRESS:
            log(
                "EFINJECT config entry %r not found; matched the Ultra by address %s "
                "instead. Someone renamed the ef_ble entry.",
                TARGET_TITLE, TARGET_ADDRESS,
            )
            return entry
    return None


# ---------------------------------------------------------------------------
# The file knobs, as data.
#
# Every tuning knob is a file under /config, which is what makes them reachable from
# a shell with no restart. The entity layer (number/switch/button) drives exactly
# these same files rather than reaching into the Injector, so the UI and the shell go
# through one validated path and produce one log line. See number.py.
# ---------------------------------------------------------------------------
FLAG_FILES = {
    "stop": STOP_FILE,
    "modbus": MODBUS_FILE,
    "interleave": INTERLEAVE_FILE,
    "shadow": SHADOW_FILE,
    "quiet": QUIET_FILE,
    "ab": AB_FILE,
    "causation": CAUSATION_FILE,
    "noslew": NOSLEW_FILE,
    "nokick": NOKICK_FILE,
}


def stat_flags():
    """Blocking. Runs in the executor. One pass over every knob file."""
    return {name: os.path.exists(path) for name, path in FLAG_FILES.items()}


def write_text_atomic(path, text):
    """Blocking. Runs in the executor.

    Temp-file-and-rename, because the bias file is read at the poll rate and a
    human's `echo` was never racing anything, but a UI control is. A truncated read
    is already handled safely (unparseable -> REFUSED, previous value stands), so
    this is about not manufacturing log noise, not about safety.
    """
    tmp = path + ".tmp"
    with open(tmp, "w") as f:
        f.write(text)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)


def set_flag_file(path, present):
    """Blocking. Runs in the executor. Create or remove an existence-flag file."""
    if present:
        with open(path, "w") as f:
            f.write("")
        return
    try:
        os.remove(path)
    except FileNotFoundError:
        pass


class ModbusMeter:
    """One persistent Modbus TCP connection to one Shelly Pro 3EM.

    Extracted from the Injector so both meters run the identical, tested code
    path rather than a copy. Holds the socket open: a fresh TCP connect per read
    pushed p95 to ~97ms, which threw away most of the point of leaving HTTP.
    """

    def __init__(self, host, name, port=None):
        self.host = host
        self.name = name
        # Pinned per instance rather than read from the global at connect time, so
        # two meters are genuinely independent endpoints.
        self.port = MODBUS_PORT if port is None else port
        self._reader = None
        self._writer = None
        self._tid = 0
        self.reads = 0          # successful reads
        self.err = 0            # failed reads (each drops the socket)
        self.reopens = 0        # times the socket had to be established
        self.last_value = None
        self.last_change_ts = 0.0   # when last_value last actually moved

    async def close(self):
        """Drop the socket. Any protocol error invalidates the stream, because a
        desynced reply would be parsed as the next request's answer."""
        w = self._writer
        self._reader = None
        self._writer = None
        if w is None:
            return
        try:
            w.close()
            await w.wait_closed()
        except Exception:
            pass

    async def read(self):
        """One FC4 read of phase C active power. Returns watts or None.

        The socket is reopened lazily, so a Shelly reboot costs one failed cycle
        rather than the transport.
        """
        if self._writer is None or self._writer.is_closing():
            await self.close()
            try:
                self._reader, self._writer = await asyncio.wait_for(
                    asyncio.open_connection(self.host, self.port),
                    timeout=MODBUS_TIMEOUT_SEC,
                )
            except Exception as e:
                self.err += 1
                await self.close()
                dbg("EFINJECT modbus[%s] connect failed: %s", self.name, e)
                return None
            self.reopens += 1

        self._tid = (self._tid + 1) & 0xFFFF
        req = struct.pack(
            ">HHHBBHH",
            self._tid, 0, 6, MODBUS_UNIT,
            MODBUS_FC_READ_INPUT, MODBUS_REG_C_ACT_POWER, 2,
        )
        try:
            self._writer.write(req)
            await self._writer.drain()
            head = await asyncio.wait_for(
                self._reader.readexactly(9), timeout=MODBUS_TIMEOUT_SEC
            )
            tid, proto, _ln, _unit, fc = struct.unpack(">HHHBB", head[:8])
            if tid != self._tid or proto != 0:
                raise ValueError(
                    "MBAP mismatch tid=%d/%d proto=%d" % (tid, self._tid, proto)
                )
            if fc & 0x80:
                # Exception response: len is 3, so head[8] is already the code
                # and there is nothing further to read. FC3 lands here with 2.
                raise ValueError("modbus exception fc=0x%02x code=%d" % (fc, head[8]))
            nbytes = head[8]
            if nbytes != 4:
                raise ValueError("expected 4 data bytes, got %d" % nbytes)
            body = await asyncio.wait_for(
                self._reader.readexactly(nbytes), timeout=MODBUS_TIMEOUT_SEC
            )
        except Exception as e:
            self.err += 1
            await self.close()
            dbg("EFINJECT modbus[%s] read failed: %s", self.name, e)
            return None

        # Low word first: swap the two registers, then unpack big-endian float32.
        v = struct.unpack(">f", body[2:4] + body[0:2])[0]
        if v != v or v in (float("inf"), float("-inf")):
            self.err += 1
            return None
        self.reads += 1
        # Track WHEN this meter last published a new number. With two meters this
        # is what lets us pick the fresher one, and on its own it measures how
        # stale the value we inject actually is.
        if self.last_value is None or abs(v - self.last_value) >= WRITE_ON_CHANGE_W:
            self.last_change_ts = time.time()
        self.last_value = v
        return float(v)


class Injector:
    def __init__(self, hass: HomeAssistant):
        self.hass = hass
        self.sent = 0
        self.skipped_stale = 0
        self.skipped_read_err = 0
        self.skipped_no_identity = 0
        self.write_err = 0
        self.last_local = None      # (t, watts)
        self.last_sent_w = None
        self.identity = None        # (has_meter, model, sn, a, b)
        self.running = False
        self._send_lock = asyncio.Lock()
        self._session: aiohttp.ClientSession | None = None
        # verbose accounting
        self._recent_sent = []      # [(t, watts)] recent writes, for echo matching
        self.echo_ours = 0          # echoes matching a value we sent
        self.echo_cloud = 0         # echoes matching nothing we sent (cloud wins)
        self.echo_lag_sum = 0.0
        self.echo_lag_n = 0
        self._last_echo_c = None
        self._frames = 0
        # A/B scoring, judged only from local Shelly samples
        self.ab_mode = None         # "INJ" | "CLOUD" | None
        self._ab_block = 0
        self._ab = {}               # mode -> dict(n, abs_sum, sq_sum, worst, excursions)
        # causation test
        self.batt_w = None          # latest pow_get_bp_cms (negative = discharging)
        self._caus_offset = 0       # watts added to truth during a pulse
        self.paused = False         # causation test owns the link
        # bias accounting
        self.biased = 0             # sends that carried the import cushion
        self.bias_idle = 0          # sends with bias suppressed (not discharging)
        # Last bias ACTUALLY applied, after the proximity fade, as opposed to `bias_w`
        # which is the configured ceiling. Stored only so the entity layer can show it:
        # nothing in the loop reads it back, so it can never influence regulation.
        self.bias_applied = 0
        self._recent_local = []     # rolling local grid samples, for effect reporting
        # event-driven re-assert
        self._wake = asyncio.Event()
        self._last_send_ts = 0.0
        self._cloud_seen_ts = None  # set when a cloud overwrite is detected
        self.reasserts = 0          # writes triggered by a detected cloud overwrite
        self.reassert_ms_sum = 0.0
        # cloud overwrite counter-kick. `_kick_w` is the measured delta the cloud
        # commanded, `_kick_left` how many more writes should carry it at FULL size
        # before the release ramp starts, and `_kick_ts` when `_kick_w` last moved,
        # which is what makes the release a rate rather than a step.
        self._kick_w = 0.0
        self._kick_left = 0
        self._kick_ts = 0.0
        self.kicks = 0              # writes that carried a counter-kick
        self.kick_events = 0        # overwrites that armed one
        self.kick_w_sum = 0.0       # sum of kick watts applied, for a mean
        self.kick_worst_w = 0.0     # largest single kick applied, watts
        self.kick_skipped = 0       # overwrites that needed no kick (delta <= 0)
        # self-healing device attachment
        self._device_ref = None         # DeviceBase we are currently attached to
        self._cancel_listener = None    # detach callback for that attachment
        self.device_swaps = 0           # times ef_ble handed us a different object
        self.skipped_not_ready = 0      # cycles skipped because the link was unusable
        self._backoff = NOT_READY_BACKOFF_START
        self._last_not_ready_log = 0.0
        self._last_err_log = 0.0
        self._err_since_log = 0
        self._last_frame_ts = 0.0       # last DisplayPropertyUpload seen
        self._not_ready_why = None      # why _ready_device() last refused
        self.skipped_silent = 0         # refusals for a connected-but-silent link
        self._last_hb = 0.0             # last heartbeat line
        # meter transport
        self._mb = ModbusMeter(MODBUS_HOST, "bound")     # the meter EcoFlow is bound to
        self._mb2 = ModbusMeter(SECOND_HOST, "grid")     # second opinion / interleave
        self._mb_mode = None            # last observed state of the toggle file
        self._second_mode = None        # last observed state of shadow/interleave
        self.transport = None           # "modbus" | "http", as last actually used
        # second-meter accounting
        self.sec_used = 0               # writes that took the SECOND meter's value
        self.sec_diffs = []             # (v_bound - v_grid), QUIET samples only
        self.sec_diff_absmax = 0.0
        self.sec_diff_last = float("nan")   # mean of the last completed verdict
        self.interleave_locked = False  # latched off after sustained divergence
        self._quiet_hist = []           # (ts, v_bound, v_grid) for the quiescence gate
        self.sec_bad_run = 0            # consecutive over-limit verdicts
        self.sec_good_run = 0           # consecutive within-recover-limit verdicts
        self.sec_verdicts = 0           # completed verdicts this session
        self.sec_skipped_busy = 0       # samples discarded as not-quiet
        self.interleave_locks = 0       # times the guard latched off
        self.interleave_recoveries = 0  # times it recovered on its own
        self.interleave_rearms = 0      # times an operator re-armed it
        self._recovery_times = []       # recent recoveries, for escalating hysteresis
        self._interleave_file_seen = None   # None = never looked, for edge detect
        self.fresh_ms_sum = 0.0         # staleness of the value we injected
        self.fresh_ms_n = 0
        # The most recent sample, not the running mean. A session mean flattens out
        # after a few thousand writes and would hide a transport regression happening
        # right now, which is the only reason to look at this number at all.
        self.fresh_ms_last = None
        self._src_change_ts = 0.0       # when the value we just read was published
        # rise slew limiter
        self._slew_ref_w = None         # last effective value, the anchor for the rise
        self._slew_ref_ts = 0.0
        # The one-shot floor. True means SLEW_UP_MIN_W has not yet been spent on the
        # rise currently in progress. Consumed by a rise that needed more than the rate
        # term alone, restored as soon as the reading stops rising. See SLEW_UP_MIN_W.
        self._slew_floor_ready = True
        self.slew_clamps = 0            # cycles where a rise was held back
        self.slew_worst_w = 0.0         # largest single rise suppressed, watts
        self.slew_held_w = 0.0          # sum of watts withheld, for a mean
        # poll/write decoupling
        self._last_src_w = None         # meter value at the last write
        self.polls = 0                  # meter polls
        self.writes_on_change = 0       # writes triggered by a new meter value
        self.writes_keepalive = 0       # writes triggered by the keepalive floor
        self.polls_no_write = 0         # polls that correctly wrote nothing
        self._deadline = 0.0            # next poll deadline, for honest pacing
        self._t_start = 0.0             # START timestamp, for the true send rate
        # runtime bias override
        self.bias_w = BIAS_W            # bias actually in force right now
        self._bias_raw = _BIAS_UNSET    # raw override text last seen (None = absent)
        self._bias_next_check = 0.0     # throttle for the executor read
        self.bias_overrides = 0         # accepted runtime changes
        self.bias_rejected = 0          # refused values (typo, positive, oversized)
        # Last observed state of the knob FILES, refreshed once per status tick in the
        # executor. The entity layer reads this dict instead of calling os.path.exists
        # in a property: entity properties run on the event loop and are read often,
        # and a 5s-old view of a knob a human turns is indistinguishable from a live one.
        self.flags = {}

    # -- device -------------------------------------------------------------
    def _device(self):
        entry = find_ef_ble_entry(self.hass)
        return getattr(entry, "runtime_data", None) if entry is not None else None

    def _attach(self, device):
        """Point the message listener at `device`, detaching from any previous one."""
        if self._cancel_listener is not None:
            try:
                self._cancel_listener()
            except Exception:
                _LOGGER.exception("EFINJECT failed detaching previous listener")
        self._device_ref = device
        self._cancel_listener = device.on_message_processed(self._on_msg)
        # Start the FRAME_MAX_AGE_SEC grace window now rather than leaving the
        # timestamp at 0. A fresh attachment has legitimately not seen a frame yet,
        # and treating that as "silent since the epoch" would refuse every write.
        self._last_frame_ts = time.time()

    def _detach(self):
        if self._cancel_listener is not None:
            try:
                self._cancel_listener()
            except Exception:
                _LOGGER.exception("EFINJECT failed detaching listener")
        self._cancel_listener = None
        self._device_ref = None

    def _ready_device(self):
        """Return the CURRENT live device if it can accept a write, else None.

        Deliberately re-resolved on every call: ef_ble swaps the DeviceBase object on
        config-entry reload and the abandoned one keeps `_conn = None` forever, so any
        cached reference is a latent permanent failure. See the note by
        NOT_READY_BACKOFF_START.
        """
        device = self._device()
        if device is None:
            self._not_ready_why = "no device in entry.runtime_data"
            return None
        if device is not self._device_ref:
            self.device_swaps += 1
            log(
                "EFINJECT device object changed (swap #%d) -> re-attaching listener; "
                "the previous reference is dead and is being dropped",
                self.device_swaps,
            )
            self._attach(device)
        # is_connected is `self._conn is not None and self._conn.is_connected`, which is
        # exactly the condition whose absence produced the NoneType.sendPacket storm.
        if not device.is_connected:
            self._not_ready_why = "BLE not connected"
            return None
        # Connected is necessary but NOT sufficient: see FRAME_MAX_AGE_SEC. Without
        # this, a link whose pings have gone unanswered accepts writes that go nowhere
        # and reports every one of them as a success.
        age = time.time() - self._last_frame_ts
        if age > FRAME_MAX_AGE_SEC:
            self.skipped_silent += 1
            self._not_ready_why = "connected but silent for %.0fs" % age
            return None
        self._not_ready_why = None
        return device

    async def _not_ready(self, why):
        """Back off instead of hammering a link that cannot accept writes."""
        self.skipped_not_ready += 1
        now = time.time()
        if now - self._last_not_ready_log >= ERR_LOG_MIN_GAP_SEC:
            log(
                "EFINJECT link not usable (%s), backing off %.0fs "
                "(not_ready=%d swaps=%d) -> cloud keeps control meanwhile",
                why, self._backoff, self.skipped_not_ready, self.device_swaps,
            )
            self._last_not_ready_log = now
        await asyncio.sleep(self._backoff)
        self._backoff = min(self._backoff * 2, NOT_READY_BACKOFF_MAX)

    def _verbose(self):
        return VERBOSE and not os.path.exists(QUIET_FILE)

    async def _pace(self):
        """Wait until the next poll DEADLINE, returning early if the cloud
        overwrites us.

        Deadline-based, not sleep-based. The old version slept a flat period
        *after* the cycle's work, so the true rate was period + read + BLE write:
        measured 500 + 5 + 110 = ~615ms, i.e. 1.6 Hz against a configured 2 Hz,
        and the same arithmetic explained the 1.4 Hz seen on HTTP (500+79+110).
        Sleeping to a deadline makes the configured rate the actual rate and
        absorbs the write latency instead of adding to it.

        Also enforces MIN_SEND_GAP_SEC so a burst of cloud frames cannot turn into
        a burst of BLE writes.
        """
        now = time.time()
        self._deadline = max(now, getattr(self, "_deadline", 0.0) + POLL_PERIOD_SEC)
        # If we fell far behind (a long BLE stall, a blocked event loop), do not
        # try to catch up by bursting. Resync to now instead.
        if self._deadline - now > POLL_PERIOD_SEC:
            self._deadline = now + POLL_PERIOD_SEC
        wait = self._deadline - now
        if not REASSERT_ON_CLOUD:
            if wait > 0:
                await asyncio.sleep(wait)
            return
        if wait > 0:
            try:
                await asyncio.wait_for(self._wake.wait(), timeout=wait)
            except (asyncio.TimeoutError, TimeoutError):
                return
            finally:
                self._wake.clear()
        else:
            self._wake.clear()
        gap = time.time() - self._last_send_ts
        if gap < MIN_SEND_GAP_SEC:
            await asyncio.sleep(MIN_SEND_GAP_SEC - gap)

    def _on_msg(self, msg):
        if msg.DESCRIPTOR.name != "DisplayPropertyUpload":
            return
        if not msg.HasField("cloud_metter"):
            return
        cm = msg.cloud_metter
        # Track identity so every write can pin it exactly.
        self.identity = (
            cm.has_meter,
            cm.model,
            cm.sn,
            cm.phase_a_power,
            cm.phase_b_power,
        )

        self._frames += 1
        c = cm.phase_c_power
        now = time.time()
        self._last_frame_ts = now
        self.batt_w = msg.pow_get_bp_cms

        # Only classify when the echoed value CHANGES, otherwise we count the same
        # state repeatedly and the ratio becomes meaningless.
        if c == self._last_echo_c:
            return
        self._last_echo_c = c

        # Did we send this value recently? Keep a short window.
        self._recent_sent = [(t, w) for (t, w) in self._recent_sent if now - t <= 6.0]
        match = None
        for t, w in reversed(self._recent_sent):
            if abs(w - c) <= ECHO_MATCH_TOL:
                match = (t, w)
                break

        if match is not None:
            self.echo_ours += 1
            lag = now - match[0]
            self.echo_lag_sum += lag
            self.echo_lag_n += 1
            if self._verbose():
                dbg(
                    "EFINJECT echo OURS c=%dW lag=%.2fs grid=%.0f batt=%.0f",
                    c, lag, msg.pow_get_sys_grid, msg.pow_get_bp_cms,
                )
        else:
            self.echo_cloud += 1
            local = self.last_local[1] if self.last_local else float("nan")
            # The cloud just clobbered our value. Wake the loop now rather than
            # letting the stale value ride until the next tick.
            if REASSERT_ON_CLOUD and not self.paused:
                self._cloud_seen_ts = now
                self._wake.set()
            kick = 0.0
            if CLOUD_KICK and not self.flags.get("nokick") and not self.paused:
                kick = self._arm_kick(c, local, now)
            else:
                self.kick_skipped += 1
            # NOT gated on _verbose(). This is the only per-event record of the one
            # mechanism that still puts energy on the grid, and inferring it from the
            # 5s counter tick cost a ±5s timing resolution that made a real
            # correlation unprovable. ~51 lines an hour measured, against 120 for the
            # summary line, so the log stays readable.
            log(
                "EFINJECT echo CLOUD c=%dW (local now %.0fW, delta %+.0fW) "
                "kick=%.0fW grid=%.0f batt=%.0f",
                c, local, c - local, kick,
                msg.pow_get_sys_grid, msg.pow_get_bp_cms,
            )

    # -- local meter --------------------------------------------------------
    def _modbus_wanted(self):
        return os.path.exists(MODBUS_FILE)

    def _interleave_wanted(self):
        """Actually USE the second meter. Latches off after sustained divergence."""
        return (
            not self.interleave_locked
            and self._modbus_wanted()
            and os.path.exists(INTERLEAVE_FILE)
        )

    def _check_interleave_rearm(self):
        """Clear the latch on an operator re-arm: `rm` then `touch` the toggle.

        This is a CONVENIENCE, not a requirement. The guard recovers on its own
        (see _recover_strikes_needed); this only skips the wait and resets the
        escalation, for when you know why it tripped and have fixed it.

        Only the absent -> present TRANSITION counts. Presence alone cannot clear
        it, or the latch would be re-armed on every single cycle and could never
        hold at all. `_interleave_file_seen` starts as None so the first
        observation of an already-present file is not mistaken for an edge.
        """
        present = os.path.exists(INTERLEAVE_FILE)
        was, self._interleave_file_seen = self._interleave_file_seen, present
        if was is not False or not present:
            return
        if not self.interleave_locked:
            return
        self.interleave_locked = False
        self._recovery_times = []           # a deliberate act clears the escalation
        self.interleave_rearms += 1
        self.sec_bad_run = self.sec_good_run = 0
        self.sec_diffs = []
        log(
            "EFINJECT INTERLEAVE RE-ARMED by hand (toggle file recreated). The "
            "divergence guard is judging from scratch and the escalation is reset to "
            "%d verdicts; if the meters really do disagree it will latch off again "
            "within a few quiet windows. This is never REQUIRED, the guard recovers "
            "on its own.",
            INTERLEAVE_RECOVER_STRIKES,
        )

    async def _refresh_bias(self):
        """Pick up a runtime bias override. Returns the bias now in force.

        Compares the file's CONTENT, not its mtime. An mtime-and-size cache looked
        cheaper but silently misses `echo -100` followed by `echo -120` inside one
        filesystem mtime tick: same size, same second, change lost. The raw string
        is also what suppresses repeated log lines for a value that stays bad.

        Failure mode is deliberately STICKY-SAFE. A bad value leaves the previous
        good one in place rather than reverting to the default, because reverting
        would silently RAISE the cushion after a typo, and changing regulation
        silently in either direction is the thing to avoid.
        """
        now = time.time()
        if now < self._bias_next_check:
            return self.bias_w
        self._bias_next_check = now + BIAS_REFRESH_SEC
        try:
            raw = await self.hass.async_add_executor_job(_read_bias_file)
        except Exception as e:                       # executor unavailable, etc.
            log("EFINJECT BIAS could not read %s (%s); keeping %+dW",
                BIAS_FILE, e, self.bias_w)
            return self.bias_w
        if raw == self._bias_raw:
            return self.bias_w
        self._bias_raw = raw

        if raw is None:                          # override removed, or never there
            if self.bias_w != BIAS_W:
                log(
                    "EFINJECT BIAS override removed, reverting to the built-in "
                    "%+dW (cushion ~%+.0fW import)", BIAS_W, bias_settle_w(BIAS_W),
                )
            self.bias_w = BIAS_W
            return self.bias_w

        try:
            new = int(round(float(raw)))
        except ValueError:
            self.bias_rejected += 1
            log(
                "EFINJECT BIAS override REFUSED, %s does not contain a number (%r). "
                "Keeping %+dW.", BIAS_FILE, raw[:40], self.bias_w,
            )
            return self.bias_w

        if not bias_is_safe(new):
            self.bias_rejected += 1
            log(
                "EFINJECT BIAS override REFUSED: %+dW is unsafe (must be <=0 and "
                "|bias|<=%d). A positive bias would command ramp-UP and manufacture "
                "real export. Keeping %+dW.", new, BIAS_MAX_ABS, self.bias_w,
            )
            return self.bias_w

        if new == self.bias_w:
            return self.bias_w                   # e.g. "-68" vs "-68.0"
        old = self.bias_w
        self.bias_w = new
        self.bias_overrides += 1
        log(
            "EFINJECT BIAS %+dW -> %+dW (predicted resting cushion %+.0fW -> %+.0fW "
            "of real import, fade to zero at %+.0fW). No restart needed.",
            old, new, bias_settle_w(old), bias_settle_w(new), BIAS_FADE_W,
        )
        return self.bias_w

    def _interleave_state(self):
        """Human-readable state for the log. `locked=True/False` read backwards to
        everyone including me, so say what is actually happening instead."""
        if not (self._modbus_wanted() and os.path.exists(INTERLEAVE_FILE)):
            return "off(not enabled)"
        if self.interleave_locked:
            # Always say how far away recovery is, so the state is never mistaken
            # for something that needs a human.
            return "OFF(locked, self-recovers at %d/%d good)" % (
                self.sec_good_run, self._recover_strikes_needed())
        return "ON"

    def _second_wanted(self):
        """Poll the second meter at all. Interleaving implies observing."""
        return self._modbus_wanted() and (
            os.path.exists(SHADOW_FILE) or os.path.exists(INTERLEAVE_FILE)
        )

    async def _modbus_close(self):
        await self._mb.close()
        await self._mb2.close()

    def _signal_quiet(self, now):
        """True when phase C has been flat long enough for the meters to be
        comparable at all.

        The two publish 0.412s apart, so while the signal moves they are reporting
        DIFFERENT INSTANTS and their difference measures the ramp, not the meters.
        Requiring a flat band across more than one full publication period means
        both have certainly refreshed inside it, so whatever difference remains is
        attributable to the instruments. This is what makes a short window
        trustworthy instead of merely diluted.

        BOTH series have to be flat, not just the bound one. A step the bound meter
        has not published yet is still a step, and it lands entirely in the
        difference: see the 2026-08-25 espresso trip in the notes above. Requiring
        both costs only verdict rate, which there is plenty of.
        """
        if not self._quiet_hist or now - self._quiet_hist[0][0] < INTERLEAVE_QUIET_SEC:
            return False                    # not enough history to prove flatness
        win = [(a, b) for ts, a, b in self._quiet_hist
               if ts >= now - INTERLEAVE_QUIET_SEC]
        if len(win) < 2:
            return False
        return all(max(s) - min(s) <= INTERLEAVE_QUIET_W for s in zip(*win))

    def _track_divergence(self, v1, v2, now=None):
        """Quiescence-gated divergence guard. See INTERLEAVE_DIVERGE_W."""
        now = time.time() if now is None else now
        d = v1 - v2
        if abs(d) > self.sec_diff_absmax:
            self.sec_diff_absmax = abs(d)   # kept for visibility, never a trigger

        self._quiet_hist.append((now, v1, v2))
        cut = now - INTERLEAVE_QUIET_SEC * 2
        while self._quiet_hist and self._quiet_hist[0][0] < cut:
            del self._quiet_hist[0]

        if not self._signal_quiet(now):
            self.sec_skipped_busy += 1
            return

        self.sec_diffs.append(d)
        if len(self.sec_diffs) < INTERLEAVE_DIVERGE_WINDOW:
            return

        mean = sum(self.sec_diffs) / len(self.sec_diffs)
        self.sec_diffs = []                 # verdicts are NON-OVERLAPPING, so a
        self.sec_verdicts += 1              # strike is a genuinely new window and
        self.sec_diff_last = mean           # not the same bad data counted twice
        self._judge_divergence(mean)

    def _recent_recoveries(self, now=None):
        """Recoveries still counting toward escalation, pruning expired ones."""
        now = time.time() if now is None else now
        cut = now - INTERLEAVE_RECOVERY_DECAY_SEC
        self._recovery_times = [t for t in self._recovery_times if t >= cut]
        return len(self._recovery_times)

    def _recover_strikes_needed(self, now=None):
        """How many consecutive good verdicts the NEXT recovery must produce.

        Doubles per recent recovery so a flapping pair throttles itself, capped so
        the answer is always a finite number of quiet windows away. Never returns
        infinity: recovery must always remain reachable without a human.
        """
        n = self._recent_recoveries(now)
        return min(INTERLEAVE_RECOVER_STRIKES * (2 ** n), INTERLEAVE_RECOVER_MAX_STRIKES)

    def _judge_divergence(self, mean):
        """Strikes and escalating hysteresis on completed verdicts."""
        self.sec_bad_run = self.sec_bad_run + 1 if abs(mean) > INTERLEAVE_DIVERGE_W else 0
        self.sec_good_run = self.sec_good_run + 1 if abs(mean) <= INTERLEAVE_RECOVER_W else 0

        if not self.interleave_locked:
            if self.sec_bad_run < INTERLEAVE_STRIKES:
                return
            self.interleave_locked = True
            self.interleave_locks += 1
            self.sec_good_run = 0
            need = self._recover_strikes_needed()
            log(
                "EFINJECT INTERLEAVE LOCKED OFF: %s and %s disagree by %+.0fW across "
                "%d consecutive quiet windows of %d samples (limit %.0fW). Falling "
                "back to the bound meter alone. It will re-enable ITSELF after %d "
                "consecutive verdicts within %.0fW%s. No action needed from you.",
                MODBUS_HOST, SECOND_HOST, mean, self.sec_bad_run,
                INTERLEAVE_DIVERGE_WINDOW, INTERLEAVE_DIVERGE_W,
                need, INTERLEAVE_RECOVER_W,
                "" if need == INTERLEAVE_RECOVER_STRIKES else
                " (raised from %d because it already recovered %d time(s) in the last "
                "%.0f min, so a flapping pair asks for more proof each time)"
                % (INTERLEAVE_RECOVER_STRIKES, self._recent_recoveries(),
                   INTERLEAVE_RECOVERY_DECAY_SEC / 60.0),
            )
            return

        # Locked, but still measuring. Recovery is deliberately harder than
        # tripping: it needs the tighter RECOVER_W band, so the pair has to come
        # back to genuine agreement rather than merely stop being awful.
        need = self._recover_strikes_needed()
        if self.sec_good_run < need:
            return
        now = time.time()
        self.interleave_locked = False
        self.interleave_recoveries += 1
        self._recovery_times.append(now)
        self.sec_bad_run = 0
        self.sec_good_run = 0            # the next recovery starts its count fresh
        log(
            "EFINJECT INTERLEAVE RECOVERED BY ITSELF: %s and %s now agree to %+.0fW "
            "across %d consecutive quiet windows (limit %.0fW). Re-enabling the "
            "interleave. If it trips again within %.0f min the next recovery will "
            "demand %d verdicts instead of %d.",
            MODBUS_HOST, SECOND_HOST, mean, need, INTERLEAVE_RECOVER_W,
            INTERLEAVE_RECOVERY_DECAY_SEC / 60.0,
            self._recover_strikes_needed(now), need,
        )

    async def _read_meters(self):
        """Read the bound meter, and the second one if wanted, concurrently.

        Returns (watts, source_name, change_ts) or (None, None, 0.0).
        """
        self._check_interleave_rearm()
        if self._second_wanted():
            v1, v2 = await asyncio.gather(self._mb.read(), self._mb2.read())
        else:
            v1, v2 = await self._mb.read(), None

        if v1 is not None and v2 is not None:
            self._track_divergence(v1, v2)

        if self._interleave_wanted():
            # Take whichever meter published most recently. Both measure the same
            # conductor (verified to 0.29%), they are offset by 0.41 of the 1 Hz
            # period, so the fresher one is on average ~250ms less stale.
            if v1 is not None and v2 is not None:
                if self._mb2.last_change_ts > self._mb.last_change_ts:
                    self.sec_used += 1
                    return v2, "grid", self._mb2.last_change_ts
                return v1, "bound", self._mb.last_change_ts
            # Redundancy: either meter alone still keeps regulation alive. This is
            # the failure independence that a wired link would have bought.
            if v1 is None and v2 is not None:
                self.sec_used += 1
                return v2, "grid", self._mb2.last_change_ts
        if v1 is not None:
            return v1, "bound", self._mb.last_change_ts
        return None, None, 0.0

    def _slew_limit(self, w, now):
        """Clamp how fast the injected reading may RISE. Falls pass untouched.

        Returns the value to regulate on. Never returns more than `w`, so this can
        only ever command LESS inverter output than the raw reading would, and a bug
        here therefore fails toward import rather than toward export.

        Called once per cycle and stateful, so it must not be called twice for the
        same reading: the second call would re-anchor on the first call's output and
        let the clamp walk upward a step at a time.
        """
        if self.flags.get("noslew"):
            # Still track the anchor, so toggling the flag back on does not clamp
            # against a reference from minutes ago.
            self._slew_ref_w, self._slew_ref_ts = w, now
            self._slew_floor_ready = True
            return w

        dt = now - self._slew_ref_ts
        # dt of exactly zero is NOT a reset: two reads in the same instant means no
        # time has passed, so the rise allowance is the floor, not everything. Only a
        # negative dt (the clock moved backwards) or a real gap abandons the anchor,
        # because in both of those cases the anchor is not evidence about now.
        if self._slew_ref_w is None or dt < 0 or dt > SLEW_RESET_SEC:
            self._slew_ref_w, self._slew_ref_ts = w, now
            self._slew_floor_ready = True
            return w

        # A reading that is not above the anchor ends the rise episode, which is what
        # makes the floor a one-shot rather than a per-poll grant. Checked BEFORE the
        # ceiling so a fall can never be counted as a clamp.
        if w <= self._slew_ref_w:
            self._slew_ref_w, self._slew_ref_ts = w, now
            self._slew_floor_ready = True
            return w

        by_rate = SLEW_UP_W_PER_SEC * dt
        # max(), not sum: the floor is an alternative to the rate term, not a bonus on
        # top of it, so a long dt is governed by the rate alone and a short one by the
        # floor. Summing them would make every clamp SLEW_UP_MIN_W too generous.
        allowance = max(SLEW_UP_MIN_W, by_rate) if self._slew_floor_ready else by_rate
        # Spend the floor only when the rise actually needed it. A rise that fits
        # inside the rate term costs nothing, which is why steady noise never
        # exhausts the allowance and never earns a clamp on the cycle after.
        if self._slew_floor_ready and (w - self._slew_ref_w) > by_rate:
            self._slew_floor_ready = False

        ceiling = self._slew_ref_w + allowance
        if w > ceiling:
            held = w - ceiling
            self.slew_clamps += 1
            self.slew_held_w += held
            self.slew_worst_w = max(self.slew_worst_w, held)
            w = ceiling

        self._slew_ref_w, self._slew_ref_ts = w, now
        return w

    def _arm_kick(self, cloud_w, local_w, now=None):
        """Arm a counter-kick for an overwrite the cloud just made. See CLOUD_KICK.

        `cloud_w - local_w` is how much extra output the cloud's value just commanded,
        so it is the amount to cancel. Only a positive delta is armed: a cloud value
        below ours commands less output, which lands as import.
        """
        delta = cloud_w - local_w
        if not (delta > 0.0) or delta != delta:      # also rejects NaN local
            self.kick_skipped += 1
            return 0.0
        want = min(delta * CLOUD_KICK_GAIN, CLOUD_KICK_MAX_W)
        # A second overwrite while a kick is still running replaces it rather than
        # adding to it. Adding would let a burst of frames stack into an unbounded
        # pull-down, which is import we never chose. Note this can RAISE the standing
        # correction in one step, which is the safe direction, and can also lower it
        # mid-release; a lower replacement is a rise on the wire, so it is left to the
        # release ramp rather than applied here. See _kick_now.
        if want >= self._kick_w:
            self._kick_w = want
        self._kick_left = CLOUD_KICK_CYCLES
        self._kick_ts = time.time() if now is None else now
        self.kick_events += 1
        return want

    def _kick_now(self, now):
        """The correction to subtract on this cycle, bleeding off the release ramp.

        Called once per cycle that gets as far as computing a value, and stateful, so
        it must not be called twice. The hold phase (`_kick_left`) keeps it at full
        size; after that it decays at SLEW_UP_W_PER_SEC.

        The rate is shared with the slew limiter on purpose. The limiter governs the
        meter READING and the kick is applied after it, so nothing here is visible to
        `_slew_ref_w`; reusing the rate is what keeps the guarantee anyway, that the
        release cannot command a rise the limiter would have refused. Dropping the
        correction in one write instead measurably made export WORSE. See CLOUD_KICK.
        """
        if self._kick_w <= 0.0:
            return 0
        dt = now - self._kick_ts
        # Hold at full size while cycles are owed. A clock that went backwards is
        # treated as no elapsed time, which stalls the release rather than jumping it.
        if self._kick_left <= 0 and dt > 0.0:
            self._kick_w = max(0.0, self._kick_w - SLEW_UP_W_PER_SEC * dt)
        self._kick_ts = now
        return int(round(min(self._kick_w, CLOUD_KICK_MAX_W)))

    async def _read_http(self):
        """The original RPC path. Returns watts or None."""
        try:
            assert self._session is not None
            async with self._session.get(
                SHELLY_URL, timeout=aiohttp.ClientTimeout(total=HTTP_TIMEOUT_SEC)
            ) as r:
                if r.status != 200:
                    return None
                data = await r.json()
        except Exception:
            return None
        w = data.get("c_act_power")
        return None if w is None else float(w)

    async def _read_local(self):
        self.polls += 1
        self._src_change_ts = 0.0
        if self._modbus_wanted():
            w, src, change_ts = await self._read_meters()
            if w is None:
                # Fall back for THIS cycle only. A missed read means the last
                # injected value rides for another tick, and during a fading
                # surplus that stale value is exactly what lets real export
                # through. HTTP is slower, not wrong, so it is strictly better
                # than skipping. Compliance beats latency on the failure path.
                w = await self._read_http()
                if w is not None:
                    self.transport = "http-fallback"
            else:
                self.transport = "modbus:" + src
                self._src_change_ts = change_ts
        else:
            w = await self._read_http()
            if w is not None:
                self.transport = "http"

        if w is None:
            self.skipped_read_err += 1
            return None
        if abs(w) > MAX_ABS_W:
            self.skipped_read_err += 1
            log("EFINJECT implausible reading %.0fW, skipped", w)
            return None
        self.last_local = (time.time(), float(w))
        self._recent_local.append(float(w))
        if len(self._recent_local) > 240:
            del self._recent_local[:-240]
        self._score(float(w))
        return float(w)

    def _score(self, w):
        """Score this local sample against the current A/B mode."""
        if self.ab_mode is None:
            return
        d = self._ab.setdefault(
            self.ab_mode,
            {"n": 0, "abs_sum": 0.0, "sq_sum": 0.0, "worst": 0.0, "exc": 0},
        )
        d["n"] += 1
        d["sum"] = d.get("sum", 0.0) + w
        d["imp"] = d.get("imp", 0) + (1 if w > 0 else 0)
        d["abs_sum"] += abs(w)
        d["sq_sum"] += w * w
        d["worst"] = max(d["worst"], abs(w))
        if abs(w) > EXCURSION_W:
            d["exc"] += 1

    def _ab_report(self):
        parts = []
        for mode in ("INJ", "CLOUD"):
            d = self._ab.get(mode)
            if not d or not d["n"]:
                continue
            parts.append(
                "%s n=%d mean=%+.1fW avg|W|=%.1f rms=%.1f worst=%.0f "
                "import%%=%.1f exc>%dW=%.1f%%"
                % (
                    mode, d["n"], d.get("sum", 0.0) / d["n"], d["abs_sum"] / d["n"],
                    (d["sq_sum"] / d["n"]) ** 0.5, d["worst"],
                    100.0 * d.get("imp", 0) / d["n"],
                    EXCURSION_W, 100.0 * d["exc"] / d["n"],
                )
            )
        return " | ".join(parts) if parts else "no data"

    # -- causation test -----------------------------------------------------
    async def _caus_sample(self, secs, bucket):
        """Poll the LOCAL Shelly for `secs`, collecting (grid_w, batt_w)."""
        end = time.time() + secs
        while time.time() < end:
            w = await self._read_local()
            if w is not None:
                bucket.append((w, self.batt_w))
            await asyncio.sleep(0.5)

    async def _caus_pulse(self, device, pb, secs, offset, bucket):
        """Hold `truth + offset` on the wire for `secs`, sampling throughout."""
        end = time.time() + secs
        self._caus_offset = offset
        try:
            while time.time() < end:
                w = await self._read_local()
                if w is None:
                    await asyncio.sleep(CAUS_PULSE_PERIOD)
                    continue
                bucket.append((w, self.batt_w))
                if self.identity is None or self.identity[0] is not True:
                    await asyncio.sleep(CAUS_PULSE_PERIOD)
                    continue
                has_meter, model, sn, pa, pb_ = self.identity
                if sn != EXPECTED_SN:
                    await asyncio.sleep(CAUS_PULSE_PERIOD)
                    continue
                val = int(round(w + offset))
                if abs(val) > MAX_ABS_W:
                    await asyncio.sleep(CAUS_PULSE_PERIOD)
                    continue
                msg = pb.ConfigWrite(
                    cfg_cloud_metter=pb.CloudMeter(
                        has_meter=has_meter, model=model, sn=sn,
                        phase_a_power=pa, phase_b_power=pb_,
                        phase_c_power=val,
                    )
                )
                try:
                    async with self._send_lock:
                        await device._send_config_packet(msg)
                    self.sent += 1
                except Exception as e:
                    self.write_err += 1
                    log("EFINJECT caus write err: %s", e)
                await asyncio.sleep(CAUS_PULSE_PERIOD)
        finally:
            self._caus_offset = 0

    async def _causation(self, device, pb):
        """Paired REAL/SHAM pulses. Returns nothing; logs a verdict."""
        log(
            "EFINJECT CAUSATION arming: offset=%dW trials=%d pulse=%ss",
            CAUS_OFFSET_W, CAUS_TRIALS, CAUS_PULSE_SEC,
        )
        self.paused = True
        results = {"REAL": [], "SHAM": []}
        try:
            for i in range(CAUS_TRIALS):
                arm = "REAL" if i % 2 == 0 else "SHAM"

                pre = []
                await self._caus_sample(CAUS_PRE_SEC, pre)
                if not pre:
                    log("EFINJECT CAUSATION trial %d: no local reads, skip", i)
                    continue

                pre_grid = sum(p[0] for p in pre) / len(pre)
                bws = [p[1] for p in pre if p[1] is not None]
                pre_batt = sum(bws) / len(bws) if bws else None

                # Preconditions, re-checked per trial so a changing house does not
                # silently invalidate later trials.
                if abs(pre_grid) > CAUS_MAX_BASELINE_W:
                    log(
                        "EFINJECT CAUSATION trial %d %s SKIP: grid too busy (%.0fW)",
                        i, arm, pre_grid,
                    )
                    continue
                if pre_batt is None or pre_batt > -CAUS_MIN_DISCHARGE_W:
                    log(
                        "EFINJECT CAUSATION trial %d %s SKIP: not discharging "
                        "enough (batt=%s)", i, arm, pre_batt,
                    )
                    continue

                offset = CAUS_OFFSET_W if arm == "REAL" else 0
                pulse = []
                await self._caus_pulse(device, pb, CAUS_PULSE_SEC, offset, pulse)
                post = []
                await self._caus_sample(CAUS_POST_SEC, post)

                obs = pulse + post
                if not obs:
                    continue
                # A regulator told "you are exporting" cuts output, so the house
                # pulls from the grid: local grid goes MORE POSITIVE.
                peak = max(o[0] for o in obs) - pre_grid
                obw = [o[1] for o in obs if o[1] is not None]
                # Discharge shrinking = battery power rising toward zero.
                dbatt = (max(obw) - pre_batt) if (obw and pre_batt is not None) else 0.0
                results[arm].append((peak, dbatt))
                log(
                    "EFINJECT CAUSATION trial %d %s: pre_grid=%.0f pre_batt=%.0f "
                    "-> peak_grid_delta=%+.0fW batt_delta=%+.0fW",
                    i, arm, pre_grid, pre_batt, peak, dbatt,
                )

            def mean(a):
                return sum(a) / len(a) if a else float("nan")

            r_g = mean([r[0] for r in results["REAL"]])
            s_g = mean([r[0] for r in results["SHAM"]])
            r_b = mean([r[1] for r in results["REAL"]])
            s_b = mean([r[1] for r in results["SHAM"]])
            log(
                "EFINJECT CAUSATION RESULT n_real=%d n_sham=%d | grid REAL=%+.0fW "
                "SHAM=%+.0fW diff=%+.0fW | batt REAL=%+.0fW SHAM=%+.0fW diff=%+.0fW",
                len(results["REAL"]), len(results["SHAM"]),
                r_g, s_g, r_g - s_g, r_b, s_b, r_b - s_b,
            )
            if not results["REAL"] or not results["SHAM"]:
                log("EFINJECT CAUSATION VERDICT: INCONCLUSIVE (an arm has no trials)")
            elif (r_g - s_g) > CAUS_DECISION_W or (r_b - s_b) > CAUS_DECISION_W:
                log(
                    "EFINJECT CAUSATION VERDICT: REGULATOR ACTS on cfg_cloud_metter "
                    "(REAL exceeds SHAM by >%dW)", CAUS_DECISION_W
                )
            else:
                log(
                    "EFINJECT CAUSATION VERDICT: NO EFFECT -> field appears DISPLAY "
                    "ONLY for regulation (REAL indistinguishable from SHAM)"
                )
        except Exception:
            _LOGGER.exception("EFINJECT causation crashed")
        finally:
            self._caus_offset = 0
            self.paused = False
            log("EFINJECT CAUSATION done, normal injection resumes")

    # -- main loop ----------------------------------------------------------
    async def run(self):
        from custom_components.ef_ble.eflib.pb import bk_series_pb2 as pb

        device = self._device()
        if device is None:
            log("EFINJECT ABORT: no runtime device for %s", TARGET_TITLE)
            return

        self._session = async_get_clientsession(self.hass)
        self._attach(device)
        try:
            log("EFINJECT observing %ds before first write", BASELINE_SEC)
            await asyncio.sleep(BASELINE_SEC)

            # Wait for usable telemetry instead of aborting. The BLE link flaps, so
            # "no cloud_metter yet" is transient, not fatal; the old permanent ABORT
            # meant an HA restart was required before injection could ever resume.
            # Re-resolving via _ready_device() here is essential: if ef_ble swaps the
            # device object while we wait, a listener bound to the old object would
            # never deliver telemetry and we would wait forever.
            waited = 0.0
            last_wait_log = 0.0
            while True:
                if os.path.exists(STOP_FILE):
                    log("EFINJECT stop file present before first write -> not starting")
                    return
                dev_now = self._ready_device()
                ident = self.identity
                if ident is not None:
                    has_meter, model, sn, pa, pb_ = ident
                    # A wrong meter SN is a genuine hard stop, not a wait.
                    if sn and sn != EXPECTED_SN:
                        log(
                            "EFINJECT ABORT: identity mismatch sn=%r (want %s)",
                            sn, EXPECTED_SN,
                        )
                        return
                    if has_meter is True and sn == EXPECTED_SN:
                        break
                if waited - last_wait_log >= IDENTITY_LOG_GAP_SEC or waited == 0.0:
                    log(
                        "EFINJECT waiting for cloud_metter telemetry (%.0fs; "
                        "connected=%s identity=%r frames=%d swaps=%d) -> cloud in control",
                        waited, dev_now is not None, ident, self._frames,
                        self.device_swaps,
                    )
                    last_wait_log = waited
                await asyncio.sleep(IDENTITY_POLL_SEC)
                waited += IDENTITY_POLL_SEC

            # Fail closed on a dangerous bias rather than exporting. Checked on the
            # DEFAULT, so a broken constant can never start; the runtime override
            # goes through the identical rule on every read.
            if not bias_is_safe(BIAS_W):
                log(
                    "EFINJECT ABORT: unsafe BIAS_W=%s (must be <=0 and |BIAS_W|<=%d; "
                    "a positive bias commands ramp-up and can cause real export)",
                    BIAS_W, BIAS_MAX_ABS,
                )
                return

            bias_now = await self._refresh_bias()
            log(
                "EFINJECT START poll=%.2fs keepalive=%.2fs meter=%s model=%s bias=%+dW%s "
                "fade_to_zero_at=%+.0fW (expect real grid to settle near %+.0fW "
                "import; full bias only applies at or below 0W) rise_slew=%s "
                "cloud_kick=%s",
                POLL_PERIOD_SEC, KEEPALIVE_SEC, sn, model, bias_now,
                "" if bias_now == BIAS_W else " (override, default %+dW)" % BIAS_W,
                BIAS_FADE_W, bias_settle_w(bias_now),
                (
                    "DISABLED (%s present)" % NOSLEW_FILE
                    if self.flags.get("noslew")
                    else "%.0fW/s, min %.0fW once per rise"
                         % (SLEW_UP_W_PER_SEC, SLEW_UP_MIN_W)
                ),
                (
                    "DISABLED (%s present)" % NOKICK_FILE
                    if self.flags.get("nokick")
                    else "off (CLOUD_KICK=False)" if not CLOUD_KICK
                    else "gain %.2f, max %.0fW, hold %d writes, release %.0fW/s"
                         % (CLOUD_KICK_GAIN, CLOUD_KICK_MAX_W, CLOUD_KICK_CYCLES,
                            SLEW_UP_W_PER_SEC)
                ),
            )
            self.running = True
            self._t_start = time.time()
            self._deadline = self._t_start

            while self.running:
                if os.path.exists(STOP_FILE):
                    log("EFINJECT stop file present -> stopping, cloud resumes control")
                    break

                # Re-resolve the device every cycle. Never trust a cached reference:
                # ef_ble swaps the object on entry reload and the old one is dead.
                device = self._ready_device()
                if device is None:
                    await self._not_ready(self._not_ready_why or "link unusable")
                    continue

                if os.path.exists(CAUSATION_FILE):
                    try:
                        os.remove(CAUSATION_FILE)
                    except OSError:
                        pass
                    await self._causation(device, pb)
                    continue

                # A/B alternation. Blocks are wall-clock aligned so each mode sees
                # a comparable mix of household activity.
                if os.path.exists(AB_FILE):
                    blk = int(time.time() // AB_BLOCK_SEC)
                    mode = "INJ" if blk % 2 == 0 else "CLOUD"
                    if blk != self._ab_block:
                        self._ab_block = blk
                        if self.ab_mode is not None:
                            log("EFINJECT AB %s", self._ab_report())
                        log("EFINJECT AB block -> %s (%ds)", mode, AB_BLOCK_SEC)
                    self.ab_mode = mode
                elif self.ab_mode is not None:
                    self.ab_mode = None
                    log("EFINJECT AB disabled, resuming continuous injection")

                # Transport toggle, re-read every cycle so it can be flipped, and
                # rolled back, without a restart. Turning it off closes the socket
                # rather than leaving an idle connection on the meter.
                want_mb = self._modbus_wanted()
                if want_mb != self._mb_mode:
                    self._mb_mode = want_mb
                    if want_mb:
                        log(
                            "EFINJECT transport -> MODBUS %s:%d unit=%d fc=%d reg=%d "
                            "(float32, low word first); HTTP stays as per-cycle fallback",
                            MODBUS_HOST, MODBUS_PORT, MODBUS_UNIT,
                            MODBUS_FC_READ_INPUT, MODBUS_REG_C_ACT_POWER,
                        )
                    else:
                        log("EFINJECT transport -> HTTP %s", SHELLY_URL)
                    if not want_mb:
                        await self._modbus_close()

                # Second-meter mode, also live-togglable.
                want_sec = (
                    "interleave" if self._interleave_wanted()
                    else "shadow" if self._second_wanted()
                    else "off"
                )
                if want_sec != self._second_mode:
                    self._second_mode = want_sec
                    if want_sec == "interleave":
                        log(
                            "EFINJECT second meter -> INTERLEAVE %s: using whichever "
                            "of the two published last (offset measured 0.41 of the "
                            "1Hz period), guard trips at a sustained %+.0fW mean",
                            SECOND_HOST, INTERLEAVE_DIVERGE_W,
                        )
                    elif want_sec == "shadow":
                        log(
                            "EFINJECT second meter -> SHADOW %s: polling and "
                            "comparing only, injected value unchanged",
                            SECOND_HOST,
                        )
                    else:
                        log("EFINJECT second meter -> OFF")
                        await self._mb2.close()

                t_read0 = time.time()
                w = await self._read_local()
                now = time.time()
                read_ms = (now - t_read0) * 1000

                if w is None:
                    if self._verbose():
                        log("EFINJECT skip read_err (%.0fms)", read_ms)
                    await self._pace()
                    continue
                if self.last_local and now - self.last_local[0] > STALE_AFTER_SEC:
                    self.skipped_stale += 1
                    if self._verbose():
                        log("EFINJECT skip stale age=%.1fs", now - self.last_local[0])
                    await self._pace()
                    continue
                if self.identity is None or self.identity[0] is not True:
                    self.skipped_no_identity += 1
                    if self._verbose():
                        log("EFINJECT skip no_identity identity=%r", self.identity)
                    await self._pace()
                    continue

                # Re-pin identity from the LATEST telemetry every write.
                has_meter, model, sn, pa, pb_ = self.identity
                if sn != EXPECTED_SN:
                    self.skipped_no_identity += 1
                    if self._verbose():
                        log("EFINJECT skip sn mismatch %r", sn)
                    await self._pace()
                    continue

                # CLOUD block: keep reading (so both arms are scored from the same
                # sensor at the same rate) but send nothing, letting the cloud own
                # the field.
                if self.ab_mode == "CLOUD":
                    await self._pace()
                    continue

                # Do not chase a spike we cannot outrun. See SLEW_UP_W_PER_SEC.
                # Deliberately AFTER the CLOUD arm's `continue`, so the counters only
                # ever describe cycles that actually injected, which is what makes the
                # A/B comparison honest. Exactly one call per cycle: this is stateful.
                w_true = w
                w = self._slew_limit(w, now)

                # Poll/write decoupling. We poll at POLL_PERIOD_SEC to notice a new
                # meter value quickly, but there is nothing to gain from re-sending a
                # value the device already has, so only write when the reading actually
                # moved, when the keepalive is due (the cloud must still be contested
                # on a steady reading), or when a cloud overwrite is pending.
                changed = (
                    self._last_src_w is None
                    or abs(w - self._last_src_w) >= WRITE_ON_CHANGE_W
                )
                due = (now - self._last_send_ts) >= KEEPALIVE_SEC
                # A kick still owed is as good a reason to write as a new reading. Only
                # the FIRST kick cycle rides `_cloud_seen_ts`, which is cleared on that
                # write, so without this the rest of the pull-down would wait on the
                # keepalive and arrive up to KEEPALIVE_SEC after the ramp it corrects.
                # `_kick_w > 0` rather than `_kick_left > 0`, so the RELEASE keeps
                # writing too: on the keepalive alone the ramp would be sampled every
                # 0.5s and land as ~200W steps at SLEW_UP_W_PER_SEC, which is the kind
                # of step this whole change exists to remove. Writing every poll makes
                # them SLEW_UP_W_PER_SEC * POLL_PERIOD_SEC = ~100W instead, which is
                # exactly what the limiter already permits a real load rise, so a
                # released kick is indistinguishable from ordinary tracking.
                pending_cloud = self._cloud_seen_ts is not None or self._kick_w > 0.0
                if not (changed or due or pending_cloud):
                    self.polls_no_write += 1
                    await self._pace()
                    continue
                if changed:
                    self.writes_on_change += 1
                else:
                    self.writes_keepalive += 1

                # Import cushion, faded in by proximity to export. Gated on the LOCAL
                # grid reading, not on battery direction: see the note at BIAS_FADE_W
                # for why the old discharge-only gate disarmed itself during exactly
                # the PV-surplus window it was supposed to protect.
                bias = 0
                bias_w = await self._refresh_bias()
                if bias_w and bias_is_safe(bias_w):
                    if w >= BIAS_FADE_W:
                        factor = 0.0
                    elif w <= 0.0:
                        factor = 1.0
                    else:
                        factor = (BIAS_FADE_W - w) / BIAS_FADE_W
                    bias = int(round(bias_w * factor))
                    if bias:
                        self.biased += 1
                    else:
                        self.bias_idle += 1
                self.bias_applied = bias

                # Counter-kick for a cloud overwrite, on top of the cushion and after
                # the fade, because the fade is about proximity to export and this is
                # about undoing a ramp that has already been commanded. Subtracted, so
                # it can only ever reduce the output we ask for. Exactly one call per
                # cycle: this advances the release ramp. See CLOUD_KICK.
                kick = self._kick_now(now)

                val = int(round(w + bias - kick))
                if abs(val) > MAX_ABS_W:
                    self.skipped_read_err += 1
                    await self._pace()
                    continue

                meter = pb.CloudMeter(
                    has_meter=has_meter,
                    model=model,
                    sn=sn,
                    phase_a_power=pa,
                    phase_b_power=pb_,
                    phase_c_power=val,
                )
                msg = pb.ConfigWrite(cfg_cloud_metter=meter)

                try:
                    t_send0 = time.time()
                    async with self._send_lock:
                        lock_ms = (time.time() - t_send0) * 1000
                        await device._send_config_packet(msg)
                    send_ms = (time.time() - t_send0) * 1000
                    self.sent += 1
                    # A good write means the link is healthy again.
                    self._backoff = NOT_READY_BACKOFF_START
                    # Must be `val` (the biased value actually serialized), not `w`.
                    # Using `w` here makes the log misreport what went on the wire and
                    # breaks echo matching, since we never sent `w`.
                    self.last_sent_w = val
                    self._last_src_w = w
                    self._last_send_ts = time.time()
                    # How stale was the number we just injected, measured from when the
                    # meter published it. This is the metric the interleave is meant to
                    # improve, so record it rather than asserting the improvement.
                    if self._src_change_ts:
                        fresh_ms = (self._last_send_ts - self._src_change_ts) * 1000
                        self.fresh_ms_sum += fresh_ms
                        self.fresh_ms_n += 1
                        self.fresh_ms_last = fresh_ms
                    self._recent_sent.append((self._last_send_ts, self.last_sent_w))
                    # If this write was prompted by a detected cloud overwrite, record
                    # how long the cloud's value was actually exposed.
                    cst = getattr(self, "_cloud_seen_ts", None)
                    if cst is not None:
                        self.reasserts += 1
                        self.reassert_ms_sum += (self._last_send_ts - cst) * 1000
                        self._cloud_seen_ts = None
                    # Spend one cycle of the HOLD phase. Decremented HERE, on a
                    # confirmed write, not where the kick is computed: a cycle that
                    # never reached the device corrected nothing, and counting it
                    # would let the correction expire while the ramp was still up.
                    #
                    # `_kick_w` is deliberately NOT cleared when the count runs out.
                    # That is what the first version did, and zeroing it is a step up
                    # of the whole correction in one write, which is export. It is
                    # bled off by _kick_now instead.
                    if kick:
                        if self._kick_left > 0:
                            self._kick_left -= 1
                        self.kicks += 1
                        self.kick_w_sum += kick
                        self.kick_worst_w = max(self.kick_worst_w, float(kick))
                    if len(self._recent_sent) > 40:
                        del self._recent_sent[:-40]
                    if self._verbose():
                        dbg(
                            "EFINJECT send #%d true=%.0fW slewed=%.0fW bias=%+d "
                            "kick=-%d c=%dW batt=%s read=%.0fms ble=%.0fms",
                            self.sent, w_true, w, bias, kick, self.last_sent_w,
                            self.batt_w, read_ms, send_ms,
                        )
                    # Compact liveness at WARNING so the log stays readable but the
                    # loop is never silent. See HEARTBEAT_SEC.
                    if self._last_send_ts - self._last_hb >= HEARTBEAT_SEC:
                        self._last_hb = self._last_send_ts
                        log(
                            # `true` is the RAW meter reading. The value actually
                            # injected is `c`, which carries both the slew clamp and
                            # the bias, so the two differing is normal, not a fault.
                            "EFINJECT hb sent=%d true=%.0fW bias=%+d c=%dW batt=%s "
                            "frame_age=%.1fs err=%d not_ready=%d silent=%d swaps=%d "
                            "via=%s read=%.0fms stale=%.0fms",
                            self.sent, w_true, bias, self.last_sent_w, self.batt_w,
                            self._last_send_ts - self._last_frame_ts,
                            self.write_err, self.skipped_not_ready,
                            self.skipped_silent, self.device_swaps,
                            self.transport, read_ms,
                            (self._last_send_ts - self._src_change_ts) * 1000
                            if self._src_change_ts else float("nan"),
                        )
                except Exception as e:
                    self.write_err += 1
                    self._err_since_log += 1
                    now_e = time.time()
                    # Rate limited even when VERBOSE: an unusable link previously
                    # produced one WARNING per cycle, ~75k lines in a single session.
                    if (
                        self.write_err == 1
                        or now_e - self._last_err_log >= ERR_LOG_MIN_GAP_SEC
                    ):
                        log(
                            "EFINJECT write error #%d (%d since last log): %s",
                            self.write_err, self._err_since_log, e,
                        )
                        self._last_err_log = now_e
                        self._err_since_log = 0
                    # A failed write means the link is suspect. Back off; the next cycle
                    # re-resolves the device, so a swapped object heals itself.
                    await self._not_ready("write failed: %s" % (e,))
                    continue

                if self.sent and self.sent % 60 == 0:
                    lag = (
                        self.echo_lag_sum / self.echo_lag_n if self.echo_lag_n else float("nan")
                    )
                    tot = self.echo_ours + self.echo_cloud
                    # Rolling effect of the cushion, from LOCAL samples only.
                    ls = [
                        x for x in (
                            self._recent_local[-240:] if self._recent_local else []
                        )
                    ]
                    if ls:
                        eff = "mean=%+.1fW import%%=%.0f" % (
                            sum(ls) / len(ls),
                            100.0 * sum(1 for x in ls if x > 0) / len(ls),
                        )
                    else:
                        eff = "no local samples"
                    rms = (
                        self.reassert_ms_sum / self.reasserts
                        if self.reasserts
                        else float("nan")
                    )
                    log(
                        "EFINJECT SUMMARY sent=%d last=%sW bias=%+dW(settle%+.0fW) "
                        "fade=%.0fW "
                        "biased=%d bias_idle=%d stale=%d read_err=%d no_identity=%d "
                        "write_err=%d not_ready=%d silent=%d swaps=%d | LOCAL %s "
                        "| frames=%d ours=%.0f%% lag=%.2fs "
                        "| reasserts=%d avg_exposure=%.0fms "
                        "| via=%s mb_ok=%d mb_err=%d mb_reopens=%d "
                        "| polls=%d w_chg=%d w_keep=%d nowrite=%d rate=%.2f/s "
                        "stale_avg=%.0fms "
                        "| SLEW %s clamps=%d worst=%.0fW mean_held=%.0fW "
                        "| KICK %s events=%d sends=%d skipped=%d worst=%.0fW "
                        "mean=%.0fW "
                        "| SEC(%s) ok=%d err=%d used=%d d_now=%+.1fW/%d d_last=%+.1fW "
                        "d_absmax=%.0fW "
                        "| GUARD interleave=%s verdicts=%d busy=%d bad=%d good=%d/%d "
                        "locks=%d recov=%d rearms=%d",
                        self.sent, self.last_sent_w, self.bias_w,
                        bias_settle_w(self.bias_w), BIAS_FADE_W, self.biased,
                        self.bias_idle, self.skipped_stale,
                        self.skipped_read_err, self.skipped_no_identity,
                        self.write_err, self.skipped_not_ready, self.skipped_silent,
                        self.device_swaps,
                        eff, self._frames,
                        (100.0 * self.echo_ours / tot) if tot else 0.0, lag,
                        self.reasserts, rms,
                        self.transport, self._mb.reads, self._mb.err, self._mb.reopens,
                        self.polls, self.writes_on_change, self.writes_keepalive,
                        self.polls_no_write,
                        (
                            self.sent / (self._last_send_ts - self._t_start)
                            if self._t_start and self._last_send_ts > self._t_start
                            else float("nan")
                        ),
                        (
                            self.fresh_ms_sum / self.fresh_ms_n
                            if self.fresh_ms_n else float("nan")
                        ),
                        "OFF(noslew file)" if self.flags.get("noslew") else "ON",
                        self.slew_clamps, self.slew_worst_w,
                        (
                            self.slew_held_w / self.slew_clamps
                            if self.slew_clamps else 0.0
                        ),
                        (
                            "OFF(nokick file)" if self.flags.get("nokick")
                            else "OFF(const)" if not CLOUD_KICK else "ON"
                        ),
                        self.kick_events, self.kicks, self.kick_skipped,
                        self.kick_worst_w,
                        (self.kick_w_sum / self.kicks if self.kicks else 0.0),
                        self._second_mode, self._mb2.reads, self._mb2.err,
                        self.sec_used,
                        (
                            sum(self.sec_diffs) / len(self.sec_diffs)
                            if self.sec_diffs else float("nan")
                        ),
                        len(self.sec_diffs),
                        self.sec_diff_last, self.sec_diff_absmax,
                        self._interleave_state(), self.sec_verdicts,
                        self.sec_skipped_busy, self.sec_bad_run, self.sec_good_run,
                        self._recover_strikes_needed(),
                        self.interleave_locks, self.interleave_recoveries,
                        self.interleave_rearms,
                    )

                await self._pace()
        except asyncio.CancelledError:
            raise
        except Exception:
            _LOGGER.exception("EFINJECT crashed")
        finally:
            self.running = False
            self._detach()
            # session is HA's shared one; do not close it. The Modbus socket is
            # ours, so it does get closed.
            await self._modbus_close()
            log(
                "EFINJECT STOPPED sent=%d polls=%d stale=%d read_err=%d write_err=%d "
                "not_ready=%d silent=%d swaps=%d mb_ok=%d mb_err=%d mb_reopens=%d "
                "sec_ok=%d sec_err=%d sec_used=%d interleave=%s "
                "verdicts=%d locks=%d recov=%d rearms=%d",
                self.sent, self.polls, self.skipped_stale, self.skipped_read_err,
                self.write_err,
                self.skipped_not_ready, self.skipped_silent, self.device_swaps,
                self._mb.reads, self._mb.err, self._mb.reopens,
                self._mb2.reads, self._mb2.err, self.sec_used,
                self._interleave_state(), self.sec_verdicts,
                self.interleave_locks, self.interleave_recoveries,
                self.interleave_rearms,
            )


async def async_setup(hass: HomeAssistant, config: dict) -> bool:
    inj = Injector(hass)
    hass.data[DOMAIN] = inj

    async def _status_updater(_now=None):
        # Refresh the knob-file view once, in the executor, and hand it to the entity
        # layer. Deliberately failure-tolerant: a stat error must not be able to stop
        # the status tick, which is the only thing keeping sensor.ef_inject_status alive.
        try:
            inj.flags = await hass.async_add_executor_job(stat_flags)
        except Exception:
            _LOGGER.exception("EFINJECT could not stat the knob files")

        hass.states.async_set(
            "sensor.ef_inject_status",
            "running" if inj.running else "stopped",
            {
                "friendly_name": "EF Inject status",
                "sent": inj.sent,
                "last_sent_w": inj.last_sent_w,
                "skipped_stale": inj.skipped_stale,
                "skipped_read_err": inj.skipped_read_err,
                "skipped_no_identity": inj.skipped_no_identity,
                "write_err": inj.write_err,
                "skipped_not_ready": inj.skipped_not_ready,
                "skipped_silent": inj.skipped_silent,
                "not_ready_why": inj._not_ready_why,
                "device_swaps": inj.device_swaps,
                "biased": inj.biased,
                "bias_idle": inj.bias_idle,
                "bias_w": inj.bias_w,
                "bias_w_default": BIAS_W,
                "bias_override_active": inj.bias_w != BIAS_W,
                "bias_overrides": inj.bias_overrides,
                "bias_rejected": inj.bias_rejected,
                "bias_fade_w": BIAS_FADE_W,
                "bias_settle_w": round(bias_settle_w(inj.bias_w), 1),
                "frame_age_s": (
                    round(time.time() - inj._last_frame_ts, 1)
                    if inj._last_frame_ts
                    else None
                ),
                "frames": inj._frames,
                "echo_ours": inj.echo_ours,
                "echo_cloud": inj.echo_cloud,
                "avg_echo_lag_s": (
                    round(inj.echo_lag_sum / inj.echo_lag_n, 2)
                    if inj.echo_lag_n
                    else None
                ),
                "target": TARGET_TITLE,
                "meter_sn": EXPECTED_SN,
                "transport": inj.transport,
                "modbus_enabled": inj.flags.get("modbus", False),
                "mb_reads": inj._mb.reads,
                "mb_err": inj._mb.err,
                "mb_reopens": inj._mb.reopens,
                "poll_period_sec": POLL_PERIOD_SEC,
                "keepalive_sec": KEEPALIVE_SEC,
                "polls": inj.polls,
                "writes_on_change": inj.writes_on_change,
                "writes_keepalive": inj.writes_keepalive,
                "polls_no_write": inj.polls_no_write,
                "slew_enabled": not inj.flags.get("noslew"),
                "slew_clamps": inj.slew_clamps,
                "slew_worst_w": round(inj.slew_worst_w, 1),
                "slew_mean_held_w": (
                    round(inj.slew_held_w / inj.slew_clamps, 1)
                    if inj.slew_clamps else 0.0
                ),
                "kick_enabled": CLOUD_KICK and not inj.flags.get("nokick"),
                "kick_events": inj.kick_events,
                "kick_sends": inj.kicks,
                "kick_skipped": inj.kick_skipped,
                "kick_worst_w": round(inj.kick_worst_w, 1),
                "kick_mean_w": (
                    round(inj.kick_w_sum / inj.kicks, 1) if inj.kicks else 0.0
                ),
                "send_rate_hz": (
                    round(inj.sent / (time.time() - inj._t_start), 2)
                    if inj._t_start else None
                ),
                "avg_injected_staleness_ms": (
                    round(inj.fresh_ms_sum / inj.fresh_ms_n)
                    if inj.fresh_ms_n else None
                ),
                "second_meter_mode": inj._second_mode,
                "second_host": SECOND_HOST,
                "sec_reads": inj._mb2.reads,
                "sec_err": inj._mb2.err,
                "sec_used": inj.sec_used,
                "sec_diff_mean_w": (
                    round(sum(inj.sec_diffs) / len(inj.sec_diffs), 1)
                    if inj.sec_diffs else None
                ),
                "sec_diff_absmax_w": round(inj.sec_diff_absmax, 1),
                "sec_diff_last_verdict_w": (
                    None if inj.sec_diff_last != inj.sec_diff_last
                    else round(inj.sec_diff_last, 1)
                ),
                "interleave_state": inj._interleave_state(),
                "interleave_locked": inj.interleave_locked,
                "interleave_locks": inj.interleave_locks,
                "interleave_recoveries": inj.interleave_recoveries,
                "interleave_rearms": inj.interleave_rearms,
                "guard_verdicts": inj.sec_verdicts,
                "guard_skipped_busy": inj.sec_skipped_busy,
                "guard_bad_run": inj.sec_bad_run,
                "guard_good_run": inj.sec_good_run,
                # how far recovery is, and how much the escalation has raised the bar
                "guard_good_run_needed": inj._recover_strikes_needed(),
                "guard_recoveries_recent": inj._recent_recoveries(),
            },
        )
        # Wake the entities on the same 5s cadence. After async_set, so an entity that
        # reads inj.flags sees the same view the attributes were built from.
        async_dispatcher_send(hass, SIGNAL_UPDATE)

    async def _boot():
        # let ef_ble connect and start streaming first
        await asyncio.sleep(45)
        hass.async_create_background_task(inj.run(), "ef_inject_loop")
        while True:
            await _status_updater()
            await asyncio.sleep(5)

    hass.async_create_background_task(_boot(), "ef_inject_boot")

    # Ask for the UI entry. It carries NOTHING but entities: the regulator above is
    # already running by this point and never consults it, so a failed, deleted or
    # broken entry cannot affect regulation. Single-instance via the unique_id in
    # config_flow, so restarts do not accumulate entries.
    hass.async_create_task(
        hass.config_entries.flow.async_init(DOMAIN, context={"source": SOURCE_IMPORT})
    )
    return True


async def async_setup_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Set up the entity layer. Regulation does not pass through here."""
    if hass.data.get(DOMAIN) is None:
        # async_setup owns the Injector and runs first, so this is a genuine anomaly
        # rather than an ordering race. Retry rather than fail: the knobs are worth
        # having even if the YAML block was added late.
        raise ConfigEntryNotReady("ef_inject YAML setup has not created the Injector")

    ble_entry = find_ef_ble_entry(hass)
    address = getattr(ble_entry, "unique_id", None) or TARGET_ADDRESS

    # The knobs belong on the Ultra's own device page, next to the controls they
    # override. Getting there needs care on HA 2026.8: a device belongs to exactly ONE
    # config entry (DeviceEntry.config_entry_id) and identifiers are unique PER ENTRY,
    # so declaring ef_ble's identifiers in DeviceInfo does not join their device, it
    # forges a nameless second one. Observed doing exactly that on 2026.8.3.
    #
    # The supported route is one level down: an entity may point at ANY device row, so
    # the entities carry no device_info at all and pre-set `device_entry` instead
    # (entity_platform honours a pre-set device_entry when device_info is None; see
    # components/template/entity.py for the same pattern). We therefore own no device.
    dev_reg = dr.async_get(hass)
    ble_device = dev_reg.async_get_device(identifiers={("ef_ble", address)})
    if ble_device is None:
        # ef_ble registers the device once the Ultra connects. HA's own retry backoff
        # covers the ~45s BLE boot window, so no polling loop is needed here.
        raise ConfigEntryNotReady(
            f"ef_ble device {address} is not in the device registry yet"
        )

    entry.runtime_data = EfInjectRuntime(address=address, device_entry=ble_device)
    await hass.config_entries.async_forward_entry_setups(entry, PLATFORMS)

    # Self-heal: this entry must own NO devices. Earlier builds owned one (first a
    # nameless clone of ef_ble's identifiers, then a "Zero-export regulator" service
    # device), and the forward above has just moved every entity off it, so whatever is
    # left here is empty. Safe by construction rather than by pattern matching: the list
    # is scoped to THIS entry, and since config_entry_id is singular the Ultra's own
    # device belongs to ef_ble's entry and can never appear in it.
    for device in dr.async_entries_for_config_entry(dev_reg, entry.entry_id):
        log(
            "EFINJECT removing leftover device %s (%s) from an earlier layout; the "
            "knobs now live on the Ultra's own device %s",
            device.id, device.name or "unnamed", ble_device.id,
        )
        dev_reg.async_remove_device(device.id)
    return True


async def async_unload_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Unload the entity layer. The regulator keeps running; it is not ours to stop."""
    return await hass.config_entries.async_unload_platforms(entry, PLATFORMS)
