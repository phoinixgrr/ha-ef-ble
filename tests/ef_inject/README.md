# ef_inject test suite

Offline tests for `custom_components/ef_inject`, the zero-export regulator. They
stub Home Assistant entirely, so no HA install, no BLE, and no network are
needed, and nothing here touches the live box.

## Running

Use **Python 3.13**, which is what Home Assistant runs. macOS system Python is
too old and `test_bias_liveness` dies with `There is no current event loop in
thread 'MainThread'` under it, which is an interpreter artifact and not a real
failure.

```bash
cd tests/ef_inject
V=../../.venv-merge/bin/python
for t in test_interleave test_bias_liveness test_heal test_modbus; do
    printf "%-22s " "$t"; $V $t.py 2>&1 | tail -1
done
```

Expected as of 2026-08-25: all four green, 229 `PASS` lines total
(interleave 124, bias_liveness 48, heal 22, modbus 35).

Each suite exits non-zero on failure, so they are usable as a gate. Note that
`test_heal` and `test_bias_liveness` print `ALL 0 CHECKS PASSED` on success. The
count is a cosmetic bug in their own summary line, not an empty run; grep for
`^PASS` if you want the real number.

## `mod_under_test.py`

The suites load the integration by file path:

```python
spec = importlib.util.spec_from_file_location("efi", "mod_under_test.py")
```

so `mod_under_test.py` is a **copy** of `custom_components/ef_inject/__init__.py`,
not a symlink. That is deliberate: the workflow for proving a fix is to run the
same suite against the old and the new module by swapping this one file, which a
symlink would prevent. It is how the 2026-08-25 quiet-gate fix was validated
(the espresso regression test yields 8 verdicts and locks the guard against the
old module, 0 verdicts and no lock against the new one).

The cost is that it can drift. Refresh and confirm before trusting a run:

```bash
cp ../../custom_components/ef_inject/__init__.py mod_under_test.py
md5 -q mod_under_test.py ../../custom_components/ef_inject/__init__.py
```

Both were `8da062b2f159ea7806086f3e03b4dd8a` when this was committed, matching
the deployed file on `homeassistant.home`.

## What is a test and what is a probe

| File | Kind | Notes |
|---|---|---|
| `harness.py` | support | HA stubs, fake device, fake clock |
| `test_interleave.py` | test | two-meter divergence guard, quiescence gate, escalating hysteresis |
| `test_bias_liveness.py` | test | bias fade, the settle-point identity, runtime override validation |
| `test_heal.py` | test | device re-resolution across entry reload, backoff, poll pacing |
| `test_modbus.py` | test | register decode and transport fallback |
| `live_check.py` | probe | reads the running loop's state. Talks to the box. |
| `meter_probe.py` | probe | samples both Shelly meters. Talks to the network. |
| `quiet_probe.py` | probe | measures how often the quiescence gate opens. Talks to the network. |

The three probes are diagnostic tools, not part of the suite. They reach out to
real hardware, so do not wire them into a gate.
