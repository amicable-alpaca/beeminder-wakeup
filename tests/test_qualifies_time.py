import importlib.util
from pathlib import Path

# Load the wake_focus_sync module directly from its file path
spec = importlib.util.spec_from_file_location(
    "wake_focus_sync", Path(__file__).resolve().parents[1] / "scripts" / "wake_focus_sync.py"
)
wf = importlib.util.module_from_spec(spec)
spec.loader.exec_module(wf)


def test_qualifies_time_boundaries():
    assert not wf.qualifies_time(5, 59)
    assert wf.qualifies_time(6, 0)
    assert wf.qualifies_time(9, 15)
    assert not wf.qualifies_time(9, 16)
