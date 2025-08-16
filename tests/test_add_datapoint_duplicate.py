import importlib.util
from pathlib import Path
import requests

# Load module like other tests
spec = importlib.util.spec_from_file_location(
    "wake_focus_sync", Path(__file__).resolve().parents[1] / "scripts" / "wake_focus_sync.py"
)
wf = importlib.util.module_from_spec(spec)
spec.loader.exec_module(wf)


def test_add_datapoint_duplicate(monkeypatch):
    class DummyResponse:
        status_code = 422
        text = '{"errors":"Duplicate request"}'

        def json(self):
            return {"errors": "Duplicate request"}

        def raise_for_status(self):
            raise requests.HTTPError(response=self)

    def fake_post(url, data, timeout):
        return DummyResponse()

    monkeypatch.setattr(requests, "post", fake_post)
    wf.DRY_RUN = False
    wf.AUTH_TOKEN = "token"
    wf.USERNAME = "user"

    # Should not raise despite duplicate response
    result = wf.add_datapoint("goal", 1, "c", daystamp="20250101", requestid="r1")
    assert result == {"errors": "Duplicate request"}
