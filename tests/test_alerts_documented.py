"""The README must document every alerting setting, and only real ones.

Configuration you cannot find is configuration that does not exist. Alerting is
the subsystem you reach for while something is already broken, which is the
worst moment to be reading source to work out what a variable is called - so
this is asserted rather than left to discipline.

Both directions matter. An undocumented variable is unfindable; a documented one
that no longer exists sends someone to set something with no effect, which is
worse than silence because it looks like it worked.
"""

import os
import re
import sys

sys.path.insert(0, os.path.join(sys.path[0], ".."))

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PATTERN = re.compile(r"OPTERYX_ALERTS_[A-Z_]+")


def _in_code():
    names = set()
    alerts_dir = os.path.join(REPO_ROOT, "opteryx_catalog", "alerts")
    for filename in os.listdir(alerts_dir):
        if not filename.endswith(".py"):
            continue
        with open(os.path.join(alerts_dir, filename), "r", encoding="utf-8") as handle:
            source = handle.read()
        # The module docstring lists them too; both should agree with the README,
        # so scanning the whole file is right.
        names.update(PATTERN.findall(source))
    return names


def _in_readme():
    with open(os.path.join(REPO_ROOT, "README.md"), "r", encoding="utf-8") as handle:
        return set(PATTERN.findall(handle.read()))


def test_every_setting_is_documented():
    undocumented = _in_code() - _in_readme()
    assert not undocumented, f"add to README.md: {sorted(undocumented)}"


def test_the_readme_documents_no_settings_that_do_not_exist():
    phantom = _in_readme() - _in_code()
    assert not phantom, f"README.md documents settings nothing reads: {sorted(phantom)}"


def test_the_delivery_verification_script_exists():
    """The README tells people to run this when checking a real channel."""
    assert os.path.exists(os.path.join(REPO_ROOT, "scripts", "send_test_alert.py"))


def test_the_alerts_extra_is_declared():
    """The README tells people to `pip install "opteryx-catalog[alerts]"`."""
    with open(os.path.join(REPO_ROOT, "pyproject.toml"), "r", encoding="utf-8") as handle:
        content = handle.read()
    assert "alerts = [" in content
    assert "google-cloud-secret-manager" in content


if __name__ == "__main__":  # pragma: no cover
    import pytest

    pytest.main([__file__, "-v"])
