from __future__ import annotations

import configparser
import xml.etree.ElementTree as ET
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DESKTOP_FILE = ROOT / "packaging" / "linux" / "ALBIS.desktop"
METAINFO_FILE = ROOT / "packaging" / "linux" / "ALBIS.metainfo.xml"


def test_appimage_desktop_template_has_expected_fields() -> None:
    parser = configparser.ConfigParser(interpolation=None)
    parser.read_string(DESKTOP_FILE.read_text(encoding="utf-8"))
    entry = parser["Desktop Entry"]

    assert entry["Type"] == "Application"
    assert entry["Name"] == "ALBIS"
    assert entry["Exec"] == "ALBIS"
    assert entry["Icon"] == "ALBIS"
    assert "Science;" in entry["Categories"]


def test_appimage_metainfo_template_is_well_formed() -> None:
    component = ET.parse(METAINFO_FILE).getroot()

    assert component.tag == "component"
    assert component.attrib["type"] == "desktop-application"
    assert component.findtext("id") == "ALBIS"
    assert component.findtext("name") == "ALBIS"
    assert component.findtext("launchable") == "ALBIS.desktop"

    urls = {url.attrib["type"]: (url.text or "").strip() for url in component.findall("url")}
    assert urls["homepage"] == "https://github.com/SaschaAndresGrimm/ALBIS"
    assert urls["bugtracker"] == "https://github.com/SaschaAndresGrimm/ALBIS/issues"

    screenshot = component.find("./screenshots/screenshot/image")
    assert screenshot is not None
    assert (screenshot.text or "").startswith("https://")
