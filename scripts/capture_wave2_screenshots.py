"""Capture full-page screenshots of the Wave 2 surfaces.

Run with the Streamlit app already serving on http://localhost:8521.
Saves PNGs into assets/comparison/wave2/.
"""
from __future__ import annotations

import sys
import time
from pathlib import Path

from selenium import webdriver
from selenium.webdriver.chrome.options import Options


def _full_page_screenshot(driver, target: Path) -> None:
    # Force render then resize window to total scroll height for full-page capture.
    time.sleep(7)
    driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
    time.sleep(2)
    driver.execute_script("window.scrollTo(0, 0);")
    time.sleep(2)
    total_height = driver.execute_script(
        "return Math.max(document.body.scrollHeight, document.documentElement.scrollHeight);"
    )
    driver.set_window_size(1440, max(total_height + 80, 900))
    time.sleep(2)
    driver.execute_script("window.scrollTo(0, 0);")
    time.sleep(1)
    driver.save_screenshot(str(target))
    print(f"saved {target} ({target.stat().st_size} bytes)")


def _click_tab(driver, label: str) -> None:
    driver.execute_script(
        """
        const want = arguments[0];
        const tabs = Array.from(document.querySelectorAll('[role="tab"]'));
        const tab = tabs.find(t => (t.innerText || '').trim() === want);
        if (tab) { tab.click(); }
        """,
        label,
    )
    time.sleep(4)


def main() -> int:
    out_dir = Path(__file__).resolve().parents[1] / "assets" / "comparison" / "wave2"
    out_dir.mkdir(parents=True, exist_ok=True)

    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--window-size=1440,900")
    options.add_argument("--hide-scrollbars")

    driver = webdriver.Chrome(options=options)
    try:
        # Home
        driver.get("http://localhost:8521/")
        _full_page_screenshot(driver, out_dir / "home_after.png")

        # Batter Stats — overview (default tab)
        driver.set_window_size(1440, 900)
        driver.get("http://localhost:8521/render_batter_stats_page")
        _full_page_screenshot(driver, out_dir / "batter_stats_after.png")

        # Batter Stats — Longest HRs tab (leader headshot)
        driver.set_window_size(1440, 900)
        driver.get("http://localhost:8521/render_batter_stats_page")
        time.sleep(7)
        _click_tab(driver, "Longest HRs")
        _full_page_screenshot(driver, out_dir / "batter_stats_longest_hrs_after.png")

        # Batter Stats — Power tab (EV/LA scatter)
        driver.set_window_size(1440, 900)
        driver.get("http://localhost:8521/render_batter_stats_page")
        time.sleep(7)
        _click_tab(driver, "Power")
        _full_page_screenshot(driver, out_dir / "batter_stats_power_after.png")

        # Batter Stats — Profiles tab (player headshot)
        driver.set_window_size(1440, 900)
        driver.get("http://localhost:8521/render_batter_stats_page")
        time.sleep(7)
        _click_tab(driver, "Profiles")
        _full_page_screenshot(driver, out_dir / "batter_stats_profiles_after.png")

        # Pitcher Stats
        driver.set_window_size(1440, 900)
        driver.get("http://localhost:8521/render_pitcher_stats_page")
        _full_page_screenshot(driver, out_dir / "pitcher_stats_after.png")
    finally:
        driver.quit()
    return 0


if __name__ == "__main__":
    sys.exit(main())
