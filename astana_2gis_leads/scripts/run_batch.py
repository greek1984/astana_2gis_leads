from astana_2gis_leads.selenium_helpers import pick_scroll_root, first_firm_id, click_page
from astana_2gis_leads.two_gis_lead_collector import TwoGisLeadCollector

from pathlib import Path
from urllib.parse import quote
import time, random

import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait


collector = TwoGisLeadCollector("интернет магазин", "cache")

# ===== CONFIG =====
CITY_SLUG = "astana"
QUERIES_PATH = Path("queries.txt")

BASE_DIR = Path(__file__).resolve().parents[1]   # если run_batch.py лежит в scripts/
OUT_DIR = BASE_DIR / "data"
OUT_DIR.mkdir(exist_ok=True)


START_LINE = 1
END_LINE = 1          # включительно
MAX_PAGES = 13
MAX_ENRICH = None     # None = без лимита

# ===== helpers =====
def build_query_url(city_slug: str, query: str) -> str:
    return f"https://2gis.kz/{city_slug}/search/{quote(query.strip())}"

def read_queries(path: Path) -> list[str]:
    lines = path.read_text(encoding="utf-8").splitlines()
    return [ln.strip() for ln in lines if ln.strip() and not ln.strip().startswith("#")]

def norm_addr(s: str) -> str:
    return " ".join((s or "").lower().replace("\xa0", " ").split())

# ===== REQUIRED: you already have these somewhere =====
# pick_scroll_root(driver) -> sets window.__twogis_scroll_root
# first_firm_id(driver) -> reads first firm_id from root
# click_page(driver, n) -> clicks pagination

def run_one_query(driver, collector, query: str, max_pages: int, max_enrich: int | None, run_id: str, run_date: str) -> pd.DataFrame:
    query_url = build_query_url(CITY_SLUG, query)
    print(f"\n=== QUERY: {query} ===")
    print("URL:", query_url)

    driver.get(query_url)
    time.sleep(2.5)

    # важное: сброс root между запросами
    driver.execute_script("window.__twogis_scroll_root = null;")
    print("pick:", pick_scroll_root(driver))

    rows: list[dict] = []
    seen_addr: set[str] = set()
    enriched_count = 0
    page = 1

    while True:
        cards = collector.collect_cards_from_root(driver)  # list[dict]
        time.sleep(random.uniform(0.25, 0.6))

        # 1) добавить новые строки (дедуп по адресу)
        for c in cards:
            addr_key = norm_addr(c.get("address", ""))
            if not addr_key:
                continue
            if addr_key in seen_addr:
                continue
            seen_addr.add(addr_key)
            c["query"] = query  # обязательная метка источника
            rows.append(c)

        print(f"TOTAL | query={query} | page={page} | cards={len(cards)} | rows_total={len(rows)} | seen_addr={len(seen_addr)}")

        # 2) enrichment только новых строк
        for row in rows[enriched_count:]:
            if max_enrich is not None and enriched_count >= max_enrich:
                break

            row["primary_contact"], row["website"] = collector.get_primary_contact(driver, row["firm_id"])

            pc = (row.get("primary_contact") or "").lower()
            if ("whatsapp" in pc) or ("wa.me" in pc):
                row["primary_type"] = "wa"
            elif ("t.me" in pc) or pc.startswith("tg://"):
                row["primary_type"] = "tg"
            elif "instagram.com" in pc:
                row["primary_type"] = "ig"
            elif "facebook.com" in pc:
                row["primary_type"] = "fb"
            elif "vk.com" in pc:
                row["primary_type"] = "vk"
            else:
                row["primary_type"] = "none"

            enriched_count += 1

        # 3) условия остановки по страницам
        if page >= max_pages:
            break

        next_page = page + 1
        if not driver.find_elements(By.XPATH, f"//a[normalize-space()='{next_page}']"):
            break

        old_first = first_firm_id(driver)
        click_page(driver, next_page)

        WebDriverWait(driver, 12).until(lambda d: (fid := first_firm_id(d)) and fid != old_first)

        page += 1
        time.sleep(random.uniform(0.8, 1.6))

    return pd.DataFrame(rows)

def main():
    queries = read_queries(QUERIES_PATH)
    selected = queries[START_LINE - 1: END_LINE] if False else queries[START_LINE - 1: END_LINE + 1]  # END включительно
    print("Selected queries:", selected)

    options = Options()
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--start-maximized")

    driver = webdriver.Chrome(options=options)

    try:
        all_frames = []

        run_id = time.strftime("%Y-%m-%d_%H-%M")
        run_date = time.strftime("%Y-%m-%d")

        for q in selected:
            df = run_one_query(driver, collector, q, MAX_PAGES, MAX_ENRICH, run_id, run_date)

            safe_name = "".join(ch if ch.isalnum() else "_" for ch in q)[:60]
            out_path = OUT_DIR / f"firms_{safe_name}.xlsx"

            # --- служебные ---
            df["run_id"] = run_id
            df["run_date"] = run_date
            df["city"] = CITY_SLUG

            # --- CRM (пустые) ---
            for col in ["outreach_status", "outreach_date", "channel_used", "note"]:
                if col not in df.columns:
                    df[col] = ""

            df.to_excel(out_path, index=False)
            print("Saved:", out_path, "rows:", len(df))

            all_frames.append(df)

        master = pd.concat(all_frames, ignore_index=True) if all_frames else pd.DataFrame()
        master_path = OUT_DIR / f"firms_master_{START_LINE}_{END_LINE}_{run_id}.xlsx"
        master.to_excel(master_path, index=False)
        print("MASTER Saved:", master_path, "rows:", len(master))

    finally:
        driver.quit()

if __name__ == "__main__":
    # selected = queries[START_LINE - 1: END_LINE + 1]  # END включительно

    main()
