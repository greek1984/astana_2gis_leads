import time
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
import random
import pandas as pd
from pathlib import Path
from astana_2gis_leads.two_gis_lead_collector import TwoGisLeadCollector
from astana_2gis_leads.selenium_helpers import (
    pick_scroll_root,
    first_firm_id,
    click_page,
)

QUERY_URL = "https://2gis.kz/astana/search/%D0%B8%D0%BD%D1%82%D0%B5%D1%80%D0%BD%D0%B5%D1%82%20%D0%BC%D0%B0%D0%B3%D0%B0%D0%B7%D0%B8%D0%BD?m=71.432495%2C51.156985%2F15.72"

MAX_PAGES = 3
CITY_SLUG = "astana"

# def pick_scroll_root(driver):
#     return driver.execute_script("""
#     return (function () {
#       const els = Array.from(document.querySelectorAll('div._jdkjbol'));
#       if (!els.length) return { status: "NO_MATCH" };
#
#       const scored = els.map(el => {
#         const r = el.getBoundingClientRect();
#         return { el, w: r.width, h: r.height, max: el.scrollHeight - el.clientHeight };
#       });
#
#       scored.sort((a, b) => (b.w - a.w) || (b.max - a.max));
#       window.__twogis_scroll_root = scored[0].el;
#
#       return { status: "CACHED", best: { width: scored[0].w, max: scored[0].max }, count: scored.length };
#     })();
#     """)

# def first_firm_id(driver):
#     return driver.execute_script(r"""
#     function pickRoot(){
#         // 2GIS часто держит несколько div._jdkjbol — берём самый "широкий"
#         const nodes = Array.from(document.querySelectorAll("div._jdkjbol"));
#         if (!nodes.length) return null;
#
#         let best = null;
#         let bestW = -1;
#         for (const el of nodes) {
#             const w = el.getBoundingClientRect().width || 0;
#             if (w > bestW) { bestW = w; best = el; }
#         }
#         return best;
#     }
#
#     // берём кэш, но если он протух — переподбираем
#     let root = window.__twogis_scroll_root;
#     if (!root || !root.isConnected) {
#         root = pickRoot();
#         window.__twogis_scroll_root = root;
#     }
#     if (!root) return null;
#
#     const a = root.querySelector('a[href*="/firm/"]');
#     if (!a) return null;
#
#     const h = a.getAttribute("href") || "";
#     const m = h.match(/\/firm\/(\d+)/);
#     return m ? m[1] : null;
#     """)

collector = TwoGisLeadCollector("интернет магазин", "cache")

# def click_page(driver, n: int):
#     el = driver.find_element(By.XPATH, f"//a[normalize-space()='{n}']")
#     driver.execute_script("arguments[0].scrollIntoView({block:'center'});", el)
#     driver.execute_script("arguments[0].click();", el)
#     # ВАЖНО: сбрасываем кэш root, потому что выдача перерисуется
#     driver.execute_script("window.__twogis_scroll_root = null;")

options = Options()
options.add_argument("--disable-blink-features=AutomationControlled")
options.add_argument("--start-maximized")
driver = webdriver.Chrome(options=options)

driver.get(QUERY_URL)
time.sleep(3)

print("pick:", pick_scroll_root(driver))

rows = []
seen_addr = set()  # ключ дедупа: address_norm

page = 1

# enriched = False # Ставим флаг, что обогащение пока что не произошло
enriched_count = 0
# Цикл по страницам выдачи в интерфейсе 2Gis
while True:
    cards = collector.collect_cards_from_root(driver) # list[dict]
    time.sleep(random.uniform(0.3, 0.7))

    # стоп по лимиту страниц (для теста — 2)
    if page >= MAX_PAGES:
        break
    print(f"page {page}: {len(cards)}; seen: {len(seen_addr)}")
    # стоп если нет следующей страницы
    next_page = page + 1

    # если ссылки на next_page нет — стоп
    if not driver.find_elements(By.XPATH, f"//a[normalize-space()='{next_page}']"):
        break

    old_first = first_firm_id(driver)

    print("FIRST BEFORE", page, first_firm_id(driver))
    click_page(driver, next_page)

    # 1) ждём, что реально поменялась первая фирма в выдаче
    WebDriverWait(driver, 12).until(lambda d: (fid := first_firm_id(d)) and fid != old_first)

    page += 1
    print(
        f"AFTER PAGE SWITCH | now page={page} | first_firm_id={first_firm_id(driver)} | cards={len(collector.collect_cards_from_root(driver))} | rows_total={len(rows)}")
    time.sleep(random.uniform(0.8, 1.6))

    out_dir = Path("data")
    out_dir.mkdir(exist_ok=True)
    run_id = time.strftime("%Y-%m-%d_%H-%M")
    out_path = out_dir / f"firms_firms_page__{run_id}.xlsx"

    def norm_addr(s: str) -> str:
        return " ".join((s or "").lower().replace("\xa0", " ").split())

    for index, c in enumerate(cards):
        # if index == 0:
        #     print("RAW ADDRESS:", repr(c.get("address")))
        addr_key = norm_addr(c.get("address", ""))
        if not addr_key:
            continue
        if addr_key in seen_addr:
            continue
        seen_addr.add(addr_key)
        rows.append(c)
        print(f"TOTAL | page={page} | cards={len(cards)} | rows_total={len(rows)} | seen_addr={len(seen_addr)}")
    # Переменная определяет - сколько первых компаний будет обогащено контактами (website и мессенжеры)
    MAX_ENRICH = 750

    for row in rows[enriched_count:]:
        if enriched_count >= MAX_ENRICH:
            break
        # print("ENRICH PLAN", type(rows), len(rows), [r.get("firm_id") for r in rows[:3]])
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
        print(f"ENRICH | firm_id={row.get('firm_id')} | type={row.get('primary_type')} | primary={row.get('primary_contact')} | website={row.get('website')}")

    enriched_count = len(rows)

    run_id = time.strftime("%Y-%m-%d_%H-%M")
    run_date = time.strftime("%Y-%m-%d")

    df = pd.DataFrame(rows)

    # --- служебные ---
    df["run_id"] = run_id
    df["run_date"] = run_date
    df["city"] = CITY_SLUG

    # --- CRM (пустые) ---
    for col in ["outreach_status", "outreach_date", "channel_used", "note"]:
        if col not in df.columns:
            df[col] = ""

    df.to_excel(out_path, index=False)

    print("Saved:", out_path, "rows:", len(rows))



