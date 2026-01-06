from selenium.webdriver.common.by import By

def pick_scroll_root(driver):
    return driver.execute_script("""
    return (function () {
      const els = Array.from(document.querySelectorAll('div._jdkjbol'));
      if (!els.length) return { status: "NO_MATCH" };

      const scored = els.map(el => {
        const r = el.getBoundingClientRect();
        return { el, w: r.width, h: r.height, max: el.scrollHeight - el.clientHeight };
      });

      scored.sort((a, b) => (b.w - a.w) || (b.max - a.max));
      window.__twogis_scroll_root = scored[0].el;

      return { status: "CACHED", best: { width: scored[0].w, max: scored[0].max }, count: scored.length };
    })();
    """)

def click_page(driver, n: int):
    el = driver.find_element(By.XPATH, f"//a[normalize-space()='{n}']")
    driver.execute_script("arguments[0].scrollIntoView({block:'center'});", el)
    driver.execute_script("arguments[0].click();", el)
    # ВАЖНО: сбрасываем кэш root, потому что выдача перерисуется
    driver.execute_script("window.__twogis_scroll_root = null;")

def first_firm_id(driver):
    return driver.execute_script(r"""
    function pickRoot(){
        // 2GIS часто держит несколько div._jdkjbol — берём самый "широкий"
        const nodes = Array.from(document.querySelectorAll("div._jdkjbol"));
        if (!nodes.length) return null;

        let best = null;
        let bestW = -1;
        for (const el of nodes) {
            const w = el.getBoundingClientRect().width || 0;
            if (w > bestW) { bestW = w; best = el; }
        }
        return best;
    }

    // берём кэш, но если он протух — переподбираем
    let root = window.__twogis_scroll_root;
    if (!root || !root.isConnected) {
        root = pickRoot();
        window.__twogis_scroll_root = root;
    }
    if (!root) return null;

    const a = root.querySelector('a[href*="/firm/"]');
    if (!a) return null;

    const h = a.getAttribute("href") || "";
    const m = h.match(/\/firm\/(\d+)/);
    return m ? m[1] : null;
    """)