from pathlib import Path
import requests
import openpyxl
from openpyxl import load_workbook
from openpyxl.worksheet.table import Table, TableStyleInfo
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
import pandas as pd
from urllib.parse import unquote
from urllib.parse import parse_qs
import binascii
import json
import base64
import time


class TwoGisLeadCollector:
    def __init__(self, key_words: list, cache_path: str | Path, place: str = "astana"):
        """
        Сохраняем настройки:
        - ключевые слова поиска
        - город (place)
        - каталог для кэша (cache_dir: Path)
        - базовый URL (base_url)
        """
        self.cache_path = Path(cache_path)
        # Формируется именно список строк из ключевых слов для формирования далее запроса
        self.key_words = [str(k).strip() for k in key_words]
        self.place = place
        # Каталог для сохраненения страницы с данными - место для кэша
        self.cache_dir = self.cache_path / 'data'
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        # Собирается базовая часть url-адреса в зависимости от вводимой локации для гибкости
        self.base_url = f"https://2gis.kz/{place.lower()}"
        self.session = requests.Session()
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }

    def request_preparing(self):
        """
        Собрать финальную поисковую строку:
        <ключевые слова> + <город>
        Сохранить в self.query.
        """
        # Список ключевых слов перепаковывается в единый список со строкой, обозначающей локацию
        parts = [*self.key_words, self.place]
        self.query = " ".join(parts) # Соединение списка строк через пробел для формирования правильного поискового запроса
        return self.query

    def _extract_website_from_href(self, href: str) -> str | None:

        if 'link.2gis.com' in href:
            # 1. Последний сегмент ссылки
            b64_part = href.split('/')[-1]
            # 2. URL-decode
            b64_part = unquote(b64_part)

            # 3. Fix padding
            pad = len(b64_part) % 4
            if pad != 0:
                b64_part += '=' * (4 - pad)

            # 4. Base64 decode
            try:
                decoded = base64.b64decode(b64_part).decode("utf-8", errors="ignore")
            except binascii.Error:
                print("Invalid base64 in href")
                return None

            # 5. Берём первую непустую строку, начинающуюся с http
            for line in decoded.splitlines():
                line = line.strip()
                if line.startswith("http://") or line.startswith("https://"):
                    return line
        # 6. Если ссылка не декодировалась, то нужно проверить её на стандарт, чтобы не собрать 'мусор'
        else:
            if href.startswith('http'):
                return href

        return None


    def collect_cards_from_root(self, driver):
        return driver.execute_script(r"""
    const root = window.__twogis_scroll_root;
    if (!root) return [];

    const links = Array.from(root.querySelectorAll('a[href*="/firm/"]'));

    function clean(s){ return (s || "").replace(/\s+/g, " ").trim(); }

    return links.map(a => {
      const href = a.getAttribute("href") || "";
      const url = href.startsWith("http") ? href : ("https://2gis.kz" + href);

      // пробуем вытащить “карточку” вокруг ссылки
      let card = a;
      for (let i=0; i<8 && card; i++) {
        const txt = clean(card.innerText);
        if (txt.length > 20) break;
        card = card.parentElement;
      }
      card = card || a;

      const text = clean(card.innerText);
      // очень грубо: первая строка — название, следующая строка похожая на адрес — адрес
      const lines = text.split("\n").map(clean).filter(Boolean);

      const firm_id = (url.split("/firm/")[1] || "").split("?")[0];  

      return {
        firm_url: url,
        firm_id: firm_id,
        name: ((text.split("\u200b")[0] || "").replace(/\s\d+([.,]\d+)?\s\d+\sоцен.*$/u, "").trim()),
        address: ((text.split("\u200b")[1] || "").split("Закрыто")[0].trim()), // адрес часто содержит запятую
        primary_contact: ""
      };
    }).filter(x => x.firm_url);
    """)

    def get_primary_contact(self, driver, firm_id: str) -> tuple[str, str | None]:
        """
        MVP 1.2 (KZ): вариант B — останавливаемся на первом найденном primary по приоритету:
        WA -> TG -> IG -> FB -> VK

        Website — must have и собирается ОТДЕЛЬНО (не участвует в приоритете primary).
        Возврат: (primary_contact, website_url)

        ВАЖНО:
        - WA возвращаем как кликабельный URL (wa.me / api.whatsapp.com/send...), а не как "голый" телефон.
        - Для website допускаем 2GIS-редирект: кликаем -> ловим current_url -> декодируем внешний URL через _extract_website_from_href().
        """

        # Уникальный идентификатор текущего окна/вкладки браузера, в котором Selenium сейчас работает | Якорь
        main_handle = driver.current_window_handle
        company_url = f"https://2gis.kz/astana/firm/{firm_id}"

        # Это выполнение произвольного JavaScript прямо в браузере, который контролирует Selenium
        # код исполняется в контексте текущей страницы, так же, как если бы ты открыл DevTools → Console и вставил JS.
        # arguments[0]- это тот аргумент, который следует срау после JS - здесь 'company_url'
        driver.execute_script("window.open(arguments[0], '_blank');", company_url)
        firm_handle = driver.window_handles[-1]
        driver.switch_to.window(firm_handle)

        primary_contact = ""  # выбранный контакт по приоритету (URL)
        website_url: str | None = None  # сайт компании (если удастся)

        def _strip_text_param(url: str) -> str:
            """Стабилизируем WA-ссылку: убираем &text=..."""
            if not url:
                return ""           # Если ссылка не найдена, то следует вернуть пустую строку
            if "&text=" in url:
                return url.split("&text=", 1)[0] # Возвращается первый элемент слева от параметра разделения
            return url

        def _normalize_tg(url: str) -> str:
            """Приводим tg:// к https://t.me/... (если встретится)."""
            if not url:
                return ""
            u = url.strip()

            if u.startswith("tg://resolve?domain="):
                # 1. Разбиваем строку по "tg://resolve?domain=" и берём вторую часть (справа от =)
                # 2. Приписываем её к https://t.me/ — получаем нормализованную ссылку
                return "https://t.me/" + u.split("tg://resolve?domain=", 1)[1]
            return u

        # --- ПАТТЕРН 2GIS: КЛИК -> НОВАЯ ВКЛАДКА -> URL ---
        def _click_open_newtab_and_get_url(xpath_btn: str, url_ok, click_timeout=4, tab_timeout=8,
                                           url_timeout=8) -> str:
            before_handles = set(driver.window_handles) # Запоминаем, какие вкладки были до клика

            # Ищем по xpath_btn элемент и ждём, пока он станет доступен для клика (в пределах click_timeout секунд)
            el = WebDriverWait(driver, click_timeout).until(EC.element_to_be_clickable((By.XPATH, xpath_btn)))

            # Надёжный способ кликнуть (иногда .click() не срабатывает из-за перекрытий, а execute_script работает всегда)
            driver.execute_script("arguments[0].click();", el)

            # Сравниваем: появилось ли больше вкладок, чем было до клика? Если да → новая вкладка открылась
            WebDriverWait(driver, tab_timeout).until(lambda d: len(d.window_handles) > len(before_handles))

            # Находим новую вкладку по сравнению со старым списком вкладок
            new_handle = [h for h in driver.window_handles if h not in before_handles][0]
            driver.switch_to.window(new_handle) # Затем переключаем контекст драйвера на неё

            # Проверяем URL в новой вкладке — валиден ли он? Функция url_ok(...) передаётся как аргумент
            WebDriverWait(driver, url_timeout).until(lambda d: url_ok(d.current_url))
            return driver.current_url

        def _try_extract_website_from_links() -> str | None:
            """
            Пытаемся вытащить сайт из href (если реально присутствует в DOM).
            ВАЖНО: здесь разрешаем и 2GIS-редиректы, потому что _extract_website_from_href умеет их декодировать.
            """
            links = driver.find_elements(By.CSS_SELECTOR, 'a[href^="http://"], a[href^="https://"]')
            bad = ("instagram.com", "t.me", "facebook.com", "vk.com", "wa.me", "whatsapp.com")
            for a in links:
                href = (a.get_attribute("href") or "").strip()
                if not href:
                    continue

                # 1) пробуем декодировать (на случай редиректа)
                raw = self._extract_website_from_href(href) or href
                low = raw.lower()

                # 2) отсекаем мессенджеры/соцсети
                if any(b in low for b in bad):
                    continue

                # 3) если это всё ещё 2gis и не декодировалось — пропускаем
                if "2gis." in low:
                    continue

                return raw
            return None

        try:
            WebDriverWait(driver, 12).until(lambda d: firm_id in d.current_url)

            # ---------------------------------------------------------------------
            # 1) WEBSITE (must have, отдельная колонка)
            # ---------------------------------------------------------------------
            website_url = _try_extract_website_from_links()

            # Если не нашли — кликом по кнопке “Сайт/Website/Перейти”:
            # ВАЖНО: допускаем, что сначала откроется 2GIS-редирект.
            if not website_url:
                xpath_site = (
                    "//*[self::a or self::button]"
                    "[contains(translate(normalize-space(.),"
                    " 'ABCDEFGHIJKLMNOPQRSTUVWXYZАБВГДЕЁЖЗИЙКЛМНОПРСТУФХЦЧШЩЪЫЬЭЮЯ',"
                    " 'abcdefghijklmnopqrstuvwxyzабвгдеёжзийклмнопрстуфхцчшщъыьэюя'),'сайт')"
                    " or contains(translate(normalize-space(.),"
                    " 'ABCDEFGHIJKLMNOPQRSTUVWXYZАБВГДЕЁЖЗИЙКЛМНОПРСТУФХЦЧШЩЪЫЬЭЮЯ',"
                    " 'abcdefghijklmnopqrstuvwxyzабвгдеёжзийклмнопрстуфхцчшщъыьэюя'),'website')"
                    " or contains(translate(normalize-space(.),"
                    " 'ABCDEFGHIJKLMNOPQRSTUVWXYZАБВГДЕЁЖЗИЙКЛМНОПРСТУФХЦЧШЩЪЫЬЭЮЯ',"
                    " 'abcdefghijklmnopqrstuvwxyzабвгдеёжзийклмнопрстуфхцчшщъыьэюя'),'перейти')]"
                )
                try:
                    site_url = _click_open_newtab_and_get_url(
                        xpath_site,
                        # допускаем любой http/https (включая 2gis-редирект)
                        url_ok=lambda u: u.startswith("http")
                    )

                    # Декодируем внешний URL (если это 2gis redirect), иначе берём как есть
                    raw = self._extract_website_from_href(site_url) or site_url
                    low = raw.lower()

                    # Отсекаем мессенджеры/соцсети и случай, когда остались на 2gis
                    if any(b in low for b in
                           ("instagram.com", "t.me", "facebook.com", "vk.com", "wa.me", "whatsapp.com")):
                        website_url = None
                    elif "2gis." in low:
                        website_url = None
                    else:
                        website_url = raw

                except Exception:
                    website_url = None

            # ---------------------------------------------------------------------
            # 2) PRIMARY CONTACT (вариант B, Казахстан): WA -> TG -> IG -> FB -> VK
            # ---------------------------------------------------------------------
            candidates = driver.find_elements(
                By.CSS_SELECTOR,
                'a[href*="api.whatsapp.com/send"], a[href*="wa.me"], a[href*="whatsapp.com/send"],'
                'a[href*="t.me"], a[href*="tg://"],'
                'a[href*="instagram.com"],'
                'a[href*="facebook.com"],'
                'a[href*="vk.com"]'
            )

            wa_href = tg_href = ig_href = fb_href = vk_href = ""
            for a in candidates:
                href = (a.get_attribute("href") or "").strip()
                if not href:
                    continue
                low = href.lower()
                if (not wa_href) and ("whatsapp" in low or "wa.me" in low):
                    wa_href = href
                elif (not tg_href) and ("t.me" in low or low.startswith("tg://")):
                    tg_href = href
                elif (not ig_href) and ("instagram.com" in low):
                    ig_href = href
                elif (not fb_href) and ("facebook.com" in low):
                    fb_href = href
                elif (not vk_href) and ("vk.com" in low):
                    vk_href = href

            if wa_href:
                primary_contact = _strip_text_param(wa_href)
            elif tg_href:
                primary_contact = _normalize_tg(tg_href)
            elif ig_href:
                primary_contact = ig_href
            elif fb_href:
                primary_contact = fb_href
            elif vk_href:
                primary_contact = vk_href

            # Кнопочный паттерн (если href-ов нет)
            if not primary_contact:
                # WA
                xpath_wa = (
                    "//*[self::a or self::button]"
                    "[contains(translate(normalize-space(.),"
                    " 'ABCDEFGHIJKLMNOPQRSTUVWXYZАБВГДЕЁЖЗИЙКЛМНОПРСТУФХЦЧШЩЪЫЬЭЮЯ',"
                    " 'abcdefghijklmnopqrstuvwxyzабвгдеёжзийклмнoprстуфхцчшщъыьэюя'),'whatsapp')"
                    " or contains(translate(normalize-space(.),"
                    " 'ABCDEFGHIJKLMNOPQRSTUVWXYZАБВГДЕЁЖЗИЙКЛМНОПРСТУФХЦЧШЩЪЫЬЭЮЯ',"
                    " 'abcdefghijklmnopqrstuvwxyzабвгдеёжзийклмнопрстуфхцчшщъыьэюя'),'написать')"
                    " or contains(translate(normalize-space(.),"
                    " 'ABCDEFGHIJKLMNOPQRSTUVWXYZАБВГДЕЁЖЗИЙКЛМНОПРСТУФХЦЧШЩЪЫЬЭЮЯ',"
                    " 'abcdefghijklmnopqrstuvwxyzабвгдеёжзийклмнопрстуфхцчшщъыьэюя'),'в wa')]"
                )
                try:
                    wa_url = _click_open_newtab_and_get_url(
                        xpath_wa,
                        url_ok=lambda u: ("whatsapp" in u) or ("wa.me" in u)
                    )
                    primary_contact = _strip_text_param(wa_url)
                except Exception:
                    primary_contact = ""

            if not primary_contact:
                # TG
                xpath_tg = (
                    "//*[self::a or self::button]"
                    "[contains(translate(normalize-space(.),"
                    " 'ABCDEFGHIJKLMNOPQRSTUVWXYZАБВГДЕЁЖЗИЙКЛМНОПРСТУФХЦЧШЩЪЫЬЭЮЯ',"
                    " 'abcdefghijklmnopqrstuvwxyzабвгдеёжзийклмнопрстуфхцчшщъыьэюя'),'telegram')"
                    " or contains(translate(normalize-space(.),"
                    " 'ABCDEFGHIJKLMNOPQRSTUVWXYZАБВГДЕЁЖЗИЙКЛМНОПРСТУФХЦЧШЩЪЫЬЭЮЯ',"
                    " 'abcdefghijklmnopqrstuvwxyzабвгдеёжзийклмнопрстуфхцчшщъыьэюя'),'телеграм')]"
                )
                try:
                    tg_url = _click_open_newtab_and_get_url(
                        xpath_tg,
                        url_ok=lambda u: ("t.me/" in u) or u.startswith("tg://")
                    )
                    primary_contact = _normalize_tg(tg_url)
                except Exception:
                    primary_contact = ""

            if not primary_contact:
                # IG
                xpath_ig = (
                    "//*[self::a or self::button]"
                    "[contains(translate(normalize-space(.),"
                    " 'ABCDEFGHIJKLMNOPQRSTUVWXYZАБВГДЕЁЖЗИЙКЛМНОПРСТУФХЦЧШЩЪЫЬЭЮЯ',"
                    " 'abcdefghijklmnopqrstuvwxyzабвгдеёжзийклmnопрстуфхцчшщъыьэюя'),'instagram')"
                    " or contains(translate(normalize-space(.),"
                    " 'ABCDEFGHIJKLMNOPQRSTUVWXYZАБВГДЕЁЖЗИЙКЛМНОПРСТУФХЦЧШЩЪЫЬЭЮЯ',"
                    " 'abcdefghijklmnopqrstuvwxyzабвгдеёжзийклmnопрстуфхцчшщъыьэюя'),'инстаграм')]"
                )
                try:
                    ig_url = _click_open_newtab_and_get_url(
                        xpath_ig,
                        url_ok=lambda u: "instagram.com" in u
                    )
                    primary_contact = ig_url
                except Exception:
                    primary_contact = ""

            if not primary_contact:
                # FB
                xpath_fb = (
                    "//*[self::a or self::button]"
                    "[contains(translate(normalize-space(.),"
                    " 'ABCDEFGHIJKLMNOPQRSTUVWXYZАБВГДЕЁЖЗИЙКЛМНОПРСТУФХЦЧШЩЪЫЬЭЮЯ',"
                    " 'abcdefghijklmnopqrstuvwxyzабвгдеёжзийклmnопрстуфхцчшщъыьэюя'),'facebook')"
                    " or contains(translate(normalize-space(.),"
                    " 'ABCDEFGHIJKLMNOPQRSTUVWXYZАБВГДЕЁЖЗИЙКЛМНОПРСТУФХЦЧШЩЪЫЬЭЮЯ',"
                    " 'abcdefghijklmnopqrstuvwxyzабвгдеёжзийклmnопрстуфхцчшщъыьэюя'),'фейсбук')]"
                )
                try:
                    fb_url = _click_open_newtab_and_get_url(
                        xpath_fb,
                        url_ok=lambda u: "facebook.com" in u
                    )
                    primary_contact = fb_url
                except Exception:
                    primary_contact = ""

            if not primary_contact:
                # VK
                xpath_vk = (
                    "//*[self::a or self::button]"
                    "[contains(translate(normalize-space(.),"
                    " 'ABCDEFGHIJKLMNOPQRSTUVWXYZАБВГДЕЁЖЗИЙКЛМНОПРСТУФХЦЧШЩЪЫЬЭЮЯ',"
                    " 'abcdefghijklmnopqrstuvwxyzабвгдеёжзийклmnопрстуфхцчшщъыьэюя'),'vk')"
                    " or contains(translate(normalize-space(.),"
                    " 'ABCDEFGHIJKLMNOPQRSTUVWXYZАБВГДЕЁЖЗИЙКЛМНОПРСТУФХЦЧШЩЪЫЬЭЮЯ',"
                    " 'abcdefghijklmnopqrstuvwxyzабвгдеёжзийклmnопрстуфхцчшщъыьэюя'),'вк')]"
                )
                try:
                    vk_url = _click_open_newtab_and_get_url(
                        xpath_vk,
                        url_ok=lambda u: "vk.com" in u
                    )
                    primary_contact = vk_url
                except Exception:
                    primary_contact = ""

            return primary_contact or "", website_url

        finally:
            # --- ГАРАНТИРОВАННАЯ УБОРКА ВКЛАДОК ---
            try:
                for h in list(driver.window_handles):
                    if h != main_handle:
                        driver.switch_to.window(h)
                        driver.close()
            finally:
                driver.switch_to.window(main_handle)




