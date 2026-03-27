import asyncio
import random
import re
import os
import sys
import traceback
import requests
from datetime import datetime
from urllib.parse import urlparse, unquote
from html import unescape

from playwright.async_api import async_playwright

# ══════════════════════════════════════════════════════════════
#  НАСТРОЙКИ
# ══════════════════════════════════════════════════════════════

# Snov.io API — бесплатно 50 кредитов/мес — https://snov.io/api
SNOV_CLIENT_ID     = ""
SNOV_CLIENT_SECRET = ""

# Сколько сайтов парсить email одновременно
EMAIL_WORKERS = 10

# Таймаут загрузки страницы в мс
PAGE_TIMEOUT = 10000

# Сколько раз повторить запрос к Google Maps при ошибке
MAPS_RETRIES = 3

# Пауза между повторами (сек)
RETRY_PAUSE = 5

# ══════════════════════════════════════════════════════════════

SELECTOR_SIDEBAR        = "div[role='feed']"
SELECTOR_CARD_LINK      = "a.hfpxzc"
SELECTOR_DETAILS_HEADER = "h1.DUwDvf"

# Признаки капчи / блокировки Google
CAPTCHA_MARKERS = [
    "recaptcha", "captcha", "unusual traffic",
    "detected unusual", "not a robot", "verify you",
    "are you a human", "blocked", "access denied",
]

EMAIL_REGEX = re.compile(
    r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}",
    re.IGNORECASE
)

EMAIL_BLACKLIST = {
    "example.com", "sentry.io", "wixpress.com", "domain.com",
    "email.com", "yourdomain.com", "yoursite.com", "site.com",
    "company.com", "test.com", "wix.com", "squarespace.com",
    "wordpress.com", "elementor.com", "cloudflare.com",
    "schema.org", "google.com", "w3.org", "jquery.com",
    "sentry-next.wixpress.com", "instagram.com", "facebook.com",
    "twitter.com", "tiktok.com", "youtube.com", "linkedin.com",
    "apple.com", "microsoft.com", "amazonaws.com",
}

CONTACT_KEYWORDS = [
    "contact", "reach", "about", "get in touch",
    "appointment", "schedule", "impressum", "kontakt",
]

FALLBACK_PATHS = [
    "/contact", "/contacts", "/contact-us", "/kontakt",
    "/about", "/about-us", "/get-in-touch", "/support",
    "/appointment", "/schedule",
]


# ──────────────────────────────────────────
#  Утилиты email
# ──────────────────────────────────────────

def _clean_email(email: str) -> str:
    return email.lower().strip(" \t\r\n.,;:'\")]>")

def _email_domain(email: str) -> str:
    return email.split("@", 1)[-1].lower().lstrip("www.")

def _is_blacklisted(email: str) -> bool:
    domain = _email_domain(email)
    return any(domain == bad or domain.endswith("." + bad) for bad in EMAIL_BLACKLIST)

def _looks_valid(email: str) -> bool:
    if len(email) < 6:
        return False
    parts = email.split("@")
    if len(parts) != 2:
        return False
    local, dom = parts
    if not local or "." not in dom or len(dom) < 4:
        return False
    return True

def _normalize_obfuscations(text: str) -> str:
    t = text
    t = re.sub(r"\s*\[\s*at\s*\]\s*",  "@", t, flags=re.IGNORECASE)
    t = re.sub(r"\s*\(\s*at\s*\)\s*",  "@", t, flags=re.IGNORECASE)
    t = re.sub(r"\s+\bat\b\s+",         "@", t, flags=re.IGNORECASE)
    t = re.sub(r"\s*\[\s*dot\s*\]\s*", ".", t, flags=re.IGNORECASE)
    t = re.sub(r"\s*\(\s*dot\s*\)\s*", ".", t, flags=re.IGNORECASE)
    t = re.sub(r"\s+\bdot\b\s+",        ".", t, flags=re.IGNORECASE)
    return t

def extract_emails_from_blob(blob: str) -> list:
    if not blob:
        return []
    try:
        blob = unescape(blob)
        blob = unquote(blob)
        blob = _normalize_obfuscations(blob)
        found = EMAIL_REGEX.findall(blob)
        seen, clean = set(), []
        for e in found:
            e = _clean_email(e)
            if not _looks_valid(e):
                continue
            if _is_blacklisted(e):
                continue
            if e not in seen:
                seen.add(e)
                clean.append(e)
        return clean
    except Exception:
        return []


# ──────────────────────────────────────────
#  Snov.io
# ──────────────────────────────────────────

_snov_token = None

def _snov_get_token():
    global _snov_token
    if not SNOV_CLIENT_ID or not SNOV_CLIENT_SECRET:
        return None
    if _snov_token:
        return _snov_token
    try:
        r = requests.post("https://api.snov.io/v1/oauth/access_token", data={
            "grant_type":    "client_credentials",
            "client_id":     SNOV_CLIENT_ID,
            "client_secret": SNOV_CLIENT_SECRET,
        }, timeout=10)
        _snov_token = r.json().get("access_token")
        return _snov_token
    except Exception:
        return None

def snov_find_emails(domain: str) -> list:
    token = _snov_get_token()
    if not token:
        return []
    try:
        r = requests.get("https://api.snov.io/v2/domain-emails-with-info", params={
            "access_token": token,
            "domain":       domain,
            "type":         "all",
            "limit":        5,
        }, timeout=10)
        emails = []
        for item in r.json().get("emails", []):
            addr = item.get("email", "")
            if addr and _looks_valid(addr) and not _is_blacklisted(addr):
                emails.append(addr.lower())
        return emails
    except Exception:
        return []


# ──────────────────────────────────────────
#  Логирование ошибок в файл
# ──────────────────────────────────────────

def log_error(msg: str):
    """Пишет ошибку в errors.log — не останавливает скрипт."""
    try:
        with open("errors.log", "a", encoding="utf-8") as f:
            f.write(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}\n")
    except Exception:
        pass


# ══════════════════════════════════════════════════════════════
#  Основной парсер
# ══════════════════════════════════════════════════════════════

class GoogleMapsParser:
    def __init__(self, browser):
        self.browser = browser
        self.results = []
        self.email_cache: dict[str, str] = {}
        self._sem = asyncio.Semaphore(EMAIL_WORKERS)

    # ── Проверка капчи ─────────────────────────────────────────

    async def _check_captcha(self, page) -> bool:
        try:
            content = (await page.content()).lower()
            url = page.url.lower()
            for marker in CAPTCHA_MARKERS:
                if marker in content or marker in url:
                    return True
        except Exception:
            pass
        return False

    # ── Скроллинг ──────────────────────────────────────────────

    async def human_scroll(self, page, sidebar_locator):
        print("🖱️  Скроллинг результатов...")
        try:
            last_h = await sidebar_locator.evaluate("el => el.scrollHeight")
        except Exception as e:
            log_error(f"human_scroll init: {e}")
            return

        stale_count = 0
        while True:
            try:
                await sidebar_locator.evaluate("el => el.scrollTo(0, el.scrollHeight)")
                await asyncio.sleep(random.uniform(1.5, 2.5))
                new_h = await sidebar_locator.evaluate("el => el.scrollHeight")

                end = False
                for txt in ["You've reached the end", "Больше результатов нет", "Вы просмотрели все"]:
                    try:
                        if await page.locator(f"text={txt}").count() > 0:
                            end = True
                            break
                    except Exception:
                        pass

                if end:
                    print("✅ Конец списка.")
                    break

                if new_h == last_h:
                    stale_count += 1
                    if stale_count >= 3:
                        print("✅ Скроллинг завершён (высота не меняется).")
                        break
                else:
                    stale_count = 0

                last_h = new_h

            except Exception as e:
                log_error(f"human_scroll loop: {e}")
                break

    # ── Извлечение email со страницы ───────────────────────────

    async def _extract_mailto(self, tab) -> list:
        try:
            hrefs = await tab.locator("a[href^='mailto:']").evaluate_all(
                "els => els.map(a => a.getAttribute('href') || '').filter(Boolean)",
                timeout=3000
            )
            out = []
            for href in hrefs:
                href = unquote(unescape(href))
                addr_part = href.split("mailto:", 1)[-1].split("?", 1)[0]
                for piece in addr_part.split(","):
                    piece = _clean_email(piece)
                    if EMAIL_REGEX.fullmatch(piece) and _looks_valid(piece) and not _is_blacklisted(piece):
                        out.append(piece)
            return sorted(set(out))
        except Exception:
            return []

    async def _extract_from_page(self, tab) -> list:
        try:
            emails = await self._extract_mailto(tab)
            if emails:
                return emails
        except Exception:
            pass

        try:
            text = await asyncio.wait_for(
                tab.evaluate("() => document.body ? document.body.innerText : ''"),
                timeout=5
            )
            emails = extract_emails_from_blob(text)
            if emails:
                return sorted(set(emails))
        except Exception:
            pass

        try:
            html = await asyncio.wait_for(tab.content(), timeout=5)
            return sorted(set(extract_emails_from_blob(html)))
        except Exception:
            return []

    def _normalize_url(self, url: str) -> str:
        try:
            url = url.strip()
            if not url or url == "Нет данных":
                return ""
            if not url.startswith("http"):
                url = "https://" + url
            return re.sub(r"\?.*$", "", url).rstrip("/")
        except Exception:
            return ""

    async def _get_contact_links(self, tab, base_domain: str) -> list:
        try:
            raw = await tab.locator("nav a, header a, footer a").evaluate_all(
                "els => els.map(el => ({href: el.href, text: (el.innerText||'').trim().toLowerCase()}))",
                timeout=3000
            )
            links, seen = [], set()
            for item in raw:
                href = (item.get("href") or "").strip()
                text = (item.get("text") or "")
                if not href or href in seen or not href.startswith("http"):
                    continue
                if urlparse(href).netloc.lower() != base_domain:
                    continue
                if any(kw in text or kw in href.lower() for kw in CONTACT_KEYWORDS):
                    links.append(href)
                    seen.add(href)
            return links
        except Exception:
            return []

    async def scrape_emails_from_website(self, context, website_url: str) -> str:
        website_url = self._normalize_url(website_url)
        if not website_url:
            return "Нет данных"

        base_domain = urlparse(website_url).netloc.lower()
        if base_domain in self.email_cache:
            return self.email_cache[base_domain]

        async with self._sem:
            tab = None
            try:
                tab = await context.new_page()

                # Блокируем тяжёлые ресурсы — ускоряет загрузку
                async def block_heavy(route):
                    try:
                        if route.request.resource_type in ("image", "media", "font"):
                            await route.abort()
                        else:
                            await route.continue_()
                    except Exception:
                        pass

                await tab.route("**/*", block_heavy)

                # Шаг 1: Главная
                try:
                    await tab.goto(website_url, timeout=PAGE_TIMEOUT, wait_until="domcontentloaded")
                    await asyncio.sleep(random.uniform(0.3, 0.7))
                except Exception as e:
                    log_error(f"goto {website_url}: {e}")
                    self.email_cache[base_domain] = "Нет данных"
                    return "Нет данных"

                emails = await self._extract_from_page(tab)
                if emails:
                    result = ", ".join(emails)
                    print(f"      📧 [{base_domain}]: {emails}")
                    self.email_cache[base_domain] = result
                    return result

                # Шаг 2: Contact-ссылки
                contact_links = await self._get_contact_links(tab, base_domain)
                if not contact_links:
                    contact_links = [website_url.rstrip("/") + p for p in FALLBACK_PATHS]

                for url in contact_links[:2]:
                    try:
                        await tab.goto(url, timeout=PAGE_TIMEOUT, wait_until="domcontentloaded")
                        await asyncio.sleep(random.uniform(0.3, 0.6))
                        emails = await self._extract_from_page(tab)
                        if emails:
                            result = ", ".join(emails)
                            print(f"      📧 контакты [{base_domain}]: {emails}")
                            self.email_cache[base_domain] = result
                            return result
                    except Exception as e:
                        log_error(f"contact page {url}: {e}")
                        continue

                # Шаг 3: Snov.io
                if SNOV_CLIENT_ID:
                    try:
                        s_emails = snov_find_emails(base_domain)
                        if s_emails:
                            result = ", ".join(s_emails)
                            print(f"      📧 Snov [{base_domain}]: {s_emails}")
                            self.email_cache[base_domain] = result
                            return result
                    except Exception as e:
                        log_error(f"snov {base_domain}: {e}")

            except Exception as e:
                log_error(f"scrape_emails crash {website_url}: {e}\n{traceback.format_exc()}")
            finally:
                if tab:
                    try:
                        await tab.close()
                    except Exception:
                        pass

        self.email_cache[base_domain] = "Нет данных"
        return "Нет данных"

    # ── Парсинг карточки ───────────────────────────────────────

    async def parse_detail_data(self, page) -> dict:
        data = {
            "Название": "Нет данных",
            "Телефон":  "Нет данных",
            "Веб-сайт": "Нет данных",
            "Адрес":    "Нет данных",
            "Email":    "Нет данных",
        }
        try:
            try:
                data["Название"] = await page.locator(SELECTOR_DETAILS_HEADER).inner_text(timeout=3000)
            except Exception: pass

            try:
                loc = page.locator("button[data-item-id='address']")
                if await loc.count() > 0:
                    label = await loc.get_attribute("aria-label")
                    if label:
                        data["Адрес"] = label.replace("Адрес: ", "").replace("Address: ", "").strip()
            except Exception: pass

            try:
                loc = page.locator("a[data-item-id='authority']")
                if await loc.count() > 0:
                    data["Веб-сайт"] = await loc.get_attribute("href") or "Нет данных"
            except Exception: pass

            try:
                loc = page.locator("button[data-item-id*='phone:tel:']")
                if await loc.count() > 0:
                    label = await loc.get_attribute("aria-label")
                    if label:
                        data["Телефон"] = label.replace("Телефон: ", "").replace("Phone: ", "").strip()
            except Exception: pass

        except Exception as e:
            log_error(f"parse_detail_data: {e}")
        return data

    async def find_search_input(self, page):
        for sel in [
            "input#searchboxinput", "input[name='q']",
            "input[aria-label='Search Google Maps']",
            "input[placeholder='Search Google Maps']",
            "input[class*='searchbox']",
            "input[type='text']",
        ]:
            try:
                loc = page.locator(sel).first
                if await loc.count() > 0:
                    await loc.wait_for(state="visible", timeout=4000)
                    return loc
            except Exception:
                continue
        return None

    # ── Загрузка Google Maps с ретраями ────────────────────────

    async def _goto_maps(self, page, query: str) -> bool:
        encoded = query.replace(" ", "+")
        url = f"https://www.google.com/maps/search/{encoded}/?hl=en"

        for attempt in range(1, MAPS_RETRIES + 1):
            try:
                print(f"   🌍 Попытка {attempt}/{MAPS_RETRIES}...")
                await page.goto(url, timeout=60000, wait_until="domcontentloaded")

                for btn_text in ["Accept all", "I agree", "Принять все", "Согласен"]:
                    try:
                        btn = page.locator(f"button:has-text('{btn_text}')").first
                        if await btn.count() > 0:
                            await btn.click(timeout=3000)
                            await asyncio.sleep(1)
                            break
                    except Exception:
                        pass

                await asyncio.sleep(2)

                if await self._check_captcha(page):
                    print(f"   ⚠️  Капча/блокировка. Пауза {RETRY_PAUSE * attempt} сек...")
                    log_error(f"Капча на запросе '{query}', попытка {attempt}")
                    await asyncio.sleep(RETRY_PAUSE * attempt)
                    continue

                try:
                    await page.wait_for_selector(SELECTOR_SIDEBAR, timeout=12000)
                    return True
                except Exception:
                    inp = await self.find_search_input(page)
                    if inp:
                        await inp.click()
                        await asyncio.sleep(0.4)
                        await inp.fill(query)
                        await asyncio.sleep(0.4)
                        await page.keyboard.press("Enter")
                        try:
                            await page.wait_for_selector(SELECTOR_SIDEBAR, timeout=15000)
                            return True
                        except Exception:
                            pass

                    log_error(f"Sidebar не найден, попытка {attempt}, '{query}'")
                    await asyncio.sleep(RETRY_PAUSE)

            except Exception as e:
                log_error(f"_goto_maps attempt {attempt}: {e}")
                await asyncio.sleep(RETRY_PAUSE)

        return False

    # ── Основной цикл ──────────────────────────────────────────

    async def run(self, search_query, context):
        page = await context.new_page()
        try:
            loaded = await self._goto_maps(page, search_query)
            if not loaded:
                print(f"❌ Не удалось загрузить Maps для '{search_query}'. Пропускаем.")
                log_error(f"Пропущен '{search_query}' — не загрузился после {MAPS_RETRIES} попыток")
                return

            await self.human_scroll(page, page.locator(SELECTOR_SIDEBAR))

            try:
                card_hrefs = await page.locator(SELECTOR_CARD_LINK).evaluate_all(
                    "cards => cards.map(c => c.href)"
                )
            except Exception as e:
                log_error(f"Сбор карточек: {e}")
                card_hrefs = []

            total = len(card_hrefs)
            print(f"📊 Карточек: {total}")
            if total == 0:
                print("⚠️ Карточек не найдено, пропускаем запрос.")
                return

            # ── Шаг 1: Сбор данных карточек ────────────────────
            print("\n📋 Сбор данных карточек...")
            for i, href in enumerate(card_hrefs):
                details = {
                    "Название": f"Карточка {i+1}",
                    "Телефон":  "Нет данных",
                    "Веб-сайт": "Нет данных",
                    "Адрес":    "Нет данных",
                    "Email":    "Нет данных",
                }
                try:
                    await page.goto(href, timeout=12000, wait_until="domcontentloaded")

                    if await self._check_captcha(page):
                        print(f"   ⚠️  Капча на карточке {i+1}. Пауза 30 сек...")
                        log_error(f"Капча на карточке {i+1}, '{search_query}'")
                        await asyncio.sleep(30)
                        loaded = await self._goto_maps(page, search_query)
                        if not loaded:
                            print("❌ После капчи Maps не восстановился. Сохраняем что есть.")
                            self.results.append(details)
                            break

                    try:
                        await page.wait_for_selector(SELECTOR_DETAILS_HEADER, timeout=7000)
                    except Exception:
                        pass

                    await asyncio.sleep(random.uniform(0.7, 1.3))
                    details = await self.parse_detail_data(page)

                    site_info = f"🌐 {details['Веб-сайт']}" if details['Веб-сайт'] != 'Нет данных' else "❌ нет сайта"
                    print(f"   [{i+1}/{total}] {details['Название']} | {site_info}")

                except Exception as e:
                    log_error(f"Карточка {i+1} ({href}): {e}")
                    print(f"   ⚠️  [{i+1}/{total}] Ошибка карточки — пропускаем")

                finally:
                    # Сохраняем карточку в любом случае — даже пустую
                    self.results.append(details)

            # ── Шаг 2: Параллельный парсинг email ──────────────
            print(f"\n📧 Поиск email параллельно (до {EMAIL_WORKERS} одновременно)...")
            sites = [
                (i, r["Веб-сайт"])
                for i, r in enumerate(self.results)
                if r["Веб-сайт"] and r["Веб-сайт"] != "Нет данных"
            ]
            print(f"   Сайтов: {len(sites)}")

            async def fetch_email(i, url):
                try:
                    email = await self.scrape_emails_from_website(context, url)
                    self.results[i]["Email"] = email
                    status = "✅" if email != "Нет данных" else "—"
                    print(f"   {status} [{i+1}] {self.results[i]['Название']} → {email}")
                except Exception as e:
                    log_error(f"fetch_email {url}: {e}")
                    self.results[i]["Email"] = "Нет данных"

            # return_exceptions=True — одна упавшая задача не убивает остальные
            await asyncio.gather(
                *[fetch_email(i, url) for i, url in sites],
                return_exceptions=True
            )

            self.save_data(search_query)

        except Exception as e:
            log_error(f"run() crash '{search_query}': {e}\n{traceback.format_exc()}")
            print(f"❌ Критическая ошибка: {e}")
            # Всё равно сохраняем что успели
            if self.results:
                print("💾 Сохраняем частичные результаты...")
                self.save_data(search_query + "_partial")
        finally:
            try:
                await page.close()
            except Exception:
                pass

    # ── Сохранение ─────────────────────────────────────────────

    def save_data(self, query):
        if not self.results:
            print("⚠️ Нет данных для сохранения.")
            return

        clean_q   = re.sub(r'[\\/*?:"<>|]', "", query)
        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M")
        filename  = f"Gmaps_{clean_q}_{timestamp}.txt"

        try:
            with open(filename, "w", encoding="utf-8") as f:
                for row in self.results:
                    f.write(" ; ".join([
                        row.get("Название", "Нет данных"),
                        row.get("Телефон",  "Нет данных"),
                        row.get("Веб-сайт", "Нет данных"),
                        row.get("Адрес",    "Нет данных"),
                        row.get("Email",    "Нет данных"),
                    ]) + "\n")
            print(f"💾 Сохранено: {filename}")
        except Exception as e:
            # Если файл не пишется — выводим данные прямо в консоль
            log_error(f"save_data: {e}")
            print(f"⚠️ Не смог записать файл: {e}")
            print("📋 Данные (скопируй из консоли):")
            for row in self.results:
                print(" ; ".join([
                    row.get("Название", ""),
                    row.get("Телефон",  ""),
                    row.get("Веб-сайт", ""),
                    row.get("Адрес",    ""),
                    row.get("Email",    ""),
                ]))

        total      = len(self.results)
        with_email = sum(1 for r in self.results if r.get("Email", "Нет данных") != "Нет данных")
        pct        = round(with_email / total * 100) if total else 0
        print(f"📊 Итого: {total} клиник | {with_email} с email ({pct}%)")


# ══════════════════════════════════════════════════════════════
#  Точка входа
# ══════════════════════════════════════════════════════════════

async def main():
    filename = "keywords.txt"
    if not os.path.exists(filename):
        with open(filename, "w", encoding="utf-8") as f:
            f.write("dentist New York\ndentist Los Angeles\n")
        print(f"📝 Создан {filename}. Добавь запросы и запусти снова.")
        return

    with open(filename, "r", encoding="utf-8") as f:
        queries = [l.strip() for l in f if l.strip()]

    if not queries:
        print("⚠️ keywords.txt пустой!")
        return

    print(f"📋 Запросов: {len(queries)}")
    print(f"⚙️  Email воркеров: {EMAIL_WORKERS}")

    async with async_playwright() as p:
        browser = None
        try:
            browser = await p.chromium.launch(
                headless=True,
                args=[
                    "--no-sandbox",
                    "--disable-dev-shm-usage",
                    "--disable-blink-features=AutomationControlled",
                    "--disable-gpu",
                    "--window-size=1920,1080",
                ]
            )
            context = await browser.new_context(
                viewport={"width": 1920, "height": 1080},
                user_agent=(
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/120.0.0.0 Safari/537.36"
                ),
                locale="en-US",
            )

            for idx, query in enumerate(queries):
                print(f"\n{'='*50}\n🚀 [{idx+1}/{len(queries)}] {query}\n{'='*50}")
                try:
                    parser = GoogleMapsParser(browser)
                    await parser.run(query, context)
                except Exception as e:
                    # Один запрос упал — идём к следующему
                    log_error(f"Запрос '{query}': {e}\n{traceback.format_exc()}")
                    print(f"❌ '{query}' завершился с ошибкой, переходим к следующему")

                if idx < len(queries) - 1:
                    pause = random.uniform(8, 15)
                    print(f"⏳ Пауза {pause:.0f} сек...")
                    await asyncio.sleep(pause)

            await context.close()

        except Exception as e:
            log_error(f"main() crash: {e}\n{traceback.format_exc()}")
            print(f"❌ Глобальная ошибка: {e}")
        finally:
            if browser:
                try:
                    await browser.close()
                except Exception:
                    pass

    print("\n🎉 Готово! Проверь errors.log если были проблемы.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n⛔ Остановлено вручную.")
    except Exception as e:
        log_error(f"asyncio.run crash: {e}\n{traceback.format_exc()}")
        print(f"❌ Фатальная ошибка: {e}")
        sys.exit(1)
