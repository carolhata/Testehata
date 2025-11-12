# streamlit_app.py
# Scraper adaptado para QuintoAndar (melhor esforço): extrai resultados de busca (listings) e gera Excel
import re
import json
import logging
from io import BytesIO
from typing import List, Optional, Dict, Any
from datetime import datetime, timezone
from urllib.parse import urlparse, urljoin, urlencode, parse_qs, urlsplit, urlunsplit

import streamlit as st

# MUST be the first Streamlit command
st.set_page_config(page_title="QuintoAndar → Listings → Excel", page_icon="🏘️", layout="centered")

import requests
from bs4 import BeautifulSoup
import pandas as pd
from pydantic import BaseModel, Field, PositiveInt

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("qa-listings-scraper")

# ------------------------
# Settings & session_state
# ------------------------
class AppSettings(BaseModel):
    max_pages: PositiveInt = Field(default=5)
    per_request_delay_sec: float = Field(default=0.5)

if "settings" not in st.session_state:
    st.session_state.settings = AppSettings().dict()
else:
    # normalize
    try:
        defaults = AppSettings().dict()
        for k, v in defaults.items():
            if k not in st.session_state.settings:
                st.session_state.settings[k] = v
    except Exception:
        st.session_state.settings = AppSettings().dict()

if "scrape_df" not in st.session_state:
    st.session_state.scrape_df = None
if "scrape_url" not in st.session_state:
    st.session_state.scrape_url = ""
if "last_raw_sample" not in st.session_state:
    st.session_state.last_raw_sample = None  # for diagnostics

# ------------------------
# Helpers: normalization
# ------------------------
money_rx = re.compile(r"[\d\.\s]*\d+[,\.]?\d*", re.UNICODE)
m2_rx = re.compile(r"(\d{1,4})\s*(?:m²|m2|m\^2)", re.IGNORECASE)
rooms_rx = re.compile(r"(\d+)\s*(quartos|qtos|dormit[oó]rios|dormit[oó]rio)", re.IGNORECASE)
suite_rx = re.compile(r"(\d+)\s*(su[ií]tes?|su[ií]te)", re.IGNORECASE)
vaga_rx = re.compile(r"(\d+)\s*(vagas?|vaga)", re.IGNORECASE)

def parse_money_to_int(s: Optional[str]) -> Optional[int]:
    if not s:
        return None
    # remove R$, spaces, thousands separators; treat comma as decimal separator
    s = str(s)
    s = s.replace("R$", "").replace("r$", "").strip()
    # keep numbers, dots and commas
    s = re.sub(r"[^\d\,\.]", "", s)
    if not s:
        return None
    # if contains comma and dot, remove thousands separators
    if "," in s and "." in s:
        # assume format 1.234.567,89 -> remove dots, replace comma with dot
        s = s.replace(".", "").replace(",", ".")
    else:
        # remove dots as thousands separator
        s = s.replace(".", "")
        s = s.replace(",", ".")
    try:
        val = float(s)
        return int(round(val))
    except Exception:
        # fallback: extract digits
        digits = re.sub(r"[^\d]", "", s)
        return int(digits) if digits else None

def parse_m2_to_int(s: Optional[str]) -> Optional[int]:
    if not s:
        return None
    if isinstance(s, (int, float)):
        return int(s)
    m = m2_rx.search(str(s))
    if m:
        try:
            return int(m.group(1))
        except:
            return None
    # fallback: digits
    d = re.findall(r"\d{1,4}", s)
    return int(d[0]) if d else None

def parse_int_from_text(s: Optional[str]) -> Optional[int]:
    if not s:
        return None
    d = re.search(r"(\d+)", str(s))
    return int(d.group(1)) if d else None

def now_isoutc():
    return datetime.now(timezone.utc).isoformat()

# ------------------------
# Helpers: URL / pagination
# ------------------------
def build_page_url(base_url: str, page_index: int) -> str:
    """
    Attempt to create a paginated URL. QuintoAndar often uses query params for pagination.
    Strategy:
      - If URL already has 'page' or 'pagina' param, replace it.
      - Else append '?page=2' (or '&page=2' if ? already exists).
    """
    try:
        parts = list(urlsplit(base_url))
        qs = parse_qs(parts[3])
        if "page" in qs or "pagina" in qs:
            qs["page"] = [str(page_index)]
            parts[3] = urlencode(qs, doseq=True)
            return urlunsplit(parts)
        # some sites use ?pagina=2 or ?pagina=1; add page param
        if "?" in base_url:
            return base_url + "&page=" + str(page_index)
        else:
            return base_url + "?page=" + str(page_index)
    except Exception:
        return base_url

# ------------------------
# JSON extraction helper (for SPA pages)
# ------------------------
def try_extract_json_from_scripts(soup: BeautifulSoup) -> Optional[Any]:
    """
    Busca por JSON embutido nos <script> da página (heurístico).
    Retorna o primeiro objeto JSON contendo chaves relacionadas a listings.
    """
    scripts = soup.find_all("script")
    candidate_keys = ["listings", "properties", "searchResults", "houses", "items", "results"]
    for s in scripts:
        text = s.string or s.get_text(" ", strip=True) or ""
        # heurística: procurar '=' seguido de '{' ou '[' e terminar com ';' ou </script>
        # extração por regex: capture {...} or [...]
        # tentamos várias tentativas para não quebrar em JSON inválido
        # 1) find first '{' or '[' and try to json.loads progressive slices
        idx = None
        if "=" in text:
            # split on '=' and check right side
            parts = text.split("=", 1)
            right = parts[1].strip()
            # try to find first { or [
            m = re.search(r"([\{\[])", right)
            if m:
                start = m.start()
                # do progressive extraction until a valid JSON is found
                for end in range(len(right)-1, start+1, -1):
                    snippet = right[start:end+1].strip().rstrip(";,")
                    try:
                        obj = json.loads(snippet)
                        # check if object contains candidate keys
                        if isinstance(obj, dict):
                            keys = " ".join(list(obj.keys())).lower()
                            if any(k.lower() in keys for k in candidate_keys):
                                return obj
                            # also search nested
                            if contains_listing_like(obj):
                                return obj
                        elif isinstance(obj, list):
                            # list of objects
                            for item in obj:
                                if isinstance(item, dict) and contains_listing_like(item):
                                    return obj
                    except Exception:
                        continue
        # 2) fallback: try to find JSON-looking substring with searchResults word
        if "searchResults" in text or "listings" in text or "properties" in text:
            # try to extract JSON by finding first { and last }
            try:
                first = text.index("{")
                last = text.rindex("}")
                snippet = text[first:last+1]
                obj = json.loads(snippet)
                if contains_listing_like(obj):
                    return obj
            except Exception:
                continue
    return None

def contains_listing_like(obj: Any) -> bool:
    """
    Heurística simples: check if object or nested contains price/valor/link keys.
    """
    if isinstance(obj, dict):
        keys = " ".join(obj.keys()).lower()
        if any(k in keys for k in ["price", "valor", "priceLabel", "listing", "property", "link", "slug", "id"]):
            return True
        for v in obj.values():
            if contains_listing_like(v):
                return True
    elif isinstance(obj, list):
        for it in obj:
            if contains_listing_like(it):
                return True
    return False

# ------------------------
# Recursive finder for listing dicts
# ------------------------
def find_listing_dicts(obj: Any) -> List[Dict[str, Any]]:
    found = []
    if isinstance(obj, dict):
        # candidate: has price or valor and has some url/id
        lowkeys = {k.lower(): v for k, v in obj.items()}
        if any(k in lowkeys for k in ["price", "valor", "priceLabel"]) and any(k in lowkeys for k in ["url", "link", "slug", "id", "path"]):
            found.append(obj)
        # also if has 'results' or 'listings' keys inspect them
        for k, v in obj.items():
            found += find_listing_dicts(v)
    elif isinstance(obj, list):
        for item in obj:
            found += find_listing_dicts(item)
    return found

# ------------------------
# Extract from JSON listing dict (best-effort mapping)
# ------------------------
def extract_from_listing_dict(d: Dict[str, Any], base_url: str) -> Dict[str, Optional[Any]]:
    # flatten keys to search
    js = json.dumps(d)
    # link detection
    link = None
    for candidate in ("url", "link", "slug", "path", "permalink"):
        if candidate in d and d[candidate]:
            link = d[candidate]
            break
    # sometimes nested
    if not link:
        for k, v in d.items():
            if isinstance(v, str) and v.startswith("/imovel"):
                link = v
                break
    if link:
        link = urljoin(base_url, link)
    # address
    address = None
    for candidate in ("address", "endereco", "location", "displayAddress", "formattedAddress"):
        if candidate in d and d[candidate]:
            address = d[candidate]
            break
    # price
    valor = None
    for candidate in ("price", "valor", "listedPrice", "displayPrice"):
        if candidate in d and d[candidate]:
            valor = d[candidate]
            break
    # condominium
    cond = None
    for candidate in ("condominium", "condominio", "condominiumFee", "condo"):
        if candidate in d and d[candidate]:
            cond = d[candidate]
            break
    # iptu
    iptu = None
    for candidate in ("iptu", "propertyTax", "iptuValue"):
        if candidate in d and d[candidate]:
            iptu = d[candidate]
            break
    # m2
    m2 = None
    for candidate in ("area", "usableArea", "m2", "size"):
        if candidate in d and d[candidate]:
            m2 = d[candidate]
            break
    # rooms
    quartos = None
    for candidate in ("bedrooms", "rooms", "quartos", "bedroomCount"):
        if candidate in d and d[candidate] is not None:
            quartos = d[candidate]
            break
    # suites
    suites = None
    for candidate in ("suites", "suiteCount", "bathrooms"):
        if candidate in d and d[candidate] is not None:
            suites = d[candidate]
            break
    # vagas
    vagas = None
    for candidate in ("parkingSpaces", "garage", "vagas", "parking"):
        if candidate in d and d[candidate] is not None:
            vagas = d[candidate]
            break
    # bairro / cidade attempts
    bairro = None
    cidade = None
    if "city" in d:
        cidade = d.get("city")
    if "neighborhood" in d:
        bairro = d.get("neighborhood")
    if isinstance(address, str):
        # try to split "Rua X - Bairro - Cidade"
        parts = [p.strip() for p in re.split(r"[,\-–—]", address) if p.strip()]
        if parts:
            # attempt heuristics
            if len(parts) >= 3:
                rua = parts[0]
                bairro = bairro or parts[1]
                cidade = cidade or parts[-1]
            elif len(parts) == 2:
                rua = parts[0]
                bairro = bairro or parts[1]
            else:
                rua = parts[0]
        else:
            rua = address
    else:
        rua = None

    # ano de construcao (try common keys)
    ano = None
    for candidate in ("yearBuilt", "constructionYear", "builtYear", "anoConstrucao", "ano"):
        if candidate in d and d[candidate]:
            try:
                ano = int(d[candidate])
                break
            except:
                pass

    return {
        "Endereço": address,
        "RuaAvenida": rua,
        "Bairro": bairro,
        "Cidade": cidade,
        "Valor_raw": str(valor) if valor is not None else None,
        "Valor": parse_money_to_int(valor),
        "Condominio_raw": str(cond) if cond is not None else None,
        "Condominio": parse_money_to_int(cond),
        "IPTU_raw": str(iptu) if iptu is not None else None,
        "IPTU": parse_money_to_int(iptu),
        "M2": parse_m2_to_int(m2),
        "Quartos": parse_int_from_text(quartos),
        "Suites": parse_int_from_text(suites),
        "Vaga": parse_int_from_text(vagas),
        "Link": link,
        "bairro": bairro,
        "data_coleta": now_isoutc(),
        "ano_de_construcao": ano
    }

# ------------------------
# HTML fallback card parser (if JSON not found)
# ------------------------
def parse_cards_with_bs(html: str, base_url: str) -> List[Dict[str, Any]]:
    soup = BeautifulSoup(html, "lxml")
    cards = []
    # find probable item containers
    candidates = soup.select("[class*='listing'], [class*='card'], [class*='result'], [class*='property'], li, article")
    seen = set()
    for c in candidates:
        text = c.get_text(" ", strip=True)
        if len(text) < 40:
            continue
        key = text[:200]
        if key in seen:
            continue
        seen.add(key)
        # ignore obvious ads
        if re.search(r"patrocinad|anúncio|promovido|sponsored|publicidade", text, re.I):
            continue
        # try extract link
        a = c.find("a", href=True)
        link = None
        if a:
            link = urljoin(base_url, a.get("href"))
        # value
        money_search = re.search(r"R\$\s?[\d\.\,kKmM]+", text)
        valor = money_search.group(0) if money_search else None
        # m2
        m2 = None
        m = m2_rx.search(text)
        if m:
            m2 = m.group(1)
        # quartos suites vagas
        quartos = None
        m = rooms_rx.search(text)
        if m:
            quartos = m.group(1)
        suites = None
        m = suite_rx.search(text)
        if m:
            suites = m.group(1)
        vagas = None
        m = vaga_rx.search(text)
        if m:
            vagas = m.group(1)
        # condominio / iptu
        cond = None
        m = re.search(r"Condom[ií]nio[:\s]*R?\$?[\s\d\.\,kKmM]+", text, re.I)
        if m:
            cond = re.sub(r"Condom[ií]nio[:\s]*", "", m.group(0), flags=re.I).strip()
        iptu = None
        m = re.search(r"IPTU[:\s]*R?\$?[\s\d\.\,kKmM]+", text, re.I)
        if m:
            iptu = re.sub(r"IPTU[:\s]*", "", m.group(0), flags=re.I).strip()
        # address heuristic
        address = None
        m = re.search(r"(Rua|Av\.|Avenida|Travessa|Alameda|R\.)\s+[^\n,]{5,80}", text)
        if m:
            address = m.group(0).strip()
        cards.append({
            "Endereço": address,
            "RuaAvenida": address,
            "Bairro": None,
            "Cidade": None,
            "Valor_raw": valor,
            "Valor": parse_money_to_int(valor),
            "Condominio_raw": cond,
            "Condominio": parse_money_to_int(cond),
            "IPTU_raw": iptu,
            "IPTU": parse_money_to_int(iptu),
            "M2": parse_m2_to_int(m2),
            "Quartos": parse_int_from_text(quartos),
            "Suites": parse_int_from_text(suites),
            "Vaga": parse_int_from_text(vagas),
            "Link": link,
            "bairro": None,
            "data_coleta": now_isoutc(),
            "ano_de_construcao": None
        })
    return cards

# ------------------------
# Main parse function per page (tries JSON then HTML)
# ------------------------
def parse_search_page(url: str) -> List[Dict[str, Any]]:
    headers = {"User-Agent": "Mozilla/5.0 (compatible; QuintoAndarScraper/1.0)", "Accept-Language": "pt-BR,pt;q=0.9"}
    resp = requests.get(url, headers=headers, timeout=30)
    resp.raise_for_status()
    html = resp.text
    soup = BeautifulSoup(html, "lxml")
    st.session_state.last_raw_sample = {"url": url, "html_head": html[:5000]}
    # try JSON extraction
    obj = try_extract_json_from_scripts(soup)
    results = []
    base_url = resp.url
    if obj:
        # find listing dicts inside JSON
        listing_dicts = find_listing_dicts(obj)
        if listing_dicts:
            for d in listing_dicts:
                extracted = extract_from_listing_dict(d, base_url)
                results.append(extracted)
            return results
        # sometimes JSON has a 'results' field that is list
        if isinstance(obj, dict):
            for key in obj.keys():
                if isinstance(obj[key], list):
                    for it in obj[key]:
                        if isinstance(it, dict) and contains_listing_like(it):
                            listing_dicts += find_listing_dicts(it)
            if listing_dicts:
                for d in listing_dicts:
                    extracted = extract_from_listing_dict(d, base_url)
                    results.append(extracted)
                return results
    # fallback: parse cards with bs
    cards = parse_cards_with_bs(html, base_url)
    return cards

# ------------------------
# Orchestrator: parse multiple pages (with attempt to paginate)
# ------------------------
def parse_multiple_pages(base_url: str, max_pages: int = 5) -> pd.DataFrame:
    all_rows = []
    for i in range(1, max_pages + 1):
        page_url = build_page_url(base_url, i)
        try:
            rows = parse_search_page(page_url)
            if not rows:
                logger.info("No rows found on page %s", page_url)
            for r in rows:
                all_rows.append(r)
        except Exception as e:
            logger.exception("error parsing page %s", page_url)
        # small polite delay (configurable)
        import time
        time.sleep(float(st.session_state.settings.get("per_request_delay_sec", 0.5)))
    if not all_rows:
        return pd.DataFrame()
    # dedupe by Link or Endereço+Valor
    seen = set()
    clean = []
    for r in all_rows:
        key = (r.get("Link") or "") + "|" + str(r.get("Valor_raw") or "") + "|" + str(r.get("Endereço") or "")
        if key in seen:
            continue
        seen.add(key)
        clean.append(r)
    df = pd.DataFrame(clean)
    # ensure columns order
    cols = ["Endereço", "RuaAvenida", "Bairro", "Cidade", "Valor_raw", "Valor", "Condominio_raw", "Condominio",
            "IPTU_raw", "IPTU", "M2", "Quartos", "Suites", "Vaga", "Link", "bairro", "data_coleta", "ano_de_construcao"]
    for c in cols:
        if c not in df.columns:
            df[c] = None
    df = df[cols]
    return df

# ------------------------
# UI
# ------------------------
st.title("🏘️ QuintoAndar — Extrator de resultados → Excel")
st.markdown("Cole a URL da página de busca do QuintoAndar (ex.: bairros). O scraper tentará extrair **apenas** os resultados de busca (ignorando anúncios pagos) e gerar um Excel com as colunas solicitadas.")

col1, col2 = st.columns([4, 1])
with col1:
    url_input = st.text_input("URL da página de busca (QuintoAndar)", value=st.session_state.scrape_url or "", placeholder="https://www.quintoandar.com.br/comprar/imovel/...")
with col2:
    run_btn = st.button(f"Extrair ({st.session_state.settings['max_pages']} páginas)", use_container_width=True)

with st.sidebar:
    st.header("Opções")
    st.number_input("Páginas a raspar (padrão 5)", min_value=1, max_value=50, value=int(st.session_state.settings["max_pages"]), key="max_pages_input")
    st.slider("Delay entre requests (segundos)", 0.0, 5.0, float(st.session_state.settings.get("per_request_delay_sec", 0.5)), step=0.1, key="delay_input")
    if st.button("Resetar estado (debug)"):
        for k in list(st.session_state.keys()):
            del st.session_state[k]
        st.experimental_rerun()

# sync sidebar inputs
try:
    st.session_state.settings["max_pages"] = int(st.session_state.get("max_pages_input", st.session_state.settings["max_pages"]))
    st.session_state.settings["per_request_delay_sec"] = float(st.session_state.get("delay_input", st.session_state.settings["per_request_delay_sec"]))
except Exception:
    pass

if run_btn and url_input:
    st.session_state.scrape_url = url_input
    max_pages = int(st.session_state.settings.get("max_pages", 5))
    st.info(f"Iniciando extração de até {max_pages} páginas (modo: JSON-first → fallback HTML).")
    try:
        df = parse_multiple_pages(url_input, max_pages)
        if df.empty:
            st.warning("Não foram encontrados resultados de listings nas páginas fornecidas (tente abrir a página no navegador e verificar se há carregamento dinâmico via XHR).")
            # show sample of raw head HTML for debugging
            if st.session_state.last_raw_sample:
                with st.expander("Amostra do HTML (diagnóstico)"):
                    st.text(st.session_state.last_raw_sample["html_head"])
        else:
            st.success(f"Extraídos {len(df)} registros (após deduplicação).")
            st.session_state.scrape_df = df
            st.dataframe(df.head(50))
            # ask user if want to continue to next pages (if > configured)
            if max_pages >= 5:
                if st.button("Raspar mais páginas (próximas 5)"):
                    st.session_state.settings["max_pages"] = max_pages + 5
                    st.experimental_rerun()
    except Exception as e:
        logger.exception("Falha ao extrair")
        st.error(f"Falha ao extrair: {e}")

st.markdown("---")
st.subheader("Exportar Excel")
def df_to_excel_bytes(df: pd.DataFrame, sheet_name: str = "listings"):
    bio = BytesIO()
    with pd.ExcelWriter(bio, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name=sheet_name)
    bio.seek(0)
    return bio.read()

if st.session_state.get("scrape_df") is not None:
    df_preview = st.session_state.scrape_df.copy()
    st.markdown("Prévia dos resultados:")
    st.dataframe(df_preview.head(200))
    fname = f"quintoandar_listings_{datetime.now().strftime('%Y%m%d-%H%M%S')}.xlsx"
    st.download_button("Baixar Excel (listings)", data=df_to_excel_bytes(df_preview), file_name=fname, mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
else:
    st.info("Sem DataFrame disponível. Execute uma extração.")

with st.expander("Diagnóstico / Amostras (útil para ajustes)"):
    st.write("URL atual:", st.session_state.get("scrape_url"))
    st.write("Config:", st.session_state.settings)
    st.write("Última amostra (head):")
    st.text(st.session_state.get("last_raw_sample", {}).get("html_head", "")[:2000])

st.caption("Notas: O scraper tenta extrair dados visíveis na listagem (sem abrir anúncios individuais). Valores monetários e áreas são normalizados para inteiros; campos ausentes ficam como None. Se precisar de precisão total, posso ajustar seletores específicos do domínio com base no HTML completo.")

