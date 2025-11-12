# streamlit_app.py
# QuintoAndar-specific scraper: JSON-first extraction from __NEXT_DATA__ or window.__NEXT_DATA__
import re
import json
import logging
import time
from io import BytesIO
from typing import List, Optional, Dict, Any
from datetime import datetime, timezone
from urllib.parse import urljoin, urlsplit, urlunsplit, parse_qs, urlencode

import streamlit as st

# set_page_config MUST be first Streamlit command
st.set_page_config(page_title="QuintoAndar → Listings → Excel", page_icon="🏘️", layout="centered")

import requests
from bs4 import BeautifulSoup
import pandas as pd
from pydantic import BaseModel, Field, PositiveInt

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("qa-listings-scraper")

# -------------------------
# App settings & session
# -------------------------
class AppSettings(BaseModel):
    max_pages: PositiveInt = Field(default=5)
    per_request_delay_sec: float = Field(default=0.6)

if "settings" not in st.session_state:
    st.session_state.settings = AppSettings().dict()
else:
    # normalize defaults
    defaults = AppSettings().dict()
    for k, v in defaults.items():
        if k not in st.session_state.settings:
            st.session_state.settings[k] = v

if "scrape_url" not in st.session_state:
    st.session_state.scrape_url = ""
if "scrape_df" not in st.session_state:
    st.session_state.scrape_df = None
if "last_raw_sample" not in st.session_state:
    st.session_state.last_raw_sample = None

# -------------------------
# Normalizers / helpers
# -------------------------
money_rx = re.compile(r"[Rr]\$?\s*[\d\.\,kKmM\s]+")
m2_rx = re.compile(r"(\d{1,4})\s*(?:m²|m2|m\^2)?", re.IGNORECASE)

def now_utc_iso():
    return datetime.now(timezone.utc).isoformat()

def parse_money_to_int(value: Optional[Any]) -> Optional[int]:
    if value is None:
        return None
    s = str(value)
    # handle numeric types
    if isinstance(value, (int, float)):
        return int(round(float(value)))
    # remove currency symbols and letters
    s = s.replace("R$", "").replace("r$", "").strip()
    s = re.sub(r"[^\d\,\.]", "", s)
    if not s:
        return None
    # unify thousands and decimals: if both present, assume '.' thousands and ',' decimal
    if "." in s and "," in s and s.rfind(",") > s.rfind("."):
        s = s.replace(".", "").replace(",", ".")
    else:
        s = s.replace(".", "").replace(",", ".")
    try:
        return int(round(float(s)))
    except:
        digits = re.sub(r"[^\d]", "", s)
        return int(digits) if digits else None

def parse_m2_to_int(value: Optional[Any]) -> Optional[int]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return int(value)
    s = str(value)
    m = m2_rx.search(s)
    if m:
        try:
            return int(m.group(1))
        except:
            pass
    digits = re.findall(r"\d{1,4}", s)
    return int(digits[0]) if digits else None

def parse_int(value: Optional[Any]) -> Optional[int]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return int(value)
    s = re.search(r"(\d+)", str(value))
    return int(s.group(1)) if s else None

# -------------------------
# URL pagination helper
# -------------------------
def build_page_url(base_url: str, page: int) -> str:
    # try to replace common pagination query params
    try:
        parts = list(urlsplit(base_url))
        qs = parse_qs(parts[3])
        # QuintoAndar sometimes uses 'page' or 'pagina' or 'offset'
        if "page" in qs or "pagina" in qs:
            qs["page"] = [str(page)]
            parts[3] = urlencode(qs, doseq=True)
            return urlunsplit(parts)
        # else add page param
        if parts[3]:
            parts[3] = parts[3] + "&page=" + str(page)
        else:
            parts[3] = "page=" + str(page)
        return urlunsplit(parts)
    except Exception:
        if "?" in base_url:
            return base_url + f"&page={page}"
        return base_url + f"?page={page}"

# -------------------------
# JSON extraction from scripts (__NEXT_DATA__ or window.__NEXT_DATA__)
# -------------------------
# Substitua a função extract_json_from_page e find_listing_objects por esta versão mais robusta.

import json
import re
from bs4 import BeautifulSoup
from typing import Any, Dict, List

def contains_listing_like(obj: Any) -> bool:
    """Heurística: procura chaves/valores que sugerem um objeto de listing."""
    if isinstance(obj, dict):
        keys = " ".join(k.lower() for k in obj.keys())
        if any(k in keys for k in ["price", "valor", "listing", "property", "id", "slug", "address", "bedrooms"]):
            return True
        for v in obj.values():
            if contains_listing_like(v):
                return True
    elif isinstance(obj, list):
        for it in obj:
            if contains_listing_like(it):
                return True
    return False

def try_json_load(s: str):
    """Tenta json.loads com limpeza incremental — retorna objeto ou None."""
    s = s.strip()
    # quick reject
    if not s or (not (s.startswith("{") or s.startswith("["))):
        return None
    # tentativa direta
    try:
        return json.loads(s)
    except Exception:
        pass
    # tentativa progressiva: expandindo até achar JSON válido (mais lenta mas robusta)
    # encontre primeiro '{' e tente cortar até diferentes finais válidos
    start = None
    for i, ch in enumerate(s):
        if ch in "{[":
            start = i
            break
    if start is None:
        return None
    # tente encontrar matching brace por heurística (procura último '}' ou ']' e tenta parse)
    for end in range(len(s)-1, start, -1):
        if s[end] in "}]":
            snippet = s[start:end+1]
            try:
                return json.loads(snippet)
            except Exception:
                continue
    return None

def extract_json_from_page(html: str) -> Optional[Dict[str, Any]]:
    """
    Versão robusta para encontrar JSON embutido no HTML:
    - tenta <script id="__NEXT_DATA__"> (Next.js)
    - busca por window.__NEXT_DATA__ = {...}
    - busca por window.__INITIAL_STATE__ = {...}
    - varre scripts buscando o maior JSON válido que contenha dados de listing
    """
    soup = BeautifulSoup(html, "lxml")

    # 1) script id __NEXT_DATA__
    script = soup.find("script", id="__NEXT_DATA__")
    if script and (script.string or script.get_text()):
        s = script.string if script.string else script.get_text()
        parsed = try_json_load(s)
        if parsed:
            return parsed

    # 2) window.__NEXT_DATA__ assignment in any script
    for s in soup.find_all("script"):
        text = s.string or s.get_text(" ", strip=True) or ""
        if "window.__NEXT_DATA__" in text or "window.__INITIAL_STATE__" in text or "window.__PRELOADED_STATE__" in text:
            # try to extract the first JSON-looking substring after '='
            m = re.search(r"(window\.__NEXT_DATA__\s*=\s*|window\.__INITIAL_STATE__\s*=\s*|window\.__PRELOADED_STATE__\s*=\s*)(?P<json>[\s\S]+)", text)
            if m:
                right = m.group("json").strip()
                # strip trailing semicolon if exists
                right = re.sub(r";\s*$", "", right)
                parsed = try_json_load(right)
                if parsed:
                    return parsed
                # if not, attempt to find the first opening brace and progressive parse
                parsed = try_json_load(right)
                if parsed:
                    return parsed

    # 3) scan all scripts and try to find a JSON substring that looks like listings
    candidate_scripts = []
    for s in soup.find_all("script"):
        txt = s.string or s.get_text(" ", strip=True) or ""
        # quick filter: script must contain a brace and at least one keyword
        if ("{" in txt or "[" in txt) and any(k in txt.lower() for k in ("list", "listing", "property", "search", "results", "props")):
            candidate_scripts.append(txt)

    # sort by length descending (prefer big embedded JSON blobs)
    candidate_scripts = sorted(set(candidate_scripts), key=lambda x: len(x), reverse=True)

    for txt in candidate_scripts:
        parsed = try_json_load(txt)
        if parsed and contains_listing_like(parsed):
            return parsed
        # if direct parse failed, attempt progressive extraction of JSON-like substrings within txt
        # look for all occurrences of '{' and try to parse from there progressively
        for m in re.finditer(r"[\{\[]", txt):
            sub = txt[m.start():m.start()+500000]  # window
            parsed = try_json_load(sub)
            if parsed and contains_listing_like(parsed):
                return parsed

    # nothing found
    return None

def find_listing_objects(obj: Any) -> List[Dict[str, Any]]:
    """
    Heurística recursiva para extrair estruturas de listing a partir do objeto JSON.
    Retorna lista de dicionários candidatos (potenciais listings).
    """
    found = []
    if isinstance(obj, dict):
        low = {k.lower(): v for k, v in obj.items()}
        # se o dict tem identificador e preço/valor, é forte candidato
        if any(k in low for k in ("id", "listingid", "houseid", "propertyid", "uuid")) and \
           any(k in low for k in ("price", "valor", "priceinfo", "pricing", "listedprice")):
            found.append(obj)
        # explorar chaves que sejam listas/objetos
        for v in obj.values():
            found += find_listing_objects(v)
    elif isinstance(obj, list):
        for item in obj:
            found += find_listing_objects(item)
    return found




# -------------------------
# Map JSON listing -> normalized row
# -------------------------
def map_listing_json_to_row(d: Dict[str, Any], base_url: str) -> Dict[str, Any]:
    # common key names on QuintoAndar JSON structure (best-effort)
    # link
    link = None
    if "link" in d and d["link"]:
        link = d["link"]
    if not link:
        for k in ("url", "permalink", "slug", "path"):
            if k in d and d[k]:
                link = d[k]; break
    if link and not link.startswith("http"):
        link = urljoin(base_url, link)
    # address
    address = None
    rua = None; bairro = None; cidade = None
    if "address" in d and d.get("address"):
        address = d.get("address")
        if isinstance(address, dict):
            rua = address.get("street") or address.get("logradouro") or address.get("streetAddress") or None
            bairro = address.get("neighborhood") or address.get("bairro") or None
            cidade = address.get("city") or address.get("cidade") or None
            # formatted full
            if not rua:
                rua = address.get("formatted") or address.get("display") or None
        elif isinstance(address, str):
            addr_text = address
            parts = [p.strip() for p in re.split(r"[,\-–]", addr_text) if p.strip()]
            if len(parts) >= 3:
                rua = parts[0]; bairro = parts[1]; cidade = parts[-1]
            elif len(parts) == 2:
                rua = parts[0]; bairro = parts[1]
            else:
                rua = addr_text
    # valor
    valor = None
    for k in ("price", "valor", "salePrice", "listedPrice", "displayPrice"):
        if k in d and d[k]:
            valor = d[k]; break
    # condo / iptu
    cond = None; iptu = None
    for k in ("condo", "condominium", "condominiumFee", "condoFee"):
        if k in d and d[k]:
            cond = d[k]; break
    for k in ("iptu", "propertyTax", "tax"):
        if k in d and d[k]:
            iptu = d[k]; break
    # m2 / area
    m2 = None
    for k in ("area", "usableArea", "size", "area_m2"):
        if k in d and d[k]:
            m2 = d[k]; break
    # quartos suites vagas
    quartos = None; suites = None; vagas = None
    for k in ("bedrooms", "bedroomCount", "quartos"):
        if k in d and d[k] is not None:
            quartos = d[k]; break
    for k in ("suites", "suiteCount", "bathrooms"):
        if k in d and d[k] is not None:
            suites = d[k]; break
    for k in ("parkingSpaces", "garage", "vagas", "parking"):
        if k in d and d[k] is not None:
            vagas = d[k]; break
    # ano de construcao
    ano = None
    for k in ("yearBuilt", "constructionYear", "builtYear", "anoConstrucao"):
        if k in d and d[k]:
            try:
                ano = int(d[k]); break
            except:
                pass
    # finalize
    return {
        "Endereço": address or None,
        "Rua": rua,
        "Bairro": bairro,
        "Cidade": cidade,
        "Valor_raw": str(valor) if valor is not None else None,
        "Valor": parse_money_to_int(valor),
        "Condominio_raw": str(cond) if cond is not None else None,
        "Condominio": parse_money_to_int(cond),
        "IPTU_raw": str(iptu) if iptu is not None else None,
        "IPTU": parse_money_to_int(iptu),
        "M2": parse_m2_to_int(m2),
        "Quartos": parse_int(quartos),
        "Suites": parse_int(suites),
        "Vaga": parse_int(vagas),
        "Link": link,
        "bairro": bairro,
        "data_coleta": now_utc_iso(),
        "ano_de_construcao": ano
    }

# -------------------------
# Parse a single search page: JSON-first -> fallback to HTML card parsing
# -------------------------
def parse_search_page(url: str) -> List[Dict[str, Any]]:
    headers = {"User-Agent": "Mozilla/5.0 (compatible; QuintoAndarScraper/1.0)", "Accept-Language": "pt-BR,pt;q=0.9"}
    resp = requests.get(url, headers=headers, timeout=30)
    resp.raise_for_status()
    html = resp.text
    # keep head sample for diagnostics
    st.session_state.last_raw_sample = html[:8000]
    # try JSON extraction
    json_obj = extract_json_from_page(html)
    rows = []
    base_url = resp.url
    if json_obj:
        # find candidate listing objects inside json
        listing_dicts = find_listing_objects(json_obj)
        if listing_dicts:
            for d in listing_dicts:
                row = map_listing_json_to_row(d, base_url)
                # ignore rows that look like non-listing (e.g., neighborhoods widget)
                if row.get("Link") or row.get("Valor") or row.get("M2"):
                    rows.append(row)
            if rows:
                return rows
        # fallback: scan for arrays inside json that contain listing-like dicts
        # traverse keys to find lists
        def traverse_for_lists(o):
            found = []
            if isinstance(o, dict):
                for k, v in o.items():
                    if isinstance(v, list):
                        found.append(v)
                    else:
                        found += traverse_for_lists(v)
            elif isinstance(o, list):
                for it in o:
                    found += traverse_for_lists(it)
            return found
        lists = traverse_for_lists(json_obj)
        for lst in lists:
            for it in lst:
                if isinstance(it, dict):
                    candidate = find_listing_objects(it)
                    if candidate:
                        for d in candidate:
                            row = map_listing_json_to_row(d, base_url)
                            if row.get("Link") or row.get("Valor"):
                                rows.append(row)
        if rows:
            return rows
    # HTML fallback: parse candidate cards with BeautifulSoup (less reliable)
    soup = BeautifulSoup(html, "lxml")
    cards = soup.select("[data-testid*='property-card'], [class*='listing'], [class*='card'], [class*='result'], li, article")
    seen = set()
    for c in cards:
        text = c.get_text(" ", strip=True)
        if len(text) < 40:
            continue
        key = text[:200]
        if key in seen:
            continue
        seen.add(key)
        # skip neighborhood widgets heuristically
        if re.search(r"bairros próximos|valor médio|imóveis para comprar|valor médio", text, re.I):
            continue
        # skip sponsored markers
        if re.search(r"patrocinad|anúncio|promovido|sponsored|publicidade", text, re.I):
            continue
        # extract link (first anchor)
        a = c.find("a", href=True)
        link = urljoin(base_url, a.get("href")) if a else None
        # valor
        mval = re.search(r"R\$\s?[\d\.\,kKmM]+", text)
        valor_raw = mval.group(0) if mval else None
        # condo/iptu heuristics
        mcond = re.search(r"Condom[ií]nio[:\s]*R?\$?[^\s\,;]+", text, re.I)
        cond_raw = None
        if mcond:
            cond_raw = re.sub(r"Condom[ií]nio[:\s]*", "", mcond.group(0), flags=re.I).strip()
        miptu = re.search(r"IPTU[:\s]*R?\$?[^\s\,;]+", text, re.I)
        iptu_raw = None
        if miptu:
            iptu_raw = re.sub(r"IPTU[:\s]*", "", miptu.group(0), flags=re.I).strip()
        # m2, quartos, suites, vagas
        m2 = None
        m = re.search(r"(\d{1,4})\s*(m²|m2|m\^2)", text, re.I)
        if m:
            m2 = m.group(1)
        quartos = None
        m = re.search(r"(\d+)\s*(quartos|dormit[oó]rios|qtos?)", text, re.I)
        if m:
            quartos = m.group(1)
        suites = None
        m = re.search(r"(\d+)\s*(su[ií]tes?|su[ií]te)", text, re.I)
        if m:
            suites = m.group(1)
        vagas = None
        m = re.search(r"(\d+)\s*(vagas?|vaga)", text, re.I)
        if m:
            vagas = m.group(1)
        # address heuristics
        address = None
        m = re.search(r"(Rua|Av\.|Avenida|Travessa|Alameda|R\.)\s+[^\n,]{5,80}", text)
        if m:
            address = m.group(0).strip()
        row = {
            "Endereço": address,
            "Rua": address,
            "Bairro": None,
            "Cidade": None,
            "Valor_raw": valor_raw,
            "Valor": parse_money_to_int(valor_raw),
            "Condominio_raw": cond_raw,
            "Condominio": parse_money_to_int(cond_raw),
            "IPTU_raw": iptu_raw,
            "IPTU": parse_money_to_int(iptu_raw),
            "M2": parse_m2_to_int(m2),
            "Quartos": parse_int(quartos),
            "Suites": parse_int(suites),
            "Vaga": parse_int(vagas),
            "Link": link,
            "bairro": None,
            "data_coleta": now_utc_iso(),
            "ano_de_construcao": None
        }
        rows.append(row)
    return rows

# -------------------------
# Orchestrator: parse multiple pages and dedupe
# -------------------------
def parse_multiple_pages(base_url: str, pages: int) -> pd.DataFrame:
    all_rows = []
    for p in range(1, pages + 1):
        page_url = build_page_url(base_url, p)
        logger.info("Fetching page %s", page_url)
        try:
            items = parse_search_page(page_url)
            for it in items:
                # ignore empty or neighborhood-like entries
                if not it.get("Valor") and not it.get("M2") and not it.get("Link"):
                    continue
                all_rows.append(it)
        except Exception as e:
            logger.exception("Error parsing page %s", page_url)
        time.sleep(float(st.session_state.settings.get("per_request_delay_sec", 0.6)))
    if not all_rows:
        return pd.DataFrame()
    # dedupe by Link or (Endereco+Valor)
    seen = set()
    clean = []
    for r in all_rows:
        key = (r.get("Link") or "") + "|" + str(r.get("Valor") or "") + "|" + str(r.get("Endereço") or "")
        if key in seen:
            continue
        seen.add(key)
        clean.append(r)
    df = pd.DataFrame(clean)
    # ensure columns order
    cols = ["Endereço", "Rua", "Bairro", "Cidade", "Valor_raw", "Valor", "Condominio_raw", "Condominio",
            "IPTU_raw", "IPTU", "M2", "Quartos", "Suites", "Vaga", "Link", "bairro", "data_coleta", "ano_de_construcao"]
    for c in cols:
        if c not in df.columns:
            df[c] = None
    df = df[cols]
    return df

# -------------------------
# UI
# -------------------------
st.title("🏘️ QuintoAndar — Extrator de resultados → Excel")
st.markdown("Cole a URL da página de busca do QuintoAndar (ex.: bairros). O scraper extrai apenas anúncios reais (ignora bairros/promos) e normaliza valores para Excel.")

col1, col2 = st.columns([4, 1])
with col1:
    url_input = st.text_input("URL da página de busca (QuintoAndar)", value=st.session_state.scrape_url or "", placeholder="https://www.quintoandar.com.br/comprar/imovel/bela-vista-sao-paulo-sp-brasil...")
with col2:
    run_btn = st.button(f"Extrair ({st.session_state.settings['max_pages']} páginas)", use_container_width=True)

with st.sidebar:
    st.header("Opções")
    st.number_input("Páginas a raspar (padrão 5)", min_value=1, max_value=50, value=int(st.session_state.settings["max_pages"]), key="max_pages_input")
    st.slider("Delay entre requests (segundos)", 0.0, 5.0, float(st.session_state.settings.get("per_request_delay_sec", 0.6)), step=0.1, key="delay_input")
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
    pages = int(st.session_state.settings.get("max_pages", 5))
    st.info(f"Iniciando extração de até {pages} páginas (JSON-first).")
    try:
        df = parse_multiple_pages(url_input, pages)
        if df.empty:
            st.warning("Nenhum listing identificado. Verifique se a URL é realmente uma página de resultados e se a página carrega via XHR. Veja diagnóstico abaixo.")
            if st.session_state.last_raw_sample:
                with st.expander("Amostra do HTML (diagnóstico)"):
                    st.text(st.session_state.last_raw_sample[:2000])
        else:
            st.success(f"Extraídos {len(df)} registros (após deduplicação).")
            st.session_state.scrape_df = df
            st.dataframe(df.head(80))
            # ask to continue
            if pages >= 5:
                if st.button("Raspar mais páginas (próximas 5)"):
                    st.session_state.settings["max_pages"] = pages + 5
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
    st.text(st.session_state.get("last_raw_sample", "")[:2000])

st.caption("Notas: O scraper usa JSON embutido (quando disponível) no QuintoAndar para máxima precisão. Valores monetários e áreas são normalizados; campos ausentes ficam como None. Se precisar que eu abra cada anúncio para preencher IPTU/Condomínio/ano de construção, posso adicionar essa opção (será mais lento).")
