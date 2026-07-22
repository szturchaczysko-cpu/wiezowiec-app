import streamlit as st
import vertexai
from vertexai.generative_models import GenerativeModel, Content, Part, SafetySetting, HarmCategory, HarmBlockThreshold
from google.oauth2 import service_account
from datetime import datetime, timedelta
import json, re, pytz, time
import pandas as pd
import firebase_admin
from firebase_admin import credentials, firestore
import requests

# --- MODUŁ FORUM ---
try:
    from forum_module import execute_forum_actions, forum_read, discover_roots, auto_load_forum_context, save_forum_memory, load_forum_memory, check_forum_answer
    FORUM_ENABLED = True
except ImportError:
    FORUM_ENABLED = False

# --- TEST MODE ---
TEST_MODE = True
_COL_PREFIX = "test_" if TEST_MODE else ""
def col(name):
    """Prefixuje nazwę kolekcji w trybie testowym."""
    return f"{_COL_PREFIX}{name}"

# --- KONFIGURACJA ---
st.set_page_config(page_title="🧪 Wieżowiec TEST", layout="wide", page_icon="🧪")

if not firebase_admin._apps:
    creds_dict = json.loads(st.secrets["FIREBASE_CREDS"])
    creds = credentials.Certificate(creds_dict)
    firebase_admin.initialize_app(creds)
db = firestore.client()

# --- AUTO-SEED (test mode) ---
if TEST_MODE:
    _seed_doc = db.collection(col("operator_configs")).document("Sylwia").get()
    if not _seed_doc.exists:
        # Kopiuj config Sylwii z produkcji lub ustaw defaulty
        _prod = db.collection("operator_configs").document("Sylwia").get().to_dict() or {}
        _seed = _prod if _prod else {
            "role": "Operatorzy_DE",
            "prompt_url": "https://raw.githubusercontent.com/szturchaczysko-cpu/szturchacz/refs/heads/main/prompt4624.txt",
            "prompt_name": "v4",
            "assigned_key_index": 1,
            "tel": False,
        }
        db.collection(col("operator_configs")).document("Sylwia").set(_seed, merge=True)
    
    # Seed custom_prompts (prompt forum)
    _prompts_doc = db.collection(col("admin_config")).document("custom_prompts").get()
    if not _prompts_doc.exists or not (_prompts_doc.to_dict() or {}).get("urls"):
        # Kopiuj z produkcji + dodaj prompt forum
        _prod_prompts = db.collection("admin_config").document("custom_prompts").get().to_dict() or {}
        _urls = _prod_prompts.get("urls", {})
        _urls["v4 forum"] = "https://raw.githubusercontent.com/szturchaczysko-cpu/szturchacz-test/refs/heads/main/v4_forum.txt"
        db.collection(col("admin_config")).document("custom_prompts").set({"urls": _urls}, merge=True)

# --- BRAMKA HASŁA ---
if "password_correct" not in st.session_state:
    st.session_state.password_correct = False

if not st.session_state.password_correct:
    st.header("🧪 Wieżowiec TEST — Logowanie")
    pwd = st.text_input("Hasło admina:", type="password")
    if st.button("Zaloguj"):
        if pwd == st.secrets["ADMIN_PASSWORD"]:
            st.session_state.password_correct = True
            st.rerun()
        else:
            st.error("Błędne hasło")
    st.stop()

# --- PROJEKTY GCP ---
try:
    GCP_PROJECTS = list(st.secrets["GCP_PROJECT_IDS"])
except:
    GCP_PROJECTS = []
    st.error("🚨 Brak GCP_PROJECT_IDS w secrets!")

# --- PROMPTY WIEŻOWCA ---
# Wieżowiec używa tej samej listy promptów co operatorzy (z repo szturchacz-test).
# Default = aktualny default warstwy B ustawiony w zakładce 🧪 Prompty.
WIEZOWIEC_PROMPT_URLS = {}

# Dodaj custom prompts z Firestore (legacy)
custom_data = (db.collection(col("admin_config")).document("custom_prompts").get().to_dict() or {}).get("urls", {})
for name, url in custom_data.items():
    WIEZOWIEC_PROMPT_URLS[name] = url


@st.cache_data(ttl=3600)
def get_remote_prompt(url):
    try:
        r = requests.get(url)
        r.raise_for_status()
        return r.text
    except Exception as e:
        st.error(f"Błąd pobierania promptu: {e}")
        return ""


# ==========================================
# FIRESTORE: ZARZĄDZANIE WSADAMI
# ==========================================
# Kolekcja: ew_wsady
# Dokumenty: "swinka", "uszki", "szturchacz"
# Pole: "data" = tekst wsadu, "updated_at" = timestamp

WSADY_COLLECTION = "ew_wsady"

def load_wsad(name):
    """Pobierz wsad z bazy"""
    doc = db.collection(WSADY_COLLECTION).document(name).get()
    if doc.exists:
        return doc.to_dict().get("data", "")
    return ""

def save_wsad(name, data):
    """Zapisz wsad (nadpisz)"""
    db.collection(WSADY_COLLECTION).document(name).set({
        "data": data,
        "updated_at": firestore.SERVER_TIMESTAMP,
    })

def clear_all_wsady():
    """Wyczyść wszystkie wsady"""
    for name in ["swinka", "uszki", "szturchacz"]:
        db.collection(WSADY_COLLECTION).document(name).delete()

def parse_szturchacz_blocks(text):
    """Dzieli tekst szturchacza na bloki per zamówienie (NrZam → tekst bloku).
    
    Rozpoznaje formaty:
    - NrZam: 366000 (z prefiksem)
    - ZN366000 (z prefiksem ZN)
    - 366000 (gołe 6+ cyfrowe numery na początku linii — format tabeli)
    """
    if not text or not text.strip():
        return {}
    
    blocks = {}
    lines = text.split('\n')
    current_block = []
    current_nr = None
    
    for line in lines:
        stripped = line.strip()
        
        # Szukaj NrZam w różnych formatach
        nr_match = None
        
        # Format 1: NrZam: XXXXX lub NrZam XXXXX
        nr_match = re.search(r'NrZam[:\s]+(\S+)', line, re.IGNORECASE)
        
        # Format 2: ZN + cyfry
        if not nr_match:
            nr_match = re.match(r'^(ZN\d+)', stripped)
        
        # Format 3: gołe 5-7 cyfrowe numery na początku linii (format tabeli szturchacza)
        # Nie łap numerów listów przewozowych (13+ cyfr) ani dat (8 cyfr z myślnikami)
        if not nr_match:
            nr_match = re.match(r'^(\d{5,7})\s', stripped)
        
        if nr_match:
            # Zapisz poprzedni blok
            if current_nr and current_block:
                blocks[current_nr] = '\n'.join(current_block)
            # Rozpocznij nowy blok
            candidate = nr_match.group(1).strip().rstrip(',').rstrip('|')
            # Filtruj fałszywe matche (nagłówki tabeli itp.)
            if candidate.lower() in ('data', 'zama', 'nr', 'nrzam', 'mail', 'tel', 'kraj'):
                current_block.append(line) if current_block is not None else None
            else:
                current_nr = candidate
                current_block = [line]
        else:
            if current_block is not None:
                current_block.append(line)
    
    # Zapisz ostatni blok
    if current_nr and current_block:
        blocks[current_nr] = '\n'.join(current_block)
    
    # Jeśli parser nie znalazł bloków, zwróć cały tekst jako jeden blok
    if not blocks and text.strip():
        blocks["_RAW_"] = text.strip()
    
    return blocks

def merge_szturchacz(existing_text, new_text):
    """
    Dopełnij istniejący wsad szturchacza nowymi zamówieniami.
    Jeśli zamówienie o tym samym NrZam istnieje — nadpisz nowszą wersją.
    Jeśli nie istnieje — dodaj.
    """
    existing_blocks = parse_szturchacz_blocks(existing_text)
    new_blocks = parse_szturchacz_blocks(new_text)
    
    # Merge: nowe nadpisują istniejące, reszta pozostaje
    merged = {**existing_blocks, **new_blocks}
    
    added = len([k for k in new_blocks if k not in existing_blocks])
    updated = len([k for k in new_blocks if k in existing_blocks])
    
    # Złóż z powrotem w tekst
    merged_text = '\n\n'.join(merged.values())
    
    return merged_text, added, updated, len(merged)

def count_lines(text):
    """Policz ile zamówień (bloków) jest w tekście"""
    if not text or not text.strip():
        return 0
    blocks = parse_szturchacz_blocks(text)
    # Nie licz klucza _RAW_ jako zamówienia
    count = len([k for k in blocks if k != "_RAW_"])
    return max(count, 1 if text.strip() and count == 0 else 0)


# ==========================================
# PARSER WYJŚCIA WIEŻOWCA (bez zmian)
# ==========================================
def parse_wiezowiec_output(text):
    cases = []
    current_grupa = None
    grupa_patterns = {
        "DE": r'▬+\s*OPERATORZY\s+DE',
        "FR": r'▬+\s*OPERATORZY\s+FR',
        "UKPL": r'▬+\s*OPERATORZY\s+UKPL',
    }
    lines = text.split('\n')
    i = 0
    while i < len(lines):
        line = lines[i].strip()
        for grupa, pattern in grupa_patterns.items():
            if re.search(pattern, line):
                current_grupa = grupa
                break
        # Nagłówek: [SCORE=XXX] ikona | ...
        score_match = re.match(r'^\[SCORE=(\d+)\]\s*([🔴🟡⚪📦])\s*\|\s*(.*)', line)
        if not score_match:
            # Alternatywny format: ikona [score] | ...
            score_match = re.match(r'^([🔴🟡⚪📦])\s*\[(\d+)\]\s*\|\s*(.*)', line)
            if score_match:
                icon = score_match.group(1)
                score = int(score_match.group(2))
                label = score_match.group(3).strip()
            else:
                score_match = None
        else:
            score = int(score_match.group(1))
            icon = score_match.group(2)
            label = score_match.group(3).strip()
        
        if score_match and current_grupa:
            naglowek = line
            i += 1
            blok_lines = []
            # Zbierz linie: punktacja + pełna linia szturchacza
            while i < len(lines):
                nl = lines[i].strip()
                if nl == '---' or nl.startswith('▬') or nl.startswith('═══'):
                    break
                if re.match(r'^\[SCORE=\d+\]', nl) or re.match(r'^[🔴🟡⚪📦]\s*\[\d+\]', nl):
                    break
                if nl:
                    blok_lines.append(lines[i])
                i += 1
            
            pelna_linia = '\n'.join(blok_lines).strip()
            
            # Wyciągnij numer zamówienia
            numer = None
            for p in [r'NrZam[:\s]+(\S+)', r'Nr\s*Zam[:\s]+(\S+)', r'(ZN\d+)', r'(ZW\d+[/]\d+)']:
                m = re.search(p, pelna_linia, re.IGNORECASE)
                if m:
                    numer = m.group(1).strip().rstrip(',').rstrip('|')
                    break
            
            # Fallback: szukaj gołego 5-7 cyfrowego numeru na początku linii (format tabeli)
            if not numer:
                for bl in blok_lines:
                    m = re.match(r'^\s*(\d{5,7})\s', bl)
                    if m:
                        numer = m.group(1)
                        break
            
            # Fallback 2: szukaj gołego numeru gdziekolwiek w nagłówku lub label
            if not numer:
                for src in [naglowek, label]:
                    m = re.search(r'(\d{5,7})', src)
                    if m:
                        numer = m.group(1)
                        break
            
            idx_m = re.search(r'Index:\s*(\S+)', label)
            index_handlowy = idx_m.group(1) if idx_m else ""
            if not index_handlowy:
                lindx_m = re.search(r'lindexy[:\s]+(\S+)', pelna_linia, re.IGNORECASE)
                if lindx_m:
                    index_handlowy = lindx_m.group(1)
            
            if pelna_linia and numer:
                cases.append({
                    "numer_zamowienia": numer,
                    "score": score,
                    "priority_icon": icon,
                    "priority_label": label,
                    "grupa": current_grupa,
                    "index_handlowy": index_handlowy,
                    "pelna_linia_szturchacza": pelna_linia,
                    "naglowek_priorytetowy": naglowek,
                })
            continue
        
        if 'ALERT' in line and 'BRAK W SZTURCHACZU' in line:
            i += 1
            while i < len(lines) and not lines[i].strip().startswith('═══'):
                i += 1
            continue
        i += 1
    return cases


# ==========================================
# GŁÓWNY INTERFEJS
# ==========================================
st.title("🧪 Wieżowiec TEST (forum)")
st.caption("System zarządzania priorytetami — wsady z pamięcią")

# --- Funkcje autopilota (globalne — używane przez oba taby) ---
AUTOPILOT_DOC = db.collection(col("autopilot_config")).document("status")

def get_autopilot_status():
    try:
        doc = AUTOPILOT_DOC.get()
        if doc.exists:
            return doc.to_dict()
    except Exception:
        pass
    return {"state": "idle", "processed": 0, "total": 0, "current_nrzam": "", "last_error": ""}

def set_autopilot_status(data):
    AUTOPILOT_DOC.set(data, merge=True)

GRUPA_MAP_GLOBAL = {"DE": "Operatorzy_DE", "FR": "Operatorzy_FR", "UKPL": "Operatorzy_UK/PL"}

def build_autopilot_queue(percent, obsada, ap_work_date_str):
    """Buduje kolejkę autopilota: top X% casów globalnie po score, round-robin per grupa."""
    all_wolne_docs = db.collection(col("ew_cases")).where("status", "==", "wolny").get()
    wolne = []
    for cdoc in all_wolne_docs:
        cdata = cdoc.to_dict()
        cdata["_doc_id"] = cdoc.id
        # Case w WORECZKU telefonicznym NIE wchodzi do puli standardu — jest obsługiwany telefonem,
        # nie może być przerabiany 2x (raz w standardzie, raz przez woreczek).
        if cdata.get("telefon_do_wykonania"):
            continue
        if cdata.get("autopilot_status") != "calculated":
            g = cdata.get("grupa", "")
            if g in obsada:  # tylko grupy z obsadą
                wolne.append(cdata)
    
    # Sortuj GLOBALNIE po score (mieszaj grupy)
    wolne.sort(key=lambda c: -c.get("score", 0))
    
    # Weź top X%
    count = max(1, int(len(wolne) * percent / 100)) if wolne else 0
    top_cases = wolne[:count]
    
    # Round-robin per grupa
    group_counters = {g: 0 for g in obsada}
    case_queue = []
    
    for wc in top_cases:
        g = wc.get("grupa", "")
        if g not in obsada or not obsada[g]:
            continue
        ops = obsada[g]
        assigned_op = ops[group_counters[g] % len(ops)]
        group_counters[g] += 1
        case_queue.append({
            "doc_id": wc["_doc_id"],
            "nrzam": wc.get("numer_zamowienia", "?"),
            "operator": assigned_op,
            "grupa": g,
            "grupa_operatorska": GRUPA_MAP_GLOBAL.get(g, "Operatorzy_DE"),
        })
        db.collection(col("ew_cases")).document(wc["_doc_id"]).update({
            "autopilot_assigned_to": assigned_op,
        })
    
    return case_queue, len(wolne)

# --- LISTA PROMPTÓW Z GITHUBA (real-time, używana w panelu Prompty) ---
@st.cache_data(ttl=600)  # 10 min: mniej zapytan do GitHub API -> rzadziej rate limit
def _fetch_github_prompts(force_refresh=False):
    import requests as _req
    try:
        _gh_token = st.secrets.get("GITHUB_TOKEN", None)
        _headers = {"Authorization": f"token {_gh_token}"} if _gh_token else {}
        _api_url = "https://api.github.com/repos/szturchaczysko-cpu/szturchacz-test/contents/"
        _r = _req.get(_api_url, headers=_headers, timeout=10)
        _r.raise_for_status()
        _files = _r.json()
        _prompts = []
        for _f in _files:
            if _f.get("name", "").endswith(".txt"):
                _prompts.append({
                    "name": _f["name"].replace(".txt", "").replace("_", " "),
                    "filename": _f["name"],
                    "raw_url": _f["download_url"],
                    "github_link": _f["html_url"],
                    "sha": _f.get("sha", ""),
                })
        _prompts.sort(key=lambda x: x["filename"], reverse=True)
        # Zapamietaj OSTATNIA DOBRA liste w Firestore -> lista dziala nawet gdy GitHub API zwroci
        # rate limit (przezywa restart apki, bez tokenu). Odswieza sie przy kazdym udanym pobraniu.
        try:
            db.collection(col("admin_config")).document("github_prompts_cache").set(
                {"prompts": _prompts}, merge=True)
        except Exception:
            pass
        return _prompts
    except Exception as _e:
        # rate limit / blad -> ostatnia dobra lista z Firestore (skoro dzialalo wczesniej, jest zapisana)
        try:
            _fallback = (db.collection(col("admin_config")).document("github_prompts_cache")
                         .get().to_dict() or {}).get("prompts")
            if _fallback:
                return _fallback
        except Exception:
            pass
        return {"error": str(_e)}


# ==========================================
# 📊 WSPÓLNE FUNKCJE STATYSTYK (trwała kronika — Dolewka + Diamentoza, ta sama mechanika)
# ==========================================
from datetime import timedelta as _td_stat

_GRUPA_KEYS = ["DE", "FR", "UKPL"]
_GRUPA_FLAGS = {"DE": "🇩🇪", "FR": "🇫🇷", "UKPL": "🇬🇧"}


def _stat_date_list(d_from, d_to):
    out, cur = [], d_from
    while cur <= d_to:
        out.append(cur.strftime("%Y-%m-%d"))
        cur += _td_stat(days=1)
    return out


def read_chronicle_operators(d_from, d_to):
    """Suma per-operator z TRWAŁEGO ew_operator_stats po zakresie dat.
    cases_completed = WSZYSTKIE ruchy (standard+odwrotne); kanały = rozbicie."""
    agg = {}
    for ds in _stat_date_list(d_from, d_to):
        try:
            ops = db.collection(col("ew_operator_stats")).document(ds).collection("operators").stream()
        except Exception:
            continue
        for odoc in ops:
            d = odoc.to_dict() or {}
            a = agg.setdefault(odoc.id, {"pobrane": 0, "zakonczone": 0, "pominiete": 0,
                                         "wa": 0, "mail": 0, "forum": 0, "standard": 0,
                                         "poza_planem": 0, "grupa": "?"})
            a["pobrane"] += int(d.get("cases_taken", 0) or 0)
            a["zakonczone"] += int(d.get("cases_completed", 0) or 0)
            a["pominiete"] += int(d.get("cases_skipped", 0) or 0)
            a["wa"] += int(d.get("cases_completed_wa", 0) or 0)
            a["mail"] += int(d.get("cases_completed_mail", 0) or 0)
            a["forum"] += int(d.get("cases_completed_forum", 0) or 0)
            a["standard"] += int(d.get("cases_completed_standard", 0) or 0)
            a["poza_planem"] += int(d.get("poza_planem", 0) or 0)
            if d.get("grupa"):
                a["grupa"] = d.get("grupa")
    return agg


def read_chronicle_group_daily(d_from, d_to):
    """Per dzień (ds) -> per grupa: plan (total/odsiane/obrabialne) + zakonczone/pominiete/autopilot.
    Plan z ew_operator_stats/{ds}.plan; liczniki z płaskich kluczy gz_/gp_/apc_ (Increment)."""
    rows = {}
    for ds in _stat_date_list(d_from, d_to):
        try:
            data = db.collection(col("ew_operator_stats")).document(ds).get().to_dict() or {}
        except Exception:
            data = {}
        plan = data.get("plan", {}) or {}
        day = {}
        for g in _GRUPA_KEYS:
            gp = plan.get(g, {}) or {}
            day[g] = {
                "total": int(gp.get("total", 0) or 0),
                "odsiane": int(gp.get("odsiane", 0) or 0),
                "obrabialne": int(gp.get("obrabialne", 0) or 0),
                "zakonczone": int(data.get(f"gz_{g}", 0) or 0),
                "pominiete": int(data.get(f"gp_{g}", 0) or 0),
                "autopilot": int(data.get(f"apc_{g}", 0) or 0),
                "has_plan": bool(gp),
            }
        rows[ds] = day
    return rows


@st.cache_data(ttl=120)
def _fetch_diamonds_range(d_from_s, d_to_s, prefix):
    """Trwałe diamenty z {prefix}ew_diamond_log w zakresie (cache 120s)."""
    out = []
    try:
        d_f = datetime.strptime(d_from_s, "%Y-%m-%d").date()
        d_t = datetime.strptime(d_to_s, "%Y-%m-%d").date()
    except Exception:
        return out
    cur = d_f
    while cur <= d_t:
        ds = cur.strftime("%Y-%m-%d")
        try:
            for doc in db.collection(f"{prefix}ew_diamond_log").document(ds).collection("numbers").stream():
                data = doc.to_dict() or {}
                out.append({
                    "date_str": data.get("date_str", ds),
                    "numer": str(data.get("numer_zamowienia", doc.id)).strip(),
                    "operator": data.get("operator", "?"),
                    "source_type": str(data.get("source_type", "operator")),
                    "grupa": str(data.get("grupa") or "").upper(),
                    "typ_zlecenia": data.get("typ_zlecenia", "inne"),
                    "czy_diament": data.get("czy_diament"),
                })
        except Exception:
            pass
        cur += _td_stat(days=1)
    return out


@st.cache_data(ttl=120)
def _fetch_phone_log_range(d_from_s, d_to_s, prefix):
    """Trwałe telefony z {prefix}ew_phone_log w zakresie (cache 120s)."""
    out = []
    try:
        d_f = datetime.strptime(d_from_s, "%Y-%m-%d").date()
        d_t = datetime.strptime(d_to_s, "%Y-%m-%d").date()
    except Exception:
        return out
    cur = d_f
    while cur <= d_t:
        ds = cur.strftime("%Y-%m-%d")
        try:
            for doc in db.collection(f"{prefix}ew_phone_log").document(ds).collection("calls").stream():
                data = doc.to_dict() or {}
                out.append({
                    "date_str": data.get("data_str", ds),
                    "godzina": str(data.get("godzina", "")),
                    "numer": str(data.get("numer_zamowienia", "")).strip(),
                    "operator": data.get("operator", "?"),
                    "grupa": str(data.get("grupa") or "?").upper(),
                    "wynik": data.get("wynik", "kontakt_bez_konkretu"),
                    "kurier_ustalony": bool(data.get("kurier_ustalony")),
                    "zrodlo": data.get("zrodlo", "operator_dzwoniacy"),
                })
        except Exception:
            pass
        cur += _td_stat(days=1)
    return out


def _is_auto_src(src):
    """Czatoszturek = źródło zaczynające się od 'auto' (NIE pole operator)."""
    return str(src or "").strip().lower().startswith("auto")


def _is_czato_row(r):
    """Spójne z is_czatoszturek() w Diamentozie: source 'auto*' LUB (reczny + operator=='Czatoszturek')."""
    s = str(r.get("source_type", "")).strip().lower()
    if s.startswith("auto"):
        return True
    if s == "reczny" and str(r.get("operator", "")).strip() == "Czatoszturek":
        return True
    return False


def _is_diament_row(r):
    if r.get("czy_diament") is not None:
        return bool(r.get("czy_diament"))
    return r.get("typ_zlecenia", "inne") not in ("zmiana", "cofniete", "ponowienie")


def diamonds_human_by_op(rows):
    out = {}
    for r in rows:
        if _is_diament_row(r) and not _is_czato_row(r):
            out[r["operator"]] = out.get(r["operator"], 0) + 1
    return out


def diamonds_czato_by_grupa(rows):
    out = {g: 0 for g in _GRUPA_KEYS}
    for r in rows:
        if _is_diament_row(r) and _is_czato_row(r):
            if r["grupa"] in out:
                out[r["grupa"]] += 1
    return out


# ---------- KAFELEK GRUPOWY ----------
def _render_group_box(label, d):
    total, odr, zak, pom = d["total"], d["odroczony"], d["zakonczony"], d["pominiety"]
    obr = total - odr  # obrabialne (pominięte ZOSTAJĄ w obrabialnych → obniżają %)
    todo = max(0, obr - zak - pom)  # do zrobienia: w toku / czeka w kolejce (niedomknięte, niepominięte)
    pct = round(zak / obr * 100, 1) if obr > 0 else 0.0
    pct_pom = round(pom / obr * 100, 1) if obr > 0 else 0.0
    st.markdown(f"**{label}**")
    st.markdown(f"ogółem **{total}** · w przyszłości **{odr}** · obrabialne **{obr}**")
    st.markdown(f"✅ zakończone **{zak}/{obr}** ({pct}%)")
    st.markdown(f"⏭️ pominięte **{pom}** ({pct_pom}%)")
    st.markdown(f"⏳ do zrobienia **{todo}** (w toku / czeka)")
    st.progress(min(pct / 100, 1.0))


def render_group_summary_now():
    """Kafelek grupowy — STAN BIEŻĄCY z żywej puli ew_cases (per grupa + cała firma)."""
    try:
        docs = db.collection(col("ew_cases")).limit(8000).get()
    except Exception:
        docs = []
    g = {x: {"total": 0, "odroczony": 0, "zakonczony": 0, "pominiety": 0} for x in _GRUPA_KEYS}
    firma = {"total": 0, "odroczony": 0, "zakonczony": 0, "pominiety": 0}
    for d in docs:
        dd = d.to_dict()
        grp = dd.get("grupa", "")
        if grp not in g:
            continue
        s = dd.get("status", "wolny")
        g[grp]["total"] += 1
        firma["total"] += 1
        if s in ("odroczony", "zakonczony", "pominiety"):
            key = "odroczony" if s == "odroczony" else ("zakonczony" if s == "zakonczony" else "pominiety")
            g[grp][key] += 1
            firma[key] += 1
    cols = st.columns(3)
    for c, gname in zip(cols, _GRUPA_KEYS):
        with c:
            _render_group_box(f"{_GRUPA_FLAGS[gname]} {gname}", g[gname])
    _render_group_box("🏢 CAŁA FIRMA", firma)


def _render_group_box_range(label, d, ndays):
    total, odr, obr, zak, pom = d["total"], d["odsiane"], d["obrabialne"], d["zakonczone"], d["pominiete"]
    todo = max(0, obr - zak - pom)  # do zrobienia: w toku / czeka (niedomknięte, niepominięte)
    pct = round(zak / obr * 100, 1) if obr > 0 else 0.0
    pct_pom = round(pom / obr * 100, 1) if obr > 0 else 0.0
    st.markdown(f"**{label}**")
    st.markdown(f"ogółem **{total}** · w przyszłości **{odr}** · obrabialne **{obr}**")
    st.markdown(f"✅ zakończone **{zak}/{obr}** ({pct}%)")
    st.markdown(f"⏭️ pominięte **{pom}** ({pct_pom}%)")
    st.markdown(f"⏳ do zrobienia **{todo}** (w toku / czeka)")
    st.progress(min(pct / 100, 1.0))
    if ndays > 1 and obr > 0:
        st.caption(f"Σ {ndays} dni · średnio/dzień: obrabialne {round(obr / ndays)}, zakończone {round(zak / ndays)}")


def _group_counts_from_pool():
    """Per-grupa liczby z ŻYWEJ puli ew_cases — dla DZIŚ (pula jest kompletna do czyszczenia).
    Zwraca strukturę zgodną z trybem zakresu: total/odsiane/obrabialne/zakonczone/pominiete."""
    try:
        docs = db.collection(col("ew_cases")).limit(8000).get()
    except Exception:
        docs = []
    raw = {x: {"total": 0, "odroczony": 0, "zakonczony": 0, "pominiety": 0} for x in _GRUPA_KEYS}
    for d in docs:
        dd = d.to_dict()
        grp = dd.get("grupa", "")
        if grp not in raw:
            continue
        raw[grp]["total"] += 1
        s = dd.get("status", "wolny")
        if s in ("odroczony", "zakonczony", "pominiety"):
            raw[grp][s] += 1
    out = {}
    for g in _GRUPA_KEYS:
        t, od = raw[g]["total"], raw[g]["odroczony"]
        out[g] = {"total": t, "odsiane": od, "obrabialne": t - od,
                  "zakonczone": raw[g]["zakonczony"], "pominiete": raw[g]["pominiety"], "has_plan": t > 0}
    return out


def render_group_summary_range(d_from, d_to):
    """Kafelek grupowy — ZAKRES DAT. Kafelki liczą TYLKO dni ZAKOŃCZONE z planem (bez dziś,
    bez dni bez planu) → % i średnia nie są zaniżone. Dziś (w toku) pokazany OSOBNO."""
    today_s = datetime.now(pytz.timezone('Europe/Warsaw')).date().strftime("%Y-%m-%d")
    include_today = d_from.strftime("%Y-%m-%d") <= today_s <= d_to.strftime("%Y-%m-%d")
    daily = read_chronicle_group_daily(d_from, d_to)
    agg = {x: {"total": 0, "odsiane": 0, "obrabialne": 0, "zakonczone": 0, "pominiete": 0} for x in _GRUPA_KEYS}
    firma = {"total": 0, "odsiane": 0, "obrabialne": 0, "zakonczone": 0, "pominiete": 0}
    ndays = 0
    for ds, day in daily.items():
        if include_today and ds == today_s:
            continue  # dziś osobno (niepełny)
        # tylko dni z PLANEM wchodzą do podsumowania planu
        if not any(day[g]["has_plan"] for g in _GRUPA_KEYS):
            continue
        for gname in _GRUPA_KEYS:
            dd = day[gname]
            for k in ("total", "odsiane", "obrabialne", "zakonczone", "pominiete"):
                agg[gname][k] += dd[k]
                firma[k] += dd[k]
        ndays += 1
    if ndays > 0:
        cols = st.columns(3)
        for c, gname in zip(cols, _GRUPA_KEYS):
            with c:
                _render_group_box_range(f"{_GRUPA_FLAGS[gname]} {gname}", agg[gname], ndays)
        _render_group_box_range("🏢 CAŁA FIRMA", firma, ndays)
        st.caption(f"☑️ Powyżej: {ndays} dni ZAKOŃCZONYCH z planem (bez dzisiejszego). Średnia/dzień liczona "
                   "tylko z tych dni — dzięki temu nie jest zaniżona niepełnym dniem.")
    else:
        st.info("Brak zakończonych dni z planem w tym zakresie (poza dzisiejszym).")
    # DZIŚ — osobno, kompaktowo, NIE wliczane do sum/średnich
    if include_today:
        pool = _group_counts_from_pool()
        parts = []
        for gname in _GRUPA_KEYS:
            p = pool[gname]
            obr, zak, pom = p["obrabialne"], p["zakonczone"], p["pominiete"]
            todo = max(0, obr - zak - pom)
            pct = f"{round(zak / obr * 100)}%" if obr > 0 else "—"
            parts.append(f"**{gname}** {zak}/{obr} ({pct}), do zrobienia {todo}")
        st.markdown("📍 **Dziś (w toku — z żywej puli, liczony OSOBNO):** " + " · ".join(parts))
        st.caption("Dzień się jeszcze nie skończył → NIE wliczamy go do powyższych kafelków/średnich, "
                   "żeby nie zaniżać. Pełny obraz dnia bieżącego masz w zakładce „Dolewka + Status”.")


# ---------- TABELA OPERATORA (skuteczność) ----------
def render_operator_table(d_from, d_to, key_prefix=""):
    ops_agg = read_chronicle_operators(d_from, d_to)
    diamonds = _fetch_diamonds_range(d_from.strftime("%Y-%m-%d"), d_to.strftime("%Y-%m-%d"), _COL_PREFIX)
    diam_human = diamonds_human_by_op(diamonds)
    # Czatoszturek: liczymy TYLKO ruchy zakończone diamentem (zamówiony kurier) — jeden łączny
    # wynik, bez podziału na grupy i BEZ nocnych draftów. To jego realny wkład do podsumowania.
    czato_diamenty = sum(1 for r in diamonds if _is_diament_row(r) and _is_czato_row(r))

    # "W toku" — migawka z żywej puli (nie sumujemy po dniach)
    wtoku = {}
    try:
        for d in db.collection(col("ew_cases")).where("status", "in", ["przydzielony", "w_toku"]).limit(8000).get():
            dd = d.to_dict()
            if dd.get("assigned_to"):
                wtoku[dd["assigned_to"]] = wtoku.get(dd["assigned_to"], 0) + 1
    except Exception:
        # fallback: pełny skan (gdyby filtr 'in' wymagał indeksu w danym projekcie)
        try:
            for d in db.collection(col("ew_cases")).limit(8000).get():
                dd = d.to_dict()
                if dd.get("status") in ("przydzielony", "w_toku") and dd.get("assigned_to"):
                    wtoku[dd["assigned_to"]] = wtoku.get(dd["assigned_to"], 0) + 1
        except Exception:
            pass

    # operator z diamentami, ale bez ruchu w kronice → też wiersz
    for op in diam_human:
        ops_agg.setdefault(op, {"pobrane": 0, "zakonczone": 0, "pominiete": 0,
                                "wa": 0, "mail": 0, "forum": 0, "standard": 0, "grupa": "?"})

    # skuteczność HUMAN = KONWERSJA NA KURIERA: 💎 diamenty ÷ wszystkie ZAKOŃCZONE ruchy
    # (standardowe + WSZYSTKIE odwrotne — łącznie ze spoza puli) = a["zakonczone"].
    # Diament powstaje podczas domykanego ruchu (kurier = zamknięty ruch), więc ≤100% — cichy
    # cap, bez ⚠️. Ruchy odwrotne spoza puli SĄ w mianowniku (są częścią a["zakonczone"]).
    eff = {}
    for op, a in ops_agg.items():
        moves = a["zakonczone"]   # standardowe + wszystkie odwrotne
        eff[op] = (diam_human.get(op, 0) / moves * 100) if moves > 0 else None
    ranked = sorted([(op, min(e, 100.0)) for op, e in eff.items() if e is not None], key=lambda x: -x[1])
    top3 = set(op for op, _ in ranked[:3])

    rows = []
    for op, a in sorted(ops_agg.items(), key=lambda x: -x[1]["zakonczone"]):
        diam = diam_human.get(op, 0)
        rev = a["wa"] + a["mail"] + a["forum"]      # odwrotne (osobny tor, łącznie ze spoza puli)
        zak_std = a["zakonczone"] - rev              # ZAKOŃCZONE = tylko standardowe domknięcia
        raw = eff[op]
        if raw is None:
            sk = "n/d"
        else:
            shown = min(round(raw), 100)             # cichy cap do 100% (kurier = domknięty ruch)
            sk = f"{shown}%" + (" ⭐" if op in top3 else "")
        rows.append({
            "Operator": op, "Grupa": a["grupa"],
            "📥 Pobrane": a["pobrane"], "✅ Zakończone": zak_std, "⏭️ Pominięte": a["pominiete"],
            "🔄 W toku": wtoku.get(op, 0), "🔁 Odwrotne": rev,
            "💎 Diamenty": diam, "🎯 Skuteczność": sk,
        })

    # 🤖 Czatoszturek — JEDEN wiersz, bez podziału na grupy. Liczone TYLKO ruchy zakończone
    # diamentem (zamówiony kurier) — nocne drafty pominięte. Pokazujemy zawsze (0 = prawdziwa liczba).
    rows.append({
        "Operator": "🤖 Czatoszturek", "Grupa": "—",
        "📥 Pobrane": "—", "✅ Zakończone": czato_diamenty, "⏭️ Pominięte": "—",
        "🔄 W toku": "—", "🔁 Odwrotne": "—",
        "💎 Diamenty": czato_diamenty, "🎯 Skuteczność": "—",
    })

    if rows:
        st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)
        st.caption("🎯 Skuteczność = 💎 diamenty ÷ wszystkie ZAKOŃCZONE ruchy (✅ standardowe + 🔁 odwrotne, "
                   "łącznie ze spoza puli). Czyli na ile domkniętych ruchów przypadł zamówiony kurier. "
                   "Kurier = domknięty ruch, więc ≤100%. ⭐ = TOP 3. "
                   "🤖 Czatoszturek liczony TYLKO z ruchów zakończonych diamentem — nocne drafty pominięte.")
        st.caption("ℹ️ ✅ Zakończone = tylko ruchy STANDARDOWE; 🔁 Odwrotne = osobny tor (WA/MAIL/FORUM). "
                   "Oba liczą się do mianownika skuteczności. Ruchy poza dzisiejszym planem są w osobnej "
                   "tabeli „➕ Poza planem” poniżej.")
    else:
        st.info("Brak danych operatorów w wybranym zakresie.")


# ---------- ROZBICIE WSADÓW ODWROTNYCH (per osoba) ----------
def render_reverse_breakdown(d_from, d_to, key_prefix=""):
    ops_agg = read_chronicle_operators(d_from, d_to)
    rows = []
    for op, a in sorted(ops_agg.items(), key=lambda x: -(x[1]["wa"] + x[1]["mail"] + x[1]["forum"])):
        s = a["wa"] + a["mail"] + a["forum"]
        if s == 0:
            continue
        rows.append({"Operator": op, "Grupa": a["grupa"], "📱 WA": a["wa"],
                     "✉️ MAIL": a["mail"], "💬 FORUM": a["forum"], "Σ Odwrotne": s})
    if rows:
        st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)
    else:
        st.caption("Brak wsadów odwrotnych (WA/MAIL/FORUM) w wybranym zakresie.")


# ---------- POZA PLANEM (ruchy na case'ach spoza dnia planu) ----------
def render_poza_planem(d_from, d_to, key_prefix=""):
    """Ruchy (kliknięcia „Zakończ") na case'ach, których data_obrobki ≠ dzień domknięcia —
    zaległości z innych dni i wsady odwrotne nieprzewidziane na dziś. Liczy RUCHY (nie sprawy):
    ten sam case obrobiony 3× = 3. NIE wchodzi do „% z planu". Per operator + per grupa."""
    ops_agg = read_chronicle_operators(d_from, d_to)
    rows = []
    per_grupa = {}
    for op, a in sorted(ops_agg.items(), key=lambda x: -x[1].get("poza_planem", 0)):
        pp = a.get("poza_planem", 0)
        if pp == 0:
            continue
        g = a["grupa"]
        rows.append({"Operator": op, "Grupa": g, "➕ Ruchy poza planem": pp})
        per_grupa[g] = per_grupa.get(g, 0) + pp
    if rows:
        st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)
        if per_grupa:
            podsum = " · ".join(f"{g}: {n}" for g, n in sorted(per_grupa.items()))
            st.caption(f"Σ per grupa — {podsum}. Liczone w RUCHACH (kliknięciach), nie sprawach: ten sam "
                       "case dobity kilka razy liczy się tyle razy (operator poświęcił czas). To robota poza "
                       "dzisiejszym planem (zaległości + odwrotne spoza dziś) — NIE wchodzi do „% z planu”.")
    else:
        st.caption("Brak ruchów poza planem w wybranym zakresie (wszystko domknięte w swoim dniu planu). "
                   "Licznik nalicza się od wdrożenia tej wersji.")


# ---------- 📞 TELEFONY (moduł Telefony — Etap 1: liczenie) ----------
def render_phone_stats(d_from, d_to, key_prefix=""):
    """Operatorzy dzwoniący: wykonane / przełożenia / % efektywnych / diamentofony.
    + kubełek zewnętrzny (telefoniści spoza systemu) per kraj. Diamentofon = telefon 'konkret'
    z kurier_ustalony, którego NUMER ma realnego kuriera w ew_diamond_log (dopięcie po numerze,
    bez okna). Jeden diamentofon na numer (ostatni telefon-kurier po dacie+godzinie)."""
    df_s, dt_s = d_from.strftime("%Y-%m-%d"), d_to.strftime("%Y-%m-%d")
    calls = _fetch_phone_log_range(df_s, dt_s, _COL_PREFIX)
    diamonds = _fetch_diamonds_range(df_s, dt_s, _COL_PREFIX)
    diam_numers = set(r["numer"] for r in diamonds if _is_diament_row(r) and r["numer"])

    # dopięcie diamentofonów po numerze (ostatni telefon-kurier na numer)
    best_by_numer = {}
    for c in calls:
        if not c["kurier_ustalony"] or not c["numer"]:
            continue
        prev = best_by_numer.get(c["numer"])
        if prev is None or (c["date_str"], c["godzina"]) > (prev["date_str"], prev["godzina"]):
            best_by_numer[c["numer"]] = c
    diamentofon_op, diamentofon_kraj = {}, {}
    for numer, c in best_by_numer.items():
        if numer not in diam_numers:
            continue  # telefon twierdził „kurier", ale realnego kuriera brak → nie diamentofon
        if c["zrodlo"] == "telefonista_zewn":
            diamentofon_kraj[c["grupa"]] = diamentofon_kraj.get(c["grupa"], 0) + 1
        else:
            diamentofon_op[c["operator"]] = diamentofon_op.get(c["operator"], 0) + 1

    # agregacja per operator dzwoniący + telefony telefonistów per kraj
    ops, ext_wykonane = {}, {}
    for c in calls:
        if c["zrodlo"] == "telefonista_zewn":
            ext_wykonane[c["grupa"]] = ext_wykonane.get(c["grupa"], 0) + 1
            continue
        a = ops.setdefault(c["operator"], {"wykonane": 0, "przelozenia": 0, "konkret": 0, "grupa": c["grupa"]})
        a["wykonane"] += 1
        if c["wynik"] == "przelozenie":
            a["przelozenia"] += 1
        if c["wynik"] == "konkret":
            a["konkret"] += 1
        if c["grupa"] != "?":
            a["grupa"] = c["grupa"]

    st.markdown("##### 👤 Operatorzy dzwoniący")
    if ops:
        ranked = []
        rows = []
        for op, a in ops.items():
            dia = diamentofon_op.get(op, 0)
            eff = round(a["konkret"] / a["wykonane"] * 100) if a["wykonane"] else 0
            diap = round(dia / a["wykonane"] * 100) if a["wykonane"] else 0
            ranked.append((op, diap))
            rows.append({"_op": op, "Operator": op, "Grupa": a["grupa"],
                         "📞 Wykonane": a["wykonane"], "🔁 Przełożenia": a["przelozenia"],
                         "✅ Efektywne": a["konkret"], "🎯 % efekt.": f"{eff}%",
                         "💎📞 Diamentofony": dia, "🏆 % diamentof.": f"{diap}%"})
        top3 = set(op for op, v in sorted(ranked, key=lambda x: -x[1])[:3] if v > 0)
        for r in rows:
            if r["_op"] in top3 and r["💎📞 Diamentofony"] > 0:
                r["🏆 % diamentof."] += " ⭐"
            del r["_op"]
        rows.sort(key=lambda r: -r["💎📞 Diamentofony"])
        st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)
        st.caption("🎯 % efekt. = ✅ efektywne (konkret — padła data/decyzja) ÷ 📞 wykonane. "
                   "🏆 % diamentof. = 💎📞 diamentofony ÷ wykonane (ranking „kto najefektywniejszy”, ⭐ TOP 3). "
                   "Diamentofon = telefon, którego numer dostał REALNEGO kuriera (dopięcie po numerze). "
                   "Nieodebrane liczą się do 📞 wykonanych (próba była).")
    else:
        st.info("Brak zarejestrowanych telefonów operatorów dzwoniących w tym zakresie. "
                "Licznik nalicza się od wdrożenia tej wersji.")

    st.markdown("##### 📞 Telefoniści (spoza systemu) — kurierzy per kraj")
    kraje = ["DE", "FR", "UKPL"]
    if any(diamentofon_kraj.get(k, 0) or ext_wykonane.get(k, 0) for k in kraje):
        erows = [{"Kraj": k, "📞 Wciągnięte telefony": ext_wykonane.get(k, 0),
                  "💎📞 Diamentofony (kurierzy)": diamentofon_kraj.get(k, 0)} for k in kraje]
        st.dataframe(pd.DataFrame(erows), use_container_width=True, hide_index=True)
        st.caption("Ile kurierów dowiozły telefony telefonistów (spoza aplikacji), wciągnięte z forum przez "
                   "operatorów. Pokazuje przeciek: kurierzy z telefonów, których NIE zrobiliśmy przez system. "
                   "Łapie tylko telefony oznaczone „spoza systemu” i powiązane z realnym kurierem po numerze.")
    else:
        st.caption("Brak wciągniętych telefonów telefonistów (spoza systemu) w tym zakresie.")


def render_woreczek_stats(d_from, d_to, key_prefix=""):
    """Stan WORECZKA telefonicznego (moduł Telefony) + audyt usunięć.
    Live: ile spraw czeka/odroczonych per grupa, co POMINIĘTE (≥3 próby lub >24h w woreczku).
    Audyt: kto usunął z woreczka (trwały log ew_woreczek_log, przeżywa codzienny reset wsadu)."""
    st.markdown("##### 📞 Stan woreczka telefonicznego")
    # --- LIVE: aktualny woreczek (where telefon_do_wykonania==True) ---
    tz_pl = pytz.timezone("Europe/Warsaw")
    now_pl = datetime.now(tz_pl)
    _GRUPY = ["DE", "FR", "UKPL"]
    agg = {g: {"czeka": 0, "odroczony": 0, "pominiete": 0} for g in _GRUPY}
    pominiete_rows = []
    try:
        _docs = list(db.collection(col("ew_cases")).where("telefon_do_wykonania", "==", True).limit(500).stream())
    except Exception as _e:
        _docs = []
        st.caption(f"(nie udało się odczytać woreczka: {_e})")
    for _d in _docs:
        w = _d.to_dict() or {}
        g = w.get("grupa", "?")
        if g not in agg:
            continue
        st_w = (w.get("telefon_status") or "czeka")
        if st_w == "odroczony":
            agg[g]["odroczony"] += 1
        else:
            agg[g]["czeka"] += 1
        proby = w.get("telefon_proby") or 0
        flagged = w.get("telefon_flagged_at")
        stary = False
        try:
            if flagged is not None and hasattr(flagged, "timestamp"):
                stary = (now_pl - flagged.astimezone(tz_pl)) > timedelta(hours=24)
        except Exception:
            stary = False
        if proby >= 3 or stary:
            agg[g]["pominiete"] += 1
            pominiete_rows.append({
                "NrZam": w.get("numer_zamowienia", "?"), "Grupa": g,
                "PZ": (w.get("telefon_pz") or ""), "Status": st_w,
                "Prób": proby, "Zlecił": w.get("telefon_zlecil", ""),
            })
    total_wor = sum(agg[g]["czeka"] + agg[g]["odroczony"] for g in _GRUPY)
    if total_wor:
        rows = [{"Grupa": g, "⏳ Czeka": agg[g]["czeka"], "🔁 Odroczone": agg[g]["odroczony"],
                 "⚠️ Pominięte": agg[g]["pominiete"]} for g in _GRUPY
                if (agg[g]["czeka"] + agg[g]["odroczony"]) > 0]
        st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)
        st.caption("⏳ Czeka = zlecony telefon bez próby. 🔁 Odroczone = dzwoniono bez skutku (oddzwon2h). "
                   "⚠️ Pominięte = ≥3 próby LUB >24h w woreczku — wymaga uwagi.")
        if pominiete_rows:
            with st.expander(f"⚠️ Pominięte / zaległe ({len(pominiete_rows)})", expanded=False):
                st.dataframe(pd.DataFrame(pominiete_rows), use_container_width=True, hide_index=True)
    else:
        st.info("Woreczek pusty — brak spraw oczekujących na telefon.")

    # --- AUDYT: kto usunął z woreczka (log w zakresie dat) ---
    df_s, dt_s = d_from.strftime("%Y-%m-%d"), d_to.strftime("%Y-%m-%d")
    usun = []
    try:
        _d_cur = d_from
        while _d_cur <= d_to:
            _ds = _d_cur.strftime("%Y-%m-%d")
            try:
                for _u in db.collection(col("ew_woreczek_log")).document(_ds).collection("usuniete").stream():
                    ud = _u.to_dict() or {}
                    usun.append(ud)
            except Exception:
                pass
            _d_cur = _d_cur + timedelta(days=1)
    except Exception:
        pass
    st.markdown("###### 🗑️ Usunięcia z woreczka")
    if usun:
        by_op = {}
        for u in usun:
            o = u.get("przez", "?")
            e = by_op.setdefault(o, {"ile": 0, "powody": {}})
            e["ile"] += 1
            p = u.get("powod", "inny")
            e["powody"][p] = e["powody"].get(p, 0) + 1
        urows = [{"Kto": o, "🗑️ Usunął": e["ile"],
                  "Powody": ", ".join(f"{k}: {v}" for k, v in e["powody"].items())}
                 for o, e in sorted(by_op.items(), key=lambda x: -x[1]["ile"])]
        st.dataframe(pd.DataFrame(urows), use_container_width=True, hide_index=True)
        st.caption(f"Łącznie usunięć w zakresie {df_s}…{dt_s}: {len(usun)}.")
    else:
        st.caption(f"Brak usunięć z woreczka w zakresie {df_s}…{dt_s}.")


# ---------- WSAD PER DZIEŃ (dyscyplina) ----------
def render_wsad_per_day(d_from, d_to, key_prefix=""):
    today_s = datetime.now(pytz.timezone('Europe/Warsaw')).date().strftime("%Y-%m-%d")
    include_today = d_from.strftime("%Y-%m-%d") <= today_s <= d_to.strftime("%Y-%m-%d")
    daily = read_chronicle_group_daily(d_from, d_to)
    pool_today = _group_counts_from_pool() if include_today else None
    rows = []
    all_days = sorted(set(list(daily.keys()) + ([today_s] if include_today else [])))
    for ds in all_days:
        if include_today and ds == today_s:
            day = pool_today          # dziś z żywej puli (komplet)
            label = f"{ds} (dziś · z puli)"
        else:
            day = daily.get(ds)
            label = ds
        if not day:
            continue
        for gname in _GRUPA_KEYS:
            dd = day[gname]
            if not dd.get("has_plan") and dd["zakonczone"] == 0 and dd["pominiete"] == 0:
                continue
            obr, zak = dd["obrabialne"], dd["zakonczone"]
            nieprzer = max(obr - zak, 0)
            rows.append({
                "Data": label, "Grupa": gname,
                "Ogółem": dd["total"], "W przyszłości": dd["odsiane"], "Obrabialne": obr,
                "✅ Przerobione": zak, "⛔ Nieprzerobione": nieprzer, "⏭️ Pominięte": dd["pominiete"],
                "%": (f"{round(zak / obr * 100)}%" if obr > 0 else "—"),
            })
    if rows:
        st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)
        st.caption("Wiersze per dzień planu (data_obrobki), BEZ sumowania. „Nieprzerobione” = obrabialne minus przerobione "
                   "tego dnia; nie liczy się, kiedy ewentualnie domknięto je później (następny dzień = nowa kolejka, "
                   "liczba się nie kumuluje). Pominięte zawierają się w nieprzerobionych. Dziś z żywej puli; "
                   "wcześniejsze dni z trwałej kroniki (od wdrożenia).")
    else:
        st.info("Brak zapisanego planu w tym zakresie (dane od wdrożenia — przy generowaniu raportu).")


tab_wsady, tab_generuj, tab_autopilot, tab_batches, tab_cases, tab_skipped, tab_prompty, tab_hasla, tab_zastepstwa, tab_diamentoza = st.tabs([
    "📂 Wsady",
    "⚡ Generuj + Autopilot",
    "🤖 Dolewka + Status",
    "📦 Historia partii",
    "📋 Przegląd casów",
    "⏭️ Pominięte (archiwum)",
    "🧪 Prompty",
    "🔐 Hasła operatorów",
    "🔁 Zastępstwa",
    "💎 Diamentoza",
])

# ==========================================
# SPRZEDAWCY — lista do zastępstw
# ==========================================
# Sprzedawca NIE jest zapisywany w bazie — AI czyta go z trzeciego elementu wsadu.
# Ta lista służy WYŁĄCZNIE zakładce zastępstw i musi być identyczna z SPRZEDAWCY
# w forum_module.py (tam odpala się podmiana odbiorcy przy wysyłce wpisu).
# Kraj jest opisowy — lista rozwijana przy każdym sprzedawcy zawiera WSZYSTKICH,
# bo rotacja bywa międzyjęzykowa.
SPRZEDAWCY_LISTA = [
    ("magda",     "DE"),
    ("kinga",     "DE"),
    ("klaudia",   "DE"),
    ("emilia",    "DE"),
    ("sylwia",    "DE"),
    ("oliwia_m",  "DE"),
    ("kasia_k",   "FR"),
    ("klaudia_k", "FR"),
    ("anna_m",    "FR"),
    ("Andy",      "UK/PL"),
    ("marta",     "UK/PL"),
]
SPRZEDAWCY_NICKI = [n for n, _ in SPRZEDAWCY_LISTA]


def load_zastepstwa_map():
    try:
        doc = db.collection(col("admin_config")).document("sprzedawcy_zastepstwa").get()
        if getattr(doc, "exists", False):
            return (doc.to_dict() or {}).get("mapa", {}) or {}
    except Exception:
        pass
    return {}


def save_zastepstwa_map(mapa, kto="admin"):
    db.collection(col("admin_config")).document("sprzedawcy_zastepstwa").set({
        "mapa": mapa,
        "updated_at": firestore.SERVER_TIMESTAMP,
        "updated_by": kto,
    }, merge=True)


# ==========================================
# 📂 ZAKŁADKA: WSADY
# ==========================================
with tab_wsady:
    st.subheader("📂 Zarządzanie wsadami")
    st.markdown("**Świnka / Uszki** → nowy plik NADPISUJE poprzedni  \n"
                "**Szturchacz** → nowy plik DOPEŁNIA istniejącą pulę (to samo NrZam = aktualizacja)")
    
    # Pokaż aktualny stan
    st.markdown("---")
    st.markdown("### 📊 Aktualny stan wsadów w bazie")
    
    cur_swinka = load_wsad("swinka")
    cur_uszki = load_wsad("uszki")
    cur_szturchacz = load_wsad("szturchacz")
    
    cs1, cs2, cs3 = st.columns(3)
    with cs1:
        n_sw = count_lines(cur_swinka)
        st.metric("🐷 Świnka", f"{n_sw} zamówień" if cur_swinka else "Brak")
    with cs2:
        st.metric("📦 Uszki", "Załadowane" if cur_uszki else "Brak")
    with cs3:
        n_sz = count_lines(cur_szturchacz)
        st.metric("📋 Szturchacz (pula)", f"{n_sz} zamówień" if cur_szturchacz else "Brak")
    
    st.markdown("---")
    
    # --- ŁADOWANIE WSADÓW ---
    st.markdown("### ⬆️ Załaduj wsady")
    
    col_w1, col_w2, col_w3 = st.columns(3)
    
    with col_w1:
        st.markdown("**🐷 ŚWINKA** (nadpisuje)")
        wsad_swinka = st.text_area("Wklej świnkę:", height=250, key="input_swinka")
        if st.button("💾 Załaduj świnkę", key="btn_swinka"):
            if wsad_swinka.strip():
                save_wsad("swinka", wsad_swinka.strip())
                st.success(f"✅ Świnka załadowana ({count_lines(wsad_swinka)} zamówień). Poprzednia nadpisana.")
                st.rerun()
            else:
                st.error("Pole jest puste!")
    
    with col_w2:
        st.markdown("**📦 USZKI** (nadpisuje)")
        wsad_uszki = st.text_area("Wklej uszki:", height=250, key="input_uszki")
        if st.button("💾 Załaduj uszki", key="btn_uszki"):
            if wsad_uszki.strip():
                save_wsad("uszki", wsad_uszki.strip())
                st.success("✅ Uszki załadowane. Poprzednie nadpisane.")
                st.rerun()
            else:
                st.error("Pole jest puste!")
    
    with col_w3:
        st.markdown("**📋 SZTURCHACZ** (dopełnia pulę)")
        wsad_szturchacz = st.text_area("Wklej szturchacza:", height=250, key="input_szturchacz")
        if st.button("💾 Załaduj szturchacza (dopełnij)", key="btn_szturchacz"):
            if wsad_szturchacz.strip():
                existing = load_wsad("szturchacz")
                merged, added, updated, total = merge_szturchacz(existing, wsad_szturchacz.strip())
                save_wsad("szturchacz", merged)
                st.success(f"✅ Szturchacz dopełniony — dodano {added} nowych, "
                           f"zaktualizowano {updated} istniejących. Pula razem: {total} zamówień.")
                st.rerun()
            else:
                st.error("Pole jest puste!")
    
    st.markdown("---")
    
    # --- CZYSZCZENIE ---
    st.markdown("### 🗑️ Czyszczenie")
    col_clr1, col_clr2 = st.columns(2)
    with col_clr1:
        if st.button("🗑️ Wyczyść WSZYSTKIE wsady", type="primary"):
            clear_all_wsady()
            st.success("🗑️ Wszystkie wsady wyczyszczone (świnka + uszki + szturchacz).")
            st.rerun()
    with col_clr2:
        if st.button("🗑️ Wyczyść kolejkę casów (ew_cases)"):
            # Pobierz WSZYSTKIE casy z bazy (nie po batch_id)
            all_ew = db.collection(col("ew_cases")).limit(5000).get()
            deleted = 0
            archived = 0
            for c in all_ew:
                cdata = c.to_dict()
                # Case z nienaprawionym komentarzem → archiwizuj
                if cdata.get("skip_reason") and not cdata.get("skip_fixed"):
                    cdata["archived_at"] = firestore.SERVER_TIMESTAMP
                    cdata["archived_from_batch"] = cdata.get("batch_id", "unknown")
                    db.collection(col("ew_cases_archived")).document(c.id).set(cdata)
                    archived += 1
                db.collection(col("ew_cases")).document(c.id).delete()
                deleted += 1
            # Wyczyść też wszystkie batche
            all_batches = db.collection(col("ew_batches")).get()
            for bdoc in all_batches:
                db.collection(col("ew_batches")).document(bdoc.id).delete()
            msg = f"🗑️ Usunięto {deleted} casów i {len(all_batches)} batchy. Czysta baza."
            if archived > 0:
                msg += f" ⏭️ {archived} pominiętych (nienaprawionych) przeniesiono do archiwum."
            st.success(msg)
            st.rerun()
    
    # Podgląd
    st.markdown("---")
    with st.expander("👀 Podgląd aktualnej puli szturchacza"):
        if cur_szturchacz:
            st.text(cur_szturchacz[:5000] + ("\n\n... (obcięto podgląd)" if len(cur_szturchacz) > 5000 else ""))
        else:
            st.info("Pula szturchacza jest pusta.")


# ==========================================
# ⚡ ZAKŁADKA: GENERUJ RAPORT
# ==========================================
with tab_generuj:
    st.subheader("⚡ Generuj raport priorytetów")
    st.caption("Używa aktualnie załadowanych wsadów z zakładki Wsady")
    
    # Sprawdź co jest załadowane
    cur_swinka = load_wsad("swinka")
    cur_uszki = load_wsad("uszki")
    cur_szturchacz = load_wsad("szturchacz")
    
    s1, s2, s3 = st.columns(3)
    with s1:
        st.metric("🐷 Świnka", "✅" if cur_swinka else "❌ Brak")
    with s2:
        st.metric("📦 Uszki", "✅" if cur_uszki else "⚠️ Opcjonalnie")
    with s3:
        st.metric("📋 Szturchacz", f"✅ ({count_lines(cur_szturchacz)})" if cur_szturchacz else "❌ Brak")
    
    if not cur_swinka or not cur_szturchacz:
        st.warning("⚠️ Potrzebujesz minimum świnki i szturchacza. Załaduj wsady w zakładce 📂 Wsady.")
        st.stop()
    
    st.markdown("---")
    
    col1, col2 = st.columns(2)
    with col1:
        # Pobierz prompty z GitHuba (taka sama lista jak operatorskie)
        _gh_prompts_w = _fetch_github_prompts()
        _all_wiezowiec_urls = dict(WIEZOWIEC_PROMPT_URLS)
        if isinstance(_gh_prompts_w, list):
            for _p in _gh_prompts_w:
                _all_wiezowiec_urls[_p["name"]] = _p["raw_url"]
        
        # Default = aktualny default warstwy B
        _default_b_w = (db.collection(col("admin_config")).document("default_prompt").get().to_dict() or {}).get("prompt_name", "")
        _opts_w = list(_all_wiezowiec_urls.keys())
        _idx_w = 0
        if _default_b_w and _default_b_w in _opts_w:
            _idx_w = _opts_w.index(_default_b_w)
        
        if _opts_w:
            sel_prompt = st.selectbox("Prompt Wieżowca:", _opts_w, index=_idx_w)
            sel_prompt_url = _all_wiezowiec_urls[sel_prompt]
        else:
            st.error("⚠️ Brak promptów. Wgraj prompt do repo szturchacz-test.")
            sel_prompt = None
            sel_prompt_url = None
    with col2:
        if GCP_PROJECTS:
            proj_opts = [f"{i+1} - {p}" for i, p in enumerate(GCP_PROJECTS)]
            sel_proj = st.selectbox("Projekt GCP:", proj_opts)
            proj_idx = int(sel_proj.split(" - ")[0]) - 1
            current_project = GCP_PROJECTS[proj_idx]
        else:
            current_project = ""
        model_choice = st.selectbox("Model AI:", ["gemini-2.5-pro", "gemini-2.5-flash"])
    
    # --- DATA OBRÓBKI (obowiązkowa) ---
    data_obrobki = st.date_input("📅 Data obróbki (kiedy operatorzy będą obrabiać te casy):", value=None, key="data_obrobki")
    if data_obrobki:
        st.success(f"📅 Data obróbki: **{data_obrobki.strftime('%d.%m.%Y')}** — prompt potraktuje tę datę jako 'dziś'.")
    else:
        st.warning("⚠️ Wybierz datę obróbki żeby rozpocząć przeliczanie.")
    
    st.markdown("---")
    
    # ==========================================
    # 👥 OBSADA + AUTOPILOT (wspólne parametry)
    # ==========================================
    st.markdown("### 👥 Obsada operatorów + Autopilot")
    st.caption("Wybierz operatorów per grupa. Po wygenerowaniu raportu autopilot automatycznie przelicza X% casów.")
    
    ALL_OPERATORS_LIST = ["Emilia", "Oliwia", "Magda", "Ewelina", "Iwona", "Marlena", "Sylwia", "EwelinaG", "Andrzej", "Marta", "Klaudia", "Kasia", "Romana", "oliwia_m"]
    GRUPA_MAP = {"DE": "Operatorzy_DE", "FR": "Operatorzy_FR", "UKPL": "Operatorzy_UK/PL"}
    ROLE_TO_GRUPA = {"Operatorzy_DE": "DE", "Operatorzy_FR": "FR", "Operatorzy_UK/PL": "UKPL"}
    
    ops_by_grupa = {"DE": [], "FR": [], "UKPL": []}
    for op_name_candidate in ALL_OPERATORS_LIST:
        try:
            cfg_doc = db.collection(col("operator_configs")).document(op_name_candidate).get()
            if cfg_doc.exists:
                role = cfg_doc.to_dict().get("role", "Operatorzy_DE")
                grupa = ROLE_TO_GRUPA.get(role, "DE")
                ops_by_grupa[grupa].append(op_name_candidate)
            else:
                ops_by_grupa["DE"].append(op_name_candidate)
        except Exception:
            ops_by_grupa["DE"].append(op_name_candidate)
    
    col_obs1, col_obs2, col_obs3 = st.columns(3)
    with col_obs1:
        st.markdown("**🇩🇪 DE**")
        gen_ops_de = st.multiselect("Operatorzy DE:", ops_by_grupa["DE"], key="gen_ops_de")
    with col_obs2:
        st.markdown("**🇫🇷 FR**")
        gen_ops_fr = st.multiselect("Operatorzy FR:", ops_by_grupa["FR"], key="gen_ops_fr")
    with col_obs3:
        st.markdown("**🇬🇧 UKPL**")
        gen_ops_ukpl = st.multiselect("Operatorzy UKPL:", ops_by_grupa["UKPL"], key="gen_ops_ukpl")
    
    gen_obsada = {}
    if gen_ops_de: gen_obsada["DE"] = gen_ops_de
    if gen_ops_fr: gen_obsada["FR"] = gen_ops_fr
    if gen_ops_ukpl: gen_obsada["UKPL"] = gen_ops_ukpl
    
    if gen_obsada:
        summary_parts = [f"{g}: {', '.join(ops)} ({len(ops)} os.)" for g, ops in gen_obsada.items()]
        st.success(f"📋 Obsada: {' | '.join(summary_parts)}")
    
    # Procent autopilota
    col_pct1, col_pct2 = st.columns(2)
    with col_pct1:
        autopilot_pct = st.slider("🤖 % casów do przeliczenia autopilotem:", min_value=0, max_value=100, value=30, step=5, key="autopilot_pct")
    with col_pct2:
        st.caption(f"Po raporcie autopilot przelicza **{autopilot_pct}%** najwyżej punktowanych casów (globalnie po score, mieszając grupy).")
    
    # Zaawansowane parametry autopilota
    with st.expander("⚙️ Parametry autopilota"):
        # Lista promptów z GitHuba (taka sama jak w zakładce 🧪 Prompty)
        PROMPT_URLS_hardcoded = {
            "Prompt Stabilny (prompt4624)": "https://raw.githubusercontent.com/szturchaczysko-cpu/szturchacz/refs/heads/main/prompt4624.txt",
        }
        custom_prompts_data = (db.collection(col("admin_config")).document("custom_prompts").get().to_dict() or {}).get("urls", {})
        
        # Pobierz prompty z GitHuba (cache 60s)
        _github_prompts = _fetch_github_prompts()
        _github_prompts_dict = {}
        if isinstance(_github_prompts, list):
            for _p in _github_prompts:
                _github_prompts_dict[_p["name"]] = _p["raw_url"]
        
        ALL_OP_PROMPT_URLS = {**PROMPT_URLS_hardcoded, **custom_prompts_data, **_github_prompts_dict}
        
        # Default w dropdownie: aktualny default warstwy B (jeśli ustawiony)
        _default_b = (db.collection(col("admin_config")).document("default_prompt").get().to_dict() or {}).get("prompt_name", "")
        _default_idx = 0
        _options_list = list(ALL_OP_PROMPT_URLS.keys())
        if _default_b and _default_b in _options_list:
            _default_idx = _options_list.index(_default_b)
        
        col_ap1, col_ap2, col_ap3 = st.columns(3)
        with col_ap1:
            ap_prompt_name = st.selectbox("Prompt operatorski:", _options_list, index=_default_idx, key="gen_ap_prompt")
            ap_prompt_url = ALL_OP_PROMPT_URLS[ap_prompt_name]
        with col_ap2:
            ap_pause = st.slider("⏱️ Pauza (sek):", min_value=5, max_value=120, value=30, step=5, key="gen_ap_pause")
            ap_model = st.selectbox("Model AI (autopilot):", ["gemini-2.5-pro", "gemini-2.5-flash"], key="gen_ap_model")
        with col_ap3:
            available_keys = [f"{i+1} - {p}" for i, p in enumerate(GCP_PROJECTS)]
            ap_keys = st.multiselect("🔑 Klucze do rotacji:", available_keys, default=available_keys, key="gen_ap_keys")
            ap_key_indices = [int(k.split(" - ")[0]) - 1 for k in ap_keys]
    
    # Zapisz do session_state żeby dolewka mogła czytać
    st.session_state["_gen_obsada"] = gen_obsada
    st.session_state["_gen_ap_prompt_name"] = ap_prompt_name
    st.session_state["_gen_ap_prompt_url"] = ap_prompt_url
    st.session_state["_gen_ap_pause"] = ap_pause
    st.session_state["_gen_ap_model"] = ap_model
    st.session_state["_gen_ap_key_indices"] = ap_key_indices
    st.session_state["_gen_data_obrobki"] = data_obrobki
    
    st.markdown("---")
    
    # --- PRZYGOTOWANIE PARTII (analiza bez przeliczania) ---
    if st.button("📊 Przygotuj partycje (bez przeliczania)", type="secondary"):
        if not data_obrobki:
            st.error("⚠️ Wybierz datę obróbki!")
            st.stop()
        if not current_project:
            st.error("Brak projektu GCP!")
            st.stop()
        
        WIEZOWIEC_PROMPT = get_remote_prompt(sel_prompt_url)
        if not WIEZOWIEC_PROMPT:
            st.error("Nie udało się pobrać promptu!")
            st.stop()
        
        tz_pl = pytz.timezone('Europe/Warsaw')
        now = datetime.now(tz_pl)
        
        # --- TRYB INKREMENTALNY: sprawdź istniejące casy w bazie ---
        existing_docs = db.collection(col("ew_cases")).limit(5000).get()
        existing_cases_map = {}  # NrZam → {status, score, priority_icon, priority_label, naglowek, grupa, ...}
        for edoc in existing_docs:
            ed = edoc.to_dict()
            enr = ed.get("numer_zamowienia", "")
            if enr:
                # Priorytet: w_toku > przydzielony > zakonczony > wolny
                prio_map = {"w_toku": 4, "przydzielony": 3, "zakonczony": 2, "wolny": 1}
                if enr in existing_cases_map:
                    if prio_map.get(ed.get("status"), 0) > prio_map.get(existing_cases_map[enr].get("status"), 0):
                        existing_cases_map[enr] = ed
                else:
                    existing_cases_map[enr] = ed
        
        # Rozdziel NrZamy z puli szturchacza na kategorie
        # Używamy tego samego parsera co merge_szturchacz
        szturchacz_blocks = parse_szturchacz_blocks(cur_szturchacz)
        szturchacz_nrzams = set(szturchacz_blocks.keys())
        # Usuń klucz _RAW_ jeśli parser nie rozpoznał bloków
        szturchacz_nrzams.discard("_RAW_")
        
        # Kategorie:
        # DO_PRZELICZENIA: nowe (nie ma w bazie) + zakończone (mogły się zmienić) + wspólne-zakończone
        # GOTOWE: wolne z bazy (score się nie zmienił) + przydzielone + w_toku
        nrzam_do_przeliczenia = set()
        nrzam_gotowe = {}  # NrZam → dane z bazy
        
        for nrzam in szturchacz_nrzams:
            if nrzam not in existing_cases_map:
                # Nowy case — nie było go w bazie
                nrzam_do_przeliczenia.add(nrzam)
            else:
                status = existing_cases_map[nrzam].get("status", "wolny")
                if status == "zakonczony":
                    # Zakończony — przelicz od nowa (operator mógł zmienić dane)
                    nrzam_do_przeliczenia.add(nrzam)
                else:
                    # Wolny / przydzielony / w_toku — gotowy wynik, nie przeliczaj
                    nrzam_gotowe[nrzam] = existing_cases_map[nrzam]
        
        # Dodaj też zakończone z bazy, które NIE są w aktualnym szturchaczu
        # (były w starym wsadzie, operator je zakończył — AI musi je widzieć)
        for nrzam, edata in existing_cases_map.items():
            if nrzam not in szturchacz_nrzams and edata.get("status") == "zakonczony":
                nrzam_do_przeliczenia.add(nrzam)
        
        is_incremental = len(nrzam_gotowe) > 0
        
        # Debug: pokaż co parser znalazł
        with st.expander(f"🔍 Debug: parser znalazł {len(szturchacz_nrzams)} NrZam w puli szturchacza", expanded=False):
            if szturchacz_nrzams:
                st.text(f"NrZamy ({len(szturchacz_nrzams)}): {', '.join(sorted(list(szturchacz_nrzams))[:30])}")
                if len(szturchacz_nrzams) > 30:
                    st.text(f"...+{len(szturchacz_nrzams)-30} więcej")
            else:
                st.warning("⚠️ Parser nie znalazł żadnych NrZam! Sprawdź format wsadu szturchacza.")
                st.text(f"Pierwsze 500 znaków puli:\n{cur_szturchacz[:500]}")
            
            if existing_cases_map:
                st.text(f"\nCasy w bazie ({len(existing_cases_map)}): {', '.join(sorted(list(existing_cases_map.keys()))[:30])}")
            else:
                st.text("\nBrak casów w bazie (pierwszy wsad).")
            
            st.text(f"\nDo przeliczenia: {len(nrzam_do_przeliczenia)}")
            st.text(f"Gotowe (z bazy): {len(nrzam_gotowe)}")
        
        # Wyświetl info o trybie
        if is_incremental:
            st.info(
                f"🔄 **Tryb inkrementalny:**\n"
                f"- **{len(nrzam_do_przeliczenia)}** zamówień do przeliczenia (nowe + zakończone)\n"
                f"- **{len(nrzam_gotowe)}** zamówień z gotowym wynikiem (wolne/przydzielone/w toku)"
            )
        else:
            st.info(f"🆕 **Pierwszy wsad:** {len(szturchacz_nrzams)} zamówień do przeliczenia od zera.")
        
        # --- Buduj partie zamówień do przeliczenia ---
        BATCH_SIZE = 60  # max zamówień na jedno wywołanie AI
        
        # Zbierz bloki szturchacza do przeliczenia
        nowe_szturchacz_parts = []
        nrzam_order = []  # zachowaj kolejność
        for nrzam in nrzam_do_przeliczenia:
            block = None
            if nrzam in szturchacz_blocks:
                block = szturchacz_blocks[nrzam]
            elif nrzam in existing_cases_map:
                saved_line = existing_cases_map[nrzam].get("pelna_linia_szturchacza", "")
                if saved_line:
                    block = saved_line
            if block:
                nowe_szturchacz_parts.append((nrzam, block))
                nrzam_order.append(nrzam)
        
        # Podziel na partie
        batches_to_process = []
        for i in range(0, len(nowe_szturchacz_parts), BATCH_SIZE):
            batch_chunk = nowe_szturchacz_parts[i:i+BATCH_SIZE]
            batches_to_process.append(batch_chunk)
        
        total_batches = len(batches_to_process)
        if total_batches == 0 and not nrzam_gotowe:
            st.warning("⚠️ Brak zamówień do przeliczenia.")
            st.stop()
        
        # Zapisz przygotowane partycje do session_state
        old_total = len(st.session_state.get("_ew_batches_to_process", []))
        st.session_state["_ew_batches_to_process"] = batches_to_process
        # Resetuj postęp tylko jeśli partycje się zmieniły (inny wsad)
        if total_batches != old_total:
            st.session_state["_ew_batches_done"] = 0
            st.session_state["_ew_all_cases"] = []
            st.session_state["_ew_all_raw_outputs"] = []
        st.session_state["_ew_nrzam_gotowe"] = nrzam_gotowe if is_incremental else {}
        st.session_state["_ew_is_incremental"] = is_incremental
        st.session_state["_ew_prompt_name"] = sel_prompt
        st.session_state["_ew_model"] = model_choice
        st.session_state["_ew_prompt_url"] = sel_prompt_url
        st.session_state["_ew_project"] = current_project
        
        batches_done = st.session_state.get("_ew_batches_done", 0)
        st.success(f"📦 **{total_batches} partii** (po ~{BATCH_SIZE} zamówień). "
                   f"{len(nowe_szturchacz_parts)} do przeliczenia"
                   + (f", {len(nrzam_gotowe)} już w bazie (gotowe)" if nrzam_gotowe else "")
                   + (f". **{batches_done} partii już przeliczonych** — kontynuuj od partii {batches_done+1}." if batches_done > 0 else "")
                   + ".")
        st.rerun()
    
    # --- PANEL PRZELICZANIA PARTII ---
    batches_to_process = st.session_state.get("_ew_batches_to_process", [])
    def _save_cases_to_db(batch_cases, batch_num, total_batches):
        """Zapisz casy z jednej paczki do bazy natychmiast."""
        
        # === W1: KOMPRESJA ZABLOKOWANYCH KLIENTÓW ===
        # Jeśli case ma "Zablokowany klient" w danych, grupuj po emailu.
        # Z grupy bierz tylko jeden (najwyższy score), resztę oznacz jako zablokowane.
        def extract_email(text):
            """Wyciągnij email z pelna_linia_szturchacza"""
            m = re.search(r'[\w.+-]+@[\w.-]+\.\w+', text)
            return m.group(0).lower() if m else None
        
        blocked_by_email = {}  # email -> [cases]
        normal_cases = []
        
        for case in batch_cases:
            linia = case.get("pelna_linia_szturchacza", "")
            if "zablokowany klient" in linia.lower() or "Zablokowany klient" in linia:
                email = extract_email(linia)
                if email:
                    if email not in blocked_by_email:
                        blocked_by_email[email] = []
                    blocked_by_email[email].append(case)
                else:
                    normal_cases.append(case)
            else:
                normal_cases.append(case)
        
        # Z każdej grupy zablokowanych bierz tylko najwyższy score
        compressed_count = 0
        for email, cases_group in blocked_by_email.items():
            cases_group.sort(key=lambda c: c.get("score", 0), reverse=True)
            normal_cases.append(cases_group[0])  # najwyższy score
            compressed_count += len(cases_group) - 1
        
        if compressed_count > 0:
            st.toast(f"🔗 Skompresowano {compressed_count} casów zablokowanych klientów (po emailu)")
        
        batch_cases = normal_cases
        # === KONIEC W1 ===
        
        # === W2: KOREKTA GRUPY PO KRAJU ===
        DE_COUNTRIES = {"germany", "austria", "switzerland", "liechtenstein"}
        FR_COUNTRIES = {"france", "belgium", "spain", "italy"}
        # UKPL = cała reszta (Luxembourg, Portugal, Sweden, Netherlands, Poland, UK, itd.)
        
        def detect_country_grupa(text):
            """Wykryj kraj z pelna_linia_szturchacza i zwróć poprawną grupę.
            DE/FR — jawna lista. Każdy inny wykryty kraj → UKPL. Brak kraju → None."""
            text_lower = text.lower()
            # Szukaj DE
            for country in DE_COUNTRIES:
                if country in text_lower:
                    return "DE"
            # Szukaj FR
            for country in FR_COUNTRIES:
                if country in text_lower:
                    return "FR"
            # Szukaj znanych krajów → UKPL
            known_countries = [
                "luxembourg", "poland", "portugal", "netherlands", "sweden", "denmark",
                "finland", "norway", "ireland", "united kingdom", "uk", "england",
                "czech", "slovakia", "hungary", "romania", "bulgaria", "croatia",
                "slovenia", "greece", "turkey", "serbia", "estonia", "latvia",
                "lithuania", "malta", "cyprus", "scotland", "wales",
            ]
            for country in known_countries:
                if country in text_lower:
                    return "UKPL"
            # Nie wykryto żadnego kraju
            return None
        
        corrected = 0
        no_country = 0
        for case in batch_cases:
            linia = case.get("pelna_linia_szturchacza", "")
            detected = detect_country_grupa(linia)
            if detected:
                if detected != case.get("grupa") or not case.get("grupa"):
                    case["grupa"] = detected
                    corrected += 1
            elif not case.get("grupa"):
                no_country += 1
        
        if corrected > 0:
            st.toast(f"🌍 Skorygowano/przypisano grupę dla {corrected} casów (po kraju)")
        if no_country > 0:
            st.toast(f"⚠️ {no_country} casów bez rozpoznanego kraju — brak grupy!")
        # === KONIEC W2 ===
        tz_pl = pytz.timezone('Europe/Warsaw')
        now = datetime.now(tz_pl)
        batch_id = f"batch_{now.strftime('%Y%m%d_%H%M%S')}_p{batch_num}"
        
        # Pobierz istniejące casy — zbierz WSZYSTKIE doc_id per NrZam (nie tylko jeden)
        existing_cases_docs = db.collection(col("ew_cases")).limit(5000).get()
        existing_by_nrzam = {}  # NrZam → [{"doc_id": ..., "status": ...}, ...]
        for edoc in existing_cases_docs:
            edata = edoc.to_dict()
            enr = edata.get("numer_zamowienia", "")
            if enr:
                if enr not in existing_by_nrzam:
                    existing_by_nrzam[enr] = []
                existing_by_nrzam[enr].append({"doc_id": edoc.id, "status": edata.get("status", "wolny")})
        
        saved = 0
        skipped = 0
        deleted = 0
        _seen_nrzam = set()  # dedup W TEJ partii — ten sam numer 2x w wyjściu AI tworzyłby drugi dokument wolny

        for i, case in enumerate(batch_cases):
            nrzam = case.get("numer_zamowienia", "")
            # Ten sam numer już obrobiony w tej partii → pomiń (anty-duplikat).
            if nrzam and nrzam in _seen_nrzam:
                skipped += 1
                continue
            existing_list = existing_by_nrzam.get(nrzam, [])

            # Sprawdź czy ktoś pracuje nad tym casem
            active = [e for e in existing_list if e["status"] in ("przydzielony", "w_toku")]
            if active:
                skipped += 1
                continue
            
            # Usuń WSZYSTKIE stare wolne/zakończone z tym NrZam
            for e in existing_list:
                if e["status"] in ("wolny", "zakonczony"):
                    db.collection(col("ew_cases")).document(e["doc_id"]).delete()
                    deleted += 1
            
            # ID DETERMINISTYCZNE po numerze → ponowny zapis tego samego numeru NADPISUJE, nie duplikuje.
            # Fallback na indeks tylko gdy brak numeru.
            # UWAGA: numer może zawierać "/" (np. ZW123/45) — w Firestore "/" w doc-id to separator ścieżki
            #        i rozbija zapis. Zamień "/" (oraz inne znaki ścieżki) na "_" zanim użyjesz jako ID.
            _nrzam_id = re.sub(r'[/\\.#$\[\]]', '_', nrzam) if nrzam else ""
            case_id = f"ew_{_nrzam_id}" if _nrzam_id else f"{batch_id}_{case.get('grupa', 'XX')}_{i+1:04d}"
            # Odroczony = case którego prompt nie wypisał (dodany przez uzupełnianie brakujących)
            case_status = case.get("_forced_status", "wolny")
            # NEXT=zakonczony → sprawa DOMKNIĘTA, nie wchodzi do kolejki dnia (operator jej nie pobierze,
            # autopilot pomija — kolejka bierze tylko status=="wolny"). Nic dalej się z nią nie da zrobić.
            if re.search(r'next\s*=\s*zakonczony', case.get("pelna_linia_szturchacza", ""), re.IGNORECASE):
                case_status = "zakonczony"
            # data_obrobki (dzień planu) — trwale na casie; baza tabeli wsad-per-dzień.
            _data_obrobki_str = data_obrobki.strftime("%Y-%m-%d") if data_obrobki else None
            db.collection(col("ew_cases")).document(case_id).set({
                "batch_id": batch_id,
                "numer_zamowienia": nrzam,
                "score": case.get("score", 0),
                "priority_icon": case.get("priority_icon", "⚪"),
                "priority_label": case.get("priority_label", ""),
                "grupa": case.get("grupa") or "",
                "index_handlowy": case.get("index_handlowy", ""),
                "pelna_linia_szturchacza": case.get("pelna_linia_szturchacza", ""),
                "naglowek_priorytetowy": case.get("naglowek_priorytetowy", ""),
                "status": case_status,
                "data_obrobki": _data_obrobki_str,
                "assigned_to": None,
                "assigned_at": None,
                "completed_at": None,
                "result_tag": None,
                "result_pz": None,
                "sort_order": i,
                "created_at": firestore.SERVER_TIMESTAMP,
            })
            saved += 1
            if nrzam:
                _seen_nrzam.add(nrzam)

        # Zapisz batch info
        db.collection(col("ew_batches")).document(batch_id).set({
            "created_at": firestore.SERVER_TIMESTAMP,
            "created_by": "admin",
            "date_label": now.strftime("%Y-%m-%d"),
            "total_cases": len(batch_cases),
            "status": "active",
            "summary": f"Partia {batch_num}/{total_batches}: {saved} zapisanych, {skipped} pominiętych, {deleted} duplikatów usuniętych",
            "prompt_used": st.session_state.get("_ew_prompt_name", "?"),
            "model_used": st.session_state.get("_ew_model", "?"),
        })
        
        st.toast(f"💾 Partia {batch_num}: {saved} casów zapisanych do bazy" + (f", {skipped} pominiętych" if skipped else ""))

    # --- FUNKCJA PRZELICZANIA JEDNEJ PARTII (z rerun po zakończeniu) ---
    def _do_single_batch(batch_idx):
        """Przelicz jedną partię i zapisz postęp. Po powrocie nastąpi rerun."""
        batches = st.session_state.get("_ew_batches_to_process", [])
        model_choice = st.session_state.get("_ew_model", "gemini-2.5-pro")
        prompt_url = st.session_state.get("_ew_prompt_url", "")
        project = st.session_state.get("_ew_project", "")
        
        WIEZOWIEC_PROMPT = get_remote_prompt(prompt_url)
        if not WIEZOWIEC_PROMPT:
            st.error("Nie udało się pobrać promptu!")
            return
        
        if not GCP_PROJECTS:
            st.error("Brak kluczy GCP!")
            return
        
        cur_swinka = load_wsad("swinka")
        cur_uszki = load_wsad("uszki")
        
        tz_pl = pytz.timezone('Europe/Warsaw')
        now = datetime.now(tz_pl)
        total_batches = len(batches)
        
        safety_settings = [
            SafetySetting(category=HarmCategory.HARM_CATEGORY_DANGEROUS_CONTENT, threshold=HarmBlockThreshold.BLOCK_NONE),
            SafetySetting(category=HarmCategory.HARM_CATEGORY_HARASSMENT, threshold=HarmBlockThreshold.BLOCK_NONE),
            SafetySetting(category=HarmCategory.HARM_CATEGORY_HATE_SPEECH, threshold=HarmBlockThreshold.BLOCK_NONE),
            SafetySetting(category=HarmCategory.HARM_CATEGORY_SEXUALLY_EXPLICIT, threshold=HarmBlockThreshold.BLOCK_NONE),
        ]
        
        all_cases = st.session_state.get("_ew_all_cases", [])
        all_raw_outputs = st.session_state.get("_ew_all_raw_outputs", [])
        
        batch_chunk = batches[batch_idx]
        batch_num = batch_idx + 1
        batch_szturchacz = '\n\n'.join([block for _, block in batch_chunk])
        
        progress_bar = st.progress(0, text=f"🏢 Partia {batch_num}/{total_batches} ({len(batch_chunk)} zamówień)...")
        
        # --- ROTACJA KLUCZY: zmień projekt per partia ---
        if GCP_PROJECTS:
            rot_project = GCP_PROJECTS[batch_idx % len(GCP_PROJECTS)]
            st.toast(f"🔑 Partia {batch_num}: klucz {batch_idx % len(GCP_PROJECTS) + 1}/{len(GCP_PROJECTS)} ({rot_project[:20]}...)")
            try:
                ci = json.loads(st.secrets["FIREBASE_CREDS"])
                cv = service_account.Credentials.from_service_account_info(ci)
                vertexai.init(project=rot_project, location=st.secrets.get("GCP_LOCATION", "us-central1"), credentials=cv)
            except Exception as e:
                st.error(f"Błąd Vertex AI (klucz {batch_idx % len(GCP_PROJECTS) + 1}): {e}")
                return
        
        user_msg = f"""Data dzisiejsza: {data_obrobki.strftime('%d.%m.%Y')}

Przelicz priorytety dla poniższych zamówień.
{"Partia " + str(batch_num) + " z " + str(total_batches) + "." if total_batches > 1 else ""}

=== WSAD 1: ŚWINKA ===
{cur_swinka}

=== WSAD 2: SZTURCHACZ — ZAMÓWIENIA DO PRZELICZENIA ({len(batch_chunk)} szt.) ===
{batch_szturchacz}

=== WSAD 3: STANY USZKÓW ===
{cur_uszki if cur_uszki else '(brak danych o uszkach)'}
"""
        
        ai_text = None
        FALLBACK_CHAIN = ["gemini-2.5-pro", "gemini-2.5-flash"]
        models_to_try = [model_choice]
        for fb in FALLBACK_CHAIN:
            if fb != model_choice and fb not in models_to_try:
                models_to_try.append(fb)
        
        for try_model in models_to_try:
            is_fallback = (try_model != model_choice)
            if is_fallback:
                st.toast(f"🔄 Partia {batch_num}: przełączam na {try_model}...")
            
            for attempt in range(3):  # max 3 próby (nie 5 — websocket timeout)
                try:
                    model = GenerativeModel(try_model, system_instruction=WIEZOWIEC_PROMPT)
                    chat = model.start_chat(response_validation=False)
                    resp = chat.send_message(
                        user_msg,
                        generation_config={"temperature": 0.0, "max_output_tokens": 65536},
                        safety_settings=safety_settings,
                    )
                    if resp.candidates:
                        candidate = resp.candidates[0]
                        if candidate.content and candidate.content.parts:
                            ai_text = candidate.content.parts[0].text
                    else:
                        ai_text = resp.text
                    
                    if ai_text:
                        if is_fallback:
                            st.toast(f"⚡ Partia {batch_num}: odpowiedź z {try_model}")
                        break
                except Exception as e:
                    err_str = str(e)
                    if "429" in err_str or "Quota" in err_str or "ResourceExhausted" in err_str or "503" in err_str or "unavailable" in err_str.lower():
                        # Rotacja klucza przy quota/503
                        if GCP_PROJECTS and len(GCP_PROJECTS) > 1:
                            next_key_idx = (batch_idx + attempt + 1) % len(GCP_PROJECTS)
                            rot_project = GCP_PROJECTS[next_key_idx]
                            try:
                                ci = json.loads(st.secrets["FIREBASE_CREDS"])
                                cv = service_account.Credentials.from_service_account_info(ci)
                                vertexai.init(project=rot_project, location=st.secrets.get("GCP_LOCATION", "us-central1"), credentials=cv)
                                st.toast(f"🔑 Partia {batch_num}: rotacja na klucz {next_key_idx+1}/{len(GCP_PROJECTS)}")
                            except Exception:
                                pass
                        wait_time = min(5 * (attempt + 1), 10)  # 5s, 10s, 10s (max 25s total)
                        st.toast(f"⏳ {try_model}, partia {batch_num}, próba {attempt+1}/3, czekam {wait_time}s...")
                        time.sleep(wait_time)
                    elif "Finish reason: 2" in err_str or "response_validation" in err_str:
                        st.toast(f"⚠️ Safety block, partia {batch_num}, próba {attempt+1}/3...")
                        time.sleep(5)
                    else:
                        st.error(f"Błąd AI ({try_model}, partia {batch_num}): {err_str[:300]}")
                        break
            
            if ai_text:
                break
        
        if ai_text:
            all_raw_outputs.append(f"=== PARTIA {batch_num}/{total_batches} ({len(batch_chunk)} zam.) ===\n{ai_text}")
            batch_cases = parse_wiezowiec_output(ai_text)
            all_cases.extend(batch_cases)
            if batch_cases:
                st.toast(f"✅ Partia {batch_num}: {len(batch_cases)} casów")
                _save_cases_to_db(batch_cases, batch_num, total_batches)
            
            # --- UZUPEŁNIJ BRAKUJĄCE ---
            parsed_nrzams = set(c.get("numer_zamowienia", "") for c in batch_cases)
            input_nrzams = set(nrzam for nrzam, _ in batch_chunk)
            missing_nrzams = input_nrzams - parsed_nrzams
            
            if missing_nrzams:
                missing_cases = []
                for nrzam in missing_nrzams:
                    wsad_block = ""
                    for nr, block in batch_chunk:
                        if nr == nrzam:
                            wsad_block = block
                            break
                    
                    # --- P1: INTELIGENTNY STATUS BRAKUJĄCEGO CASE'A ---
                    # Szukaj prawidłowego tagu: C#:...;NEXT=dd.mm
                    # Prawidłowy = zaczyna się od C# i zawiera ;NEXT=data
                    block_lower = wsad_block.lower()
                    has_delivered = "delivered" in block_lower
                    
                    is_future = False
                    has_valid_tag = False
                    tag_match = re.search(r'c#:.*?;next=(\d{2}\.\d{2})', wsad_block, re.IGNORECASE)
                    if tag_match and data_obrobki:
                        has_valid_tag = True
                        try:
                            ns = tag_match.group(1)  # dd.mm
                            nd = datetime.strptime(ns + f".{data_obrobki.year}", "%d.%m.%Y").date()
                            if nd > data_obrobki:
                                is_future = True
                        except:
                            pass
                    
                    if not wsad_block.strip():
                        reason = "pusty_blok"
                        forced_status = "odroczony"
                    elif has_valid_tag and is_future:
                        reason = f"odroczony (tag NEXT={tag_match.group(1)})"
                        forced_status = "odroczony"
                    elif has_valid_tag and not is_future:
                        reason = f"termin_ok (tag NEXT={tag_match.group(1)})"
                        forced_status = "wolny"
                    elif has_delivered:
                        reason = "prompt_pominął (Delivered, brak tagu)"
                        forced_status = "wolny"
                    else:
                        reason = "brak_delivered"
                        forced_status = "odroczony"
                    # --- KONIEC P1 ---
                    
                    missing_cases.append({
                        "numer_zamowienia": nrzam,
                        "score": 0,
                        "priority_icon": "⚪",
                        "priority_label": f"NIEPRZYDZIELONY — {reason}",
                        "grupa": "",
                        "index_handlowy": "",
                        "pelna_linia_szturchacza": wsad_block,
                        "naglowek_priorytetowy": f"[SCORE=0] ⚪ | {reason}",
                        "_forced_status": forced_status,
                    })
                if missing_cases:
                    _save_cases_to_db(missing_cases, batch_num, total_batches)
                    st.toast(f"📋 Partia {batch_num}: {len(missing_cases)} casów nieprzydzielonych dodano do bazy")
            
            if not batch_cases and not missing_nrzams:
                st.toast(f"ℹ️ Partia {batch_num}: 0 casów po filtracji")
        else:
            all_raw_outputs.append(f"=== PARTIA {batch_num}/{total_batches} — BRAK ODPOWIEDZI ===")
            st.warning(f"⚠️ Partia {batch_num}: brak odpowiedzi AI")
        
        progress_bar.progress(1.0, text=f"✅ Partia {batch_num} gotowa!")
        
        # Zapisz postęp NATYCHMIAST (nie czekaj na resztę)
        st.session_state["_ew_batches_done"] = batch_idx + 1
        st.session_state["_ew_all_cases"] = all_cases
        st.session_state["_ew_all_raw_outputs"] = all_raw_outputs
        st.session_state["_ew_raw_ai_output"] = '\n\n'.join(all_raw_outputs)
    
    # --- PANEL PRZELICZANIA PARTII (przyciski) ---
    if batches_to_process:
        total_batches = len(batches_to_process)
        batches_done = st.session_state.get("_ew_batches_done", 0)
        
        # AUTO-CONTINUE: jeśli flaga ustawiona i zostały partie → przelicz następną
        if st.session_state.get("_ew_auto_continue") and batches_done < total_batches:
            st.info(f"🔄 Auto-continue: partia {batches_done+1}/{total_batches}...")
            _do_single_batch(batches_done)
            st.rerun()
        
        st.markdown("---")
        st.markdown(f"### 📦 Partycje: {batches_done}/{total_batches} przeliczonych")
        
        # Pasek postępu globalny
        if batches_done > 0:
            st.progress(batches_done / total_batches, text=f"✅ {batches_done}/{total_batches} partii gotowych")
        
        # Info per partia
        for bi, bc in enumerate(batches_to_process):
            status_icon = "✅" if bi < batches_done else ("⏳" if bi == batches_done else "⬜")
            st.caption(f"{status_icon} Partia {bi+1}: {len(bc)} zamówień")
        
        if batches_done < total_batches:
            if not data_obrobki:
                st.error("⚠️ Wybierz datę obróbki żeby rozpocząć przeliczanie!")
            else:
                if st.session_state.get("_ew_auto_continue"):
                    st.warning(f"🔄 Tryb automatyczny — przelicza partie jedna po drugiej.")
                    if st.button("⏸️ STOP auto-continue"):
                        st.session_state.pop("_ew_auto_continue", None)
                        st.rerun()
                else:
                    col_btn1, col_btn2 = st.columns(2)
                    with col_btn1:
                        if st.button(f"🚀 Przelicz następną paczkę (partia {batches_done+1})", type="primary"):
                            _do_single_batch(batches_done)
                            st.rerun()
                    with col_btn2:
                        if st.button(f"🚀 Przelicz wszystkie pozostałe ({total_batches - batches_done} partii)"):
                            st.session_state["_ew_auto_continue"] = True
                            _do_single_batch(batches_done)
                            st.rerun()
        else:
            st.session_state.pop("_ew_auto_continue", None)
            st.success(f"✅ Wszystkie {total_batches} partii przeliczone!")

            # === ZAPIS TRWAŁEGO PLANU DNIA (idempotentny, z puli wg data_obrobki) ===
            # plan = total / odsiane (odroczony) / obrabialne per grupa → ew_operator_stats/{data}.plan
            # Przeżywa czyszczenie puli ew_cases. SET (overwrite) = bez podwójnego liczenia przy re-generacji.
            if data_obrobki:
                _ds_plan = data_obrobki.strftime("%Y-%m-%d")
                if st.session_state.get("_ew_plan_written_for") != _ds_plan:
                    try:
                        _plan_docs = db.collection(col("ew_cases")).where("data_obrobki", "==", _ds_plan).limit(8000).get()
                        _plan_count = {g: {"total": 0, "odsiane": 0} for g in ["DE", "FR", "UKPL"]}
                        for _pd in _plan_docs:
                            _pdd = _pd.to_dict()
                            _pg = _pdd.get("grupa", "")
                            if _pg in _plan_count:
                                _plan_count[_pg]["total"] += 1
                                if _pdd.get("status") == "odroczony":
                                    _plan_count[_pg]["odsiane"] += 1
                        _plan_payload = {
                            g: {"total": v["total"], "odsiane": v["odsiane"], "obrabialne": v["total"] - v["odsiane"]}
                            for g, v in _plan_count.items()
                        }
                        db.collection(col("ew_operator_stats")).document(_ds_plan).set({"plan": _plan_payload}, merge=True)
                        st.session_state["_ew_plan_written_for"] = _ds_plan
                        st.toast(f"📅 Plan dnia {_ds_plan} zapisany (trwałe statystyki).")
                    except Exception as _e_plan:
                        st.caption(f"⚠️ Nie udało się zapisać planu dnia: {str(_e_plan)[:120]}")

            # === AUTO-START AUTOPILOTA na X% ===
            if autopilot_pct > 0 and gen_obsada and not st.session_state.get("_ew_autopilot_started"):
                ap_state = get_autopilot_status().get("state", "idle")
                if ap_state == "idle":
                    work_date_str = data_obrobki.strftime('%d.%m') if data_obrobki else "?"
                    case_queue, total_wolne = build_autopilot_queue(autopilot_pct, gen_obsada, work_date_str)
                    
                    if case_queue:
                        set_autopilot_status({
                            "state": "running",
                            "processed": 0,
                            "total": len(case_queue),
                            "current_nrzam": "",
                            "last_error": "",
                            "pause_seconds": ap_pause,
                            "model": ap_model,
                            "prompt_url": ap_prompt_url,
                            "prompt_name": ap_prompt_name,
                            "work_date": work_date_str,
                            "tryb": "od_szturchacza",
                            "key_indices": ap_key_indices,
                            "obsada": {g: ops for g, ops in gen_obsada.items()},
                            "started_at": firestore.SERVER_TIMESTAMP,
                        })
                        db.collection(col("autopilot_config")).document("queue").set({
                            "cases": case_queue,
                        })
                        st.session_state["_ew_autopilot_started"] = True
                        st.toast(f"🤖 Autopilot auto-start: {len(case_queue)} casów ({autopilot_pct}% z {total_wolne})")
                        st.rerun()
                    else:
                        st.info("🤖 Autopilot: 0 casów do przeliczenia (brak obsady lub wolnych).")
                elif ap_state == "running":
                    st.info("🤖 Autopilot działa — przejdź do zakładki **Dolewka + Status**.")
        
        # Reset
        if st.button("🗑️ Wyczyść partycje (zacznij od nowa)"):
            for k in list(st.session_state.keys()):
                if k.startswith("_ew_"):
                    del st.session_state[k]
            st.rerun()
    
    # Podgląd surowego outputu (jeśli jest)
    raw_output = st.session_state.get("_ew_raw_ai_output", "")
    if raw_output:
        with st.expander("📄 Surowy wynik AI (kliknij żeby zobaczyć)", expanded=False):
            st.text(raw_output[:20000])
        
        all_cases = st.session_state.get("_ew_all_cases", [])
        if all_cases:
            de = [c for c in all_cases if c.get("grupa") == "DE"]
            fr = [c for c in all_cases if c.get("grupa") == "FR"]
            ukpl = [c for c in all_cases if c.get("grupa") == "UKPL"]
            st.success(f"📊 Dotychczas przeliczono: **{len(all_cases)}** casów — DE={len(de)} | FR={len(fr)} | UKPL={len(ukpl)}")


# ==========================================
# 🤖 ZAKŁADKA: DOLEWKA + STATUS
# ==========================================
with tab_autopilot:
    st.subheader("🤖 Dolewka + Status autopilota")
    
    ap_status = get_autopilot_status()
    state = ap_status.get("state", "idle")
    
    # --- OBSADA DOLEWKI ---
    st.markdown("### 👥 Obsada dolewki")
    ALL_OPERATORS_LIST_DL = ["Emilia", "Oliwia", "Magda", "Ewelina", "Iwona", "Marlena", "Sylwia", "EwelinaG", "Andrzej", "Marta", "Klaudia", "Kasia", "Romana", "oliwia_m"]
    ROLE_TO_GRUPA_DL = {"Operatorzy_DE": "DE", "Operatorzy_FR": "FR", "Operatorzy_UK/PL": "UKPL"}
    ops_by_grupa_dl = {"DE": [], "FR": [], "UKPL": []}
    for op_c in ALL_OPERATORS_LIST_DL:
        try:
            cfg_doc = db.collection(col("operator_configs")).document(op_c).get()
            if cfg_doc.exists:
                role = cfg_doc.to_dict().get("role", "Operatorzy_DE")
                grupa = ROLE_TO_GRUPA_DL.get(role, "DE")
                ops_by_grupa_dl[grupa].append(op_c)
            else:
                ops_by_grupa_dl["DE"].append(op_c)
        except Exception:
            ops_by_grupa_dl["DE"].append(op_c)
    
    # Domyślna obsada z taba Generuj (jeśli była ustawiona)
    gen_obs = st.session_state.get("_gen_obsada", {})
    
    col_do1, col_do2, col_do3 = st.columns(3)
    with col_do1:
        dl_ops_de = st.multiselect("🇩🇪 DE:", ops_by_grupa_dl["DE"], default=[o for o in gen_obs.get("DE", []) if o in ops_by_grupa_dl["DE"]], key="dl_ops_de")
    with col_do2:
        dl_ops_fr = st.multiselect("🇫🇷 FR:", ops_by_grupa_dl["FR"], default=[o for o in gen_obs.get("FR", []) if o in ops_by_grupa_dl["FR"]], key="dl_ops_fr")
    with col_do3:
        dl_ops_ukpl = st.multiselect("🇬🇧 UKPL:", ops_by_grupa_dl["UKPL"], default=[o for o in gen_obs.get("UKPL", []) if o in ops_by_grupa_dl["UKPL"]], key="dl_ops_ukpl")
    
    dl_obsada = {}
    if dl_ops_de: dl_obsada["DE"] = dl_ops_de
    if dl_ops_fr: dl_obsada["FR"] = dl_ops_fr
    if dl_ops_ukpl: dl_obsada["UKPL"] = dl_ops_ukpl
    
    # --- PARAMETRY AUTOPILOTA (dolewka) ---
    with st.expander("⚙️ Parametry autopilota (dolewka)", expanded=False):
        PROMPT_URLS_hardcoded_dl = {
            "Prompt Stabilny (prompt4624)": "https://raw.githubusercontent.com/szturchaczysko-cpu/szturchacz/refs/heads/main/prompt4624.txt",
        }
        custom_prompts_dl = (db.collection(col("admin_config")).document("custom_prompts").get().to_dict() or {}).get("urls", {})
        
        # Pobierz prompty z GitHuba (taka sama lista jak w zakładce 🧪 Prompty)
        _github_prompts_dl = _fetch_github_prompts()
        _github_prompts_dict_dl = {}
        if isinstance(_github_prompts_dl, list):
            for _p in _github_prompts_dl:
                _github_prompts_dict_dl[_p["name"]] = _p["raw_url"]
        
        ALL_OP_PROMPT_URLS_DL = {**PROMPT_URLS_hardcoded_dl, **custom_prompts_dl, **_github_prompts_dict_dl}
        
        # Default: aktualny default warstwy B
        _default_b_dl = (db.collection(col("admin_config")).document("default_prompt").get().to_dict() or {}).get("prompt_name", "")
        _options_list_dl = list(ALL_OP_PROMPT_URLS_DL.keys())
        _default_idx_dl = 0
        if _default_b_dl and _default_b_dl in _options_list_dl:
            _default_idx_dl = _options_list_dl.index(_default_b_dl)
        
        col_dlp1, col_dlp2, col_dlp3 = st.columns(3)
        with col_dlp1:
            dl_prompt_name = st.selectbox("Prompt operatorski:", _options_list_dl, index=_default_idx_dl, key="dl_prompt")
            dl_prompt_url = ALL_OP_PROMPT_URLS_DL[dl_prompt_name]
        with col_dlp2:
            dl_pause = st.slider("⏱️ Pauza (sek):", min_value=5, max_value=120, value=30, step=5, key="dl_pause")
            dl_model = st.selectbox("Model AI:", ["gemini-2.5-pro", "gemini-2.5-flash"], key="dl_model")
        with col_dlp3:
            dl_available_keys = [f"{i+1} - {p}" for i, p in enumerate(GCP_PROJECTS)]
            dl_keys = st.multiselect("🔑 Klucze do rotacji:", dl_available_keys, default=dl_available_keys, key="dl_keys")
            dl_key_indices = [int(k.split(" - ")[0]) - 1 for k in dl_keys]
        
        dl_work_date = st.date_input("📅 Data obróbki:", value=datetime.now(pytz.timezone('Europe/Warsaw')).date(), key="dl_work_date")
    
    # --- BAK per grupa ---
    st.markdown("### 🛢️ Bak — przeliczone casy w rezerwie per grupa")
    st.caption("Ile casów autopilotem przeliczonych jeszcze czeka na operatorów (wolne + calculated)")
    
    bak_docs = db.collection(col("ew_cases")).where("status", "==", "wolny").get()
    bak_data = {"DE": {"w_baku": 0, "do_dolania": 0}, "FR": {"w_baku": 0, "do_dolania": 0}, "UKPL": {"w_baku": 0, "do_dolania": 0}}
    
    for bdoc in bak_docs:
        d = bdoc.to_dict()
        g = d.get("grupa", "")
        if g in bak_data:
            if d.get("autopilot_status") == "calculated":
                bak_data[g]["w_baku"] += 1
            else:
                bak_data[g]["do_dolania"] += 1
    
    col_bak1, col_bak2, col_bak3 = st.columns(3)
    dolewka_pcts = {}
    
    for col_b, gname, flag in [(col_bak1, "DE", "🇩🇪"), (col_bak2, "FR", "🇫🇷"), (col_bak3, "UKPL", "🇬🇧")]:
        with col_b:
            bd = bak_data[gname]
            total_g = bd["w_baku"] + bd["do_dolania"]
            st.markdown(f"**{flag} {gname}**")
            st.metric(f"🛢️ W baku", bd["w_baku"])
            st.caption(f"Do dolania: {bd['do_dolania']} | Razem wolnych: {total_g}")
            dolewka_pcts[gname] = st.slider(f"Dolej %:", min_value=0, max_value=100, value=0, step=5, key=f"dolej_{gname}")
    
    # --- 📊 STAN OPERATORÓW ---
    st.markdown("### 📊 Stan operatorów — STAN BIEŻĄCY (żywa pula)")
    st.caption("Migawka aktualnej puli ew_cases (per grupa + cała firma). Pełna historia — zakresy dat, "
               "diamenty, skuteczność, wsad-per-dzień — w zakładce 💎 Diamentoza.")
    render_group_summary_now()

    _tz_dl = pytz.timezone('Europe/Warsaw')
    _today_dl = datetime.now(_tz_dl).date()
    st.markdown("#### 👤 Operatorzy — dziś (z trwałej kroniki)")
    render_operator_table(_today_dl, _today_dl, key_prefix="dl")
    st.markdown("#### 🔁 Wsady odwrotne — dziś (per operator)")
    render_reverse_breakdown(_today_dl, _today_dl, key_prefix="dl")
    st.markdown("#### ➕ Poza planem — dziś (ruchy spoza dnia planu)")
    render_poza_planem(_today_dl, _today_dl, key_prefix="dl")
    st.markdown("#### 📞 Telefony — dziś")
    render_phone_stats(_today_dl, _today_dl, key_prefix="dl")
    render_woreczek_stats(_today_dl, _today_dl, key_prefix="dl")

    # Dolewka button
    if any(v > 0 for v in dolewka_pcts.values()):
        # Policz ile casów do dolania
        dolewka_summary = []
        for g, pct in dolewka_pcts.items():
            if pct > 0:
                count = max(1, int(bak_data[g]["do_dolania"] * pct / 100))
                dolewka_summary.append(f"{g}: {count} ({pct}% z {bak_data[g]['do_dolania']})")
        st.info(f"🎯 Dolewka: {' | '.join(dolewka_summary)}")
        
        if state == "running":
            st.warning("⚠️ Autopilot jeszcze działa — poczekaj aż skończy.")
        elif st.button("🤖 Przepilotuj dolewkę", type="primary"):
            if not dl_obsada:
                st.error("⚠️ Wybierz operatorów powyżej!")
            else:
                # Zbierz casy do dolania per grupa
                dolewka_queue = []
                group_counters = {g: 0 for g in dl_obsada}
                
                for g, pct in dolewka_pcts.items():
                    if pct <= 0 or g not in dl_obsada or not dl_obsada[g]:
                        continue
                    g_cases = []
                    for bdoc in bak_docs:
                        d = bdoc.to_dict()
                        if d.get("grupa") == g and d.get("autopilot_status") != "calculated":
                            d["_doc_id"] = bdoc.id
                            g_cases.append(d)
                    g_cases.sort(key=lambda c: -c.get("score", 0))
                    count = max(1, int(len(g_cases) * pct / 100))
                    top_g = g_cases[:count]
                    
                    ops = dl_obsada[g]
                    for wc in top_g:
                        assigned_op = ops[group_counters[g] % len(ops)]
                        group_counters[g] += 1
                        dolewka_queue.append({
                            "doc_id": wc["_doc_id"],
                            "nrzam": wc.get("numer_zamowienia", "?"),
                            "operator": assigned_op,
                            "grupa": g,
                            "grupa_operatorska": GRUPA_MAP_GLOBAL.get(g, "Operatorzy_DE"),
                        })
                        db.collection(col("ew_cases")).document(wc["_doc_id"]).update({
                            "autopilot_assigned_to": assigned_op,
                        })
                
                if dolewka_queue:
                    work_date_str = dl_work_date.strftime('%d.%m')
                    
                    set_autopilot_status({
                        "state": "running",
                        "processed": 0,
                        "total": len(dolewka_queue),
                        "current_nrzam": "",
                        "last_error": "",
                        "pause_seconds": dl_pause,
                        "model": dl_model,
                        "prompt_url": dl_prompt_url,
                        "prompt_name": dl_prompt_name,
                        "work_date": work_date_str,
                        "tryb": "od_szturchacza",
                        "key_indices": dl_key_indices,
                        "obsada": {g: ops for g, ops in dl_obsada.items()},
                        "started_at": firestore.SERVER_TIMESTAMP,
                    })
                    db.collection(col("autopilot_config")).document("queue").set({
                        "cases": dolewka_queue,
                    })
                    st.success(f"🤖 Dolewka uruchomiona: {len(dolewka_queue)} casów!")
                    st.rerun()
                else:
                    st.warning("Brak casów do dolania (brak obsady lub 0 nieprzeliczonych).")
    
    # --- STATUS AUTOPILOTA ---
    st.markdown("---")
    st.markdown("### 📊 Status autopilota")
    
    if state == "running":
        processed = ap_status.get("processed", 0)
        total = ap_status.get("total", 0)
        current = ap_status.get("current_nrzam", "")
        pct = processed / max(total, 1)
        
        st.warning(f"🔄 **Autopilot działa** — {processed}/{total} casów przeliczonych")
        st.progress(pct, text=f"Case {processed+1}/{total}: {current}")
        
        if ap_status.get("last_error"):
            st.error(f"Ostatni błąd: {ap_status['last_error']}")
        
        col_stop1, col_stop2 = st.columns(2)
        with col_stop1:
            if st.button("⏸️ STOP Autopilot", type="primary"):
                set_autopilot_status({"state": "stopping"})
                st.rerun()
        with col_stop2:
            if st.button("🔄 Odśwież postęp"):
                st.rerun()
    
    elif state == "stopping":
        processed = ap_status.get("processed", 0)
        total = ap_status.get("total", 0)
        st.warning(f"⏸️ Autopilot zatrzymany po {processed}/{total} casach.")
        col_r1, col_r2 = st.columns(2)
        with col_r1:
            if st.button("▶️ Wznów od miejsca zatrzymania", type="primary"):
                set_autopilot_status({"state": "running"})
                st.rerun()
        with col_r2:
            if st.button("🔄 Reset (zacznij od nowa)"):
                set_autopilot_status({"state": "idle", "processed": 0, "total": 0, "current_nrzam": "", "last_error": ""})
                st.rerun()
    
    elif state == "done":
        processed = ap_status.get("processed", 0)
        total = ap_status.get("total", 0)
        st.success(f"✅ **Autopilot zakończony** — przeliczono {processed}/{total} casów")
        st.progress(1.0)
        if st.button("🔄 Reset (nowa sesja)"):
            set_autopilot_status({"state": "idle", "processed": 0, "total": 0, "current_nrzam": "", "last_error": ""})
            st.rerun()
    
    else:  # idle
        st.info("💤 Autopilot nieaktywny. Uruchom z zakładki 'Generuj + Autopilot' lub użyj dolewki powyżej.")
    
    # --- CZYSZCZENIE ---
    st.markdown("---")
    with st.expander("🧹 Zarządzanie przeliczeniami nocnymi"):
        st.caption("Wyczyść nocne przeliczenia (autopilot_messages) z casów w bazie.")
        col_clean1, col_clean2 = st.columns(2)
        with col_clean1:
            if st.button("🧹 Wyczyść przeliczenia nocne", type="secondary"):
                all_docs = db.collection(col("ew_cases")).limit(5000).get()
                cleared = 0
                for doc in all_docs:
                    d = doc.to_dict()
                    if d.get("autopilot_messages") or d.get("autopilot_status") == "calculated":
                        db.collection(col("ew_cases")).document(doc.id).update({
                            "autopilot_messages": firestore.DELETE_FIELD,
                            "autopilot_status": firestore.DELETE_FIELD,
                            "autopilot_operator": firestore.DELETE_FIELD,
                            "autopilot_date": firestore.DELETE_FIELD,
                            "autopilot_calculated_at": firestore.DELETE_FIELD,
                            "autopilot_model": firestore.DELETE_FIELD,
                            "autopilot_project": firestore.DELETE_FIELD,
                            "autopilot_assigned_to": firestore.DELETE_FIELD,
                        })
                        cleared += 1
                set_autopilot_status({"state": "idle", "processed": 0, "total": 0})
                try:
                    db.collection(col("autopilot_config")).document("queue").delete()
                except:
                    pass
                st.success(f"✅ Wyczyszczono nocne przeliczenia z {cleared} casów.")
                st.rerun()
        with col_clean2:
            try:
                all_docs_check = db.collection(col("ew_cases")).limit(5000).get()
                with_autopilot = sum(1 for d in all_docs_check if d.to_dict().get("autopilot_status") == "calculated")
                st.info(f"🤖 Casów z nocnym przeliczeniem: **{with_autopilot}**")
            except:
                pass

    # ===========================================
    # PĘTLA AUTOPILOTA (działa gdy state=running)
    # Przetwarzaj JEDEN case per rerun żeby websocket nie padł
    # ===========================================
    if state == "running":
        # Pobierz konfigurację
        ap_cfg = get_autopilot_status()
        try:
            queue_doc = db.collection(col("autopilot_config")).document("queue").get()
        except Exception:
            queue_doc = None
        if not queue_doc or not queue_doc.exists:
            set_autopilot_status({"state": "idle", "last_error": "Brak kolejki casów"})
            st.rerun()
        else:
            queue = queue_doc.to_dict().get("cases", [])
            processed = ap_cfg.get("processed", 0)
            total = len(queue)
            pause_sec = ap_cfg.get("pause_seconds", 30)
            model_id = ap_cfg.get("model", "gemini-2.5-pro")
            prompt_url = ap_cfg.get("prompt_url", "")
            work_date = ap_cfg.get("work_date", "")
            tryb = ap_cfg.get("tryb", "od_szturchacza")
            key_indices = ap_cfg.get("key_indices", [0])

            # Fallback daty
            if not work_date:
                tz_pl = pytz.timezone('Europe/Warsaw')
                work_date = datetime.now(tz_pl).strftime('%d.%m')

            # Sprawdź czy jest jeszcze coś do zrobienia
            if processed >= total:
                set_autopilot_status({"state": "done", "processed": total, "current_nrzam": ""})
                st.balloons()
                st.rerun()
            else:
                # Znajdź następny case do przeliczenia (skip pominiętych)
                idx = processed
                case_info = None
                while idx < total:
                    candidate = queue[idx]
                    doc_id = candidate["doc_id"]
                    case_doc = db.collection(col("ew_cases")).document(doc_id).get()
                    if not case_doc.exists:
                        st.caption(f"⚠️ {candidate['nrzam']}: usunięty, pomijam")
                        idx += 1
                        continue
                    case_data = case_doc.to_dict()
                    if case_data.get("status") != "wolny":
                        st.caption(f"⏭️ {candidate['nrzam']}: status={case_data.get('status')} — pomijam")
                        idx += 1
                        continue
                    if case_data.get("autopilot_status") == "calculated":
                        st.caption(f"✅ {candidate['nrzam']}: już przeliczone — pomijam")
                        idx += 1
                        continue
                    wsad = case_data.get("pelna_linia_szturchacza", "")
                    if not wsad:
                        st.caption(f"⚠️ {candidate['nrzam']}: brak wsadu — pomijam")
                        idx += 1
                        continue
                    # --- MODUŁ TELEFONY: routing sprawy oczekującej na wynik telefonu (nocne przeliczanie) ---
                    # "Oczekuje na telefon" = flaga woreczka (telefon_do_wykonania) LUB token w tagu/wsadzie
                    # (FORUM_TEL=czekam_wynik / brakuje=wynik telefonu). Token jest ważny, bo codzienny wsad
                    # nadpisuje dokument i kasuje flagę — token wraca z panelu i odtwarza stan.
                    # Telefonista już odpowiedział na forum → zdejmij z woreczka i przelicz jako STANDARD.
                    # Brak odpowiedzi → wrzuć/zostaw w woreczku i NIE przeliczaj (bez precompute, bez calculated).
                    _awaits_phone = bool(case_data.get("telefon_do_wykonania")) or bool(
                        re.search(r'FORUM_TEL\s*=\s*czekam_wynik|brakuje[:=]\s*wynik[_ ]?telefonu', wsad, re.IGNORECASE))
                    if _awaits_phone:
                        _tel_ans = {"answered": False}
                        if FORUM_ENABLED:
                            try:
                                _tel_ans = check_forum_answer(db, col, candidate['nrzam'])
                            except Exception:
                                _tel_ans = {"answered": False}
                        if _tel_ans.get("answered"):
                            try:
                                db.collection(col("ew_cases")).document(doc_id).update({
                                    "telefon_do_wykonania": False,
                                    "telefon_status": "odpowiedz_telefonisty",
                                })
                            except Exception:
                                pass
                            st.caption(f"  📞→📋 {candidate['nrzam']}: telefonista odpowiedział → standard")
                            # NIE continue — leci dalej do normalnego przeliczenia jako sprawa standardowa.
                        else:
                            try:
                                _tel_pz_m = re.search(r'PZ\d+', wsad)
                                db.collection(col("ew_cases")).document(doc_id).update({
                                    "telefon_do_wykonania": True,
                                    "telefon_status": case_data.get("telefon_status") or "czeka",
                                    "telefon_pz": case_data.get("telefon_pz") or (_tel_pz_m.group(0) if _tel_pz_m else ""),
                                    "telefon_jezyk": case_data.get("telefon_jezyk") or case_data.get("grupa") or "",
                                    "telefon_wsad": case_data.get("telefon_wsad") or case_data.get("pelna_linia_szturchacza", ""),
                                    "telefon_flagged_at": firestore.SERVER_TIMESTAMP,
                                })
                            except Exception:
                                pass
                            st.caption(f"  📞 {candidate['nrzam']}: brak odpowiedzi telefonisty → woreczek (pomijam przeliczanie)")
                            idx += 1
                            continue
                    # --- FORUM: auto-odczyt pamięci forumowej ---
                    if FORUM_ENABLED:
                        forum_ctx = auto_load_forum_context(db, col, candidate['nrzam'])
                        if forum_ctx:
                            wsad = wsad + "\n\n" + forum_ctx
                            st.caption(f"  📖 Forum: kontekst załadowany dla {candidate['nrzam']}")
                    # --- KONIEC FORUM ---
                    case_info = candidate
                    break

                if case_info is None:
                    # Wszystkie pominięte/przeliczone
                    set_autopilot_status({"state": "done", "processed": total, "current_nrzam": ""})
                    st.balloons()
                    st.rerun()
                else:
                    # Przelicz JEDEN case
                    doc_id = case_info["doc_id"]
                    nrzam = case_info["nrzam"]
                    case_operator = case_info.get("operator", "Autopilot")
                    case_grupa_op = case_info.get("grupa_operatorska", "Operatorzy_DE")

                    set_autopilot_status({"processed": idx, "current_nrzam": nrzam, "last_error": ""})
                    st.info(f"🤖 Case {idx+1}/{total}: **{nrzam}** — odpytywanie AI...")

                    # Pobierz prompt operatorski
                    OP_PROMPT = get_remote_prompt(prompt_url)
                    if not OP_PROMPT:
                        set_autopilot_status({"state": "done", "last_error": "Nie udało się pobrać promptu operatorskiego"})
                        st.rerun()

                    try:
                        _apkc = db.collection(col("admin_config")).document("kurier_config").get().to_dict() or {}
                        _ap_kurier_mode = _apkc.get("mode", "operatorzy")
                    except Exception:
                        _ap_kurier_mode = "operatorzy"
                    if _ap_kurier_mode not in ("atomowki", "operatorzy"):
                        _ap_kurier_mode = "operatorzy"

                    parametry = f"""
# PARAMETRY STARTOWE
domyslny_operator={case_operator}
domyslna_data={work_date}
Grupa_Operatorska={case_grupa_op}
domyslny_tryb={tryb}
notag=TAK
analizbior=NIE
zamawianie_kurierow={_ap_kurier_mode}
"""
                    FULL_PROMPT = OP_PROMPT + parametry

                    # Safety settings
                    safety_settings = [
                        SafetySetting(category=HarmCategory.HARM_CATEGORY_DANGEROUS_CONTENT, threshold=HarmBlockThreshold.BLOCK_NONE),
                        SafetySetting(category=HarmCategory.HARM_CATEGORY_HARASSMENT, threshold=HarmBlockThreshold.BLOCK_NONE),
                        SafetySetting(category=HarmCategory.HARM_CATEGORY_HATE_SPEECH, threshold=HarmBlockThreshold.BLOCK_NONE),
                        SafetySetting(category=HarmCategory.HARM_CATEGORY_SEXUALLY_EXPLICIT, threshold=HarmBlockThreshold.BLOCK_NONE),
                    ]

                    # --- ROTACJA KLUCZY ---
                    key_idx = key_indices[idx % len(key_indices)]
                    project = GCP_PROJECTS[key_idx]

                    try:
                        ci = json.loads(st.secrets["FIREBASE_CREDS"])
                        cv = service_account.Credentials.from_service_account_info(ci)
                        vertexai.init(project=project, location=st.secrets.get("GCP_LOCATION", "us-central1"), credentials=cv)
                    except Exception as e:
                        set_autopilot_status({"last_error": f"Vertex init error: {str(e)[:200]}"})
                        st.error(f"❌ {nrzam}: Vertex init error — {str(e)[:200]}")
                        # Przejdź do następnego na rerun
                        set_autopilot_status({"processed": idx + 1})
                        time.sleep(3)
                        st.rerun()

                    # --- WYWOŁANIE AI (kaskadowy fallback) ---
                    ai_response = None
                    FALLBACK_CHAIN_AP = ["gemini-2.5-pro", "gemini-2.5-flash"]
                    ap_models_to_try = [model_id]
                    for fb in FALLBACK_CHAIN_AP:
                        if fb != model_id and fb not in ap_models_to_try:
                            ap_models_to_try.append(fb)

                    used_ap_model = model_id
                    for try_model in ap_models_to_try:
                        for attempt in range(3):
                            try:
                                model = GenerativeModel(try_model, system_instruction=FULL_PROMPT)
                                chat = model.start_chat(response_validation=False)
                                resp = chat.send_message(
                                    wsad,
                                    generation_config={"temperature": 0.0, "max_output_tokens": 8192},
                                    safety_settings=safety_settings,
                                )
                                if resp.candidates and resp.candidates[0].content and resp.candidates[0].content.parts:
                                    ai_response = resp.candidates[0].content.parts[0].text
                                else:
                                    ai_response = resp.text
                                used_ap_model = try_model
                                break
                            except Exception as e:
                                err_str = str(e)
                                if "429" in err_str or "Quota" in err_str or "ResourceExhausted" in err_str or "503" in err_str or "unavailable" in err_str.lower():
                                    wait_time = min(5 * (attempt + 1), 10)  # 5s, 10s, 10s
                                    st.caption(f"⏳ {try_model}, {nrzam}, próba {attempt+1}/3, czekam {wait_time}s...")
                                    time.sleep(wait_time)
                                else:
                                    set_autopilot_status({"last_error": f"{nrzam}: {err_str[:200]}"})
                                    st.caption(f"⚠️ {nrzam}: {try_model} — {err_str[:100]}")
                                    break

                        if ai_response:
                            break

                    # --- ZAPIS WYNIKU ---
                    if ai_response:
                        # --- E3: FORUM INTEGRATION (autopilot) ---
                        # Pętla: AI → markery → wykonaj → jeśli READ → re-send z kontekstem → powtórz
                        autopilot_conversation = [
                            {"role": "user", "content": wsad},
                            {"role": "model", "content": ai_response},
                        ]
                        
                        if FORUM_ENABLED:
                            for forum_iter in range(3):  # max 3 iteracje forum
                                if "[FORUM_WRITE|" not in ai_response and "[FORUM_READ|" not in ai_response:
                                    break

                                # MODUŁ TELEFONY: czy autopilot deleguje telefon? (raw ai_response, przed wykonaniem markerów)
                                _had_tel_deleg_ap = ("[FORUM_WRITE|" in ai_response and bool(
                                    re.search(r'user_do\s*=\s*Telefoni', ai_response, re.IGNORECASE)))
                                # REALNY język delegacji (z user_do=Telefoniści_XX). ES/IT → brak operatora
                                # dzwoniącego → NIE wkładamy do woreczka (tak jak app operatorska §8.1.1).
                                _tdl_ap = re.search(r'Telefoni[^_|\]]*_(DE|FR|PL|IT|ES|ENG)\b', ai_response, re.IGNORECASE)
                                _tel_deleg_lang_ap = _tdl_ap.group(1).upper() if _tdl_ap else ""
                                _tel_lang_ma_operatora_ap = _tel_deleg_lang_ap not in ("ES", "IT")

                                _fm_e3 = load_forum_memory(db, col, nrzam) if nrzam else {}
                                # Mapowanie: Stats nazwa → forum nick (Kasia → kasia_k)
                                _OP_FORUM_NICK_AP = {"Kasia": "kasia_k"}
                                _case_op_forum = _OP_FORUM_NICK_AP.get(case_operator, case_operator)
                                
                                # === DIAMOND DETECTION (autopilot / AutoSzturchacz) ===
                                # Te same reguły co w apce operatorskiej, tylko source_type="autoszturchacz".
                                # Diament liczony gdy cel=AUTOS_KURIERZY + PZ=PZ6 + bump=0 (1 case = 1 diament/dzień).
                                _pz_match_ap = re.search(r'PZ\s*=\s*(PZ\d+)', ai_response)
                                _bump_match_ap = re.search(r'bump\s*=\s*(\d+)', ai_response)
                                _detected_pz_ap = _pz_match_ap.group(1) if _pz_match_ap else None
                                _detected_bump_ap = int(_bump_match_ap.group(1)) if _bump_match_ap else None
                                _is_diamond_ap = (_detected_pz_ap == "PZ6" and _detected_bump_ap == 0)
                                
                                _kurier_match_ap = re.search(r'KURIER_PRZEWOZNIK\s*=\s*([A-Z_]+)', ai_response)
                                _towar_match_ap = re.search(r'TOWAR_TYP\s*=\s*([A-Z_]+)', ai_response)
                                
                                # Mapuj grupa_operatorska → krótka grupa (DE/FR/UKPL) do logu
                                _ROLE_TO_GRUPA_DIAM = {"Operatorzy_DE": "DE", "Operatorzy_FR": "FR", "Operatorzy_UK/PL": "UKPL"}
                                _grupa_short = _ROLE_TO_GRUPA_DIAM.get(case_grupa_op, case_info.get("grupa", "?"))
                                
                                _diamond_meta_ap = {
                                    "numer_zamowienia": nrzam,
                                    "operator": case_operator,
                                    "kurier": _kurier_match_ap.group(1) if _kurier_match_ap else None,
                                    "kategoria_towaru": _towar_match_ap.group(1) if _towar_match_ap else None,
                                    "grupa": _grupa_short,
                                    "pz": _detected_pz_ap,
                                    "bump": _detected_bump_ap,
                                }
                                
                                # PRYWATNOŚĆ (brief §6.3): autopilot = sesja BEZ operatora → user_od=grupa (FromUser, typ 2),
                                # faktyczny autor = konto AI "chatoszturek" → UserRzeczywisty. AiUser i tak stałe "chatoszturek".
                                forum_result = execute_forum_actions(
                                    ai_response,
                                    forum_memory=_fm_e3,
                                    user_od=case_grupa_op,
                                    ai_user="chatoszturek",  # autopilot bez operatora → UserRzeczywisty=chatoszturek
                                    db=db,
                                    source_type="autoszturchacz",
                                    diamond_prefix=_COL_PREFIX,
                                    is_diamond_candidate=_is_diamond_ap,
                                    diamond_meta=_diamond_meta_ap,
                                )
                                ai_response = forum_result["response"]
                                autopilot_conversation[-1]["content"] = ai_response
                                
                                # FORUM_WRITE → loguj wyniki + ZAPISZ DO PAMIĘCI
                                _any_success_e3 = False
                                for fw in forum_result.get("forum_writes", []):
                                    if fw.get("success"):
                                        _any_success_e3 = True
                                        st.caption(f"  📤 Forum WRITE: post {fw.get('FORUM_ID', '?')} wysłany")
                                        if nrzam and fw.get("FORUM_ID") and fw.get("cel"):
                                            save_forum_memory(db, col, nrzam, fw["cel"], fw["FORUM_ID"], fw.get("tresc_skrot", ""))
                                    else:
                                        st.caption(f"  ❌ Forum WRITE: {fw.get('error', '?')}")
                                
                                # --- v1.5.7c: last_action_source w ew_cases (autoszturchacz) ---
                                # Spójność z patchem szturchacza — tabela 'Stan operatorów' widzi nocne ruchy bota.
                                if _any_success_e3 and doc_id:
                                    try:
                                        db.collection(col("ew_cases")).document(doc_id).update({
                                            "last_action_source": "autoszturchacz",
                                            "last_action_at": firestore.SERVER_TIMESTAMP,
                                        })
                                    except Exception:
                                        pass  # nie wywróć autopilota

                                # MODUŁ TELEFONY: autopilot zlecił telefon → wpnij case do woreczka (lustro app operatora).
                                # ES/IT → brak operatora dzwoniącego → NIE do woreczka (delegacja do telefonistów + zamknięcie).
                                # telefon_jezyk = REALNY język delegacji (nie grupa!), żeby filtr woreczka po języku działał.
                                if _had_tel_deleg_ap and _tel_lang_ma_operatora_ap and _any_success_e3 and doc_id:
                                    try:
                                        db.collection(col("ew_cases")).document(doc_id).update({
                                            "telefon_do_wykonania": True,
                                            "telefon_status": "czeka",
                                            "telefon_zlecil": "autoszturchacz",
                                            "telefon_pz": _detected_pz_ap or "",
                                            "telefon_jezyk": _tel_deleg_lang_ap or _grupa_short,
                                            "telefon_wsad": wsad,
                                            "telefon_flagged_at": firestore.SERVER_TIMESTAMP,
                                        })
                                    except Exception:
                                        pass  # nie wywróć autopilota
                                
                                # FORUM_READ → wstrzyknij kontekst i odpytaj AI ponownie
                                if forum_result.get("forum_reads"):
                                    forum_context = "\n\n".join(forum_result["forum_reads"])
                                    st.caption(f"  📖 Forum READ: wstrzykuję kontekst ({len(forum_context)} zn.)")
                                    
                                    # Dodaj kontekst do konwersacji
                                    autopilot_conversation.append({"role": "user", "content": forum_context})
                                    
                                    # Re-send do AI z pełną historią
                                    try:
                                        history_for_resend = []
                                        for msg in autopilot_conversation:
                                            role_vertex = "user" if msg["role"] == "user" else "model"
                                            history_for_resend.append(
                                                Content(role=role_vertex, parts=[Part.from_text(msg["content"])])
                                            )
                                        
                                        # Wyślij ostatni message (forum_context)
                                        model_resend = GenerativeModel(used_ap_model, system_instruction=FULL_PROMPT)
                                        chat_resend = model_resend.start_chat(
                                            history=history_for_resend[:-1],
                                            response_validation=False
                                        )
                                        resp_resend = chat_resend.send_message(
                                            forum_context,
                                            generation_config={"temperature": 0.0, "max_output_tokens": 8192},
                                            safety_settings=safety_settings,
                                        )
                                        if resp_resend.candidates and resp_resend.candidates[0].content and resp_resend.candidates[0].content.parts:
                                            ai_response = resp_resend.candidates[0].content.parts[0].text
                                        else:
                                            ai_response = resp_resend.text
                                        
                                        autopilot_conversation.append({"role": "model", "content": ai_response})
                                        st.caption(f"  🤖 AI re-response po forum ({len(ai_response)} zn.)")
                                    except Exception as e_forum:
                                        st.caption(f"  ⚠️ Forum re-send error: {str(e_forum)[:100]}")
                                        break
                                else:
                                    break  # Tylko WRITE, bez READ → nie trzeba ponownie pytać AI
                        # --- KONIEC E3 ---
                        
                        db.collection(col("ew_cases")).document(doc_id).update({
                            "autopilot_status": "calculated",
                            "autopilot_messages": autopilot_conversation,
                            "autopilot_calculated_at": firestore.SERVER_TIMESTAMP,
                            "autopilot_model": used_ap_model,
                            "autopilot_project": project,
                            "autopilot_operator": case_operator,
                            "autopilot_date": work_date,
                        })
                        # Trwały licznik przeliczeń autopilota per grupa (na dzień przeliczenia)
                        _ap_grupa = case_info.get("grupa", "")
                        if _ap_grupa in ("DE", "FR", "UKPL"):
                            try:
                                _tz_apc = pytz.timezone('Europe/Warsaw')
                                _today_apc = datetime.now(_tz_apc).strftime("%Y-%m-%d")
                                db.collection(col("ew_operator_stats")).document(_today_apc).set(
                                    {f"apc_{_ap_grupa}": firestore.Increment(1)}, merge=True
                                )
                            except Exception:
                                pass
                        st.success(f"✅ {nrzam}: przeliczone ({len(ai_response)} znaków) — {case_operator} — klucz {key_idx+1}")
                    else:
                        st.warning(f"⚠️ {nrzam}: brak odpowiedzi AI — pomijam")

                    # Zapisz postęp i RERUN (websocket stays alive)
                    set_autopilot_status({"processed": idx + 1, "current_nrzam": ""})

                    # Pauza przed rerun (krótsza niż oryginalna — rerun sam dodaje delay)
                    if idx + 1 < total:
                        time.sleep(min(pause_sec, 10))

                    st.rerun()


# ==========================================
# 📦 HISTORIA PARTII
# ==========================================
with tab_batches:
    st.subheader("📦 Historia partii Wieżowca")
    try:
        batches = db.collection(col("ew_batches")).order_by("created_at", direction=firestore.Query.DESCENDING).limit(20).get()
    except Exception:
        batches = []
    if not batches:
        st.info("Brak wygenerowanych partii.")
    else:
        for bdoc in batches:
            b = bdoc.to_dict()
            bid = bdoc.id
            ico = "🟢" if b.get("status") == "active" else "⚪"
            with st.expander(f"{ico} {bid} — {b.get('date_label', '?')} | {b.get('summary', '')}"):
                c1, c2 = st.columns(2)
                with c1:
                    st.metric("Casów", b.get("total_cases", 0))
                    st.caption(f"Prompt: {b.get('prompt_used', '?')} | Model: {b.get('model_used', '?')}")
                with c2:
                    batch_cases = db.collection(col("ew_cases")).where("batch_id", "==", bid).get()
                    sc = {"wolny": 0, "przydzielony": 0, "w_toku": 0, "zakonczony": 0}
                    for c in batch_cases:
                        s = c.to_dict().get("status", "wolny")
                        sc[s] = sc.get(s, 0) + 1
                    for k, v in sc.items():
                        st.caption(f"{k}: {v}")
                if b.get("status") == "active":
                    if st.button(f"📥 Archiwizuj", key=f"arch_{bid}"):
                        db.collection(col("ew_batches")).document(bid).update({"status": "archived"})
                        st.rerun()
                
                # Surowy output AI
                raw = b.get("raw_ai_output", "")
                if raw:
                    with st.expander("📄 Surowy wynik AI tego batcha"):
                        st.text(raw[:10000])


# ==========================================
# 📋 PRZEGLĄD CASÓW
# ==========================================
with tab_cases:
    st.subheader("📋 Przegląd casów")
    
    # Pobierz WSZYSTKIE casy raz (dla filtrów i statystyk)
    try:
        all_cases_raw = db.collection(col("ew_cases")).order_by("score", direction=firestore.Query.DESCENDING).limit(2000).get()
    except Exception:
        all_cases_raw = []
    all_cases_data = [(d.id, d.to_dict()) for d in all_cases_raw]
    
    # Zbierz unikalne wartości do selectboxów
    all_operators = sorted(set(d.get("assigned_to", "") for _, d in all_cases_data if d.get("assigned_to")))
    all_operators_nocne = sorted(set(d.get("autopilot_assigned_to", "") for _, d in all_cases_data if d.get("autopilot_assigned_to")))
    
    fc1, fc2, fc3, fc4, fc5 = st.columns(5)
    with fc1:
        fg = st.selectbox("Grupa:", ["Wszystkie", "DE", "FR", "UKPL", "Brak grupy / Score 0"])
    with fc2:
        fs = st.selectbox("Status:", ["Wszystkie", "wolny", "przydzielony", "w_toku", "zakonczony", "odroczony", "pominiety"])
    with fc3:
        fo = st.selectbox("Operator:", ["Wszystkie"] + all_operators)
    with fc4:
        fp = st.selectbox("Przeliczenie:", ["Wszystkie", "Przeliczone", "Nieprzeliczone"])
    with fc5:
        f_skip = st.selectbox("Pominięcia:", ["Wszystkie", "Z komentarzem", "Naprawione"], key="f_skip")
    
    # Wyszukiwarka po indexie
    f_index = st.text_input("🔍 Szukaj po indexie:", key="f_index", placeholder="np. 125C514GRUP1")
    
    # Filtrowanie po stronie klienta
    filtered = all_cases_data
    if fg == "Brak grupy / Score 0":
        filtered = [(did, d) for did, d in filtered if not d.get("grupa") or d.get("score", 0) == 0]
    elif fg != "Wszystkie":
        filtered = [(did, d) for did, d in filtered if d.get("grupa") == fg]
    if fs != "Wszystkie":
        filtered = [(did, d) for did, d in filtered if d.get("status") == fs]
    if fo != "Wszystkie":
        filtered = [(did, d) for did, d in filtered if d.get("assigned_to") == fo]
    if fp == "Przeliczone":
        filtered = [(did, d) for did, d in filtered if d.get("autopilot_status") == "calculated"]
    elif fp == "Nieprzeliczone":
        filtered = [(did, d) for did, d in filtered if d.get("autopilot_status") != "calculated"]
    if f_skip == "Z komentarzem":
        filtered = [(did, d) for did, d in filtered if d.get("skip_reason") and not d.get("skip_fixed")]
    elif f_skip == "Naprawione":
        filtered = [(did, d) for did, d in filtered if d.get("skip_fixed")]
    if f_index and f_index.strip():
        idx_q = f_index.strip().lower()
        filtered = [(did, d) for did, d in filtered if idx_q in d.get("index_handlowy", "").lower() or idx_q in d.get("pelna_linia_szturchacza", "").lower()]
    
    if not filtered:
        st.info("Brak casów.")
    else:
        total = len(filtered)
        
        # Statystyki
        n_wolny = sum(1 for _, d in filtered if d.get("status") == "wolny")
        n_przydz = sum(1 for _, d in filtered if d.get("status") in ("przydzielony", "w_toku"))
        n_zakonczony = sum(1 for _, d in filtered if d.get("status") == "zakonczony")
        n_przeliczone = sum(1 for _, d in filtered if d.get("autopilot_status") == "calculated")
        n_nieprzeliczone = total - n_przeliczone
        n_odroczony = sum(1 for _, d in filtered if d.get("status") == "odroczony")
        n_pominiety = sum(1 for _, d in filtered if d.get("status") == "pominiety")
        n_score0 = sum(1 for _, d in all_cases_data if d.get("score", 0) == 0 or not d.get("grupa"))
        st.markdown(f"📊 **Łącznie: {total}** | 🔵 Wolne: {n_wolny} | 🟡 Pobrane: {n_przydz} | 🟢 Zakończone: {n_zakonczony} | ⏭️ Pominięte: {n_pominiety} | ⏸️ Odroczone: {n_odroczony} | 🤖 Przeliczone: {n_przeliczone} | ⚪ Nieprzeliczone: {n_nieprzeliczone}")
        
        # Przycisk: usuń UNKNOWN (śmieci z parsera)
        unknown_cases = [(did, d) for did, d in all_cases_data if d.get("numer_zamowienia", "").startswith("UNKNOWN")]
        if unknown_cases:
            col_unk1, col_unk2 = st.columns([4, 1])
            with col_unk1:
                st.warning(f"⚠️ Znaleziono **{len(unknown_cases)}** casów UNKNOWN (śmieci z parsera — alerty/self-correction)")
            with col_unk2:
                if st.button(f"🗑️ Usuń {len(unknown_cases)} UNKNOWN", key="del_unknown"):
                    for did, _ in unknown_cases:
                        db.collection(col("ew_cases")).document(did).delete()
                    st.success(f"✅ Usunięto {len(unknown_cases)} UNKNOWN z bazy!")
                    st.rerun()
        
        # Przycisk: uwolnij odroczone do kolejki
        odroczone_cases = [(did, d) for did, d in all_cases_data if d.get("status") == "odroczony"]
        if odroczone_cases:
            no_grupa = sum(1 for _, d in odroczone_cases if not d.get("grupa"))
            col_odr1, col_odr2 = st.columns([3, 2])
            with col_odr1:
                st.info(f"⏸️ **{len(odroczone_cases)}** odroczonych casów" +
                        (f" (⚠️ {no_grupa} bez grupy — nie trafią do nikogo!)" if no_grupa else ""))
            with col_odr2:
                force_grupa = st.selectbox("Wymuś grupę (dla brakujących):", ["—", "DE", "FR", "UKPL"], key="odr_force_grupa")
                if st.button(f"☢️ Uwolnij WSZYSTKIE {len(odroczone_cases)} odroczone", key="release_all_odroczone"):
                    for did, d in odroczone_cases:
                        upd = {"status": "wolny"}
                        if force_grupa != "—" and not d.get("grupa"):
                            upd["grupa"] = force_grupa
                        db.collection(col("ew_cases")).document(did).update(upd)
                    st.success(f"✅ Uwolniono {len(odroczone_cases)} casów do kolejki!")
                    st.rerun()
        
        # Paginacja z opcją pokaż wszystkie
        show_all = st.checkbox("📄 Pokaż wszystkie na jednej stronie", key="show_all_cases")
        if show_all:
            PAGE_SIZE = total
            start = 0
            end = total
            st.caption(f"Wszystkie {total} casów")
        else:
            PAGE_SIZE = 50
            total_pages = max(1, (total + PAGE_SIZE - 1) // PAGE_SIZE)
            page = st.number_input("Strona:", min_value=1, max_value=total_pages, value=1, step=1)
            start = (page - 1) * PAGE_SIZE
            end = min(start + PAGE_SIZE, total)
            st.caption(f"Strona {page}/{total_pages} (pozycje {start+1}–{end} z {total})")
        
        for doc_id, c in filtered[start:end]:
            smap = {"wolny": "🔵", "przydzielony": "🟡", "w_toku": "🟠", "zakonczony": "🟢", "odroczony": "⏸️", "pominiety": "⏭️"}
            si = smap.get(c.get("status"), "❓")
            ap_mark = "🤖" if c.get("autopilot_status") == "calculated" else ""
            idx_label = f" | 📦 {c.get('index_handlowy')}" if c.get('index_handlowy') else ""
            cc1, cc2 = st.columns([4, 1])
            with cc1:
                st.markdown(f"{si} {ap_mark} **{c.get('numer_zamowienia', '?')}** — "
                            f"{c.get('priority_icon', '')} [{c.get('score', 0)}] {c.get('priority_label', '')}{idx_label}")
            with cc2:
                st.caption(f"{c.get('grupa', '?')} | {c.get('assigned_to') or '-'} | {c.get('status', '?')}")
            
            # Komentarz pominięcia
            if c.get("skip_reason"):
                if c.get("skip_fixed"):
                    st.success(f"✅ Naprawione | ⏭️ Pominięty przez **{c.get('skipped_by', '?')}**: {c.get('skip_reason')}")
                else:
                    sc1, sc2 = st.columns([5, 1])
                    with sc1:
                        st.warning(f"⏭️ Pominięty przez **{c.get('skipped_by', '?')}**: {c.get('skip_reason')}")
                    with sc2:
                        if st.button("✅ Naprawione", key=f"fix_{doc_id}"):
                            upd = {
                                "skip_fixed": True,
                                "skip_fixed_at": firestore.SERVER_TIMESTAMP,
                            }
                            # Jeśli case był pominiety — przywróć do wolnych
                            if c.get("status") == "pominiety":
                                upd["status"] = "wolny"
                            db.collection(col("ew_cases")).document(doc_id).update(upd)
                            st.rerun()
            
            # Przycisk uwolnienia odroczonego z wyborem grupy
            if c.get("status") == "odroczony":
                oc1, oc2, oc3 = st.columns([4, 1, 1])
                with oc1:
                    cur_grupa = c.get("grupa") or "—"
                    st.caption(f"⏸️ Odroczony | grupa: **{cur_grupa}**")
                with oc2:
                    new_grupa = st.selectbox("Grupa:", ["—", "DE", "FR", "UKPL"],
                                            index=["—", "DE", "FR", "UKPL"].index(cur_grupa) if cur_grupa in ["DE", "FR", "UKPL"] else 0,
                                            key=f"grupa_{doc_id}", label_visibility="collapsed")
                with oc3:
                    if st.button("🔓 Uwolnij", key=f"release_{doc_id}"):
                        upd = {"status": "wolny"}
                        if new_grupa != "—":
                            upd["grupa"] = new_grupa
                        db.collection(col("ew_cases")).document(doc_id).update(upd)
                        st.rerun()
            
            # Podgląd nocnego przeliczenia
            if c.get("autopilot_status") == "calculated" and c.get("autopilot_messages"):
                ap_msgs = c["autopilot_messages"]
                ap_op = c.get("autopilot_operator", "?")
                ap_date = c.get("autopilot_date", "?")
                ap_model = c.get("autopilot_model", "?")
                with st.expander(f"🤖 Podgląd nocnego przeliczenia — operator: {ap_op}, data: {ap_date}, model: {ap_model}"):
                    if len(ap_msgs) >= 1:
                        st.markdown("**📥 WSAD (do AI):**")
                        st.code(ap_msgs[0].get("content", "")[:3000], language=None)
                    if len(ap_msgs) >= 2:
                        st.markdown("**🤖 ODPOWIEDŹ AI:**")
                        st.markdown(ap_msgs[1].get("content", "")[:5000])
            
            # Wsad ze szturchacza dla NIEPRZELICZONYCH casów
            elif c.get("pelna_linia_szturchacza"):
                with st.expander(f"📋 Wsad ze szturchacza"):
                    st.code(c["pelna_linia_szturchacza"][:3000], language=None)


# ==========================================
# ⏭️ ZAKŁADKA: POMINIĘTE (ARCHIWUM)
# ==========================================
with tab_skipped:
    st.subheader("⏭️ Pominięte — archiwum nienaprawionych")
    st.caption("Casy przeniesione tutaj po wyczyszczeniu kolejki. Miały komentarz pominięcia bez oznaczenia 'Naprawione'.")
    
    # Pobierz archiwum
    try:
        archived_raw = db.collection(col("ew_cases_archived")).order_by("score", direction=firestore.Query.DESCENDING).limit(500).get()
    except Exception:
        archived_raw = []
    archived_data = [(d.id, d.to_dict()) for d in archived_raw]
    
    if not archived_data:
        st.info("Brak zarchiwizowanych pominiętych casów.")
    else:
        st.markdown(f"📊 **Łącznie w archiwum: {len(archived_data)}**")
        
        # Filtr po grupie
        arc_grupy = sorted(set(d.get("grupa", "?") for _, d in archived_data))
        arc_fg = st.selectbox("Grupa:", ["Wszystkie"] + arc_grupy, key="arc_fg")
        arc_filtered = archived_data
        if arc_fg != "Wszystkie":
            arc_filtered = [(did, d) for did, d in arc_filtered if d.get("grupa") == arc_fg]
        
        for doc_id, c in arc_filtered:
            idx_label = f" | 📦 {c.get('index_handlowy')}" if c.get('index_handlowy') else ""
            st.markdown(f"⏭️ **{c.get('numer_zamowienia', '?')}** — "
                        f"{c.get('priority_icon', '')} [{c.get('score', 0)}] {c.get('priority_label', '')}{idx_label}")
            
            sc1, sc2, sc3 = st.columns([4, 1, 1])
            with sc1:
                st.warning(f"Pominięty przez **{c.get('skipped_by', '?')}**: {c.get('skip_reason', '')}")
            with sc2:
                if st.button("✅ Naprawione", key=f"arcfix_{doc_id}"):
                    db.collection(col("ew_cases_archived")).document(doc_id).update({
                        "skip_fixed": True,
                        "skip_fixed_at": firestore.SERVER_TIMESTAMP,
                    })
                    st.rerun()
            with sc3:
                if st.button("🗑️ Usuń", key=f"arcdel_{doc_id}"):
                    db.collection(col("ew_cases_archived")).document(doc_id).delete()
                    st.rerun()
            
            # Wsad ze szturchacza
            if c.get("pelna_linia_szturchacza"):
                with st.expander(f"📋 Wsad ze szturchacza"):
                    st.code(c["pelna_linia_szturchacza"][:3000], language=None)
        
        # Przycisk wyczyść całe archiwum
        st.markdown("---")
        if st.button("🗑️ Wyczyść całe archiwum pominiętych", key="clear_archive"):
            for doc_id, _ in archived_data:
                db.collection(col("ew_cases_archived")).document(doc_id).delete()
            st.success(f"🗑️ Usunięto {len(archived_data)} casów z archiwum.")
            st.rerun()



# ==========================================
# 🧪 ZAKŁADKA: PROMPTY (lista z GitHuba + override per operator)
# ==========================================
with tab_prompty:
    st.subheader("🧪 Prompty operatorów")
    st.caption("Lista promptów z repo `szturchacz-test`. Możesz przypisać prompt konkretnemu operatorowi (override) "
               "lub ustawić jako default dla warstwy B (operatorzy testowi bez override).")
    
    # Pobierz prompty z GitHuba
    _prompts_data = _fetch_github_prompts()
    
    _col_refresh1, _col_refresh2 = st.columns([5, 1])
    with _col_refresh1:
        if isinstance(_prompts_data, dict) and "error" in _prompts_data:
            st.error(f"❌ Błąd pobierania listy z GitHub: {_prompts_data['error']}")
            st.warning("Dodaj `GITHUB_TOKEN` do secrets w Streamlit Cloud żeby zwiększyć rate limit (5000/h).")
            _prompts_list = []
        elif not _prompts_data:
            st.warning("⚠️ Brak promptów na liście. Dodaj plik .txt do repo szturchacz-test.")
            _prompts_list = []
        else:
            _prompts_list = _prompts_data
            st.success(f"📚 **{len(_prompts_list)}** promptów dostępnych w repo `szturchacz-test`")
    with _col_refresh2:
        if st.button("🔄 Odśwież", key="refresh_prompts_list"):
            _fetch_github_prompts.clear()
            st.rerun()
    
    # Gdy lista promptow pusta (np. rate limit GitHub) -> pokaz komunikat (juz wyzej) i POMIN
    # reszte tej zakladki. NIE robimy st.stop() -> pozostale zakladki (Hasla/Zastepstwa/...) zyja.
    if _prompts_list:
    
        # Mapy do dropdownów
        _prompt_names = [p["name"] for p in _prompts_list]
        _prompt_map = {p["name"]: p for p in _prompts_list}
    
        st.markdown("---")
    
        # === DEFAULT WARSTWY B ===
        st.markdown("### 🏢 Default warstwy B (operatorzy testowi)")
        st.caption("Operatorzy w warstwie B (Magda, Marlena, Klaudia + każdy bez override) dostają ten prompt.")
    
        try:
            _default_b_doc = db.collection(col("admin_config")).document("default_prompt").get().to_dict() or {}
            _current_default = _default_b_doc.get("prompt_name", "")
            _current_default_file = _default_b_doc.get("prompt_filename", "")
        except Exception:
            _current_default = ""
            _current_default_file = ""
    
        if _current_default:
            st.info(f"📍 **Aktualny default:** `{_current_default}` (`{_current_default_file}`)")
        else:
            st.warning("⚠️ Brak ustawionego default — operatorzy bez override nie dostaną prompta!")
    
        _col_def1, _col_def2 = st.columns([4, 1])
        with _col_def1:
            _new_default = st.selectbox("Wybierz nowy default:", _prompt_names, key="default_b_select")
        with _col_def2:
            st.markdown("&nbsp;")
            if st.button("💾 Ustaw jako default", key="set_default_b"):
                _p = _prompt_map[_new_default]
                try:
                    db.collection(col("admin_config")).document("default_prompt").set({
                        "prompt_name": _new_default,
                        "prompt_filename": _p["filename"],
                        "prompt_url": _p["raw_url"],
                        "prompt_github_link": _p["github_link"],
                        "set_by": "Sylwia",
                        "set_at": firestore.SERVER_TIMESTAMP,
                    }, merge=True)
                    st.success(f"✅ Default ustawiony: {_new_default}")
                    st.rerun()
                except Exception as _e:
                    st.error(f"❌ Błąd: {_e}")
    
        st.markdown("---")
    
        # === OVERRIDE PER OPERATOR ===
        st.markdown("### 👤 Override per operator")
        st.caption("Możesz ustawić innym operatorom konkretne prompty. Jeśli operator nie ma override, używa defaultu warstwy B.")
    
        _ALL_OPS_PROMPTY = [
            "Sylwia",
            "Emilia", "Oliwia", "Magda", "Ewelina", "Iwona", "Marlena",
            "EwelinaG", "Andrzej", "Marta", "Klaudia", "Kasia", "Romana",
            "oliwia_m",
        ]
    
        # Pobierz aktualne override'y
        _overrides = {}
        for _op in _ALL_OPS_PROMPTY:
            try:
                _ovr_doc = db.collection(col("operator_overrides")).document(_op).get().to_dict() or {}
                _overrides[_op] = _ovr_doc
            except Exception:
                _overrides[_op] = {}
    
        # Tabela
        _ph_c1, _ph_c2, _ph_c3, _ph_c4 = st.columns([2, 3, 3, 2])
        with _ph_c1: st.markdown("**Operator**")
        with _ph_c2: st.markdown("**Aktualny prompt**")
        with _ph_c3: st.markdown("**Zmień na**")
        with _ph_c4: st.markdown("**Akcja**")
    
        st.markdown("---")
    
        for _op in _ALL_OPS_PROMPTY:
            _ovr = _overrides.get(_op, {})
            _has_override = bool(_ovr.get("prompt_name"))
        
            _pr1, _pr2, _pr3, _pr4 = st.columns([2, 3, 3, 2])
            with _pr1:
                st.markdown(f"**{_op}**")
            with _pr2:
                if _has_override:
                    st.code(f"✏️ {_ovr.get('prompt_name')}\n{_ovr.get('prompt_filename', '')}", language=None)
                else:
                    st.caption(f"(default warstwy B: {_current_default or 'brak'})")
            with _pr3:
                _sel = st.selectbox(
                    f"Prompt dla {_op}",
                    ["(brak — użyj defaultu)"] + _prompt_names,
                    key=f"ovr_select_{_op}",
                    label_visibility="collapsed",
                )
            with _pr4:
                _col_a, _col_b = st.columns(2)
                with _col_a:
                    if st.button("💾", key=f"ovr_save_{_op}", help=f"Zapisz override dla {_op}"):
                        if _sel == "(brak — użyj defaultu)":
                            # Usuń override
                            try:
                                db.collection(col("operator_overrides")).document(_op).delete()
                                st.warning(f"🗑️ {_op}: override usunięty")
                                st.rerun()
                            except Exception as _e:
                                st.error(f"❌ {_e}")
                        else:
                            _p = _prompt_map[_sel]
                            try:
                                db.collection(col("operator_overrides")).document(_op).set({
                                    "prompt_name": _sel,
                                    "prompt_filename": _p["filename"],
                                    "prompt_url": _p["raw_url"],
                                    "prompt_github_link": _p["github_link"],
                                    "set_by": "Sylwia",
                                    "set_at": firestore.SERVER_TIMESTAMP,
                                }, merge=True)
                                st.success(f"✅ {_op}: prompt zmieniony na {_sel}")
                                st.rerun()
                            except Exception as _e:
                                st.error(f"❌ {_e}")
                with _col_b:
                    if _has_override:
                        if st.button("🗑️", key=f"ovr_clear_{_op}", help=f"Usuń override (wróci do defaultu)"):
                            try:
                                db.collection(col("operator_overrides")).document(_op).delete()
                                st.warning(f"🗑️ {_op}: override usunięty")
                                st.rerun()
                            except Exception as _e:
                                st.error(f"❌ {_e}")
    
        st.markdown("---")
    
        # === BULK ACTIONS ===
        st.markdown("### 🔧 Akcje hurtowe")
    
        with st.expander("🏢 Ustaw wybrany prompt wszystkim (warstwa B)"):
            st.caption("Przypisuje wybrany prompt jako override dla wszystkich operatorów warstwy B (Magda, Marlena, Klaudia).")
            _bulk_p = st.selectbox("Prompt:", _prompt_names, key="bulk_set_b")
            if st.button("🚀 Ustaw wszystkim w warstwie B", key="bulk_set_b_btn"):
                _p = _prompt_map[_bulk_p]
                _set_count = 0
                for _op in ["Magda", "Marlena", "Klaudia"]:
                    try:
                        db.collection(col("operator_overrides")).document(_op).set({
                            "prompt_name": _bulk_p,
                            "prompt_filename": _p["filename"],
                            "prompt_url": _p["raw_url"],
                            "prompt_github_link": _p["github_link"],
                            "set_by": "Sylwia (bulk)",
                            "set_at": firestore.SERVER_TIMESTAMP,
                        }, merge=True)
                        _set_count += 1
                    except Exception:
                        pass
                st.success(f"✅ Ustawiono prompt {_bulk_p} dla {_set_count} operatorów warstwy B")
                st.rerun()
    
        with st.expander("🗑️ Wyzeruj wszystkie overrides"):
            st.caption("Wszyscy operatorzy wrócą do defaultu warstwy B.")
            if st.checkbox("Potwierdzam", key="confirm_clear_all_ovr"):
                if st.button("🗑️ Wyzeruj wszystkie", key="clear_all_ovr_btn"):
                    _cleared = 0
                    for _op in _ALL_OPS_PROMPTY:
                        try:
                            db.collection(col("operator_overrides")).document(_op).delete()
                            _cleared += 1
                        except Exception:
                            pass
                    st.warning(f"🗑️ Wyzerowano {_cleared} overrides")
                    st.rerun()


    # ==========================================
    # 🔐 ZAKŁADKA: HASŁA OPERATORÓW (plain text — widoczne dla admina)
    # ==========================================
with tab_hasla:
    st.subheader("🔐 Zarządzanie hasłami operatorów")
    st.caption("Hasła operatorów do Szturchacza. Operator bez hasła nie zaloguje się. "
               "Hasła są widoczne tylko w Wieżowcu (admin). Sylwia ustawia hasła i przekazuje je operatorom.")
    
    # Pełna lista operatorów (jak w Szturchaczu)
    _ALL_OPS_HASLA = [
        "Sylwia",
        "Emilia", "Oliwia", "Magda", "Ewelina", "Iwona", "Marlena",
        "EwelinaG", "Andrzej", "Marta", "Klaudia", "Kasia", "Romana",
        "oliwia_m",
    ]
    
    # Pobierz aktualne hasła
    _hasla_plain = {}
    for _op in _ALL_OPS_HASLA:
        try:
            _doc = db.collection(col("operator_configs")).document(_op).get().to_dict() or {}
            _hasla_plain[_op] = _doc.get("password", "")
        except Exception:
            _hasla_plain[_op] = ""
    
    _count_ok = sum(1 for v in _hasla_plain.values() if v)
    _count_brak = len(_ALL_OPS_HASLA) - _count_ok
    
    if _count_brak > 0:
        st.warning(f"⚠️ **{_count_brak}** operatorów bez hasła — nie mogą się zalogować. ({_count_ok}/{len(_ALL_OPS_HASLA)} ma hasło)")
    else:
        st.success(f"✅ Wszyscy operatorzy mają hasła ({_count_ok}/{len(_ALL_OPS_HASLA)})")
    
    st.markdown("---")
    st.markdown("### 📝 Tabela haseł")
    
    # Pobierz aktualne grupy operatorów
    _grupy_state = {}
    _ROLE_MAP = {"Operatorzy_DE": "DE", "Operatorzy_FR": "FR", "Operatorzy_UK/PL": "UKPL"}
    _ROLE_MAP_REV = {"DE": "Operatorzy_DE", "FR": "Operatorzy_FR", "UKPL": "Operatorzy_UK/PL"}
    
    for _op in _ALL_OPS_HASLA:
        try:
            _doc = db.collection(col("operator_configs")).document(_op).get().to_dict() or {}
            _grupy_state[_op] = _ROLE_MAP.get(_doc.get("role", ""), "DE")
        except Exception:
            _grupy_state[_op] = "DE"
    
    # Header
    _hc1, _hc2, _hc3, _hc4, _hc5 = st.columns([2, 2, 2, 3, 2])
    with _hc1: st.markdown("**Operator**")
    with _hc2: st.markdown("**Grupa**")
    with _hc3: st.markdown("**Aktualne hasło**")
    with _hc4: st.markdown("**Zmień / ustaw nowe**")
    with _hc5: st.markdown("**Akcja**")
    
    st.markdown("---")
    
    # Wiersze operatorów
    for _op in _ALL_OPS_HASLA:
        _c1, _c2, _c3, _c4, _c5 = st.columns([2, 2, 2, 3, 2])
        with _c1:
            st.markdown(f"**{_op}**")
        with _c2:
            _curr_grupa = _grupy_state.get(_op, "DE")
            _options = ["DE", "FR", "UKPL"]
            _new_grupa = st.selectbox(
                f"Grupa dla {_op}",
                _options,
                index=_options.index(_curr_grupa) if _curr_grupa in _options else 0,
                key=f"grupa_select_{_op}",
                label_visibility="collapsed",
            )
            # Auto-save grupa jeśli zmienione
            if _new_grupa != _curr_grupa:
                try:
                    db.collection(col("operator_configs")).document(_op).set(
                        {"role": _ROLE_MAP_REV[_new_grupa]},
                        merge=True
                    )
                    st.toast(f"✅ {_op}: grupa zmieniona na {_new_grupa}", icon="🔄")
                    st.rerun()
                except Exception as _e:
                    st.error(f"❌ Błąd zmiany grupy {_op}: {_e}")
        with _c3:
            _curr = _hasla_plain[_op]
            if _curr:
                st.code(_curr, language=None)
            else:
                st.markdown("🔴 *(brak)*")
        with _c4:
            _new_pw = st.text_input(
                f"Hasło dla {_op}",
                key=f"hasla_pw_{_op}",
                label_visibility="collapsed",
                placeholder="Wpisz nowe hasło...",
            )
        with _c5:
            _col_save, _col_clear = st.columns(2)
            with _col_save:
                if st.button("💾", key=f"hasla_save_{_op}", help=f"Zapisz hasło dla {_op}"):
                    if not _new_pw or len(_new_pw) < 4:
                        st.warning(f"⚠️ {_op}: hasło min. 4 znaki")
                    else:
                        try:
                            db.collection(col("operator_configs")).document(_op).set(
                                {"password": _new_pw},
                                merge=True
                            )
                            st.success(f"✅ {_op}: hasło ustawione na '{_new_pw}'")
                            st.rerun()
                        except Exception as _e:
                            st.error(f"❌ Błąd zapisu: {_e}")
            with _col_clear:
                if _hasla_plain[_op]:
                    if st.button("🗑️", key=f"hasla_clear_{_op}", help=f"Usuń hasło dla {_op}"):
                        try:
                            db.collection(col("operator_configs")).document(_op).update({
                                "password": firestore.DELETE_FIELD
                            })
                            st.warning(f"🗑️ {_op}: hasło usunięte")
                            st.rerun()
                        except Exception as _e:
                            st.error(f"❌ Błąd: {_e}")
    
    st.markdown("---")
    
    st.markdown("---")
    st.markdown("### 📞 Ustawienia telefoniczne operatorów")
    st.caption("**Dzwoni** = ma prawo dzwonić sam. **Języki** = w jakich językach dzwoni (ustalenie — może być węższe niż realne umiejętności). "
               "**Woreczek/forum** = gdy operator NIE dzwoni sam i deleguje telefon: „woreczek” dokłada sprawę do woreczka dzwoniących (follow-up + sprawdzenie odpowiedzi), "
               "„forum” zostawia tylko wpis do Telefonistów. Domyślnie „forum”. Zmiana wchodzi od razu.")
    _LANGI = ["DE", "FR", "PL", "IT", "ES", "ENG"]
    _tel_state = {}
    for _op in _ALL_OPS_HASLA:
        try:
            _dd = db.collection(col("operator_configs")).document(_op).get().to_dict() or {}
        except Exception:
            _dd = {}
        _tel_state[_op] = {
            "dzwoni": _dd.get("dzwoni", False),
            "jezyki": [str(x).upper() for x in _dd.get("jezyki_dzwoniacy", [])],
            "woreczek": _dd.get("woreczek_telefon", "forum"),
        }
    _thh1, _thh2, _thh3, _thh4, _thh5 = st.columns([2, 1.3, 3, 2, 1.3])
    with _thh1: st.markdown("**Operator**")
    with _thh2: st.markdown("**Dzwoni**")
    with _thh3: st.markdown("**Języki**")
    with _thh4: st.markdown("**Woreczek/forum**")
    with _thh5: st.markdown("**Zapisz**")
    for _op in _ALL_OPS_HASLA:
        _ss = _tel_state[_op]
        _ttc1, _ttc2, _ttc3, _ttc4, _ttc5 = st.columns([2, 1.3, 3, 2, 1.3])
        with _ttc1:
            st.markdown(f"**{_op}**")
        with _ttc2:
            _dzw = st.checkbox("dzwoni", value=bool(_ss["dzwoni"]), key=f"_tel_dzwoni_{_op}", label_visibility="collapsed")
        with _ttc3:
            _jz = st.multiselect("języki", _LANGI, default=[l for l in _ss["jezyki"] if l in _LANGI],
                                 key=f"_tel_jezyki_{_op}", label_visibility="collapsed")
        with _ttc4:
            _wor = st.selectbox("woreczek", ["forum", "woreczek"],
                                index=(1 if _ss["woreczek"] == "woreczek" else 0),
                                key=f"_tel_wor_{_op}", label_visibility="collapsed")
        with _ttc5:
            if st.button("💾", key=f"_tel_save_{_op}", help=f"Zapisz telefon dla {_op}"):
                try:
                    db.collection(col("operator_configs")).document(_op).set({
                        "dzwoni": _dzw, "jezyki_dzwoniacy": _jz, "woreczek_telefon": _wor,
                    }, merge=True)
                    st.success(f"✅ {_op}: dzwoni={_dzw}, języki={_jz or '—'}, {_wor}")
                    st.rerun()
                except Exception as _e:
                    st.error(f"❌ Błąd zapisu telefonu {_op}: {_e}")

    # Bulk actions
    st.markdown("### 🔧 Akcje hurtowe")
    
    with st.expander("⚠️ Ustaw wszystkim domyślne hasło (gdy brak haseł u wielu)"):
        st.caption("Nadaje hasło wszystkim operatorom którzy nie mają hasła. Sylwia musi potem nadać indywidualnie.")
        _bulk_pw = st.text_input("Domyślne hasło:", key="bulk_default_pw")
        if st.button("🔓 Nadaj wszystkim bez hasła", key="bulk_set_default"):
            if not _bulk_pw or len(_bulk_pw) < 4:
                st.error("Hasło min. 4 znaki")
            else:
                _set_count = 0
                for _op in _ALL_OPS_HASLA:
                    if not _hasla_plain[_op]:
                        try:
                            db.collection(col("operator_configs")).document(_op).set(
                                {"password": _bulk_pw},
                                merge=True
                            )
                            _set_count += 1
                        except Exception:
                            pass
                st.success(f"✅ Ustawiono hasło '{_bulk_pw}' dla {_set_count} operatorów")
                st.rerun()
    
    with st.expander("🚨 Wyzeruj wszystkie hasła (BLOKUJE logowanie wszystkim)"):
        st.caption("Po tej akcji żaden operator się nie zaloguje aż nadasz mu nowe hasło.")
        _confirm = st.checkbox("Potwierdzam — chcę wyzerować wszystkie hasła", key="confirm_clear_all_pwd")
        if _confirm and st.button("🚨 Wyzeruj wszystko", key="bulk_clear_all"):
            _cleared = 0
            for _op in _ALL_OPS_HASLA:
                try:
                    db.collection(col("operator_configs")).document(_op).update({
                        "password": firestore.DELETE_FIELD
                    })
                    _cleared += 1
                except Exception:
                    pass
            st.warning(f"🗑️ Wyzerowano hasła dla {_cleared} operatorów")
            st.rerun()


# ==========================================
# 🔁 ZAKŁADKA: ZASTĘPSTWA SPRZEDAWCÓW
# ==========================================
with tab_zastepstwa:
    st.subheader("🔁 Zastępstwa")

    # === TRYB ZAMAWIANIA KURIERA (globalny przelacznik, jeden prompt v13) ===
    st.markdown("### 🚚 Tryb zamawiania kuriera")
    st.caption(
        "Globalny przełącznik kanału zamawiania kuriera dla całego systemu (Szturchacz + Autopilot). "
        "Jeden prompt (v13) obsługuje oba tory — zmiana wchodzi w życie od razu, bez podmiany promptu. "
        "**atomówki** = kuriera zamawia dział atomówek na podstawie wpisu na forum. "
        "**operatorzy** = kuriera zamawia operator sam i potwierdza wpisem kontrolnym do swojej grupy."
    )
    try:
        _kc_cur = db.collection(col("admin_config")).document("kurier_config").get().to_dict() or {}
    except Exception:
        _kc_cur = {}
    _mode_cur = _kc_cur.get("mode", "operatorzy")
    if _mode_cur not in ("atomowki", "operatorzy"):
        _mode_cur = "operatorzy"
    _mode_labels = {
        "atomowki": "🤖 Atomówki (dział zamawia)",
        "operatorzy": "👤 Operatorzy (operator zamawia sam)",
    }
    _mode_keys = ["atomowki", "operatorzy"]
    _sel_mode = st.radio(
        "Kto zamawia kuriera:",
        _mode_keys,
        index=_mode_keys.index(_mode_cur),
        format_func=lambda m: _mode_labels[m],
        key="_kurier_mode_radio",
        horizontal=True,
    )
    _ckm1, _ckm2 = st.columns([1, 3])
    with _ckm1:
        if st.button("💾 Zapisz tryb kuriera", type="primary", key="_kurier_mode_save"):
            try:
                db.collection(col("admin_config")).document("kurier_config").set({
                    "mode": _sel_mode,
                    "updated_at": firestore.SERVER_TIMESTAMP,
                    "updated_by": "admin",
                }, merge=True)
                st.success(f"✅ Zapisano tryb kuriera: {_mode_labels[_sel_mode]}. Wchodzi w życie od razu.")
                st.rerun()
            except Exception as _e:
                st.error(f"❌ Błąd zapisu: {_e}")
    with _ckm2:
        if _sel_mode != _mode_cur:
            st.info(f"Niezapisana zmiana: **{_mode_cur}** → **{_sel_mode}**")
        else:
            st.caption(f"Aktualny tryb: **{_mode_cur}**")

    st.markdown("---")
    st.markdown("### 👥 Zastępstwa sprzedawców")
    st.markdown(
        "Sprzedawca na urlopie / w rotacji → wpisy do niego lecą do **zastępcy**.\n\n"
        "`kinga → kinga` znaczy, że **jest w pracy** i zastępstwa nie potrzebuje. "
        "`kinga → emilia` przekierowuje wszystkie wpisy z zamówień Kingi do Emilii."
    )
    st.caption(
        "Podmiana działa na poziomie wysyłki wpisu — AI dalej czyta sprzedawcę ze wsadu, "
        "a moduł forum podmienia odbiorcę tuż przed wysłaniem i dokleja stopkę "
        "„Zastępstwo za: [nick]”. Zmiana działa najpóźniej po minucie, bez restartu apki. "
        "Grupy (Telefoniści_*, Operatorzy_*, EA) i justyna są nietykalne."
    )

    _zast_mapa = load_zastepstwa_map()

    # --- podsumowanie aktywnych zastępstw ---
    _aktywne = {k: v for k, v in _zast_mapa.items()
                if v and v != k and k in SPRZEDAWCY_NICKI}
    if _aktywne:
        st.warning(f"⚠️ Aktywne zastępstwa: **{len(_aktywne)}**")
        st.dataframe(
            pd.DataFrame([{"Sprzedawca (ze wsadu)": k, "→ Wpisy trafiają do": v}
                          for k, v in sorted(_aktywne.items())]),
            use_container_width=True, hide_index=True,
        )
    else:
        st.success("✅ Brak aktywnych zastępstw — wszyscy sprzedawcy w pracy.")

    st.markdown("---")
    st.markdown("### Ustawienia")

    _nowa_mapa = {}
    _kol = st.columns(3)
    for _i, (_nick, _kraj) in enumerate(SPRZEDAWCY_LISTA):
        with _kol[_i % 3]:
            _obecny = _zast_mapa.get(_nick, _nick)
            if _obecny not in SPRZEDAWCY_NICKI:
                _obecny = _nick
            _idx = SPRZEDAWCY_NICKI.index(_obecny)
            _wybor = st.selectbox(
                f"**{_nick}** ({_kraj}) →",
                SPRZEDAWCY_NICKI,
                index=_idx,
                key=f"_zast_{_nick}",
            )
            _nowa_mapa[_nick] = _wybor
            if _wybor != _nick:
                st.caption(f"↪️ zastępuje: **{_wybor}**")

    st.markdown("---")
    _cz1, _cz2 = st.columns([1, 3])
    with _cz1:
        if st.button("💾 Zapisz zastępstwa", type="primary", key="_zast_save"):
            try:
                save_zastepstwa_map(_nowa_mapa, kto="admin")
                _ile = sum(1 for k, v in _nowa_mapa.items() if v != k)
                st.success(f"✅ Zapisano. Aktywnych zastępstw: {_ile}.")
                st.rerun()
            except Exception as _e:
                st.error(f"❌ Błąd zapisu: {_e}")
    with _cz2:
        if st.button("♻️ Wyczyść wszystkie (wszyscy w pracy)", key="_zast_reset"):
            try:
                save_zastepstwa_map({n: n for n in SPRZEDAWCY_NICKI}, kto="admin")
                st.success("✅ Wyczyszczono — wszystkie wpisy wracają do własnych sprzedawców.")
                st.rerun()
            except Exception as _e:
                st.error(f"❌ Błąd: {_e}")

    _meta = {}
    try:
        _d = db.collection(col("admin_config")).document("sprzedawcy_zastepstwa").get()
        _meta = _d.to_dict() or {} if getattr(_d, "exists", False) else {}
    except Exception:
        pass
    if _meta.get("updated_at"):
        try:
            _ts = _meta["updated_at"].astimezone(pytz.timezone("Europe/Warsaw")).strftime("%d.%m.%Y %H:%M")
            st.caption(f"Ostatnia zmiana: {_ts} · {_meta.get('updated_by', '?')}")
        except Exception:
            pass


# ==========================================
# 💎 ZAKŁADKA: DIAMENTOZA (forum / ew_diamond_log)
# ==========================================
# Patch v1.5.7d "pomijamyPZ6" (09.06.2026):
# Czyta z {prefix}ew_diamond_log/{YYYY-MM-DD}/numbers/{numer_zamowienia} przez helper col().
# Źródło prawdy: moment wysłania zlecenia kuriera na forum (cel=AUTOS_KURIERZY).
# Decyzja "czy to diament" zapada w forum_module.log_diamond na podstawie treści posta
# (stop-lista + Zamówienie:NNN). Etykieta UPS punkt JEST diamentem (typ_zlecenia).
# Fallback kategorii z typ_towaru_cache dla wpisów historycznych przed patchem.
with tab_diamentoza:
    st.subheader("💎 Diamentoza — zlecenia kuriera na forum")
    st.caption(
        "Źródło prawdy: moment wysłania zlecenia kuriera na forum (cel=AUTOS_KURIERZY). "
        "Operator i czatoszturek zapisują 1 diament przy każdym takim wpisie. "
        "Filtr decyzyjny w `forum_module.log_diamond`: treść posta zawiera `Zamówienie: NNN` "
        "AND NIE zawiera słów stop-listy (bump/ponaglenie/eskalacja/dopyt/podbicie). "
        "Etykieta UPS punkt JEST diamentem (typ_zlecenia=etykieta_ups_punkt). "
        "v1.5.7e: wstrzymania/korekty/ponowienia zapisywane OSOBNO jako Anulowane (typy zmiana/cofniete/ponowienie, "
        "klucz {numer}__{typ}, dedup per data+numer+typ) — NIE liczą się do diamentów i NIE są od nich odejmowane. "
        "Kolektor = zawsze UPS (fallback + flaga anomalii). "
        "Dedup diamentów per (data, numer). Działa od wdrożenia patcha v1.5.7d — wcześniejsze dni mogą być niekompletne."
    )
    
    # v1.5.7e: guzik odśwież — czyści cache i pobiera aktualne dane (werdykt EA)
    if st.button("🔄 Odśwież dane", key="_diam_refresh"):
        st.cache_data.clear()
        st.rerun()
    
    tz_pl_d = pytz.timezone('Europe/Warsaw')
    today_d = datetime.now(tz_pl_d).date()
    
    # --- Część B: wspólna definicja wykonawcy-bota (kafle + filtr źródła + tabela) ---
    def is_czatoszturek(wpis):
        """source_type zaczyna się od 'auto' (autoszturchacz) LUB
        (source_type=='reczny' AND operator=='Czatoszturek')."""
        _stp = str(wpis.get("source_type", "")).strip().lower()
        if _stp.startswith("auto"):
            return True
        if _stp == "reczny" and str(wpis.get("operator", "")).strip() == "Czatoszturek":
            return True
        return False
    
    # ===== ➕ DODAJ DIAMENT RĘCZNIE (Część A) — przed pierwszym st.stop() zakładki =====
    with st.expander("➕ Dodaj diament ręcznie", expanded=False):
        st.caption(
            "Ręczny wpis na wypadek, gdy system nie naliczył diamentu. Zapis idzie do ew_diamond_log "
            "ze source_type='reczny' (audyt). Klucz {numer}__reczny__{timestamp} — nie nadpisuje istniejących wpisów."
        )
        _rd_c1, _rd_c2, _rd_c3 = st.columns(3)
        with _rd_c1:
            _rd_data = st.date_input("📅 Data:", value=today_d, key="_rd_data")
            _rd_godz = st.time_input("🕐 Godzina:", key="_rd_godz")
        with _rd_c2:
            _rd_numer = st.text_input("🔢 Numer zamówienia:", key="_rd_numer", placeholder="np. 374593")
            _rd_status = st.radio("Status:", ["diament", "anulowane"], horizontal=True, key="_rd_status")
        with _rd_c3:
            _rd_wykonawca = st.selectbox("👤 Wykonawca:", _ALL_OPS_HASLA + ["Czatoszturek"], key="_rd_wykonawca")
            _rd_kat = st.radio("📦 Kategoria:", ["kolektor", "skrzynia"], horizontal=True, key="_rd_kat")
        _rd_c4, _rd_c5, _rd_c6 = st.columns(3)
        with _rd_c4:
            _rd_typ = st.selectbox(
                "🚚 Typ zlecenia:",
                ["kurier", "etykieta_ups_punkt", "zmiana", "cofniete", "ponowienie", "inne"],
                key="_rd_typ",
            )
        with _rd_c5:
            if _rd_kat == "kolektor":
                _rd_kurier = "UPS"
                st.info("Kurier: **UPS** (kolektor zawsze UPS)")
            else:
                _rd_kurier_label = st.selectbox("🚛 Kurier:", ["UPS", "FedEx", "DB Schenker"], key="_rd_kurier")
                _rd_kurier = {"UPS": "UPS", "FedEx": "FEDEX", "DB Schenker": "DBSCHENKER"}[_rd_kurier_label]
        with _rd_c6:
            if _rd_wykonawca == "Czatoszturek":
                _rd_grupa_sel = st.selectbox("🌍 Grupa (WYMAGANE):", ["— wybierz —", "DE", "FR", "UKPL"], key="_rd_grupa_cz")
                _rd_grupa = None if _rd_grupa_sel == "— wybierz —" else _rd_grupa_sel
            else:
                try:
                    _rd_rola = (db.collection(col("operator_configs")).document(_rd_wykonawca).get().to_dict() or {}).get("role", "")
                except Exception:
                    _rd_rola = ""
                _rd_grupa_auto = _ROLE_MAP.get(_rd_rola, "DE")
                _rd_opts = ["DE", "FR", "UKPL"]
                _rd_grupa = st.selectbox(
                    "🌍 Grupa (auto z operatora, można nadpisać):",
                    _rd_opts,
                    index=_rd_opts.index(_rd_grupa_auto) if _rd_grupa_auto in _rd_opts else 0,
                    key="_rd_grupa_op",
                )
        _rd_potw = st.checkbox("☑️ Sprawdziłam — dodaj mimo to (potwierdzenie przy duplikacie)", key="_rd_potw")
        if st.button("💾 Zapisz diament ręcznie", type="primary", key="_rd_zapisz"):
            _rd_numer_clean = (_rd_numer or "").strip()
            if not _rd_numer_clean:
                st.error("❌ Podaj numer zamówienia.")
            elif _rd_wykonawca == "Czatoszturek" and _rd_grupa not in ("DE", "FR", "UKPL"):
                st.error("❌ Dla wykonawcy Czatoszturek grupa jest WYMAGANA — wybierz DE/FR/UKPL.")
            else:
                _rd_date_iso = _rd_data.strftime("%Y-%m-%d")
                # Soft warning: wpisy tego samego numeru TEGO SAMEGO dnia (inny dzień = bez ostrzeżenia)
                _rd_existing = []
                try:
                    _rd_docs = db.collection(f"{_COL_PREFIX}ew_diamond_log").document(_rd_date_iso).collection("numbers").stream()
                    for _dd in _rd_docs:
                        _ddd = _dd.to_dict() or {}
                        if str(_ddd.get("numer_zamowienia", "")).strip() == _rd_numer_clean:
                            _rd_existing.append(_ddd)
                except Exception:
                    pass
                if _rd_existing and not _rd_potw:
                    _rd_lines = []
                    for _e in _rd_existing:
                        _st_e = str(_e.get("source_type", "")).strip().lower()
                        _zr = ("✍️ Ręczny" if _st_e == "reczny"
                               else ("🤖 Czatoszturek" if _st_e.startswith("auto") else "🧑 Operator"))
                        _rd_lines.append(
                            f"- typ={_e.get('typ_zlecenia', '?')} · operator={_e.get('operator', '?')} "
                            f"· źródło={_zr} · godz={_e.get('godzina_reczna') or '?'}"
                        )
                    st.warning(
                        f"⚠️ Numer **{_rd_numer_clean}** ma już {len(_rd_existing)} wpis(ów) z dnia {_rd_date_iso}:\n"
                        + "\n".join(_rd_lines)
                        + "\n\nSprawdź 2x. Aby dodać mimo to — zaznacz checkbox potwierdzenia i kliknij ponownie."
                    )
                else:
                    _rd_ts = datetime.now(tz_pl_d).strftime("%Y%m%d%H%M%S%f")
                    _rd_key = f"{_rd_numer_clean}__reczny__{_rd_ts}"
                    _rd_kat_final = "Kolektor" if _rd_kat == "kolektor" else "Skrzynia biegów"
                    _rd_entry = {
                        "numer_zamowienia": _rd_numer_clean,
                        "operator": _rd_wykonawca,
                        "source_type": "reczny",
                        "czy_diament": (_rd_status == "diament"),
                        "kategoria_towaru": _rd_kat_final,
                        "kurier": _rd_kurier,
                        "typ_zlecenia": _rd_typ,
                        "grupa": _rd_grupa,
                        "date_str": _rd_date_iso,
                        "godzina_reczna": _rd_godz.strftime("%H:%M") if _rd_godz else "?",
                        "logged_at": firestore.SERVER_TIMESTAMP,
                        "dodal_przez": "admin",
                        "dodal_at": firestore.SERVER_TIMESTAMP,
                    }
                    try:
                        db.collection(f"{_COL_PREFIX}ew_diamond_log").document(_rd_date_iso).collection("numbers").document(_rd_key).set(_rd_entry, merge=False)
                        st.success(
                            f"✅ Dodano ręczny {'💎 diament' if _rd_status == 'diament' else '🚫 anulowane'}: "
                            f"{_rd_numer_clean} → {_rd_wykonawca} ({_rd_grupa}) na {_rd_date_iso}."
                        )
                        st.cache_data.clear()
                        st.rerun()
                    except Exception as _e:
                        st.error(f"❌ Błąd zapisu: {_e}")

    with st.expander("🗑️ Usuń diament", expanded=False):
        st.caption("Usuwa wpis z ew_diamond_log (np. błędnie naliczony albo dodany pomyłkowo). "
                   "Wybierz datę, znajdź wpis na liście i usuń. Działa na ręczne i automatyczne.")
        _del_data = st.date_input("📅 Data wpisu:", value=today_d, key="_del_data")
        _del_date_iso = _del_data.strftime("%Y-%m-%d")
        _del_filter = st.text_input("🔢 Filtruj po numerze (opcjonalnie):", key="_del_filter", placeholder="np. 374593")
        try:
            _del_docs = list(db.collection(f"{_COL_PREFIX}ew_diamond_log").document(_del_date_iso).collection("numbers").stream())
        except Exception:
            _del_docs = []
        _del_items = []
        for _dd in _del_docs:
            _ddd = _dd.to_dict() or {}
            _num = str(_ddd.get("numer_zamowienia", "")).strip()
            if _del_filter.strip() and _del_filter.strip() not in _num:
                continue
            _del_items.append((_dd.id, _ddd, _num))
        if not _del_items:
            st.info(f"Brak wpisów na {_del_date_iso}" + (f" dla numeru {_del_filter.strip()}." if _del_filter.strip() else "."))
        else:
            st.caption(f"Znaleziono {len(_del_items)} wpis(ów) na {_del_date_iso}:")
            for _key, _ddd, _num in _del_items:
                _stp = str(_ddd.get("source_type", "")).strip().lower()
                _zr = ("✍️ Ręczny" if _stp == "reczny" else ("🤖 Czatoszturek" if _stp.startswith("auto") else "🧑 Operator"))
                _dia = "💎" if _ddd.get("czy_diament") else "🚫"
                _cI, _cB = st.columns([5, 1])
                with _cI:
                    st.markdown(
                        f"{_dia} **{_num}** · {_ddd.get('operator', '?')} ({_ddd.get('grupa', '?')}) · "
                        f"typ={_ddd.get('typ_zlecenia', '?')} · {_zr} · godz={_ddd.get('godzina_reczna') or '?'}"
                    )
                with _cB:
                    if st.button("🗑️ Usuń", key=f"_del_btn_{_key}"):
                        try:
                            db.collection(f"{_COL_PREFIX}ew_diamond_log").document(_del_date_iso).collection("numbers").document(_key).delete()
                            st.success(f"✅ Usunięto wpis {_num} z {_del_date_iso}.")
                            st.cache_data.clear()
                            st.rerun()
                        except Exception as _e:
                            st.error(f"❌ Błąd usuwania: {_e}")
    
    # ===== Filtry =====
    fc1, fc2 = st.columns([3, 2])
    with fc1:
        from datetime import timedelta as _td_d
        d_range = st.date_input(
            "📅 Zakres dat:",
            value=(today_d - _td_d(days=6), today_d),
            key="_diam_range",
        )
    with fc2:
        d_src = st.radio(
            "👤 Źródło:",
            options=["Wszyscy", "🧑 Operatorzy", "🤖 Czatoszturek"],
            index=0, horizontal=True, key="_diam_src",
        )
    
    fc3, fc4, fc5 = st.columns(3)
    with fc3:
        d_kat = st.radio(
            "📦 Typ towaru:",
            options=["Wszystkie", "🔩 Kolektor", "🔧 Skrzynia"],
            index=0, horizontal=True, key="_diam_kat",
        )
    with fc4:
        d_typ_zlec = st.radio(
            "🚚 Typ zlecenia:",
            options=["Wszystkie", "Kurier", "Etykieta UPS"],
            index=0, horizontal=True, key="_diam_typ_zlec",
        )
    with fc5:
        d_grupa = st.selectbox(
            "🌍 Grupa:",
            options=["Wszystkie", "DE", "FR", "UKPL"],
            index=0, key="_diam_grupa",
        )
    
    # Normalizuj zakres dat (date_input zwraca tuple lub pojedynczą datę)
    if isinstance(d_range, (tuple, list)):
        if len(d_range) == 2:
            d_from, d_to = d_range
        elif len(d_range) == 1:
            d_from = d_to = d_range[0]
        else:
            d_from = d_to = today_d
    else:
        d_from = d_to = d_range
    
    # ===== Fetch z ew_diamond_log (z prefiksem TEST_MODE) =====
    @st.cache_data(ttl=60)
    def _fetch_diamond_log_diam(date_from_iso, date_to_iso, prefix):
        """Czyta {prefix}ew_diamond_log/{date}/numbers/* dzień po dniu."""
        from datetime import date as _date_d, timedelta as _td_d2
        rows = []
        try:
            d_f = _date_d.fromisoformat(date_from_iso)
            d_t = _date_d.fromisoformat(date_to_iso)
        except Exception:
            return rows
        cur = d_f
        while cur <= d_t:
            ds = cur.strftime("%Y-%m-%d")
            try:
                docs = list(db.collection(f"{prefix}ew_diamond_log").document(ds).collection("numbers").stream())
                for doc in docs:
                    data = doc.to_dict() or {}
                    rows.append({
                        "date_str": data.get("date_str", ds),
                        "numer_zamowienia": str(data.get("numer_zamowienia", doc.id)).strip(),
                        "operator": data.get("operator", "?"),
                        "source_type": data.get("source_type", "operator"),
                        "kurier": data.get("kurier"),
                        "kategoria_towaru": data.get("kategoria_towaru"),
                        "typ_zlecenia": data.get("typ_zlecenia", "inne"),
                        "grupa": data.get("grupa"),
                        "pz": data.get("pz"),
                        "bump": data.get("bump"),
                        "forum_post_id": data.get("forum_post_id"),
                        "cel": data.get("cel"),
                        # v1.5.7e: status diament/anulowane + godzina + anomalia kolektor≠UPS
                        "czy_diament": data.get("czy_diament"),
                        "anomalia_kolektor_kurier": data.get("anomalia_kolektor_kurier", False),
                        "logged_at": data.get("logged_at"),
                        "godzina_reczna": data.get("godzina_reczna"),
                    })
            except Exception:
                pass
            cur += _td_d2(days=1)
        return rows
    
    diamonds = _fetch_diamond_log_diam(d_from.strftime("%Y-%m-%d"), d_to.strftime("%Y-%m-%d"), _COL_PREFIX)
    
    if not diamonds:
        st.warning(f"🔍 Brak diamentów w `{_COL_PREFIX}ew_diamond_log` dla **{d_from} → {d_to}**.")
        st.info(
            "ℹ️ Możliwe: brak zleceń kuriera w tym zakresie · zakres sprzed wdrożenia patcha v1.5.7d · "
            f"patch `forum_module` nie wgrany do repo. Sprawdź czy apka pisze do `{_COL_PREFIX}ew_diamond_log`."
        )
        st.stop()
    
    # ===== Fallback kategorii z typ_towaru_cache + prefiks indeksu =====
    @st.cache_data(ttl=120)
    def _fetch_typ_cache_diam():
        cache = {}
        try:
            docs = list(db.collection("typ_towaru_cache").get())
            for d in docs:
                cache[str(d.id)] = d.to_dict() or {}
        except Exception:
            pass
        return cache
    
    typ_cache_d = _fetch_typ_cache_diam()
    
    def _kat_final_diam(nr, kat_log):
        # Priorytet 1: kategoria z logu (po patchu v1.5.7d ekstrakcja z treści posta)
        if kat_log in ("Kolektor", "Skrzynia biegów"):
            return kat_log
        if kat_log:
            kl = str(kat_log).upper()
            if "KOLEKTOR" in kl:
                return "Kolektor"
            if "SKRZYN" in kl:
                return "Skrzynia biegów"
        # Priorytet 2: fallback z typ_towaru_cache (po numer_zamowienia)
        info = typ_cache_d.get(str(nr).strip())
        if info:
            kat = info.get("kategoria")
            if kat in ("Kolektor", "Skrzynia biegów"):
                return kat
            # Priorytet 3: prefiks indeksu ORG/REG/BMW = kolektor (reguła biznesowa §10.10)
            idx = str(info.get("resolved_index") or "").upper().strip()
            if idx[:3] in ("ORG", "REG", "BMW"):
                return "Kolektor"
        return "Nieprzypisane"
    
    for r in diamonds:
        r["_kat_final"] = _kat_final_diam(r["numer_zamowienia"], r.get("kategoria_towaru"))
        r["_is_auto"] = is_czatoszturek(r)
        r["_typ_zlec_label"] = {
            "kurier": "🚚 Kurier",
            "etykieta_ups_punkt": "📦 Etykieta UPS punkt",
            "zmiana": "✏️ Zmiana/korekta",
            "cofniete": "🛑 Cofnięte",
            "ponowienie": "🔁 Ponowienie",
            "inne": "❓ Inne",
        }.get(r.get("typ_zlecenia", "inne"), "❓ Inne")
        # v1.5.7e: rozdział diament vs anulowane — pole czy_diament z logu;
        # fallback po typie dla wpisów historycznych (sprzed v1.5.7e)
        if r.get("czy_diament") is not None:
            r["_is_diament"] = bool(r["czy_diament"])
        else:
            r["_is_diament"] = r.get("typ_zlecenia", "inne") not in ("zmiana", "cofniete", "ponowienie")
        # v1.5.7e: godzina zamówienia z logged_at (werdykt EA: godzina w szczegółach)
        if r.get("godzina_reczna"):
            r["_godzina"] = r["godzina_reczna"]
        else:
            try:
                _la = r.get("logged_at")
                r["_godzina"] = _la.astimezone(tz_pl_d).strftime("%H:%M") if _la else "?"
            except Exception:
                r["_godzina"] = "?"
    
    # Część B: zachowaj pełny zbiór z zakresu dat PRZED górnymi filtrami UI (licznik diamentów tabeli)
    diamonds_all = list(diamonds)
    
    # ===== Filtry =====
    if d_src == "🧑 Operatorzy":
        diamonds = [r for r in diamonds if not r["_is_auto"]]
    elif d_src == "🤖 Czatoszturek":
        diamonds = [r for r in diamonds if r["_is_auto"]]
    
    if d_kat == "🔩 Kolektor":
        diamonds = [r for r in diamonds if r["_kat_final"] == "Kolektor"]
    elif d_kat == "🔧 Skrzynia":
        diamonds = [r for r in diamonds if r["_kat_final"] == "Skrzynia biegów"]
    
    if d_typ_zlec == "Kurier":
        diamonds = [r for r in diamonds if r.get("typ_zlecenia") == "kurier"]
    elif d_typ_zlec == "Etykieta UPS":
        diamonds = [r for r in diamonds if r.get("typ_zlecenia") == "etykieta_ups_punkt"]
    
    if d_grupa != "Wszystkie":
        diamonds = [r for r in diamonds if str(r.get("grupa") or "").upper() == d_grupa]
    
    ops_available = sorted({r["operator"] for r in diamonds})
    d_op = st.multiselect(
        "🧑 Operator (puste = wszyscy):",
        options=ops_available, default=[], key="_diam_op",
    )
    if d_op:
        diamonds = [r for r in diamonds if r["operator"] in d_op]
    
    if not diamonds:
        st.warning("🔍 Po filtrach brak diamentów. Poluzuj filtry.")
        st.stop()
    
    # ===== Metryki =====
    st.markdown("---")
    # ROZBICIE (zgodnie z ustaleniami): nie-diamenty rozdzielone wg typ_zlecenia.
    # 🛑 Anulowane = TYLKO cofniete (realne anulowania). 🔁 Podbicie = ponowienie.
    # ✏️ Zmiana = zmiana/korekta (w tym etykiety-reissue reklasyfikowane regułą "nowe ID = diament").
    diamenty_wlasciwe = [r for r in diamonds if r["_is_diament"]]
    _niediam = [r for r in diamonds if not r["_is_diament"]]
    anulowane = [r for r in _niediam if r.get("typ_zlecenia") == "cofniete"]
    podbicia = [r for r in _niediam if r.get("typ_zlecenia") == "ponowienie"]
    zmiany = [r for r in _niediam if r.get("typ_zlecenia") == "zmiana"]
    inne_nd = [r for r in _niediam if r.get("typ_zlecenia") not in ("cofniete", "ponowienie", "zmiana")]
    total = len(diamenty_wlasciwe)
    n_anul = len(anulowane)
    n_podb = len(podbicia)
    n_zmiana = len(zmiany)
    n_inne = len(inne_nd)
    n_oper = sum(1 for r in diamenty_wlasciwe if not r["_is_auto"])
    n_auto = sum(1 for r in diamenty_wlasciwe if r["_is_auto"])
    n_kol = sum(1 for r in diamenty_wlasciwe if r["_kat_final"] == "Kolektor")
    n_skrz = sum(1 for r in diamenty_wlasciwe if r["_kat_final"] == "Skrzynia biegów")
    n_anomalia = sum(1 for r in diamonds if r.get("anomalia_kolektor_kurier"))

    m1, m2, m3, m4 = st.columns(4)
    m1.metric("💎 Diamenty", total)
    m2.metric("🛑 Anulowane", n_anul, help="Tylko realne anulowania (cofniete).")
    m3.metric("🔁 Podbicia", n_podb, help="Ponowienia/przypomnienia (brak reakcji na wpis) — NIE anulowania.")
    m4.metric("✏️ Zmiany", n_zmiana, help="Korekty/zmiany terminu/etykiety (reissue) — NIE nowe diamenty.")
    m5, m6, m7, m8 = st.columns(4)
    m5.metric("🧑 Operatorzy", n_oper)
    m6.metric("🤖 Czatoszturek", n_auto)
    m7.metric("🔩 Kolektor", n_kol)
    m8.metric("🔧 Skrzynia", n_skrz)
    if n_inne:
        st.caption(f"ℹ️ Dodatkowo {n_inne} nie-diament(ów) o innym typie.")
    if n_anomalia:
        st.warning(f"⚠️ {n_anomalia} wpis(ów) z anomalią: Kolektor z kurierem innym niż UPS (zasada: kolektor = zawsze UPS). Szczegóły w kolumnie Status listy szczegółowej.")
    
    # ===== Wykres per dzień stacked (operator vs czatoszturek) =====
    st.markdown("---")
    st.markdown("### 📈 Diamenty per dzień (operator vs czatoszturek)")
    by_day = {}
    for r in diamenty_wlasciwe:
        ds = r["date_str"]
        by_day.setdefault(ds, {"Operatorzy": 0, "Czatoszturek": 0})
        by_day[ds]["Czatoszturek" if r["_is_auto"] else "Operatorzy"] += 1
    df_day = pd.DataFrame([
        {"Data": ds, "Operatorzy": v["Operatorzy"], "Czatoszturek": v["Czatoszturek"]}
        for ds, v in sorted(by_day.items())
    ]).set_index("Data")
    st.bar_chart(df_day, height=320)
    
    # ===== Ranking per operator =====
    st.markdown("### 📊 Ranking per operator")
    by_op = {}
    for r in diamenty_wlasciwe:
        by_op[r["operator"]] = by_op.get(r["operator"], 0) + 1
    df_op = pd.DataFrame(
        [{"Operator": k, "Diamenty": v} for k, v in by_op.items()]
    ).sort_values("Diamenty", ascending=False)
    st.bar_chart(df_op.set_index("Operator"), height=340)
    
    # ===== Rozbicia (typ zlecenia / typ × kategoria / grupa) =====
    st.markdown("### 🔍 Rozbicia")
    cc1, cc2, cc3 = st.columns(3)
    with cc1:
        st.markdown("##### 🚚 Typ zlecenia")
        by_typ = {}
        for r in diamonds:
            by_typ[r["_typ_zlec_label"]] = by_typ.get(r["_typ_zlec_label"], 0) + 1
        st.dataframe(
            pd.DataFrame([{"Typ": k, "Ilość": v} for k, v in by_typ.items()]).sort_values("Ilość", ascending=False),
            use_container_width=True, hide_index=True,
        )
    with cc2:
        st.markdown("##### 📦 Typ × Kategoria")
        grid = {}
        for r in diamonds:
            key = (r["_typ_zlec_label"], r["_kat_final"])
            grid[key] = grid.get(key, 0) + 1
        rows_grid = []
        all_kats_grid = sorted({k[1] for k in grid.keys()})
        for typ in sorted({k[0] for k in grid.keys()}):
            row = {"Typ zlec.": typ}
            for kat in all_kats_grid:
                row[kat] = grid.get((typ, kat), 0)
            rows_grid.append(row)
        if rows_grid:
            st.dataframe(pd.DataFrame(rows_grid), use_container_width=True, hide_index=True)
    with cc3:
        st.markdown("##### 🌍 Grupa")
        by_g = {}
        for r in diamonds:
            g = str(r.get("grupa") or "?").upper()
            by_g[g] = by_g.get(g, 0) + 1
        st.dataframe(
            pd.DataFrame([{"Grupa": k, "Ilość": v} for k, v in by_g.items()]).sort_values("Ilość", ascending=False),
            use_container_width=True, hide_index=True,
        )
    
    # ===== Tabela szczegółowa =====
    st.markdown("---")
    st.markdown("### 📋 Lista szczegółowa")
    df_full = pd.DataFrame([
        {
            "Data": r["date_str"],
            "Godzina": r.get("_godzina", "?"),
            "Numer": r["numer_zamowienia"],
            "Status": (
                "💎 Diament" if r["_is_diament"]
                else {"cofniete": "🛑 Anulowane", "ponowienie": "🔁 Podbicie", "zmiana": "✏️ Zmiana"}.get(
                    r.get("typ_zlecenia"), "▫️ Inne")
            ) + (" ⚠️ anomalia kolektor≠UPS" if r.get("anomalia_kolektor_kurier") else ""),
            "Operator": r["operator"],
            "Źródło": ("✍️ Ręczny" if str(r.get("source_type", "")).strip().lower() == "reczny"
                       else ("🤖 Czatoszturek" if r["_is_auto"] else "🧑 Operator")),
            "Kategoria": r["_kat_final"],
            "Typ zlecenia": r["_typ_zlec_label"],
            "Przewoźnik": str(r.get("kurier") or "?").upper(),
            "Grupa": str(r.get("grupa") or "?").upper(),
            "PZ": r.get("pz") or "?",
            "Forum ID": r.get("forum_post_id") or "?",
        }
        for r in diamonds
    ]).sort_values(by=["Data", "Operator", "Numer"])
    st.dataframe(df_full, use_container_width=True, hide_index=True)
    
    # ===== 📊 STATYSTYKI OPERATORÓW — ZAKRES DAT (trwała kronika) =====
    st.markdown("---")
    st.markdown(f"### 📊 Statystyki operatorów — zakres **{d_from} → {d_to}**")
    _today_str = datetime.now(pytz.timezone('Europe/Warsaw')).date().strftime("%Y-%m-%d")
    if d_from.strftime("%Y-%m-%d") <= _today_str <= d_to.strftime("%Y-%m-%d"):
        st.warning("⚠️ Zakres zawiera **dzisiejszy dzień (w toku)** — dzień się jeszcze nie skończył, "
                   "więc % i bilans są ZANIŻONE (pełny plan, niepełne domknięcia). Do oceny „czy się "
                   "wyrabiamy” wybierz zakres kończący się **wczoraj**, albo czytaj dzisiejszy dzień osobno.")
    st.caption("Dane z TRWAŁEJ kroniki (ew_operator_stats) — przeżywają czyszczenie puli ew_cases, więc dni "
               "wstecz liczą się poprawnie. „W toku” pozostaje migawką bieżącej puli. Diamenty/skuteczność z "
               "całego wybranego zakresu dat (przed górnymi filtrami UI).")

    # Kafelek grupowy (per grupa + cała firma) — z planu + liczników grupowych w zakresie
    render_group_summary_range(d_from, d_to)

    # Tabela operatora — skuteczność = diamenty ÷ zakończone (wszystkie kanały), ⭐ TOP3
    st.markdown("#### 👤 Operatorzy — skuteczność")
    render_operator_table(d_from, d_to, key_prefix="dz")

    # Rozbicie wsadów odwrotnych per osoba
    st.markdown("#### 🔁 Wsady odwrotne — per operator (WA / MAIL / FORUM)")
    render_reverse_breakdown(d_from, d_to, key_prefix="dz")

    # Ruchy poza dzisiejszym planem (zaległości + odwrotne spoza dziś) — per osoba i per grupa
    st.markdown("#### ➕ Poza planem — ruchy na case'ach spoza dnia planu")
    render_poza_planem(d_from, d_to, key_prefix="dz")

    # Wsad per dzień — dyscyplina (ile zaplanowano / przerobiono / nieprzerobiono), wiersze per dzień
    st.markdown("#### 📅 Wsad per dzień — czy się wyrabiamy")
    render_wsad_per_day(d_from, d_to, key_prefix="dz")

    # 📞 Telefony — operatorzy dzwoniący + kubełek zewnętrzny (Etap 1)
    st.markdown("---")
    st.markdown("### 📞 Telefony — wykonane, efektywność, diamentofony")
    render_phone_stats(d_from, d_to, key_prefix="dz")
    render_woreczek_stats(d_from, d_to, key_prefix="dz")
