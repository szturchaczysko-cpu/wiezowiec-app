import streamlit as st
import vertexai
from vertexai.generative_models import GenerativeModel, Content, Part, SafetySetting, HarmCategory, HarmBlockThreshold
from google.oauth2 import service_account
from datetime import datetime
import json, re, pytz, time
import firebase_admin
from firebase_admin import credentials, firestore
import requests

# --- KONFIGURACJA ---
st.set_page_config(page_title="Elektryczny Wieżowiec", layout="wide", page_icon="🏢")

if not firebase_admin._apps:
    creds_dict = json.loads(st.secrets["FIREBASE_CREDS"])
    creds = credentials.Certificate(creds_dict)
    firebase_admin.initialize_app(creds)
db = firestore.client()

# --- BRAMKA HASŁA ---
if "password_correct" not in st.session_state:
    st.session_state.password_correct = False

if not st.session_state.password_correct:
    st.header("🏢 Elektryczny Wieżowiec — Logowanie")
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
WIEZOWIEC_PROMPT_URLS = {
    "Wieżowiec v5 (stabilny)": "https://raw.githubusercontent.com/szturchaczysko-cpu/szturchacz/refs/heads/main/prompt_wiezowiec_v5.md",
}
custom_data = (db.collection("admin_config").document("custom_prompts").get().to_dict() or {}).get("urls", {})
for name, url in custom_data.items():
    if "wiezowiec" in name.lower() or "wieżowiec" in name.lower() or "ew_" in name.lower():
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
            current_nr = nr_match.group(1).strip().rstrip(',').rstrip('|')
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
            
            idx_m = re.search(r'Index:\s*(\S+)', label)
            index_handlowy = idx_m.group(1) if idx_m else ""
            if not index_handlowy:
                lindx_m = re.search(r'lindexy[:\s]+(\S+)', pelna_linia, re.IGNORECASE)
                if lindx_m:
                    index_handlowy = lindx_m.group(1)
            
            if pelna_linia:
                cases.append({
                    "numer_zamowienia": numer or f"UNKNOWN_{len(cases)+1}",
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
st.title("🏢 Elektryczny Wieżowiec")
st.caption("System zarządzania priorytetami — wsady z pamięcią")

tab_wsady, tab_generuj, tab_batches, tab_cases = st.tabs([
    "📂 Wsady",
    "⚡ Generuj raport",
    "📦 Historia partii",
    "📋 Przegląd casów"
])


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
            # Usuń WSZYSTKIE casy ze WSZYSTKICH batchy (wolne, w toku, zakończone — wszystko)
            all_batches = db.collection("ew_batches").get()
            deleted = 0
            for bdoc in all_batches:
                batch_cases = db.collection("ew_cases").where("batch_id", "==", bdoc.id).get()
                for c in batch_cases:
                    db.collection("ew_cases").document(c.id).delete()
                    deleted += 1
                db.collection("ew_batches").document(bdoc.id).delete()
            st.success(f"🗑️ Usunięto {deleted} casów i wszystkie batche. Czysta baza.")
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
        sel_prompt = st.selectbox("Prompt Wieżowca:", list(WIEZOWIEC_PROMPT_URLS.keys()))
        sel_prompt_url = WIEZOWIEC_PROMPT_URLS[sel_prompt]
    with col2:
        if GCP_PROJECTS:
            proj_opts = [f"{i+1} - {p}" for i, p in enumerate(GCP_PROJECTS)]
            sel_proj = st.selectbox("Projekt GCP:", proj_opts)
            proj_idx = int(sel_proj.split(" - ")[0]) - 1
            current_project = GCP_PROJECTS[proj_idx]
        else:
            current_project = ""
        model_choice = st.selectbox("Model AI:", ["gemini-2.5-pro", "gemini-2.5-flash"])
    
    st.markdown("---")
    
    if st.button("🚀 Generuj raport priorytetów", type="primary"):
        if not current_project:
            st.error("Brak projektu GCP!")
            st.stop()
        
        WIEZOWIEC_PROMPT = get_remote_prompt(sel_prompt_url)
        if not WIEZOWIEC_PROMPT:
            st.error("Nie udało się pobrać promptu!")
            st.stop()
        
        try:
            ci = json.loads(st.secrets["FIREBASE_CREDS"])
            cv = service_account.Credentials.from_service_account_info(ci)
            vertexai.init(project=current_project, location=st.secrets.get("GCP_LOCATION", "us-central1"), credentials=cv)
        except Exception as e:
            st.error(f"Błąd Vertex AI: {e}")
            st.stop()
        
        tz_pl = pytz.timezone('Europe/Warsaw')
        now = datetime.now(tz_pl)
        
        # --- TRYB INKREMENTALNY: sprawdź istniejące casy w bazie ---
        existing_docs = db.collection("ew_cases").limit(5000).get()
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
        
        # --- Buduj user_message ---
        if is_incremental:
            # Przygotuj sekcję gotowych wyników
            gotowe_lines = []
            for nrzam, edata in sorted(nrzam_gotowe.items(), key=lambda x: x[1].get("score", 0), reverse=True):
                gotowe_lines.append(
                    f"[SCORE={edata.get('score', 0)}] {edata.get('priority_icon', '?')} | "
                    f"{edata.get('priority_label', '?')} | "
                    f"NrZam: {nrzam} | Grupa: {edata.get('grupa', '?')} | "
                    f"Status: {edata.get('status', 'wolny')} | "
                    f"Linia: {edata.get('pelna_linia_szturchacza', '')}"
                )
            gotowe_text = '\n'.join(gotowe_lines)
            
            # Szturchacz tylko dla zamówień do przeliczenia (szturchacz_blocks już mamy z góry)
            nowe_szturchacz_parts = []
            for nrzam in nrzam_do_przeliczenia:
                if nrzam in szturchacz_blocks:
                    nowe_szturchacz_parts.append(szturchacz_blocks[nrzam])
                elif nrzam in existing_cases_map:
                    # Zakończony case z bazy, nie ma go w aktualnym wsadzie — użyj zapisanej linii
                    saved_line = existing_cases_map[nrzam].get("pelna_linia_szturchacza", "")
                    if saved_line:
                        nowe_szturchacz_parts.append(saved_line)
            nowe_szturchacz_text = '\n\n'.join(nowe_szturchacz_parts) if nowe_szturchacz_parts else '(brak nowych bloków szturchacza)'
            
            user_msg = f"""Data dzisiejsza: {now.strftime('%d.%m.%Y')}

TRYB INKREMENTALNY — dopełnienie puli.

=== ZADANIE ===
1. Przelicz priorytety TYLKO dla zamówień z sekcji "DO PRZELICZENIA" (nowe i zakończone).
2. Zamówienia z sekcji "GOTOWE WYNIKI" mają już przeliczone priorytety — NIE przeliczaj ich ponownie, weź ich score i dane jak są.
3. Połącz WSZYSTKO (przeliczone + gotowe) w jedną spójną posortowaną listę per grupa (DE/FR/UKPL).
4. Wynik: pełna lista WSZYSTKICH zamówień posortowana od najwyższego priorytetu, w standardowym formacie wyjściowym.

=== WSAD 1: ŚWINKA ===
{cur_swinka}

=== WSAD 2: SZTURCHACZ — TYLKO ZAMÓWIENIA DO PRZELICZENIA ({len(nrzam_do_przeliczenia)} szt.) ===
{nowe_szturchacz_text}

=== WSAD 3: STANY USZKÓW ===
{cur_uszki if cur_uszki else '(brak danych o uszkach)'}

=== GOTOWE WYNIKI Z POPRZEDNIEJ RUNDY ({len(nrzam_gotowe)} szt.) — NIE PRZELICZAJ, WSTAW DO LISTY ===
{gotowe_text}
"""
        else:
            # Pierwszy wsad — przelicz wszystko od zera
            user_msg = f"""Data dzisiejsza: {now.strftime('%d.%m.%Y')}

Generuj raport priorytetów na podstawie poniższych wsadów.

=== WSAD 1: ŚWINKA ===
{cur_swinka}

=== WSAD 2: SZTURCHACZ ===
{cur_szturchacz}

=== WSAD 3: STANY USZKÓW ===
{cur_uszki if cur_uszki else '(brak danych o uszkach)'}
"""
        
        with st.spinner("🏢 Wieżowiec analizuje... To może potrwać kilka minut."):
            ai_text = None
            for attempt in range(3):
                try:
                    safety_settings = [
                        SafetySetting(category=HarmCategory.HARM_CATEGORY_DANGEROUS_CONTENT, threshold=HarmBlockThreshold.BLOCK_NONE),
                        SafetySetting(category=HarmCategory.HARM_CATEGORY_HARASSMENT, threshold=HarmBlockThreshold.BLOCK_NONE),
                        SafetySetting(category=HarmCategory.HARM_CATEGORY_HATE_SPEECH, threshold=HarmBlockThreshold.BLOCK_NONE),
                        SafetySetting(category=HarmCategory.HARM_CATEGORY_SEXUALLY_EXPLICIT, threshold=HarmBlockThreshold.BLOCK_NONE),
                    ]
                    model = GenerativeModel(model_choice, system_instruction=WIEZOWIEC_PROMPT)
                    chat = model.start_chat()
                    resp = chat.send_message(
                        user_msg,
                        generation_config={"temperature": 0.0, "max_output_tokens": 65536},
                        safety_settings=safety_settings,
                    )
                    ai_text = resp.text
                    break
                except Exception as e:
                    if "429" in str(e) or "Quota" in str(e):
                        st.toast(f"⏳ Limit API, próba {attempt+1}/3...")
                        time.sleep(10)
                    else:
                        st.error(f"Błąd AI: {e}")
                        break
        
        if not ai_text:
            st.error("❌ Brak odpowiedzi AI.")
            st.stop()
        
        with st.expander("📄 Surowy wynik AI", expanded=False):
            st.text(ai_text)
        
        cases = parse_wiezowiec_output(ai_text)
        if not cases:
            st.warning("⚠️ Parser nie znalazł casów. Sprawdź surowy wynik.")
            st.stop()
        
        de = [c for c in cases if c["grupa"] == "DE"]
        fr = [c for c in cases if c["grupa"] == "FR"]
        ukpl = [c for c in cases if c["grupa"] == "UKPL"]
        st.success(f"✅ **{len(cases)}** casów: DE={len(de)} | FR={len(fr)} | UKPL={len(ukpl)}")
        
        pc1, pc2, pc3 = st.columns(3)
        for col, flag, grp in [(pc1, "🇩🇪 DE", de), (pc2, "🇫🇷 FR", fr), (pc3, "🇬🇧 UKPL", ukpl)]:
            with col:
                st.markdown(f"**{flag} ({len(grp)})**")
                for c in grp[:5]:
                    st.caption(f"{c['priority_icon']} [{c['score']}] {c['numer_zamowienia']}")
                if len(grp) > 5:
                    st.caption(f"...+{len(grp)-5} więcej")
        
        st.session_state["_ew_parsed_cases"] = cases
        st.session_state["_ew_prompt_name"] = sel_prompt
        st.session_state["_ew_model"] = model_choice
    
    # Przycisk zapisu
    if st.session_state.get("_ew_parsed_cases"):
        st.markdown("---")
        cases = st.session_state["_ew_parsed_cases"]
        de = [c for c in cases if c["grupa"] == "DE"]
        fr = [c for c in cases if c["grupa"] == "FR"]
        ukpl = [c for c in cases if c["grupa"] == "UKPL"]
        
        if st.button("💾 Zapisz do bazy i udostępnij operatorom", type="primary"):
            tz_pl = pytz.timezone('Europe/Warsaw')
            now = datetime.now(tz_pl)
            batch_id = f"batch_{now.strftime('%Y%m%d_%H%M%S')}"
            
            # --- SMART MERGE: sprawdź istniejące casy po NrZam ---
            # Pobierz WSZYSTKIE istniejące casy z bazy
            existing_cases_docs = db.collection("ew_cases").limit(5000).get()
            existing_by_nrzam = {}  # NrZam → {doc_id, status}
            for edoc in existing_cases_docs:
                edata = edoc.to_dict()
                enr = edata.get("numer_zamowienia", "")
                if enr:
                    # Jeśli jest wiele casów z tym samym NrZam, zachowaj ten "najbardziej aktywny"
                    if enr in existing_by_nrzam:
                        # Priorytet statusów: w_toku > przydzielony > zakonczony > wolny
                        priority = {"w_toku": 4, "przydzielony": 3, "zakonczony": 2, "wolny": 1}
                        old_prio = priority.get(existing_by_nrzam[enr]["status"], 0)
                        new_prio = priority.get(edata.get("status", "wolny"), 0)
                        if new_prio > old_prio:
                            existing_by_nrzam[enr] = {"doc_id": edoc.id, "status": edata.get("status", "wolny")}
                    else:
                        existing_by_nrzam[enr] = {"doc_id": edoc.id, "status": edata.get("status", "wolny")}
            
            # --- LOGIKA MERGE ---
            saved = 0
            skipped = 0
            replaced = 0
            reactivated = 0
            
            # Krok 1: Wyczyść stare WOLNE casy (będą zastąpione nowymi z przeliczonymi priorytetami)
            for enr, einfo in existing_by_nrzam.items():
                if einfo["status"] == "wolny":
                    db.collection("ew_cases").document(einfo["doc_id"]).delete()
            
            # Krok 2: Zmień stare ZAKOŃCZONE na usunięte (zrobimy nowe wolne)
            for enr, einfo in existing_by_nrzam.items():
                if einfo["status"] == "zakonczony":
                    db.collection("ew_cases").document(einfo["doc_id"]).delete()
            
            # Zapisz batch
            db.collection("ew_batches").document(batch_id).set({
                "created_at": firestore.SERVER_TIMESTAMP,
                "created_by": "admin",
                "date_label": now.strftime("%Y-%m-%d"),
                "total_cases": len(cases),
                "status": "active",
                "summary": f"DE: {len(de)} | FR: {len(fr)} | UKPL: {len(ukpl)}",
                "prompt_used": st.session_state.get("_ew_prompt_name", "?"),
                "model_used": st.session_state.get("_ew_model", "?"),
            })
            
            # Krok 3: Zapisz nowe casy
            progress = st.progress(0)
            for i, case in enumerate(cases):
                nrzam = case.get("numer_zamowienia", "")
                existing = existing_by_nrzam.get(nrzam)
                
                if existing and existing["status"] in ("przydzielony", "w_toku"):
                    # Operator pracuje nad tym casem — NIE RUSZAJ, NIE ZAPISUJ DUPLIKATU
                    skipped += 1
                    progress.progress((i + 1) / len(cases))
                    continue
                
                # Zapisz nowy case (wolny lub zastępujący stary wolny/zakończony)
                case_id = f"{batch_id}_{case['grupa']}_{i+1:04d}"
                db.collection("ew_cases").document(case_id).set({
                    "batch_id": batch_id,
                    "numer_zamowienia": nrzam,
                    "score": case["score"],
                    "priority_icon": case["priority_icon"],
                    "priority_label": case["priority_label"],
                    "grupa": case["grupa"],
                    "index_handlowy": case.get("index_handlowy", ""),
                    "pelna_linia_szturchacza": case["pelna_linia_szturchacza"],
                    "naglowek_priorytetowy": case["naglowek_priorytetowy"],
                    "status": "wolny",
                    "assigned_to": None,
                    "assigned_at": None,
                    "completed_at": None,
                    "result_tag": None,
                    "result_pz": None,
                    "sort_order": i,
                    "created_at": firestore.SERVER_TIMESTAMP,
                })
                
                if existing and existing["status"] == "zakonczony":
                    reactivated += 1
                elif existing and existing["status"] == "wolny":
                    replaced += 1
                else:
                    saved += 1
                
                progress.progress((i + 1) / len(cases))
            progress.empty()
            
            # Archiwizuj stare batche
            old_batches = db.collection("ew_batches").where("status", "==", "active").get()
            for ob in old_batches:
                if ob.id != batch_id:
                    db.collection("ew_batches").document(ob.id).update({"status": "archived"})
            
            st.success(
                f"✅ Zapisano!\n\n"
                f"- **{saved}** nowych casów dodanych\n"
                f"- **{replaced}** wolnych zastąpionych (nowy priorytet)\n"
                f"- **{reactivated}** zakończonych reaktywowanych (znów wolne)\n"
                f"- **{skipped}** pominiętych (operator pracuje — przydzielone/w toku)"
            )
            st.session_state["_ew_parsed_cases"] = None
            st.balloons()


# ==========================================
# 📦 HISTORIA PARTII
# ==========================================
with tab_batches:
    st.subheader("📦 Historia partii Wieżowca")
    batches = db.collection("ew_batches").order_by("created_at", direction=firestore.Query.DESCENDING).limit(20).get()
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
                    batch_cases = db.collection("ew_cases").where("batch_id", "==", bid).get()
                    sc = {"wolny": 0, "przydzielony": 0, "w_toku": 0, "zakonczony": 0}
                    for c in batch_cases:
                        s = c.to_dict().get("status", "wolny")
                        sc[s] = sc.get(s, 0) + 1
                    for k, v in sc.items():
                        st.caption(f"{k}: {v}")
                if b.get("status") == "active":
                    if st.button(f"📥 Archiwizuj", key=f"arch_{bid}"):
                        db.collection("ew_batches").document(bid).update({"status": "archived"})
                        st.rerun()


# ==========================================
# 📋 PRZEGLĄD CASÓW
# ==========================================
with tab_cases:
    st.subheader("📋 Przegląd casów")
    fc1, fc2, fc3 = st.columns(3)
    with fc1:
        fg = st.selectbox("Grupa:", ["Wszystkie", "DE", "FR", "UKPL"])
    with fc2:
        fs = st.selectbox("Status:", ["Wszystkie", "wolny", "przydzielony", "w_toku", "zakonczony"])
    with fc3:
        fo = st.text_input("Operator:", placeholder="np. Emilia")
    
    q = db.collection("ew_cases")
    if fg != "Wszystkie":
        q = q.where("grupa", "==", fg)
    if fs != "Wszystkie":
        q = q.where("status", "==", fs)
    if fo:
        q = q.where("assigned_to", "==", fo)
    q = q.order_by("score", direction=firestore.Query.DESCENDING).limit(1000)
    results = q.get()
    
    if not results:
        st.info("Brak casów.")
    else:
        total = len(results)
        st.caption(f"Znaleziono **{total}** casów")
        
        # Paginacja
        PAGE_SIZE = 50
        total_pages = max(1, (total + PAGE_SIZE - 1) // PAGE_SIZE)
        page = st.number_input("Strona:", min_value=1, max_value=total_pages, value=1, step=1)
        start = (page - 1) * PAGE_SIZE
        end = min(start + PAGE_SIZE, total)
        st.caption(f"Strona {page}/{total_pages} (pozycje {start+1}–{end} z {total})")
        
        for doc in results[start:end]:
            c = doc.to_dict()
            smap = {"wolny": "🔵", "przydzielony": "🟡", "w_toku": "🟠", "zakonczony": "🟢"}
            si = smap.get(c.get("status"), "❓")
            cc1, cc2 = st.columns([4, 1])
            with cc1:
                st.markdown(f"{si} **{c.get('numer_zamowienia', '?')}** — "
                            f"{c.get('priority_icon', '')} [{c.get('score', 0)}] {c.get('priority_label', '')}")
            with cc2:
                st.caption(f"{c.get('grupa', '?')} | {c.get('assigned_to') or '-'} | {c.get('status', '?')}")
