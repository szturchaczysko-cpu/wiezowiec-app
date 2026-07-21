"""
MODUŁ FORUM PMG — integracja Szturchacza z forum F15
Pisanie i czytanie postów przez API.

Używany przez:
- app_vertex_ew.py (Koordynator) — w trakcie sesji operatora
- app.py (Wieżowiec) — w autopilocie nocnym

Endpointy:
- POST /api/wpisy/CreatePost — tworzenie/edycja postów
- POST /api/wpisy/GetPostTree — czytanie podwątków

Nick bota: chatoszturek

=== MIGRACJA POD NOWĄ PRYWATNOŚĆ FORUM f15 (brief §5/§6.3) ===
- CreatePost: AiUser = ZAWSZE "chatoszturek" (konto AI), NIGDY nick operatora.
  Faktyczny autor (operator albo "chatoszturek" w autopilocie) → SubThread.UserRzeczywisty.
- GetPostTree: stare "login" USUNIĘTE → "LoginRequestedBy" (+ opcjonalnie LoginActingAs/MemberTypeActingAs).
  Parsowanie odpowiedzi: UserAddName/UserAddType + UserOdInGroup (autor wpisów grupowych).
- Token: stały bearer z st.secrets (NIE hardcode) — bez zmian.
- NIE wdrażać na produkcję przed wgraniem prywatności na forum (brief §9).
"""

import re
import json
import requests
import traceback
import streamlit as st


# --- KONFIGURACJA ---
FORUM_API_BASE = "https://f15.pmgtechnik.com"
FORUM_USER = "chatoszturek"

# Whitelist znanych userów indywidualnych (type=1)
# Sprzedawcy, telefoniści jako osoby, chatoszturek
KNOWN_INDIVIDUAL_USERS = {
    "chatoszturek", "chatek", "chatosztur",
    # Sprzedawcy z underscore
    "kasia_k", "anna_m", "oliwia_m", "klaudia_k",
    # Osoby z whitelist
    "sylwia", "justyna", "romana", "EwelinaG",
    # Sprzedawcy bez underscore (lista zastępstw — patrz SPRZEDAWCY niżej)
    "magda", "kinga", "klaudia", "emilia", "marta", "Andy",
}

# ==========================================================
# ZASTĘPSTWA SPRZEDAWCÓW
# ==========================================================
# Sprzedawca NIE jest nigdzie zapisywany w bazie — AI czyta go z trzeciego elementu wsadu
# i emituje user_do=<nick>. Podmiana siedzi więc tutaj, tuż przed wysyłką: widzimy user_do,
# sprawdzamy mapę zastępstw z Firestore i podmieniamy odbiorcę. Prompt o tym nie wie.
#
# Mapa: admin_config/sprzedawcy_zastepstwa -> {"mapa": {"kinga": "emilia", ...}}
# Wpis kinga->kinga (albo brak wpisu) = sprzedawca w pracy, bez podmiany.
# Ustawiana w Wieżowcu, zakładka "🔁 Zastępstwa sprzedawców".

SPRZEDAWCY = {
    # DE
    "magda", "kinga", "klaudia", "emilia", "sylwia", "oliwia_m",
    # FR
    "kasia_k", "klaudia_k", "anna_m",
    # UK/PL
    "Andy", "marta",
}

_ZAST_CACHE = {"mapa": None, "ts": 0.0}
_ZAST_TTL = 60.0  # sekundy — zmiana w Wieżowcu widoczna najpóźniej po minucie


def load_zastepstwa(db, prefix=""):
    """Mapa zastępstw sprzedawców z Firestore (cache 60s)."""
    import time as _time
    now = _time.time()
    if _ZAST_CACHE["mapa"] is not None and (now - _ZAST_CACHE["ts"]) < _ZAST_TTL:
        return _ZAST_CACHE["mapa"]
    mapa = {}
    try:
        doc = db.collection(f"{prefix}admin_config").document("sprzedawcy_zastepstwa").get()
        if getattr(doc, "exists", False):
            mapa = (doc.to_dict() or {}).get("mapa", {}) or {}
    except Exception as e:
        _flog(f"ZASTEPSTWA: nie udalo sie odczytac mapy ({e}) — jade bez podmiany")
        mapa = {}
    _ZAST_CACHE["mapa"] = mapa
    _ZAST_CACHE["ts"] = now
    return mapa


def apply_zastepstwo(user_do, mapa):
    """user_do sprzedawcy -> zastępca, jeśli ustawiony.

    Zwraca (finalny_user_do, oryginalny_nick_albo_None).
    Podmiana odpala się WYŁĄCZNIE dla nicków z SPRZEDAWCY — grupy (Telefoniści_*,
    Operatorzy_*, EA, SZTURZE_WSPARCIE) i inne osoby (justyna) są nietykalne.
    """
    if not user_do or not mapa:
        return user_do, None
    if user_do not in SPRZEDAWCY:
        return user_do, None
    zast = mapa.get(user_do)
    if not zast or zast == user_do:
        return user_do, None
    if zast not in SPRZEDAWCY:
        # bezpiecznik: ktoś wpisał do bazy nick spoza listy — nie ryzykujemy błędu 400
        return user_do, None
    return zast, user_do


def _is_individual_user(nick):
    """True jeśli nick to pojedynczy user (type=1), False jeśli grupa (type=2)."""
    if not nick:
        return True
    # Jawna whitelista
    if nick in KNOWN_INDIVIDUAL_USERS:
        return True
    if nick.lower() in {u.lower() for u in KNOWN_INDIVIDUAL_USERS}:
        return True
    # Heurystyka: grupa = wszystko WIELKIMI lub z "/"
    if nick.isupper():
        return False
    if "/" in nick:
        return False
    # Grupy "mieszane": Telefoniści_DE, Operatorzy_DE
    import re as _re
    if _re.match(r'^[A-ZĄĆĘŁŃÓŚŹŻ][a-ząćęłńóśźż]+_[A-Z]{2,}$', nick):
        return False
    # Domyślnie: user
    return True


# --- DEBUG LOG ---
FORUM_DEBUG = True  # True = loguj wszystko do session_state

def _flog(msg):
    """Loguj do session_state (widoczne w UI) + print (logi Streamlit Cloud)"""
    if not FORUM_DEBUG:
        return
    if "forum_debug_log" not in st.session_state:
        st.session_state.forum_debug_log = []
    st.session_state.forum_debug_log.append(msg)
    print(f"[FORUM_DEBUG] {msg}")

def _get_bearer():
    return st.secrets.get("FORUM_BEARER_TOKEN", "")

def _headers():
    return {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {_get_bearer()}"
    }


# ==========================================
# PISANIE — CreatePost
# ==========================================

def forum_write(post_id, do_odp_id, user_do, tresc, user_do_type=1, user_od=None, ai_user=None, tytul=None):
    """Wpis na forum (CreatePost).

    user_od:
      - chatoszturek (default) → wpis idzie od bota (autopilot)
      - Operatorzy_DE/FR/UK/PL → wpis idzie z grupy, w nawiasie pojawi się user_rzeczywisty

    ai_user: FAKTYCZNY AUTOR wpisu = login osoby/konta które naprawdę pisze (brief §3.2):
             - sesja prowadzona przez operatora → login operatora na forum (np. "kasia_k", "oliwia_m", "ewelina_g"),
             - autopilot / sesja bez operatora → "chatoszturek".
             Trafia do pola SubThread.UserRzeczywisty i wyświetla się na forum jako "GRUPA (login)".

    PRYWATNOŚĆ (brief §6.3a/§3.3): AiUser = ZAWSZE konto AI tej integracji ("chatoszturek"),
    NIGDY nick operatora-człowieka. Nick/login faktycznego autora idzie do UserRzeczywisty.
    """
    if user_od is None:
        user_od = FORUM_USER

    # Stempel autora w treści — widoczne „przerobione przez" gdy za wpisem stoi OPERATOR (nie bot).
    if ai_user and ai_user != FORUM_USER and tresc and "Przerobione przez:" not in tresc:
        tresc = f"{tresc}<br><i>Przerobione przez: {ai_user}</i>"

    # from_user_type: 1=user, 2=grupa (jeśli wysyła grupa operatorów)
    from_user_type = 1 if _is_individual_user(user_od) else 2

    # UserRzeczywisty = faktyczny autor (login): operator (sesja) lub chatoszturek (autopilot/brak).
    user_rzeczywisty = ai_user if ai_user else FORUM_USER

    _flog(f"WRITE: post_id={post_id}, do_odp_id={do_odp_id}, user_do={user_do}, type={user_do_type}")
    _flog(f"WRITE: user_od={user_od}, from_type={from_user_type}, AiUser={FORUM_USER}, UserRzeczywisty={user_rzeczywisty}, tytul='{tytul}'")
    _flog(f"WRITE: tresc={tresc[:80]}...")
    
    payload = {
        "thread": {
            "id": post_id,
            "title": tytul,
            "fromUser": user_od,
            "fromUserType": from_user_type,
            "toUser": user_do,
            "toUserType": user_do_type,
            "private": False
        },
        "subThread": {
            "id": do_odp_id,
            "text": tresc,
            "fromUser": user_od,
            "fromUserType": from_user_type,
            "toUser": user_do,
            "toUserType": user_do_type,
            "type": 0,
            "title": tytul,
            "private": False,
            "AiGenerated": 1,
            "AiUser": FORUM_USER,            # ZAWSZE konto AI (chatoszturek) — brief §6.3a/§3.3
            "UserRzeczywisty": user_rzeczywisty,  # faktyczny autor: operator (sesja) lub chatoszturek (autopilot)
            "SilentConfirmAfterCreate": False,    # brief §5.1
        }
    }
    
    try:
        resp = requests.post(
            f"{FORUM_API_BASE}/api/wpisy/CreatePost",
            headers=_headers(),
            json=payload,
            timeout=30
        )
        resp.raise_for_status()
        data = resp.json()
        
        if data.get("status") == "SUCCESS":
            msg = data.get("message", "")
            id_match = re.search(r'\(id:\s*(\d+)\)', msg)
            if not id_match:
                id_match = re.search(r'id[:\s]+(\d+)', msg, re.IGNORECASE)
            if not id_match:
                id_match = re.search(r'(\d{7,})', msg)
            new_id = int(id_match.group(1)) if id_match else None
            
            _flog(f"WRITE RESULT: success=True, new_id={new_id}, msg={msg[:100]}")
            
            if not new_id:
                import streamlit as _st
                _st.toast(f"⚠️ Forum API OK ale brak ID w: {msg[:200]}")
            
            return {
                "success": True,
                "new_post_id": new_id,
                "message": msg,
                "link": f"{FORUM_API_BASE}/Wpisy/detailWpis?id={post_id}&do_odpid={new_id}#odp-{new_id}" if new_id else None
            }
        else:
            return {"success": False, "error": data.get("message", "Nieznany błąd")}
    
    except Exception as e:
        return {"success": False, "error": str(e)}


# ==========================================
# CZYTANIE — GetPostTree
# ==========================================

def forum_read(branch_id=None, root_id=None, leaf_id=None, max_pages=5,
               login_requested_by=None, login_acting_as=None, member_type_acting_as=None):
    """Czytanie drzewka (GetPostTree).

    PRYWATNOŚĆ (brief §5.2): stare pole "login" USUNIĘTE. Autoryzacja przez stały bearer token,
    tożsamość w body:
      - login_requested_by: login wykonującego zapytanie; domyślnie konto AI "chatoszturek".
      - login_acting_as / member_type_acting_as: opcjonalne — z czyjej perspektywy czytać drzewko
        (czytanie jako grupa: login_acting_as=grupa, member_type_acting_as=2).
    """
    all_posts = []
    thread_title = ""

    for page in range(1, max_pages + 1):
        # FIX 406 (potwierdzone przez IT forum): GetPostTree WYMAGA LoginActingAs (string) oraz
        # MemberTypeActingAs (int: 1=user, 2=grupa). Bez nich serwer zwraca 406 Not Acceptable.
        # Domyślnie czytamy z perspektywy konta wykonującego zapytanie (LoginRequestedBy), typ=user.
        _lrb = login_requested_by or FORUM_USER
        _laa = login_acting_as or _lrb
        _mta = member_type_acting_as if member_type_acting_as else (
            1 if _is_individual_user(_laa) else 2)
        payload = {
            "root": root_id,
            "branch": branch_id,
            "leaf": leaf_id,
            "WholePage": None,
            "LoginRequestedBy": _lrb,           # brief §5.2 — zastępuje "login"; bez JWT
            "LoginActingAs": _laa,              # WYMAGANE (fix 406)
            "MemberTypeActingAs": _mta,         # WYMAGANE (fix 406): 1=user, 2=grupa
            "PagingInfo": {
                "CurrentPage": page
            }
        }

        try:
            resp = requests.post(
                f"{FORUM_API_BASE}/api/wpisy/GetPostTree",
                headers=_headers(),
                json=payload,
                timeout=30
            )
            resp.raise_for_status()
            data = resp.json()
            
            if data.get("status") != "SUCCESS" or not data.get("tree"):
                if page == 1:
                    return {"success": False, "error": data.get("message", "Brak danych")}
                break
            
            tree = data["tree"]
            if page == 1:
                thread_title = tree.get("Title", "")
            
            post_list = tree.get("PostList", [])
            if not post_list:
                break
            
            for p in post_list:
                all_posts.append({
                    "Id": p.get("Id"),
                    "Do_Odpid": p.get("Do_Odpid"),
                    "Text": p.get("Text", ""),
                    "UserAddName": p.get("UserAddName", ""),
                    "UserAddType": p.get("UserAddType"),       # brief §5.2: 1=user, 2=grupa
                    "UserOdInGroup": p.get("UserOdInGroup"),   # brief §5.2: faktyczny autor gdy nadawca=grupa
                    "UserToName": p.get("UserToName", ""),
                    "DateAdd": p.get("DateAdd", ""),
                    "Level": p.get("Level", 0),
                    "Hierarchy": p.get("Hierarchy", ""),
                })
            
            paging = tree.get("PagingInfo", {})
            total_pages = paging.get("TotalPages", 1)
            if page >= total_pages:
                break
        
        except Exception as e:
            if page == 1:
                return {"success": False, "error": str(e)}
            break
    
    return {
        "success": True,
        "posts": all_posts,
        "thread_title": thread_title,
        "count": len(all_posts)
    }


def forum_read_subtree(branch_id=None, leaf_id=None, root_id=None, from_post_id=None, nrzam=None):
    """
    Czyta wątek forum i wyciąga ORAZ FILTRUJE powiązane odpowiedzi.
    Rozszerzone zabezpieczenia, które ZAWSZE odcinają śmietnik z innych zamówień,
    nawet w trybie awaryjnego doczytywania (fallback leaf).
    """
    result = forum_read(branch_id=branch_id, root_id=root_id, leaf_id=leaf_id)
    if not result.get("success"):
        return result
    
    start_hierarchy = None
    root_text = ""
    for p in result["posts"]:
        if p["Id"] == from_post_id:
            start_hierarchy = p["Hierarchy"]
            root_text = p.get("Text", "")
            break
    
    # BEZPIECZNIK: Sprawdzamy, czy wczytany post faktycznie dotyczy naszego zamówienia
    if start_hierarchy and nrzam:
        if str(nrzam) not in root_text:
            _flog(f"  → UWAGA! Post {from_post_id} dotyczy innego numeru niż {nrzam}! Ignoruję to fałszywe ID.")
            start_hierarchy = None  
    
    if not start_hierarchy and not nrzam:
        return {"success": False, "error": f"Nie znaleziono wpisu {from_post_id} lub fałszywe ID"}
    
    filtered = []
    seen_ids = set()
    for p in result["posts"]:
        pid = p["Id"]
        
        # 1. Pasuje do drzewka odpowiedzi (i przeszło bezpiecznik)
        if start_hierarchy and p["Hierarchy"].startswith(start_hierarchy):
            if pid not in seen_ids:
                filtered.append(p)
                seen_ids.add(pid)
            continue
            
        # 2. Pasuje po numerze zamówienia (nawet z błędem w hierarchii lub trybem awaryjnym)
        if nrzam and str(nrzam) in p.get("Text", ""):
            if pid not in seen_ids:
                filtered.append(p)
                seen_ids.add(pid)
                
    if not filtered:
        return {"success": False, "error": f"Brak powiązanych postów dla zamówienia {nrzam}"}
        
    filtered = sorted(filtered, key=lambda x: x.get("DateAdd", ""))
    
    return {
        "success": True,
        "posts": filtered,
        "thread_title": result["thread_title"],
        "count": len(filtered)
    }


# ==========================================
# PARSOWANIE MARKERÓW Z ODPOWIEDZI AI
# ==========================================

FORUM_MARKER_PATTERN = re.compile(r'\[FORUM_(WRITE|READ)\|([^\]]+)\]', re.DOTALL)

def parse_forum_markers(ai_response):
    markers = []
    
    for m in FORUM_MARKER_PATTERN.finditer(ai_response):
        action = m.group(1).lower()
        params_str = m.group(2)
        
        params = {}
        if action == "write" and "|tresc=" in params_str:
            before_tresc, tresc = params_str.split("|tresc=", 1)
            params["tresc"] = tresc.strip()
            for part in before_tresc.split("|"):
                if "=" in part:
                    k, v = part.split("=", 1)
                    params[k.strip()] = v.strip()
        else:
            for part in params_str.split("|"):
                if "=" in part:
                    k, v = part.split("=", 1)
                    params[k.strip()] = v.strip()
        
        marker = {"type": action, "raw": m.group(0), "params": params}
        
        if action == "write":
            marker["cel"] = params.get("cel", "")
            marker["tresc"] = params.get("tresc", "")
            marker["do_odp_id"] = int(params["do_odp_id"]) if "do_odp_id" in params else None
            marker["user_do"] = params.get("user_do", None)
            marker["tytul"] = params.get("tytul", None)
        elif action == "read":
            marker["forum_id"] = int(params["forum_id"]) if "forum_id" in params else None
            marker["cel"] = params.get("cel", "")
        
        markers.append(marker)
    
    return markers


# ==========================================
# DIAMENTY v1.5.7d "pomijamyPZ6" — decyzja na podstawie TREŚCI posta
# ==========================================

# Stop-lista: posty zawierające te słowa NIE są diamentami (bumpy/ponaglenia/eskalacje).
# UWAGA: celowo BEZ "etykiet" — etykieta UPS punkt JEST diamentem (decyzja EA).
DIAMOND_STOP_WORDS = re.compile(r'(bump|ponaglenie|eskalacja|dopyt|podbicie)', re.IGNORECASE)

# Walidacja: prawdziwe zlecenie kuriera/etykiety zawsze zawiera "Zamówienie: NNN"
_RE_DIAMOND_ZAMOWIENIE = re.compile(r'Zam[óo]wienie\s*[:=]\s*(\d+)', re.IGNORECASE)

# Ekstrakcja z treści posta — tolerancja [:=] (tag C# w ai_text używa "=", treść posta ":")
_RE_DIAMOND_TOWAR = re.compile(r'TOWAR_TYP\s*[:=]\s*(KOLEKTOR|SKRZYNIA)', re.IGNORECASE)
# v1.5.7e: łapie też samo "Schenker" (normalizacja do DBSCHENKER w log_diamond)
_RE_DIAMOND_KURIER = re.compile(r'Kurier\s*[:=]\s*(UPS|FEDEX|(?:DB[\s_]?)?SCHENKER)', re.IGNORECASE)
_RE_DIAMOND_ETYKIETA = re.compile(r'(UPS_ETYKIETA_PUNKT|KURIER_OPCJA\s*[:=]\s*ETYKIETA_PUNKT)', re.IGNORECASE)
_RE_DIAMOND_KURIER_SLOWO = re.compile(r'(Kurier\s*[:=]|KURIER_PRZEWOZNIK)', re.IGNORECASE)

# v1.5.7e: typy NIE-diamentów ("anulowane") — frazy werdykt EA 1:1 (11.06.2026).
# Te wpisy ZAPISUJĄ się do logu pod kluczem {numer}__{typ} (dedup per data+numer+typ),
# ale NIE liczą się jako diamenty. Zamówione pokazują zamówione, anulowane osobno —
# bez odejmowania jednych od drugich (werdykt EA).
# zmiana: KOREKTA / zmiana terminu / nowy termin / przełożyć / zmienił datę /
#         aktualizacja zlecenia / notatk (werdykt EA: notatki idą do korekty)
_RE_TYP_ZMIANA = re.compile(
    r'(KOREKTA|zmiana\s+terminu|nowy\s+termin|prze[łl]o[żz]|zmieni[łl]\s+dat|aktualizacja\s+zlecenia|notatk)',
    re.IGNORECASE)
# cofniete: WSTRZYMANIE / anulowanie / anulować / cofnięcie / rezygnacja
_RE_TYP_COFNIETE = re.compile(
    r'(WSTRZYMA|anulowa|cofni[ęe]|rezygnacj)',
    re.IGNORECASE)
# ponowienie: ponawiam / ponowienie / przypominam / brak odpowiedzi
_RE_TYP_PONOWIENIE = re.compile(
    r'(ponawiam|ponowieni|przypominam|brak\s+odpowiedzi)',
    re.IGNORECASE)
NIE_DIAMENT_TYPY = ("zmiana", "cofniete", "ponowienie")


def _validate_diamond_from_tresc(tresc):
    """Czy treść posta na AUTOS_KURIERZY to faktyczne zlecenie (diament)?

    Warunki: treść zawiera 'Zamówienie: NNN' AND nie zawiera słów stop-listy
    (bump/ponaglenie/eskalacja/dopyt/podbicie). Decyzja zapada TUTAJ — na treści
    fizycznie wysłanego posta — a NIE na tagach PZ/bump z ai_text (te zawodziły:
    case 380457 PZ2 + ręczne zlecenie kuriera, case 378646 etykieta UPS punkt).
    """
    if not tresc:
        return False
    if not _RE_DIAMOND_ZAMOWIENIE.search(tresc):
        return False
    if DIAMOND_STOP_WORDS.search(tresc):
        return False
    return True


def _classify_typ_zlecenia(tresc):
    """Klasyfikacja typu zlecenia z treści posta.

    v1.5.7e — kolejność sprawdzania (werdykt EA): zmiana → cofniete → ponowienie
    → etykieta_ups_punkt → kurier → inne. Kolejność rozstrzyga konflikty fraz:
    korekta terminu zawiera też "Anulować" (wygrywa zmiana), treść etykiety
    zawiera też słowo KURIER (etykieta przed kurierem).
    Typy zmiana/cofniete/ponowienie to NIE-diamenty (anulowane).
    """
    if not tresc:
        return "inne"
    if _RE_TYP_ZMIANA.search(tresc):
        return "zmiana"
    if _RE_TYP_COFNIETE.search(tresc):
        return "cofniete"
    if _RE_TYP_PONOWIENIE.search(tresc):
        return "ponowienie"
    if _RE_DIAMOND_ETYKIETA.search(tresc):
        return "etykieta_ups_punkt"
    if _RE_DIAMOND_KURIER_SLOWO.search(tresc):
        return "kurier"
    return "inne"


def log_diamond(db, diamond_prefix, numer_zamowienia, operator, source_type,
                forum_post_id=None, kurier=None, kategoria_towaru=None,
                cel=None, grupa=None, pz=None, bump=None,
                tresc=None, typ_zlecenia=None, is_new_subthread=True):
    """Append-only zapis diamentu (zlecenie kuriera = PZ6) do {prefix}ew_diamond_log.
    
    Struktura: {prefix}ew_diamond_log/{YYYY-MM-DD}/numbers/{numer_zamowienia}
    (diamenty) lub .../numbers/{numer}__{typ} (v1.5.7e, nie-diamenty:
    zmiana/cofniete/ponowienie — zapisywane, ale nie liczone jako diamenty).
    Dedup: pierwszy zapis dla danego klucza wygrywa — diamenty per (data, numer),
    nie-diamenty per (data, numer, typ).
    
    Źródła wywołania:
    - app_vertex_ew.py (operator) → source_type="operator"
    - wiezowiecapp.py autopilot (AutoSzturchacz) → source_type="autoszturchacz"
    
    Wstecznie bezpieczne: gdy db=None albo numer_zamowienia=None → no-op.
    Błędy zapisu połykane (nie wywróci sesji forum).

    v1.5.7d "pomijamyPZ6": decyzja "czy to diament" zapada TUTAJ na podstawie
    treści posta (parametr tresc), NIE na tagach PZ/bump z ai_text.
    Ekstrakcja kategoria_towaru/kurier/typ_zlecenia z treści ma pierwszeństwo
    nad wartościami przekazanymi z apek.
    """
    if db is None:
        return
    # v1.5.7e: klasyfikacja typu z treści ZANIM walidacja diamentu —
    # typy zmiana/cofniete/ponowienie (NIE-diamenty, "anulowane") ZAPISUJĄ się
    # do logu pod kluczem {numer}__{typ}, ale NIE liczą się jako diamenty.
    _typ_z_tresci = _classify_typ_zlecenia(tresc) if tresc else None
    _czy_diament = True
    if _typ_z_tresci in NIE_DIAMENT_TYPY:
        typ_zlecenia = _typ_z_tresci
        _czy_diament = False
        # NIE-diament też musi mieć "Zamówienie: NNN" (klucz dokumentu);
        # stop-lista (bump/ponaglenie/eskalacja/dopyt/podbicie) odrzuca wpis całkowicie.
        if not _RE_DIAMOND_ZAMOWIENIE.search(tresc) or DIAMOND_STOP_WORDS.search(tresc):
            _flog(f"DIAMOND SKIP (nie-diament bez 'Zamówienie:' lub stop-słowo): nrzam={numer_zamowienia}")
            return
    else:
        # v1.5.7d: walidacja treści — post musi zawierać "Zamówienie: NNN"
        # i NIE może zawierać stop-słów (bump/ponaglenie/eskalacja/dopyt/podbicie)
        if tresc is not None and not _validate_diamond_from_tresc(tresc):
            _flog(f"DIAMOND SKIP (walidacja treści): nrzam={numer_zamowienia} — brak 'Zamówienie:' lub stop-słowo")
            return
    # v1.5.7d: ekstrakcja z treści posta (regexy tolerują [:=]) — pierwszeństwo nad meta z apek
    if tresc:
        _m_towar = _RE_DIAMOND_TOWAR.search(tresc)
        if _m_towar:
            kategoria_towaru = "Kolektor" if _m_towar.group(1).upper() == "KOLEKTOR" else "Skrzynia biegów"
        _m_kurier = _RE_DIAMOND_KURIER.search(tresc)
        if _m_kurier:
            kurier = _m_kurier.group(1).upper().replace(" ", "").replace("_", "")
            # v1.5.7e: samo "Schenker" normalizowane do DBSCHENKER
            if kurier == "SCHENKER":
                kurier = "DBSCHENKER"
        if not numer_zamowienia:
            _m_nr = _RE_DIAMOND_ZAMOWIENIE.search(tresc)
            if _m_nr:
                numer_zamowienia = _m_nr.group(1)
        if not typ_zlecenia:
            typ_zlecenia = _classify_typ_zlecenia(tresc)
    # REGUŁA "NOWE ID = DIAMENT" (🟥): zlecenie kuriera liczy się RAZ — przy pierwotnym
    # wpisie tworzącym nowy podwątek. Każdy wpis pod ISTNIEJĄCYM podwątkiem dla tego
    # zamówienia (reissue etykiety, korekta, "prosimy o nową etykietę", ponowne zlecenie)
    # to NIE nowy diament — to forma podbicia/zmiany. Jedno fizyczne ściągnięcie = 1 diament.
    # (Naprawia case etykiety błędnie liczonej jako diament: była odpowiedzią pod wątkiem.)
    if not is_new_subthread and _czy_diament:
        _czy_diament = False
        if typ_zlecenia not in ("zmiana", "cofniete", "ponowienie"):
            typ_zlecenia = "zmiana"
        _flog(f"DIAMOND→ZMIANA (odpowiedź pod istniejącym podwątkiem, nie nowe ID): nrzam={numer_zamowienia}, typ={typ_zlecenia}")
    # v1.5.7e: zasada biznesowa EA "jeżeli kolektor to UPS kurier, zawsze" —
    # kolektor bez wyekstrahowanego kuriera → fallback kurier=UPS;
    # kolektor + FedEx/Schenker → zapis literalny + flaga anomalii (oznaczana w Diamentozie)
    _anomalia_kolektor = False
    if kategoria_towaru == "Kolektor":
        if not kurier:
            kurier = "UPS"
        elif kurier != "UPS":
            _anomalia_kolektor = True
    if not numer_zamowienia:
        return
    try:
        from datetime import datetime
        import pytz
        from firebase_admin import firestore as _fs
        
        tz_pl = pytz.timezone('Europe/Warsaw')
        now = datetime.now(tz_pl)
        date_str = now.strftime("%Y-%m-%d")
        
        prefix = diamond_prefix or ""
        coll_name = f"{prefix}ew_diamond_log"
        # v1.5.7e: diamenty pod kluczem {numer} (wstecznie zgodne z zakładką Diamentoza);
        # NIE-diamenty (zmiana/cofniete/ponowienie) pod {numer}__{typ} — dedup per
        # (data, numer, typ): cofnięcie tego samego dnia nie blokuje (i nie jest
        # blokowane przez) prawdziwego diamentu.
        _doc_id = str(numer_zamowienia) if _czy_diament else f"{numer_zamowienia}__{typ_zlecenia}"
        doc_ref = db.collection(coll_name).document(date_str).collection("numbers").document(_doc_id)
        
        # DEDUP: jeśli już istnieje wpis dla tego klucza — nie nadpisuj
        try:
            existing = doc_ref.get()
            if existing.exists:
                _flog(f"DIAMOND DEDUP: {date_str}/{_doc_id} już istnieje, pomijam")
                return
        except Exception:
            pass
        
        entry = {
            "numer_zamowienia": str(numer_zamowienia),
            "operator": operator or "?",
            "source_type": source_type or "operator",
            "logged_at": _fs.SERVER_TIMESTAMP,
            "date_str": date_str,
            "czy_diament": _czy_diament,
        }
        if _anomalia_kolektor:
            entry["anomalia_kolektor_kurier"] = True
        if forum_post_id:
            entry["forum_post_id"] = forum_post_id
        if kurier:
            entry["kurier"] = kurier
        if kategoria_towaru:
            entry["kategoria_towaru"] = kategoria_towaru
        if cel:
            entry["cel"] = cel
        if grupa:
            entry["grupa"] = grupa
        if pz:
            entry["pz"] = pz
        if bump is not None:
            entry["bump"] = bump
        if typ_zlecenia:
            entry["typ_zlecenia"] = typ_zlecenia
        
        doc_ref.set(entry, merge=False)
        _flog(f"DIAMOND LOGGED: {date_str}/{_doc_id} | diament={_czy_diament} | op={operator} | src={source_type} | cel={cel} | typ={typ_zlecenia} | kat={kategoria_towaru} | kurier={kurier}")
    except Exception as e:
        # Połykamy błędy — log diamentu NIE może wywrócić wysyłki na forum
        _flog(f"DIAMOND LOG ERROR (połknięty): {e}")


def execute_forum_actions(ai_response, forum_memory=None, user_od=None, ai_user=None,
                          db=None, source_type="operator", diamond_prefix="",
                          is_diamond_candidate=False, diamond_meta=None):
    """Wykonuje akcje forum (WRITE/READ) z markerów AI.
    
    Nowe parametry (opcjonalne, wstecznie bezpieczne):
    - db: firestore client — jeśli podane + cel to AUTOS_KURIERZY + success=True
          → log_diamond analizuje TREŚĆ posta i decyduje czy zapisać diament (v1.5.7d)
    - source_type: "operator" | "autoszturchacz" — etykietka źródła w logu diamentów
    - diamond_prefix: prefix kolekcji ("test_" lub "")
    - is_diamond_candidate: DEPRECATED od v1.5.7d — przyjmowany dla wstecznej zgodności
          wywołań z apek (szturchacz blok E2 / wieżowiec blok E3), ale IGNOROWANY;
          decyzja "czy diament" zapada w log_diamond na podstawie treści posta
    - diamond_meta: dict z polami fallback (numer_zamowienia, operator, grupa, pz, bump);
          kategoria_towaru / kurier / typ_zlecenia moduł sam parsuje z treści posta
    """
    markers = parse_forum_markers(ai_response)
    
    if not markers:
        return {
            "response": ai_response,
            "forum_reads": [],
            "forum_writes": [],
            "had_actions": False
        }
    
    if forum_memory is None:
        forum_memory = {}
    
    modified_response = ai_response
    forum_reads = []
    forum_writes = []
    
    for marker in markers:
        if marker["type"] == "write":
            cel = marker.get("cel", "")
            tresc = marker.get("tresc", "")
            do_odp_id = marker.get("do_odp_id")
            user_do = marker.get("user_do")
            tytul = marker.get("tytul")

            # --- ZASTĘPSTWA SPRZEDAWCÓW ---
            # Sprzedawca nieobecny (urlop/rotacja) → wpis leci do zastępcy z mapy.
            # Stopka "Zastępstwo za: X" zostaje w treści, żeby zastępca wiedział, czemu
            # dostał cudzą sprawę, i żeby był ślad w wątku.
            if db is not None and user_do:
                _zast_mapa = load_zastepstwa(db, diamond_prefix)
                user_do, _zast_org = apply_zastepstwo(user_do, _zast_mapa)
                if _zast_org:
                    if tresc and "Zastępstwo za:" not in tresc:
                        tresc = f"{tresc}<br><i>Zastępstwo za: {_zast_org}</i>"
                    _flog(f"ZASTEPSTWO: {_zast_org} -> {user_do} (cel={cel})")

            # "NOWE ID = DIAMENT": czy to NOWY podwątek dla tego zamówienia+celu?
            # Nowy = cel jeszcze NIE ma wpisu w pamięci forum dla tego nrzam ORAZ AI nie wskazało
            # jawnie do_odp_id (kontynuacji). Odpowiedź pod istniejącym podwątkiem ≠ nowy diament.
            _cel_existed_before = bool(forum_memory) and (cel in forum_memory)
            _is_new_subthread = (not _cel_existed_before) and (not do_odp_id)

            result = forum_write_to_thread(
                cel=cel,
                tresc=tresc,
                user_do=user_do,
                do_odp_id=do_odp_id,
                forum_memory=forum_memory,
                user_od=user_od,
                ai_user=ai_user,
                tytul=tytul,
            )
            result["cel"] = cel
            result["tresc_skrot"] = tresc[:100] if tresc else ""
            forum_writes.append(result)
            
            if result.get("success"):
                if cel not in forum_memory:
                    forum_memory[cel] = {"id": result.get("FORUM_ID"), "new_subthread": USE_NEW_SUBTHREADS}
                
                # === DIAMENT v1.5.7d "pomijamyPZ6": KAŻDY udany post na AUTOS_KURIERZY analizowany ===
                # Warunek odpalenia: cel == AUTOS_KURIERZY (lub testowy KURIER_test) AND success AND db.
                # is_diamond_candidate z apek jest celowo IGNOROWANY — decyzja na tagach PZ/bump
                # z ai_text zawodziła (case 380457: PZ2 + ręczne zlecenie kuriera; case 378646:
                # etykieta UPS punkt — posty na forum były, diamentów w logu brak).
                # Decyzja "czy diament" zapada WEWNĄTRZ log_diamond na podstawie treści posta:
                # musi zawierać "Zamówienie: NNN", nie może zawierać stop-listy
                # (bump/ponaglenie/eskalacja/dopyt/podbicie).
                # SZTURZE_WSPARCIE / EA na wątku kurierskim = zgłoszenie problemu albo eskalacja,
                # NIGDY zamówienie kuriera. Bez tego warunku nieudane zlecenie (§11.4.3) dałoby
                # diament: treść ma "Zamówienie: NNN" i nie zawiera słów ze stop-listy.
                if (cel in ("AUTOS_KURIERZY", "KURIER_test")
                        and db is not None
                        and user_do not in ("SZTURZE_WSPARCIE", "EA")):
                    _meta = diamond_meta or {}
                    log_diamond(
                        db=db,
                        diamond_prefix=diamond_prefix,
                        numer_zamowienia=_meta.get("numer_zamowienia"),
                        operator=_meta.get("operator"),
                        source_type=source_type,
                        forum_post_id=result.get("FORUM_ID"),
                        kurier=_meta.get("kurier"),
                        kategoria_towaru=_meta.get("kategoria_towaru"),
                        cel=cel,
                        grupa=_meta.get("grupa"),
                        pz=_meta.get("pz"),
                        bump=_meta.get("bump"),
                        tresc=tresc,
                        is_new_subthread=_is_new_subthread,
                    )
                
                replacement = (
                    f"✅ Wysłałem na forum ({cel}). "
                    f"Link: {result.get('link', '?')} "
                    f"FORUM_ID={result.get('FORUM_ID', '?')}"
                )
            else:
                # FALLBACK MANUALNY — API zwróciło błąd, operator musi wysłać ręcznie
                _err = result.get('error', '?')
                _user_do_final = user_do if user_do else "?"
                _full_tresc = tresc if tresc else "(brak treści)"
                replacement = (
                    f"❌ **Nie udało się automatycznie wysłać wpisu na forum.**\n\n"
                    f"**Cel:** `{cel}` | **Do:** `{_user_do_final}` | **Błąd:** `{_err}`\n\n"
                    f"**Skopiuj poniższą treść i wklej jako nowy wpis na forum (wątek {cel}, odbiorca {_user_do_final}):**\n\n"
                    f"```\n{_full_tresc}\n```\n\n"
                    f"**Po wysłaniu** wpisz w chatcie komendę z ID nowego wpisu:\n\n"
                    f"`SESJA WYNIK [NR_ZAM] – FORUM_ID: XXXXX` (gdzie XXXXX to numer ID wpisu z forum)"
                )
                result["fallback_needed"] = True
                result["fallback_cel"] = cel
                result["fallback_user_do"] = _user_do_final
            
            modified_response = modified_response.replace(marker["raw"], replacement)
        
        elif marker["type"] == "read":
            forum_id = marker.get("forum_id")
            cel = marker.get("cel", "")
            
            # Try to extract nrzam from context for filtering
            _ctx_nrzam = st.session_state.get("chat_nrzam") if hasattr(st, 'session_state') else None
            
            if forum_id:
                if USE_NEW_SUBTHREADS:
                    # Czytaj wpis + odpowiedzi z filtrem po nrzam (jeśli znany)
                    result = forum_read_subtree(branch_id=forum_id, from_post_id=forum_id, nrzam=_ctx_nrzam)
                    if not result.get("success"):
                        thread_info = (_get_thread_raw(cel) or {})
                        result = forum_read_subtree(leaf_id=forum_id, root_id=thread_info.get("post_id"), from_post_id=forum_id, nrzam=_ctx_nrzam)
                else:
                    thread_info = (_get_thread_raw(cel) or {})
                    result = forum_read_subtree(leaf_id=forum_id, root_id=thread_info.get("post_id"), from_post_id=forum_id, nrzam=_ctx_nrzam)
            elif cel and forum_memory and cel in forum_memory:
                mem_id = forum_memory[cel].get("id")
                if mem_id:
                    if USE_NEW_SUBTHREADS:
                        result = forum_read_subtree(branch_id=mem_id, from_post_id=mem_id, nrzam=_ctx_nrzam)
                        if not result.get("success"):
                            thread_info = (_get_thread_raw(cel) or {})
                            result = forum_read_subtree(leaf_id=mem_id, root_id=thread_info.get("post_id"), from_post_id=mem_id, nrzam=_ctx_nrzam)
                    else:
                        thread_info = _get_thread_raw(cel)
                        if thread_info and thread_info.get("korzen_id") and thread_info.get("korzen_id") != "DIRECT":
                            result = forum_read_subtree(branch_id=thread_info["korzen_id"], from_post_id=mem_id, nrzam=_ctx_nrzam)
                        else:
                            result = forum_read_subtree(branch_id=mem_id, from_post_id=mem_id, nrzam=_ctx_nrzam)
                            if not result.get("success"):
                                result = forum_read_subtree(leaf_id=mem_id, root_id=thread_info.get("post_id"), from_post_id=mem_id, nrzam=_ctx_nrzam)
                else:
                    result = {"success": False, "error": f"Brak ID w pamięci dla {cel}"}
            elif cel:
                result = {
                    "success": True,
                    "posts": [],
                    "thread_title": "",
                    "count": 0
                }
            else:
                result = {"success": False, "error": "Brak forum_id i cel"}
            
            if result.get("success"):
                if result["count"] == 0:
                    cel_name = cel or "forum"
                    forum_reads.append(f"[FORUM_CONTEXT: {cel_name}] Brak wcześniejszych wpisów chatoszturka dla tego zamówienia. Jeśli trzeba pisać na forum — użyj FORUM_WRITE.")
                    replacement = f"📖 Forum ({cel_name}): brak wcześniejszych wpisów dla tego zamówienia."
                else:
                    context_parts = [f"[FORUM_CONTEXT] ({result['count']} postów)"]
                    for p in result["posts"]:
                        date_str = p['DateAdd'][:10] if p.get('DateAdd') else '?'
                        context_parts.append(
                            f"[{date_str}] {p['UserAddName']} → {p['UserToName']}: "
                            f"{_strip_html(p['Text'][:500])}"
                        )
                    forum_reads.append("\n".join(context_parts))
                    replacement = f"📖 Pobrano {result['count']} postów z forum (kontekst wstrzyknięty)."
            else:
                forum_reads.append(f"[FORUM_CONTEXT] Błąd: {result.get('error', '?')}")
                replacement = f"❌ Błąd czytania z forum: {result.get('error', '?')}"
            
            modified_response = modified_response.replace(marker["raw"], replacement)
    
    return {
        "response": modified_response,
        "forum_reads": forum_reads,
        "forum_writes": forum_writes,
        "had_actions": True
    }

def _strip_html(text):
    return re.sub(r'<[^>]+>', ' ', text).strip()


# ==========================================
# MAPOWANIE WĄTKÓW FORUM (znane post_id)
# ==========================================

FORUM_TEST_MODE = False
USE_NEW_SUBTHREADS = True

_FORUM_THREADS_PROD = {
    "AUTOS_KURIERZY": {
        "post_id": 5687, "korzen_id": None,
        # Od v1.12: atomówki nie istnieją — kuriera zamawia operator, a wpis jest zapisem
        # KONTROLNYM kierowanym do własnej grupy operatora. Prompt zawsze podaje user_do jawnie
        # (Operatorzy_DE/FR/UK/PL); "SELF" to fallback → forum_write_to_thread podstawi user_od.
        "grupa": "SELF", "grupa_type": 2,
        "opis": "Wpis kontrolny po zleceniu kuriera przez operatora (§11.4.3)",
    },
    "SPEDYCJA_REKLAMACJE": {
        "post_id": 5693, "korzen_id": None,
        "grupa": "SPEDYCJA_REKLAMACJE", "grupa_type": 2,
        "opis": "Problemy po zleceniu kuriera (§10.4)",
    },
    "CZATOSZTUR_REKLAMACJE": {
        "post_id": 5688, "korzen_id": None,
        "grupa": "DZIAŁ_EKSPERCKI", "grupa_type": 2,
        "opis": "Reklamacja 'co dalej / można szturchać' (§5.3)",
    },
    "NIEPOZAMYKANE_AUSTAUSCHE": {
        "post_id": 5692, "korzen_id": None,
        "grupa": "NIEPOZAMYKANE AUSTAUSCHE", "grupa_type": 2,
        "opis": "Niezamknięte Austausche / zielonka (§10.5)",
    },
    "CZATOSZTUR_DE": {
        "post_id": 5690, "korzen_id": None,
        "grupa": "Operatorzy_DE", "grupa_type": 2,
        "opis": "Czatosztur DE — delegacje TEL, zapytania (§8.3)",
    },
    "CZATOSZTUR_FR": {
        "post_id": 5689, "korzen_id": None,
        "grupa": "Operatorzy_FR", "grupa_type": 2,
        "opis": "Czatosztur FR (§8.3)",
    },
    "CZATOSZTUR_UKPL": {
        "post_id": 5691, "korzen_id": None,
        "grupa": "Operatorzy_UK/PL", "grupa_type": 2,
        "opis": "Czatosztur UK/PL (alias z underscore)",
    },
    "CZATOSZTUR_UK/PL": {
        "post_id": 5691, "korzen_id": None,
        "grupa": "Operatorzy_UK/PL", "grupa_type": 2,
        "opis": "Czatosztur UK/PL (z ukośnikiem — literalna nazwa)",
    },
    "szturchacz_błędy": {
        "post_id": 5703, "korzen_id": None,
        "grupa": "SZTURZE_WSPARCIE", "grupa_type": 2,
        "opis": "Zgłoszenia błędów AI / integracji forum",
    },
}

_FORUM_THREADS_TEST = {
    "AUTOS_KURIERZY": {
        "post_id": 5670, "korzen_id": 1464547,
        "grupa": "Sylwia", "grupa_type": 1,
        "opis": "TEST: Zlecenie kuriera/etykiety/atomówki",
    },
    "SPEDYCJA_REKLAMACJE": {
        "post_id": 5670, "korzen_id": 1464548,
        "grupa": "Sylwia", "grupa_type": 1,
        "opis": "TEST: Problemy po zleceniu kuriera",
    },
    "CZATOSZTUR_REKLAMACJE": {
        "post_id": 5670, "korzen_id": 1464549,
        "grupa": "Sylwia", "grupa_type": 1,
        "opis": "TEST: Reklamacja",
    },
    "NIEPOZAMYKANE_AUSTAUSCHE": {
        "post_id": 5670, "korzen_id": 1464550,
        "grupa": "Sylwia", "grupa_type": 1,
        "opis": "TEST: Niezamknięte Austausche",
    },
    "CZATOSZTUR_DE": {
        "post_id": 5670, "korzen_id": 1464551,
        "grupa": "Sylwia", "grupa_type": 1,
        "opis": "TEST: Czatosztur DE",
    },
    "CZATOSZTUR_FR": {
        "post_id": 5670, "korzen_id": 1464552,
        "grupa": "Sylwia", "grupa_type": 1,
        "opis": "TEST: Czatosztur FR",
    },
    "CZATOSZTUR_UKPL": {
        "post_id": 5670, "korzen_id": 1464553,
        "grupa": "Sylwia", "grupa_type": 1,
        "opis": "TEST: Czatosztur UKPL",
    },
    "KURIER_test": {
        "post_id": 5680, "korzen_id": "DIRECT",
        "grupa": "Sylwia", "grupa_type": 1,
        "opis": "TEST: Zlecenia kurierskie (nowy wątek)",
    },
    "REKLA_test": {
        "post_id": 5679, "korzen_id": "DIRECT",
        "grupa": "Sylwia", "grupa_type": 1,
        "opis": "TEST: Reklamacje / czy można szturchać (nowy wątek)",
    },
}

FORUM_THREADS = _FORUM_THREADS_TEST if FORUM_TEST_MODE else _FORUM_THREADS_PROD


def discover_roots():
    cached = st.session_state.get("_forum_roots", {})
    if cached:
        for key, kid in cached.items():
            if key in FORUM_THREADS:
                FORUM_THREADS[key]["korzen_id"] = kid
        return cached
    
    roots = {}
    for key, info in FORUM_THREADS.items():
        result = forum_read(root_id=info["post_id"], max_pages=1)
        if result["success"] and result["posts"]:
            for p in result["posts"]:
                if p["Do_Odpid"] == 0:
                    roots[key] = p["Id"]
                    info["korzen_id"] = p["Id"]
                    break
            if key not in roots and result["posts"]:
                roots[key] = result["posts"][0]["Id"]
                info["korzen_id"] = result["posts"][0]["Id"]
    
    st.session_state["_forum_roots"] = roots
    return roots


def get_thread_info(cel):
    """Pobierz info o wątku po celu (case-insensitive)."""
    info = _get_thread_raw(cel)
    if not info:
        return None
    if info.get("korzen_id") is None and cel not in ["KURIER_test", "REKLA_test"]:
        discover_roots()
    return info


def _get_thread_raw(cel):
    """Case-insensitive lookup w FORUM_THREADS — bez discover_roots()."""
    info = FORUM_THREADS.get(cel)
    if info:
        return info
    if not cel:
        return None
    # Case-insensitive fallback — gdyby AI wysłała 'czatosztur_fr' małymi
    for _k, _v in FORUM_THREADS.items():
        if _k.lower() == cel.lower():
            _flog(f"THREAD LOOKUP: case-insensitive match: '{cel}' → '{_k}'")
            return _v
    return None


def forum_write_to_thread(cel, tresc, user_do=None, do_odp_id=None, forum_memory=None, user_od=None, ai_user=None, tytul=None):
    info = get_thread_info(cel)
    if not info:
        _flog(f"WRITE_TO_THREAD: cel={cel} → NIEZNANY CEL")
        return {"success": False, "error": f"Nieznany cel: {cel}"}
    
    # === TYTUŁ = NUMER ZAMÓWIENIA z treści (zawsze) ===
    # AI w treści zawsze pisze "Zamówienie: 374593" — to wyciągamy jako tytuł.
    # Reszta (zlecenie kuriera, delegacja telefonu itp.) jest w treści wpisu.
    _nrzam_match = re.search(r'Zamówienie[:\s]+(\d{5,7})', tresc)
    tytul = _nrzam_match.group(1) if _nrzam_match else None
    
    _flog(f"WRITE_TO_THREAD: cel={cel}, tytul='{tytul}', do_odp_id={do_odp_id}, USE_NEW={USE_NEW_SUBTHREADS}, user_od={user_od}, ai_user={ai_user}")
    
    if do_odp_id:
        target_do_odp = do_odp_id
        _flog(f"  DECYZJA: explicit do_odp_id={do_odp_id}")
    elif forum_memory and cel in forum_memory:
        target_do_odp = forum_memory[cel].get("id")
        _flog(f"  DECYZJA: kontynuacja z forum_memory, target={target_do_odp}")
    elif USE_NEW_SUBTHREADS:
        target_do_odp = None
        _flog(f"  DECYZJA: NOWY PODWĄTEK (USE_NEW=True, do_odp_id=None)")
    else:
        target_do_odp = info.get("korzen_id")
        if target_do_odp == "DIRECT":
            target_do_odp = 0
            _flog(f"  DECYZJA: tryb DIRECT → nowy post w wątku (do_odp_id=0)")
        elif target_do_odp is not None:
            _flog(f"  DECYZJA: workaround korzen_id={target_do_odp}")
        else:
            target_do_odp = 0
            _flog(f"  DECYZJA: brak korzenia → nowy post w wątku (do_odp_id=0)")
    
    target_user = user_do or info.get("grupa", "EA")
    # "SELF" = wpis do własnej grupy piszącego (AUTOS_KURIERZY od v1.12 — §11.4.3).
    # Prompt normalnie podaje user_do jawnie; to fallback, gdyby go zabrakło.
    if target_user == "SELF":
        target_user = user_od or FORUM_USER
    target_type = info.get("grupa_type", 1) if not user_do else (1 if _is_individual_user(target_user) else 2)
    if not user_do:
        target_type = 1 if _is_individual_user(target_user) else 2
    
    tresc_with_disclaimer = tresc + CHATOSZTUREK_DISCLAIMER
    
    result = forum_write(
        post_id=info["post_id"],
        do_odp_id=target_do_odp,
        user_do=target_user,
        tresc=tresc_with_disclaimer,
        user_do_type=target_type,
        user_od=user_od,
        ai_user=ai_user,
        tytul=tytul,
    )
    
    if result.get("success"):
        result["FORUM_ID"] = result["new_post_id"]
    
    return result


def forum_read_by_forum_id(forum_id):
    result = forum_read(leaf_id=forum_id, max_pages=1)
    if result["success"] and result["posts"]:
        first = result["posts"][0]
        branch = first.get("Id") if first.get("Do_Odpid") == 0 else None
        if not branch:
            return result
    return result


CHATOSZTUREK_DISCLAIMER = (
    '<br><br>---<br>'
    '<b>Jestem Chatoszturkiem AI, asystentem działu zwrotów.</b> '
    'Jeśli ta wiadomość wymaga korekty — odpisz tutaj.'
)


# ==========================================
# PAMIĘĆ FORUMOWA (przetrwa czyszczenie casów)
# ==========================================

def save_forum_memory(db, col_fn, numer_zamowienia, cel, forum_id, co=""):
    from datetime import datetime
    import pytz
    tz_pl = pytz.timezone('Europe/Warsaw')
    data_str = datetime.now(tz_pl).strftime("%Y-%m-%d %H:%M")
    
    _flog(f"SAVE_MEMORY: nrzam={numer_zamowienia}, cel={cel}, forum_id={forum_id}")
    
    entry = {
        "id": forum_id,
        "data": data_str,
        "co": co[:100] if co else "",
        "new_subthread": USE_NEW_SUBTHREADS,
    }
    
    doc_ref = db.collection(col_fn("forum_memory")).document(str(numer_zamowienia))
    try:
        existing = doc_ref.get()
        if existing.exists:
            existing_posts = existing.to_dict().get("forum_posts", {})
            if cel in existing_posts:
                _flog(f"  → JUŻ ISTNIEJE (nie nadpisuję, pierwotny id={existing_posts[cel].get('id')})")
                return
        doc_ref.update({f"forum_posts.{cel}": entry})
        _flog(f"  → ZAPISANO (update)")
    except Exception:
        doc_ref.set({"forum_posts": {cel: entry}})
        _flog(f"  → ZAPISANO (set — nowy dokument)")


def load_forum_memory(db, col_fn, numer_zamowienia):
    try:
        doc = db.collection(col_fn("forum_memory")).document(str(numer_zamowienia)).get()
        if doc.exists:
            result = doc.to_dict().get("forum_posts", {})
            return result
    except Exception as e:
        _flog(f"LOAD_MEMORY BŁĄD: {e}")
    return {}


def auto_load_forum_context(db, col_fn, numer_zamowienia):
    _flog(f"AUTO_LOAD: start, nrzam={numer_zamowienia}")
    
    try:
        memory = load_forum_memory(db, col_fn, numer_zamowienia)
        
        if not memory:
            memory = _scan_forum_for_case(db, col_fn, str(numer_zamowienia))
        
        if not memory:
            _flog(f"AUTO_LOAD: scan też pusty → zwracam pusty kontekst")
            return ""
        
        context_parts = []
        for cel, info in memory.items():
            forum_id = info.get("id")
            if not forum_id:
                continue
            
            is_new_subthread = info.get("new_subthread", USE_NEW_SUBTHREADS)
            _flog(f"AUTO_LOAD: czytam {cel}, forum_id={forum_id}, new_sub={is_new_subthread}")
            
            thread_info = (_get_thread_raw(cel) or {})
            root_id = thread_info.get("post_id")
            
            # ZAWSZE filtruj po numerze zamówienia (nie ładuj całego wątku)
            if is_new_subthread:
                # Nowy podwątek — branch_id=forum_id, filtruj po nrzam
                result = forum_read_subtree(branch_id=forum_id, from_post_id=forum_id, nrzam=numer_zamowienia)
                if not result.get("success"):
                    # Fallback: czytaj jako leaf w korzeniu wątku z filtrem nrzam
                    result = forum_read_subtree(leaf_id=forum_id, root_id=root_id, from_post_id=forum_id, nrzam=numer_zamowienia)
            else:
                if thread_info and thread_info.get("korzen_id") and thread_info.get("korzen_id") != "DIRECT":
                    _flog(f"  → subtree: branch={thread_info['korzen_id']}, from={forum_id}")
                    result = forum_read_subtree(branch_id=thread_info["korzen_id"], from_post_id=forum_id, nrzam=numer_zamowienia)
                else:
                    _flog(f"  → subtree (brak korzenia/DIRECT): branch={forum_id}, from={forum_id}")
                    result = forum_read_subtree(branch_id=forum_id, from_post_id=forum_id, nrzam=numer_zamowienia)
                    
                    if not result.get("success"):
                         _flog(f"  → fallback leaf z filtrem: forum_id={forum_id}")
                         result = forum_read_subtree(leaf_id=forum_id, root_id=root_id, from_post_id=forum_id, nrzam=numer_zamowienia)
            
            _flog(f"  → wynik odczytu: success={result.get('success')}, postow={result.get('count', 0)}")
            co = info.get("co", cel)
            
            if result.get("success") and result.get("posts"):
                posts = result["posts"][-10:]
                
                human_replies = [p for p in posts if p.get("UserAddName") != FORUM_USER]
                
                if human_replies:
                    context_parts.append(f"[FORUM_CONTEXT: {cel}] ({co}, {result['count']} postów. Ostatnia odpowiedź od: {human_replies[-1].get('UserAddName')})")
                else:
                    context_parts.append(f"[FORUM_CONTEXT: {cel}] ({co}, brak nowych odpowiedzi)")
                
                for p in posts:
                    date_str = p['DateAdd'][:10] if p.get('DateAdd') else '?'
                    context_parts.append(
                        f"  [{date_str}] {p['UserAddName']} → {p['UserToName']}: "
                        f"{_strip_html(p['Text'][:400])}"
                    )
            else:
                err_msg = result.get("error", "API zwróciło pustą listę")
                _flog(f"  → UWAGA: błąd lub brak postów ({err_msg}). Dodaję bezpiecznik.")
                context_parts.append(f"[FORUM_CONTEXT: {cel}] ({co}, w pamięci istnieje wpis ID={forum_id}, ale odczyt nie znalazł odpowiedzi. Zakładam: brak nowych odpowiedzi.)")

        if context_parts:
            return "\n".join(context_parts)
        return ""
        
    except Exception as e:
        _flog(f"AUTO_LOAD BŁĄD KRYTYCZNY: {e}\n{traceback.format_exc()}")
        return ""


def check_forum_answer(db, col_fn, numer_zamowienia, cel_filter=None):
    """Moduł Telefony — czy pod wątkiem (delegacji) jest ODPOWIEDŹ CZŁOWIEKA (nie bota chatoszturek)?
    Używane przez nocny routing Wieżowca: jest odpowiedź telefonisty → standard; brak → woreczek.
    Reużywa pamięci forum + forum_read_subtree (jeden GetPostTree na cel — bez ciężkiego skanu).
    Decyzja per ustalenie użytkownika: liczy się KAŻDY wpis nie-bota pod wątkiem (UserAddName != FORUM_USER).
    cel_filter (opcjonalnie): zbiór/lista nazw cel — sprawdzaj tylko te wątki (np. TEL-owe).
    Zwraca: {answered: bool, last_author, last_date, last_text, cel}.
    """
    out = {"answered": False, "last_author": None, "last_date": None, "last_text": None, "cel": None}
    try:
        memory = load_forum_memory(db, col_fn, numer_zamowienia)
        if not memory:
            return out
        for cel, info in memory.items():
            if cel_filter and cel not in cel_filter:
                continue
            forum_id = info.get("id")
            if not forum_id:
                continue
            result = forum_read_subtree(branch_id=forum_id, from_post_id=forum_id, nrzam=numer_zamowienia)
            if not result.get("success"):
                # Fallback jak w auto_load_forum_context: czytaj korzeń wątku i filtruj po hierarchii+nrzam.
                # Bez tego dla części struktur podwątków gubimy realną odpowiedź telefonisty (fałszywe „brak").
                _root_id = (_get_thread_raw(cel) or {}).get("post_id")
                result = forum_read_subtree(leaf_id=forum_id, root_id=_root_id,
                                            from_post_id=forum_id, nrzam=numer_zamowienia)
            if not result.get("success") or not result.get("posts"):
                continue
            human = [p for p in result["posts"] if p.get("UserAddName") != FORUM_USER]
            if human:
                last = human[-1]
                out.update({
                    "answered": True,
                    "last_author": last.get("UserAddName"),
                    "last_date": (last.get("DateAdd") or "")[:16],
                    "last_text": _strip_html((last.get("Text") or "")[:200]),
                    "cel": cel,
                })
                return out
        return out
    except Exception as e:
        _flog(f"CHECK_FORUM_ANSWER BŁĄD: {e}")
        return out


def load_forum_context_by_id(db, col_fn, numer_zamowienia, cel, forum_id):
    _flog(f"LOAD_BY_ID: nrzam={numer_zamowienia}, cel={cel}, forum_id={forum_id}")

    thread_info = _get_thread_raw(cel)
    if thread_info and thread_info.get("korzen_id") and thread_info.get("korzen_id") != "DIRECT":
        result = forum_read_subtree(branch_id=thread_info["korzen_id"], from_post_id=forum_id, nrzam=numer_zamowienia)
    else:
        result = forum_read_subtree(branch_id=forum_id, from_post_id=forum_id, nrzam=numer_zamowienia)
        if not result.get("success"):
            result = forum_read_subtree(leaf_id=forum_id, root_id=thread_info.get("post_id"), from_post_id=forum_id, nrzam=numer_zamowienia)

    context_parts = []
    if result.get("success") and result.get("posts"):
        posts = result["posts"][-10:]
        context_parts.append(f"[FORUM_CONTEXT: {cel}] (wczytano po ID={forum_id}, {result['count']} postów)")
        for p in posts:
            date_str = p['DateAdd'][:10] if p.get('DateAdd') else '?'
            context_parts.append(
                f"  [{date_str}] {p['UserAddName']} → {p['UserToName']}: "
                f"{_strip_html(p['Text'][:400])}"
            )
        try:
            save_forum_memory(db, col_fn, numer_zamowienia, cel, forum_id, f"manual: {cel}")
        except Exception as e:
            _flog(f"  → błąd zapisu memory: {e}")
    else:
        context_parts.append(
            f"[FORUM_CONTEXT: {cel}] (wpis id={forum_id}, brak treści do odczytu — "
            f"NIE generuj FORUM_WRITE, czekaj na odpowiedź)"
        )
        try:
            save_forum_memory(db, col_fn, numer_zamowienia, cel, forum_id, f"manual_empty: {cel}")
        except Exception:
            pass

    return "\n".join(context_parts)


def _scan_forum_for_case(db, col_fn, numer_zamowienia):
    found = {}
    nrzam = str(numer_zamowienia)
    _flog(f"SCAN: szukam {nrzam} w wątkach forum")
    
    scanned_roots = set()
    for cel, info in FORUM_THREADS.items():
        post_id = info.get("post_id")
        if post_id in scanned_roots:
            continue
        scanned_roots.add(post_id)
        
        try:
            result = forum_read(root_id=post_id, max_pages=3)
            if not result.get("success") or not result.get("posts"):
                continue
            
            for post in result["posts"]:
                if post.get("UserAddName") != FORUM_USER:
                    continue
                text = post.get("Text", "")
                if nrzam not in text:
                    continue
                
                post_forum_id = post.get("Id")
                if not post_forum_id:
                    continue
                
                text_lower = text.lower()
                matched_cel = None
                for c, cinfo in FORUM_THREADS.items():
                    if cinfo.get("post_id") != post_id:
                        continue
                    if "AUTOS_KURIERZY" == c and ("kurier" in text_lower or "zlecenie kuri" in text_lower or "etykiet" in text_lower):
                        matched_cel = c
                        break
                    elif "CZATOSZTUR_" in c and ("delegacja" in text_lower or "telefon" in text_lower):
                        matched_cel = c
                        break
                    elif "SPEDYCJA" in c and ("spedycj" in text_lower or "reklamacj" in text_lower):
                        matched_cel = c
                        break
                    elif "NIEPOZAMYKANE" in c and ("austausch" in text_lower or "zielonk" in text_lower):
                        matched_cel = c
                        break
                
                if not matched_cel:
                    for c, cinfo in FORUM_THREADS.items():
                        if cinfo.get("post_id") == post_id:
                            matched_cel = c
                            break
                
                if matched_cel and matched_cel not in found:
                    is_root = post.get("Do_Odpid") == 0 or post.get("Level") == 0
                    found[matched_cel] = {
                        "id": post_forum_id,
                        "new_subthread": is_root,
                        "co": f"scan: {matched_cel}",
                    }
                    try:
                        save_forum_memory(db, col_fn, nrzam, matched_cel, post_forum_id, f"scan: {matched_cel}")
                    except Exception:
                        pass
        except Exception as e:
            _flog(f"SCAN ERROR: {e}")
            continue
    
    return found if found else None
