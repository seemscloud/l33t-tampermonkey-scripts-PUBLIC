#!/usr/bin/env python3
from __future__ import annotations

import argparse
import concurrent.futures
import threading
import sys
import time
from pathlib import Path
from typing import Any, Dict, Iterable, Optional, Tuple
from datetime import datetime
import ast
import requests
import urllib3
from requests.auth import HTTPBasicAuth


def _log(msg: str) -> None:
    sys.stderr.write(f"{msg.rstrip()}\n")


# HTTP defaults (non-configurable via CLI)
HTTP_TIMEOUT = 15
HTTP_CONNECT_TIMEOUT = 5
HTTP_RETRIES = 3
HTTP_RETRY_DELAY = 2


def _set_bearer(session: requests.Session, token: str, ctx: Dict[str, Any]) -> None:
    """Apply bearer token consistently to headers, cookies and context."""
    session.headers["Authorization"] = f"Bearer {token}"
    session.cookies.set("access_token", token, path="/")
    session.cookies.set("Authorization", f"Bearer {token}", path="/")
    # Disable Basic Auth once bearer is available to avoid overwriting Authorization.
    session.auth = None
    ctx["bearer"] = token
    # Airflow 3.x uses DAG source v2; prefer it once bearer exists.
    ctx["prefer_v2_dagsource"] = True


def build_session(
    username: str,
    password: str,
    base_url: str,
    http_timeout: int,
    http_connect_timeout: int,
    http_retries: int,
    http_retry_delay: int,
    verify_tls: bool,
) -> requests.Session:
    session = requests.Session()
    session.verify = verify_tls
    if not verify_tls:
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    # Basic Auth first; if server rejects with 401 we also try form login.
    session.auth = HTTPBasicAuth(username, password)
    session.headers.update({"Accept": "application/json"})
    session._airflow_login_ctx = {  # type: ignore[attr-defined]
        "base_url": base_url,
        "username": username,
        "password": password,
        "attempted_form_login": False,
        "attempted_api_login": False,
        "bearer": None,
        "csrf": None,
        "http_timeout": http_timeout,
        "http_connect_timeout": http_connect_timeout,
        "http_retries": http_retries,
        "http_retry_delay": http_retry_delay,
    }
    return session


def _normalize_base_path(base_path: str) -> str:
    """Ensure base path starts with / and has no trailing slash (except root)."""
    bp = (base_path or "").strip()
    if bp and not bp.startswith("/"):
        bp = f"/{bp}"
    return bp.rstrip("/")


def _probe_base_url(
    session: requests.Session,
    base_url: str,
    timeout: int,
    connect_timeout: Optional[int],
    retries: int,
    retry_delay: int,
) -> bool:
    """
    Quickly check if an Airflow API likely lives under base_url by probing
    a small set of endpoints. Treat 200/401/403 as success (path exists).
    404 means this base_url is probably wrong; other 4xx/5xx are ignored.
    At least one /api/* probe must succeed; /health alone is not sufficient.
    """
    probe_paths = ["/api/v1/health", "/api/v1/dags?limit=1", "/health"]
    api_probe_succeeded = False
    for path in probe_paths:
        url = f"{base_url.rstrip('/')}{path}"
        last_exc: Optional[Exception] = None
        for attempt in range(max(1, retries)):
            try:
                timeout_arg = (connect_timeout, timeout) if connect_timeout else timeout
                resp = session.get(url, timeout=timeout_arg, headers={"Accept": "application/json"})
            except requests.RequestException as exc:
                last_exc = exc
                _log(f"[probe] {url} attempt {attempt+1}/{retries} error: {exc}")
                if attempt < retries - 1:
                    time.sleep(retry_delay)
                    continue
                break

            _log(
                f"[probe] {url} attempt {attempt+1}/{retries} status={resp.status_code} "
                f"len={len(resp.text or '')}"
            )
            if resp.status_code in (200, 401, 403):
                if path.startswith("/api/"):
                    api_probe_succeeded = True
                    break
                # /health success is noted but not sufficient alone
                break
            if resp.status_code == 404:
                break
            if resp.status_code < 500:
                if path.startswith("/api/"):
                    api_probe_succeeded = True
                    break
                break
            if attempt < retries - 1:
                time.sleep(retry_delay)
        if last_exc:
            continue
    return api_probe_succeeded


def discover_base_url(
    session: requests.Session,
    scheme: str,
    host: str,
    port: int,
    user_base_path: str,
    timeout: int,
    connect_timeout: Optional[int],
    retries: int,
    retry_delay: int,
) -> Tuple[Optional[str], Optional[str], list[str]]:
    """
    Try a set of base path candidates to find a working Airflow API root.
    Returns (base_url, base_path_used, tried_paths_display).
    """
    candidates_raw = [
        user_base_path,
        "",
        "/airflow",
        "/airflow/api",
        "/api",
        "/api/v1/ui",
    ]
    candidates: list[str] = []
    for cand in candidates_raw:
        norm = _normalize_base_path(cand)
        if norm not in candidates:
            candidates.append(norm)

    tried_display: list[str] = []
    for cand in candidates:
        base_url = f"{scheme}://{host}:{port}{cand}"
        tried_display.append(cand or "/")
        _log(f"[probe] trying base_path='{cand or '/'}' -> {base_url}")
        if _probe_base_url(session, base_url, timeout, connect_timeout, retries, retry_delay):
            return base_url, cand, tried_display
    return None, None, tried_display


def _try_api_login(session: requests.Session) -> bool:
    """
    Try Airflow API login endpoint to obtain bearer token (JWT for Airflow 3.x, session auth for 2.x).
    """
    ctx = getattr(session, "_airflow_login_ctx", None)
    if not ctx or ctx.get("attempted_api_login"):
        return False
    ctx["attempted_api_login"] = True
    base_url = ctx.get("base_url") or ""
    username = ctx.get("username") or ""
    password = ctx.get("password") or ""
    if not base_url or not username or not password:
        return False

    # Airflow 3.x uses OAuth2PasswordBearer with /auth/token endpoint
    # Try this first as it's the new standard
    token_url = f"{base_url.rstrip('/')}/auth/token"
    try:
        payload = {"username": username, "password": password}
        resp = session.post(
            token_url,
            json=payload,
            timeout=15,
            allow_redirects=False,
            headers={"Accept": "application/json", "Content-Type": "application/json"},
        )
        _debug_response(resp, token_url)
        # Some deployments return 201 Created for token issuance; accept any 2xx.
        if 200 <= resp.status_code < 300:
            try:
                data = resp.json()
            except Exception:
                data = {}
            access_token = data.get("access_token") or data.get("token")
            if access_token:
                _set_bearer(session, access_token, ctx)
                sys.stderr.write(f"[http-debug] API login successful (Airflow 3.x), bearer token obtained\n")
                return True
    except requests.RequestException as exc:
        sys.stderr.write(f"[http-debug] /auth/token POST failed: {exc}\n")

    # Fallback: Try old Airflow 2.x endpoints for backward compatibility
    login_paths = ["/api/v1/security/login", "/api/v2/security/login"]
    for path in login_paths:
        url = f"{base_url.rstrip('/')}{path}"
        # Try GET with basic auth first
        try:
            resp = session.get(
                url,
                timeout=15,
                allow_redirects=False,
                headers={"Accept": "application/json"},
                auth=HTTPBasicAuth(username, password),
            )
        except requests.RequestException as exc:
            sys.stderr.write(f"[http-debug] api_login GET failed: {exc}\n")
            # Try POST as fallback
            try:
                payload = {"username": username, "password": password}
                resp = session.post(
                    url,
                    json=payload,
                    timeout=15,
                    allow_redirects=False,
                    headers={"Accept": "application/json"},
                )
            except requests.RequestException as exc2:
                sys.stderr.write(f"[http-debug] api_login POST failed: {exc2}\n")
                continue
        _debug_response(resp, url)
        if resp.status_code >= 400:
            continue
        try:
            data = resp.json()
        except Exception:
            data = {}
        access_token = data.get("access_token") or data.get("token")
        if access_token:
            _set_bearer(session, access_token, ctx)
            sys.stderr.write(f"[http-debug] API login successful (Airflow 2.x), bearer token obtained\n")
            return True
    return False


def _debug_response(resp: requests.Response, url: str) -> None:
    """
    Emit extended debug info about an HTTP response to stderr.
    """
    try:
        body = resp.text
    except Exception:
        body = "<unreadable body>"
    body_preview = body or ""
    headers_dict = dict(resp.headers) if resp.headers else {}
    sys.stderr.write(
        f"[http-debug] url={url} status={resp.status_code} "
        f"content_type={resp.headers.get('Content-Type','')} "
        f"content_length={len(body_preview)} "
        f"cookies={dict(resp.cookies)}\n"
    )
    if headers_dict:
        sys.stderr.write(f"[http-debug] headers: {headers_dict}\n")
    if body_preview:
        sys.stderr.write(f"[http-debug] body:\n{body_preview}\n")


def api_get(
    session: requests.Session,
    base_url: str,
    path: str,
    params: Optional[Dict[str, Any]] = None,
    timeout: Optional[int] = None,
    retries: Optional[int] = None,
    retry_delay: Optional[int] = None,
    connect_timeout: Optional[int] = None,
) -> requests.Response:
    """
    Perform GET against Airflow API.
    - Tries /api/v1 first (current default in this script).
    - If the server responds with Airflow 3 style 404 hinting that /api/v1 is removed,
      automatically retries once with /api/v2 to stay compatible with both versions.
    - If the server responds with 401 "Not authenticated", try API token login and retry once.
    - Retries up to `retries` times (default 5) with `retry_delay` seconds between attempts on
      network errors or HTTP 429/5xx responses.
    """
    ctx = getattr(session, "_airflow_login_ctx", {}) or {}
    bearer = ctx.get("bearer")
    timeout = timeout or ctx.get("http_timeout") or 30
    connect_timeout = connect_timeout or ctx.get("http_connect_timeout") or None
    retries = retries or ctx.get("http_retries") or 5
    retry_delay = retry_delay or ctx.get("http_retry_delay") or 3
    # If we have a bearer, ensure headers/cookies are aligned (Basic disabled).
    if bearer:
        _set_bearer(session, bearer, ctx)
    common_headers = {
        "Referer": base_url,
        "Origin": base_url,
        "Accept": "application/json",
    }
    if bearer:
        common_headers.setdefault("Authorization", f"Bearer {bearer}")
    url = f"{base_url.rstrip('/')}{path}"
    last_url = url
    retry_statuses = {429, 500, 502, 503, 504}

    def _get_with_retry(target_url: str) -> requests.Response:
        last_exc: Optional[Exception] = None
        for attempt in range(retries):
            try:
                timeout_arg = (connect_timeout, timeout) if connect_timeout else timeout
                response = session.get(target_url, params=params, timeout=timeout_arg, headers=common_headers)
            except requests.RequestException as exc:
                last_exc = exc
                _log(
                    f"[http-retry] GET {target_url} attempt {attempt+1}/{retries} "
                    f"failed: {exc}; retry in {retry_delay}s"
                )
                if attempt < retries - 1:
                    time.sleep(retry_delay)
                    continue
                raise RuntimeError(f"Request to {target_url} failed: {exc}") from exc

            if response.status_code in retry_statuses and attempt < retries - 1:
                _log(
                    f"[http-retry] GET {target_url} attempt {attempt+1}/{retries} "
                    f"status={response.status_code}; retry in {retry_delay}s"
                )
                time.sleep(retry_delay)
                continue

            return response

        # Should never reach here, but keep mypy happy.
        if last_exc:
            raise RuntimeError(f"Request to {target_url} failed: {last_exc}") from last_exc
        raise RuntimeError(f"Request to {target_url} failed after {retries} attempts")

    # Try with Basic Auth first if available (session.auth is set in build_session)
    resp = _get_with_retry(url)

    # Airflow 3 removes /api/v1 and returns a helpful 404 message; try /api/v2 automatically.
    if (
        resp.status_code == 404
        and "/api/v1 has been removed" in (resp.text or "")
        and "/api/v2" in (resp.text or "")
    ):
        v2_path = path.replace("/api/v1", "/api/v2", 1)
        if v2_path != path:
            url_v2 = f"{base_url.rstrip('/')}{v2_path}"
            resp = _get_with_retry(url_v2)
            last_url = url_v2
            # if still >=400 it will be handled below

    # Authentication recovery: if unauthorized, try API login then retry once.
    if resp.status_code == 401:
        if _try_api_login(session):
            # Refresh bearer/header state after login
            bearer = getattr(session, "_airflow_login_ctx", {}).get("bearer")
            if bearer:
                _set_bearer(session, bearer, ctx)
                common_headers.setdefault("Authorization", f"Bearer {bearer}")
            resp = _get_with_retry(last_url)
            sys.stderr.write(f"[http-debug] Retry after API login: status={resp.status_code}\n")

        # If still unauthorized on v1, try v2 explicitly after API login.
        if resp.status_code == 401 and "/api/v1/" in last_url:
            v2_url = last_url.replace("/api/v1/", "/api/v2/", 1)
            resp = _get_with_retry(v2_url)
            sys.stderr.write(f"[http-debug] Retry v2 after API login: status={resp.status_code}\n")
            last_url = v2_url

    if resp.status_code >= 400:
        _debug_response(resp, last_url)
        raise RuntimeError(
            f"Request to {last_url} failed with {resp.status_code}: {resp.text[:500]}"
        )
    return resp


def list_dags(
    session: requests.Session,
    base_url: str,
    only_active: bool = True,
    limit: int = 1000,
) -> Iterable[str]:
    resp = api_get(
        session,
        base_url,
        "/api/v1/dags",
        params={"only_active": str(only_active).lower(), "limit": limit},
    )
    data = resp.json()
    for dag in data.get("dags") or []:
        dag_id = dag.get("dag_id")
        if dag_id:
            yield dag_id


def get_latest_dag_run_id(
    session: requests.Session, base_url: str, dag_id: str
) -> str:
    resp = api_get(
        session,
        base_url,
        f"/api/v1/dags/{dag_id}/dagRuns",
        params={"order_by": "-execution_date", "limit": 1},
    )
    data = resp.json()
    runs = data.get("dag_runs") or []
    if not runs:
        raise RuntimeError(f"No dag runs found for {dag_id}")
    run = runs[0]
    dag_run_id = run.get("dag_run_id") or run.get("run_id")
    if not dag_run_id:
        raise RuntimeError(f"Unexpected dag run payload: {run}")
    return dag_run_id


def get_task_instances(
    session: requests.Session, base_url: str, dag_id: str, dag_run_id: str
) -> Iterable[Dict[str, Any]]:
    resp = api_get(
        session,
        base_url,
        f"/api/v1/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances",
        params={"limit": 1000},
    )
    data = resp.json()
    return data.get("task_instances") or []


def list_successful_dag_runs(
    session: requests.Session, base_url: str, dag_id: str, limit: int = 10
) -> list[Dict[str, Any]]:
    resp = api_get(
        session,
        base_url,
        f"/api/v1/dags/{dag_id}/dagRuns",
        params={"state": "success", "order_by": "-execution_date", "limit": limit},
    )
    data = resp.json()
    return data.get("dag_runs") or []


def list_tasks(
    session: requests.Session, base_url: str, dag_id: str
) -> Iterable[str]:
    resp = api_get(
        session,
        base_url,
        f"/api/v1/dags/{dag_id}/tasks",
        params=None,
    )
    data = resp.json()
    for task in data.get("tasks") or []:
        task_id = task.get("task_id")
        if task_id:
            yield task_id


def get_task_detail(
    session: requests.Session, base_url: str, dag_id: str, task_id: str
) -> Dict[str, Any]:
    resp = api_get(
        session,
        base_url,
        f"/api/v1/dags/{dag_id}/tasks/{task_id}",
        params=None,
    )
    return resp.json()


def get_dag_file_token(
    session: requests.Session, base_url: str, dag_id: str
) -> Optional[str]:
    resp = api_get(
        session,
        base_url,
        f"/api/v1/dags/{dag_id}",
        params=None,
    )
    data = resp.json()
    return data.get("file_token")


def pick_try_number(task_instance: Dict[str, Any]) -> int:
    # Prefer reported try_number; fallback to next_try_number - 1; else 1.
    try_number = task_instance.get("try_number")
    if isinstance(try_number, int) and try_number > 0:
        return try_number
    next_try = task_instance.get("next_try_number")
    if isinstance(next_try, int) and next_try > 0:
        return max(1, next_try - 1)
    return 1


def fetch_log_text(
    session: requests.Session,
    base_url: str,
    dag_id: str,
    dag_run_id: str,
    task_id: str,
    try_number: int,
) -> str:
    def _normalize_line(val: Any) -> str:
        # Convert to string and unescape common newline sequences
        s = str(val)
        return s.replace("\\r\\n", "\n").replace("\\n", "\n").replace("\\r", "\n")

    resp = api_get(
        session,
        base_url,
        f"/api/v1/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/logs/{try_number}",
        params={"full_content": "true"},
    )
    content_type = resp.headers.get("Content-Type", "")
    if "application/json" in content_type:
        try:
            payload = resp.json()
        except ValueError:
            return resp.text
        content = payload.get("content")
        if isinstance(content, list):
            # Handle lists of strings or tuples/lists; join cleanly with newlines
            parts = []
            for part in content:
                if isinstance(part, (list, tuple)) and part:
                    candidate = part[-1]
                    parts.append(_normalize_line(candidate))
                else:
                    parts.append(_normalize_line(part))
            return "\n".join(parts)
        if isinstance(content, str):
            return _normalize_line(content)
        return _normalize_line(content)
    # Fallback: try to parse Python-like list/tuple repr (Airflow sometimes returns that)
    try:
        parsed = ast.literal_eval(resp.text)
        if isinstance(parsed, list):
            parts = []
            for part in parsed:
                if isinstance(part, (list, tuple)) and part:
                    candidate = part[-1]
                    parts.append(_normalize_line(candidate))
                else:
                    parts.append(_normalize_line(part))
            return "\n".join(parts)
    except Exception:
        pass
    return _normalize_line(resp.text)


def sanitize_name(name: str) -> str:
    return "".join(c if c.isalnum() or c in ("-", "_", ".") else "_" for c in name)


def save_code(text: str, output_dir: Path, dag_id: str, task_id: str) -> Path:
    base_code_root = Path(__file__).resolve().parent / "code"
    code_dir = base_code_root / sanitize_name(dag_id)
    code_dir.mkdir(parents=True, exist_ok=True)
    path = code_dir / f"{sanitize_name(task_id)}.py"
    path.write_text(text)
    return path


def fetch_dag_source(
    session: requests.Session,
    base_url: str,
    dag_id: str,
    dag_source_cache: Dict[str, str],
    dag_source_unavailable: set[str],
    dag_source_cache_lock: threading.Lock,
    dag_source_unavailable_lock: threading.Lock,
) -> Optional[str]:
    with dag_source_cache_lock:
        if dag_id in dag_source_cache:
            cached = dag_source_cache[dag_id]
            return cached or None
    resp: Optional[requests.Response] = None
    ctx = getattr(session, "_airflow_login_ctx", {}) or {}

    # Airflow 3.x exposes /api/v2/dagSources/{dag_id}
    try:
        resp = api_get(session, base_url, f"/api/v2/dagSources/{dag_id}", params=None)
        ts = datetime.now().isoformat()
        print(f'{{"timestamp":"{ts}","dag":"{dag_id}","action":"fetch_dag_source_v2","status_code":{resp.status_code}}}')
        ctx["prefer_v2_dagsource"] = True
    except Exception as exc_v2:  # noqa: BLE001
        # Fall back to legacy v1-style endpoint if v2 is missing
        resp = None
        err_txt = str(exc_v2)
        if "dag with id" in err_txt.lower():
            # DAG truly missing; mark unavailable and stop further attempts for this dag_id
            with dag_source_cache_lock:
                dag_source_cache[dag_id] = ""
            with dag_source_unavailable_lock:
                dag_source_unavailable.add(dag_id)
            return None
        if "404" not in err_txt and "API route not found" not in err_txt:
            _log(f"[{dag_id}] v2 dagSource fetch failed ({exc_v2}); trying v1 endpoint")
    if resp is None:
        try:
            resp = api_get(session, base_url, f"/api/v1/dags/{dag_id}/dagSource", params=None)
            ts = datetime.now().isoformat()
            print(f'{{"timestamp":"{ts}","dag":"{dag_id}","action":"fetch_dag_source_v1","status_code":{resp.status_code}}}')
            ctx["prefer_v2_dagsource"] = ctx.get("prefer_v2_dagsource", False)
        except Exception as exc:  # noqa: BLE001
            _log(f"[{dag_id}] Failed to fetch DAG source: {exc}")
            with dag_source_cache_lock:
                dag_source_cache[dag_id] = ""
            with dag_source_unavailable_lock:
                dag_source_unavailable.add(dag_id)
            return None

    content_type = resp.headers.get("Content-Type", "")
    text: Optional[str]
    if "application/json" in content_type:
        try:
            payload = resp.json()
        except ValueError:
            payload = {}
        text = payload.get("content") or resp.text
    else:
        text = resp.text

    with dag_source_cache_lock:
        dag_source_cache[dag_id] = text or ""
    if not text:
        with dag_source_unavailable_lock:
            dag_source_unavailable.add(dag_id)
    return text


def fetch_task_code(
    session: requests.Session,
    base_url: str,
    dag_id: str,
    task_id: str,
    file_cache: Dict[str, str],
    dag_source_cache: Dict[str, str],
    dag_source_unavailable: set[str],
    dag_file_token_cache: Dict[str, str],
    file_cache_lock: threading.Lock,
    dag_source_cache_lock: threading.Lock,
    dag_source_unavailable_lock: threading.Lock,
    dag_file_token_cache_lock: threading.Lock,
) -> Optional[Tuple[str, Optional[str]]]:
    ctx = getattr(session, "_airflow_login_ctx", {}) or {}
    # On Airflow 3.x prefer the dagSources v2 endpoint (per DAG) before token-based fallbacks.
    if ctx.get("prefer_v2_dagsource"):
        dag_src_pref = fetch_dag_source(
            session,
            base_url,
            dag_id,
            dag_source_cache,
            dag_source_unavailable,
            dag_source_cache_lock,
            dag_source_unavailable_lock,
        )
        if dag_src_pref:
            return dag_src_pref, f"dag_source:{dag_id}"
        with dag_source_unavailable_lock:
            if dag_id in dag_source_unavailable:
                # DAG missing or dagSource unavailable; skip further attempts for this DAG
                return None

    try:
        detail = get_task_detail(session, base_url, dag_id, task_id)
    except Exception as exc:  # noqa: BLE001
        msg = str(exc)
        if "dag with id" in msg.lower() or ("404" in msg and "not found" in msg.lower()):
            with dag_source_unavailable_lock:
                dag_source_unavailable.add(dag_id)
            return None
        _log(f"[{dag_id}] Failed to get task detail for {task_id}: {exc}")
        return None

    file_token = detail.get("file_token")
    if file_token:
        with file_cache_lock:
            if file_token in file_cache:
                return file_cache[file_token], None
        try:
            resp = api_get(session, base_url, f"/api/v1/dagSources/{file_token}", params=None)
            ts = datetime.now().isoformat()
            print(f'{{"timestamp":"{ts}","dag":"{dag_id}","action":"fetch_dag_file_token","status_code":{resp.status_code}}}')
        except Exception as exc:  # noqa: BLE001
            _log(f"[{dag_id}] Failed to fetch source for {task_id} via file_token: {exc}")
            return None

        content_type = resp.headers.get("Content-Type", "")
        if "application/json" in content_type:
            try:
                payload = resp.json()
            except ValueError:
                payload = {}
            text = payload.get("content") or resp.text
        else:
            text = resp.text

        with file_cache_lock:
            file_cache[file_token] = text
        return text, None

    # Fallback 1: use DAG-level file_token (same code file for all tasks)
    dag_file_token: Optional[str] = None
    with dag_file_token_cache_lock:
        dag_file_token = dag_file_token_cache.get(dag_id)
    if dag_file_token is None:
        try:
            dag_file_token = get_dag_file_token(session, base_url, dag_id) or ""
        except Exception as exc:  # noqa: BLE001
            sys.stderr.write(f"[{dag_id}] Failed to get DAG file_token: {exc}\n")
            dag_file_token = ""
        with dag_file_token_cache_lock:
            dag_file_token_cache[dag_id] = dag_file_token

    if dag_file_token:
        with file_cache_lock:
            if dag_file_token in file_cache:
                return file_cache[dag_file_token], f"dag_file_token:{dag_file_token}"
        try:
            resp = api_get(session, base_url, f"/api/v1/dagSources/{dag_file_token}", params=None)
            ts = datetime.now().isoformat()
            print(f'{{"timestamp":"{ts}","dag":"{dag_id}","action":"fetch_dag_file_token_shared","status_code":{resp.status_code}}}')
        except Exception as exc:  # noqa: BLE001
            _log(f"[{dag_id}] Failed to fetch DAG source via file_token: {exc}")
            resp = None
        if resp:
            content_type = resp.headers.get("Content-Type", "")
            if "application/json" in content_type:
                try:
                    payload = resp.json()
                except ValueError:
                    payload = {}
                text = payload.get("content") or resp.text
            else:
                text = resp.text

            with file_cache_lock:
                file_cache[dag_file_token] = text
            return text, f"dag_file_token:{dag_file_token}"

    # Fallback 2: fetch full DAG source via dagSource endpoint
    dag_src = fetch_dag_source(
        session,
        base_url,
        dag_id,
        dag_source_cache,
        dag_source_unavailable,
        dag_source_cache_lock,
        dag_source_unavailable_lock,
    )
    if dag_src:
        return dag_src, f"dag_source:{dag_id}"

    with dag_source_unavailable_lock:
        unavailable = dag_id in dag_source_unavailable
    if unavailable:
        return None

    hint = " (dagSource unavailable)"
    _log(f"[{dag_id}] No file_token for task {task_id}; skipping code fetch{hint}.")
    return None


def save_log(text: str, output_dir: Path, dag_id: str, dag_run_id: str, task_id: str) -> Path:
    dag_dir = output_dir / sanitize_name(dag_id) / sanitize_name(dag_run_id)
    dag_dir.mkdir(parents=True, exist_ok=True)
    path = dag_dir / f"{sanitize_name(task_id)}.json"
    path.write_text(text)
    return path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Download Airflow logs and/or code for DAGs.",
        add_help=False,
    )
    parser.add_argument(
        "--help", "-?",
        action="help",
        help="Show this help message and exit",
    )
    parser.add_argument(
        "-h", "--host",
        default="127.0.0.1",
        help="Airflow host (default: 127.0.0.1)",
    )
    parser.add_argument(
        "-p", "--port",
        type=int,
        default=8080,
        help="Airflow port (default: 8080)",
    )
    parser.add_argument(
        "--base-path",
        default="",
        help="Optional URL path prefix (e.g. /airflow) if Airflow is served behind a subpath",
    )
    parser.add_argument(
        "--tls",
        action="store_true",
        help="Use HTTPS instead of HTTP when contacting Airflow",
    )
    # TLS verification is always disabled (developer mode). No flags to enable.
    parser.add_argument(
        "-U", "--username",
        default="airflow",
        help="Airflow username (default: airflow)",
    )
    parser.add_argument(
        "-P", "--password",
        default="airflow",
        help="Airflow password (default: airflow)",
    )
    parser.add_argument(
        "--source",
        action="store_true",
        help="Fetch DAG source code into code/<dag>/source.py",
    )
    parser.add_argument(
        "--logs",
        action="store_true",
        help="Fetch logs for the latest DAG run (per DAG).",
    )
    parser.add_argument(
        "-c",
        "--concurrency",
        type=int,
        default=50,
        help="Number of worker threads for fetching logs/code (default: %(default)s)",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if not args.source and not args.logs:
        sys.stderr.write("Specify at least one of --source or --logs.\n")
        return 1

    scheme = "https" if args.tls else "http"
    base_path_user = _normalize_base_path(args.base_path or "")
    initial_base_url = f"{scheme}://{args.host}:{args.port}{base_path_user}"
    username = args.username
    password = args.password
    default_output = None

    logs_enabled = bool(args.logs)
    code_enabled = bool(args.source)

    # TLS verification is always disabled (developer mode).
    verify_tls = False

    _log(
        f"[startup] init scheme={'https' if args.tls else 'http'} host={args.host} port={args.port} "
        f"base_path_user='{base_path_user or '/'}' verify_tls={verify_tls} "
        f"logs={logs_enabled} code={code_enabled} concurrency={args.concurrency} "
        f"http_timeout={HTTP_TIMEOUT}s connect_timeout={HTTP_CONNECT_TIMEOUT}s "
        f"http_retries={HTTP_RETRIES} retry_delay={HTTP_RETRY_DELAY}s"
    )
    session = build_session(
        username,
        password,
        initial_base_url,
        HTTP_TIMEOUT,
        HTTP_CONNECT_TIMEOUT,
        HTTP_RETRIES,
        HTTP_RETRY_DELAY,
        verify_tls=verify_tls,
    )

    base_url_detected, base_path_detected, tried_paths = discover_base_url(
        session,
        scheme,
        args.host,
        args.port,
        base_path_user,
        HTTP_TIMEOUT,
        HTTP_CONNECT_TIMEOUT,
        HTTP_RETRIES,
        HTTP_RETRY_DELAY,
    )
    if base_url_detected is None or base_path_detected is None:
        _log(f"Failed to find Airflow API base path. Tried: {', '.join(tried_paths)}")
        return 1
    if base_path_detected != base_path_user:
        _log(
            f"[startup] auto-selected base_path='{base_path_detected or '/'}' "
            f"(candidates tried: {', '.join(tried_paths)})"
        )
    base_path = base_path_detected
    base_url = base_url_detected
    # Update session context with the final base_url for downstream calls/login retries.
    ctx = getattr(session, "_airflow_login_ctx", None)
    if isinstance(ctx, dict):
        ctx["base_url"] = base_url

    _log(
        f"[startup] base_url set to {base_url} (base_path='{base_path or '/'}'); "
        f"timeout={HTTP_TIMEOUT}s connect_timeout={HTTP_CONNECT_TIMEOUT}s "
        f"retries={HTTP_RETRIES} retry_delay={HTTP_RETRY_DELAY}s "
        f"concurrency={args.concurrency} logs={logs_enabled} code={code_enabled}"
    )

    try:
        _log("[dags] listing DAGs…")
        dag_ids = list(list_dags(session, base_url))
        _log(f"[dags] found {len(dag_ids)} DAGs")
    except Exception as exc:  # noqa: BLE001
        _log(f"Failed to list DAGs: {exc}")
        return 1
    if not dag_ids:
        sys.stderr.write("No DAGs found.\n")
        return 1

    output_dir = Path(default_output) if default_output else Path(__file__).resolve().parent / "logs"
    total_saved_logs = 0
    total_saved_code = 0
    file_cache: Dict[str, str] = {}
    dag_source_cache: Dict[str, str] = {}
    dag_source_unavailable: set[str] = set()
    dag_file_token_cache: Dict[str, str] = {}
    shared_code_written: Dict[str, str] = {}
    file_cache_lock = threading.Lock()
    dag_source_cache_lock = threading.Lock()
    dag_source_unavailable_lock = threading.Lock()
    dag_file_token_cache_lock = threading.Lock()
    shared_code_written_lock = threading.Lock()
    max_workers = max(1, args.concurrency)
    executor_logs: Optional[concurrent.futures.ThreadPoolExecutor] = (
        concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) if logs_enabled else None
    )
    futures_logs: list[concurrent.futures.Future[Optional[str]]] = []
    futures_logs_lock = threading.Lock()
    executor_code: Optional[concurrent.futures.ThreadPoolExecutor] = (
        concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) if code_enabled else None
    )
    futures_code: list[concurrent.futures.Future[Optional[str]]] = []
    futures_code_lock = threading.Lock()

    dag_workers = max(1, min(len(dag_ids), max_workers))
    with concurrent.futures.ThreadPoolExecutor(max_workers=dag_workers) as dag_executor:
        def _process_dag(dag_id: str) -> None:
            task_ids_for_code: Optional[list[str]] = None

            if logs_enabled and executor_logs:
                try:
                    dag_runs = list_successful_dag_runs(session, base_url, dag_id, limit=10)
                except Exception as exc:  # noqa: BLE001
                    _log(f"[{dag_id}] Failed to list successful DAG runs: {exc}")
                    dag_runs = []

                if not dag_runs:
                    _log(f"[{dag_id}] No successful DAG runs found.")

                for dag_run in dag_runs:
                    dag_run_id = dag_run.get("dag_run_id") or dag_run.get("run_id")
                    if not dag_run_id:
                        continue
                    try:
                        task_instances = list(
                            get_task_instances(session, base_url, dag_id, dag_run_id)
                        )
                    except Exception as exc:  # noqa: BLE001
                        _log(f"[{dag_id}] Failed to list task instances for {dag_run_id}: {exc}")
                        task_instances = []

                    if task_instances:
                        def _log_worker(dag_id_local: str, dag_run_id_local: str, ti: Dict[str, Any]) -> Optional[str]:
                            task_id_local = ti.get("task_id") or "unknown_task"
                            try_number_local = pick_try_number(ti)
                            try:
                                log_text_local = fetch_log_text(
                                    session,
                                    base_url,
                                    dag_id_local,
                                    dag_run_id_local,
                                    task_id_local,
                                    try_number_local,
                                )
                                path_local = save_log(
                                    log_text_local, output_dir, dag_id_local, dag_run_id_local, task_id_local
                                )
                                ts_log = datetime.now().isoformat()
                                rel_path_log = path_local
                                try:
                                    rel_path_log = str(
                                        Path(path_local).relative_to(Path(__file__).resolve().parent)
                                    )
                                except ValueError:
                                    rel_path_log = path_local
                                print(
                                    f'{{"timestamp":"{ts_log}","dag":"{dag_id_local}","dag_run_id":"{dag_run_id_local}","path":"{rel_path_log}","action":"save_log"}}'
                                )
                                return str(path_local)
                            except Exception as exc:  # noqa: BLE001
                                sys.stderr.write(f"[{dag_id_local}] Failed to fetch log for {task_id_local} (run {dag_run_id_local}): {exc}\n")
                                return None

                        for ti in task_instances:
                            with futures_logs_lock:
                                futures_logs.append(executor_logs.submit(_log_worker, dag_id, dag_run_id, ti))
                    # Reuse last task ids for code if present (from most recent run in the loop)
                    task_ids_for_code = [ti.get("task_id") or "unknown_task" for ti in task_instances]

            if code_enabled and executor_code:
                _log(f"[{dag_id}] fetching code (logs_enabled={logs_enabled})")
                if task_ids_for_code is None:
                    try:
                        task_ids_for_code = list(list_tasks(session, base_url, dag_id))
                    except Exception as exc:  # noqa: BLE001
                        _log(f"[{dag_id}] Failed to list tasks for code fetch: {exc}")
                        task_ids_for_code = []

                def _code_worker(dag_id_local: str, task_id_local: str) -> Optional[str]:
                    try:
                        code_res = fetch_task_code(
                            session,
                            base_url,
                            dag_id_local,
                            task_id_local,
                            file_cache,
                            dag_source_cache,
                            dag_source_unavailable,
                            dag_file_token_cache,
                            file_cache_lock,
                            dag_source_cache_lock,
                            dag_source_unavailable_lock,
                            dag_file_token_cache_lock,
                        )
                    except Exception as exc:  # noqa: BLE001
                        sys.stderr.write(f"[{dag_id_local}] Failed to fetch code for {task_id_local}: {exc}\n")
                        return None
                    if code_res is None:
                        return None
                    code_text, _shared_key = code_res
                    ts = datetime.now().isoformat()
                    rel_path = f"code/{sanitize_name(dag_id_local)}/source.py"
                    with shared_code_written_lock:
                        already_written = shared_code_written.get(dag_id_local, False)
                    if already_written:
                        _log(f"[{dag_id_local}] code already written; skipping duplicate save.")
                        return None

                    # Save single code file per DAG: code/<dag>/source.py
                    code_path = save_code(code_text, output_dir, dag_id_local, "source")
                    with shared_code_written_lock:
                        shared_code_written[dag_id_local] = True
                    _log(f"[{dag_id_local}] code saved to {code_path}")
                    print(
                        f'{{"timestamp":"{ts}","dag":"{dag_id_local}","path":"{rel_path}","action":"save"}}'
                    )
                    return str(code_path)

                # Submit only one code fetch per DAG to avoid redundant saves.
                first_tid = (task_ids_for_code or ["__dag_source__"])[0]
                with shared_code_written_lock:
                    if shared_code_written.get(dag_id):
                        first_tid = None
                if first_tid is not None:
                    with futures_code_lock:
                        futures_code.append(executor_code.submit(_code_worker, dag_id, first_tid))

        for dag_id in dag_ids:
            dag_executor.submit(_process_dag, dag_id)

    if executor_logs:
        for fut in concurrent.futures.as_completed(futures_logs):
            path_res = fut.result()
            if path_res:
                total_saved_logs += 1
        executor_logs.shutdown(wait=True)

    if executor_code:
        for fut in concurrent.futures.as_completed(futures_code):
            code_path_res = fut.result()
            if code_path_res:
                total_saved_code += 1
        executor_code.shutdown(wait=True)

    return 0 if (total_saved_logs or total_saved_code) else 1


if __name__ == "__main__":
    raise SystemExit(main())

