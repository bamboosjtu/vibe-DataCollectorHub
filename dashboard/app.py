"""
Streamlit Dashboard for Data Collector Hub v1.0

A simple dashboard for viewing and managing:
- Plugin status
- Raw data
- Normalized data
- Task statistics
- Logs
- Runtime plugin configuration

Usage:
    streamlit run dashboard/app.py
"""

import json
import sqlite3
import sys
from pathlib import Path
from urllib import error, request

import pandas as pd
import streamlit as st

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from core.plugin_config_validator import validate_plugin_runtime_config
from health.dataset_health import get_context_coverage, get_daily_meeting_date_health, get_dataset_health
from health.domain_health import get_domain_health
from health.job_health import get_job_health
from health.summary import get_health_summary
from storage.sqlite_store import SQLiteStore

# Page config
st.set_page_config(
    page_title="Data Collector Hub v1.0",
    page_icon="📊",
    layout="wide"
)

# Database path
DB_PATH = Path(__file__).parent.parent / "data" / "collector.db"


def get_connection():
    """Create a new database connection for each call (thread-safe)"""
    # Don't use PARSE_DECLTYPES to avoid timestamp parsing issues
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


@st.cache_data(ttl=5)
def get_plugin_list():
    """Get list of plugins (cached)"""
    conn = get_connection()
    try:
        cursor = conn.execute("SELECT * FROM plugins")
        plugins = []
        for row in cursor.fetchall():
            plugin = dict(row)
            plugin['config'] = json.loads(plugin['config']) if plugin['config'] else {}
            plugin['dependencies'] = json.loads(plugin['dependencies']) if plugin['dependencies'] else []

            # Get tags
            tag_cursor = conn.execute("SELECT tag FROM plugin_tags WHERE plugin_id = ?", (plugin['id'],))
            plugin['tags'] = [r['tag'] for r in tag_cursor.fetchall()]
            plugins.append(plugin)
        return plugins
    finally:
        conn.close()


@st.cache_data(ttl=5)
def get_plugin_runtime_config(plugin_id):
    """Get plugin runtime config through the shared store layer."""
    runtime = SQLiteStore(DB_PATH).get_plugin_runtime_config(plugin_id)
    return runtime["config"]


def save_plugin_runtime_config(plugin_id, config):
    """Save plugin runtime config through the shared store layer."""
    SQLiteStore(DB_PATH).save_plugin_runtime_config(plugin_id, config)


def _api_json(method: str, path: str, payload: dict | None = None) -> tuple[int, dict]:
    base_url = st.session_state.get("api_base_url", "http://127.0.0.1:8000").rstrip("/")
    body = None
    headers = {"Content-Type": "application/json"}
    if payload is not None:
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    req = request.Request(f"{base_url}{path}", data=body, method=method, headers=headers)
    try:
        with request.urlopen(req, timeout=30) as resp:
            text = resp.read().decode("utf-8")
            return resp.status, json.loads(text) if text else {}
    except error.HTTPError as exc:
        text = exc.read().decode("utf-8")
        try:
            return exc.code, json.loads(text) if text else {}
        except json.JSONDecodeError:
            return exc.code, {"detail": text}
    except Exception as exc:
        return 0, {"detail": str(exc)}


@st.cache_data(ttl=5)
def get_counts():
    """Get table counts (cached)"""
    conn = get_connection()
    try:
        result = {}
        cursor = conn.execute("SELECT COUNT(*) as count FROM plugins")
        result['plugins'] = cursor.fetchone()['count']

        cursor = conn.execute("SELECT COUNT(*) as count FROM raw_data")
        result['raw_data'] = cursor.fetchone()['count']

        cursor = conn.execute("SELECT COUNT(*) as count FROM raw_events")
        result['raw_events'] = cursor.fetchone()['count']

        cursor = conn.execute("SELECT COUNT(*) as count FROM normalized_data")
        result['normalized_data'] = cursor.fetchone()['count']

        cursor = conn.execute("SELECT COUNT(*) as count FROM logs")
        result['logs'] = cursor.fetchone()['count']

        return result
    finally:
        conn.close()


@st.cache_data(ttl=5)
def get_task_stats():
    """Get task statistics (cached)"""
    conn = get_connection()
    try:
        cursor = conn.execute("""
            SELECT plugin_id, run_count, fail_count, last_run, consecutive_fails
            FROM task_stats
            ORDER BY last_run DESC
            LIMIT 10
        """)
        return [dict(row) for row in cursor.fetchall()]
    finally:
        conn.close()


@st.cache_data(ttl=5)
def get_raw_data(plugin_filter, limit):
    """Get raw data (cached)"""
    conn = get_connection()
    try:
        if plugin_filter == "All":
            cursor = conn.execute("""
                SELECT id, plugin_id, source, data, created_at
                FROM raw_data
                ORDER BY id DESC
                LIMIT ?
            """, (limit,))
        else:
            cursor = conn.execute("""
                SELECT id, plugin_id, source, data, created_at
                FROM raw_data
                WHERE plugin_id = ?
                ORDER BY id DESC
                LIMIT ?
            """, (plugin_filter, limit))
        return [dict(row) for row in cursor.fetchall()]
    finally:
        conn.close()


@st.cache_data(ttl=5)
def get_raw_events(dataset_filter, limit):
    """Get MVP raw_event rows (cached)."""
    conn = get_connection()
    try:
        if dataset_filter == "All":
            cursor = conn.execute("""
                SELECT id, dataset_key, collection, page_name, api_name, source_file,
                       occurred_at, collected_at, source_system, source_record_id,
                       source_record_hash, source_record_key, raw_event_key
                FROM raw_events
                ORDER BY id DESC
                LIMIT ?
            """, (limit,))
        else:
            cursor = conn.execute("""
                SELECT id, dataset_key, collection, page_name, api_name, source_file,
                       occurred_at, collected_at, source_system, source_record_id,
                       source_record_hash, source_record_key, raw_event_key
                FROM raw_events
                WHERE dataset_key = ?
                ORDER BY id DESC
                LIMIT ?
            """, (dataset_filter, limit))
        return [dict(row) for row in cursor.fetchall()]
    finally:
        conn.close()


@st.cache_data(ttl=5)
def get_raw_event_dataset_keys():
    """Get distinct raw event dataset keys."""
    conn = get_connection()
    try:
        cursor = conn.execute(
            "SELECT DISTINCT dataset_key FROM raw_events WHERE dataset_key IS NOT NULL ORDER BY dataset_key"
        )
        return [row["dataset_key"] for row in cursor.fetchall()]
    finally:
        conn.close()


@st.cache_data(ttl=5)
def get_normalized_data(plugin_filter, event_type_filter, limit):
    """Get normalized data (cached)"""
    conn = get_connection()
    try:
        query = """
            SELECT id, raw_data_id, plugin_id, event_type, event_source,
                   entity, event_timestamp, unique_key, payload, confidence, created_at
            FROM normalized_data
            WHERE 1=1
        """
        params = []

        if plugin_filter != "All":
            query += " AND plugin_id = ?"
            params.append(plugin_filter)

        if event_type_filter != "All":
            query += " AND event_type = ?"
            params.append(event_type_filter)

        query += " ORDER BY id DESC LIMIT ?"
        params.append(limit)

        cursor = conn.execute(query, params)
        return [dict(row) for row in cursor.fetchall()]
    finally:
        conn.close()


@st.cache_data(ttl=5)
def get_logs(level_filter, plugin_filter, limit):
    """Get logs (cached)"""
    conn = get_connection()
    try:
        query = """
            SELECT id, plugin_id, level, message, details, created_at
            FROM logs
            WHERE 1=1
        """
        params = []

        if level_filter != "All":
            query += " AND level = ?"
            params.append(level_filter)

        if plugin_filter != "All":
            query += " AND plugin_id = ?"
            params.append(plugin_filter)

        query += " ORDER BY id DESC LIMIT ?"
        params.append(limit)

        cursor = conn.execute(query, params)
        return [dict(row) for row in cursor.fetchall()]
    finally:
        conn.close()


@st.cache_data(ttl=5)
def get_statistics():
    """Get all task statistics (cached)"""
    conn = get_connection()
    try:
        cursor = conn.execute("""
            SELECT plugin_id, run_count, fail_count, last_run, last_fail, consecutive_fails
            FROM task_stats
            ORDER BY last_run DESC
        """)
        return [dict(row) for row in cursor.fetchall()]
    finally:
        conn.close()


@st.cache_data(ttl=5)
def get_event_types():
    """Get distinct event types (cached)"""
    conn = get_connection()
    try:
        cursor = conn.execute(
            "SELECT DISTINCT event_type FROM normalized_data WHERE event_type IS NOT NULL"
        )
        return [row['event_type'] for row in cursor.fetchall()]
    finally:
        conn.close()


@st.cache_data(ttl=5)
def get_collection_job_summary(plugin_id: str):
    conn = get_connection()
    try:
        where = ""
        params: list[object] = []
        if plugin_id != "All":
            where = "WHERE plugin_id = ?"
            params.append(plugin_id)
        cursor = conn.execute(
            f"""
            SELECT
                SUM(CASE WHEN status = 'queued' THEN 1 ELSE 0 END) AS queued,
                SUM(CASE WHEN status = 'running' THEN 1 ELSE 0 END) AS running,
                SUM(CASE WHEN status = 'succeeded' THEN 1 ELSE 0 END) AS succeeded,
                SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) AS failed,
                MAX(created_at) AS last_job_at
            FROM external_collection_jobs
            {where}
            """,
            params,
        )
        row = cursor.fetchone()
        return dict(row) if row else {}
    finally:
        conn.close()


@st.cache_data(ttl=5)
def get_collection_jobs(plugin_id: str, status_filter: str, profile_filter: str, limit: int):
    jobs = SQLiteStore(DB_PATH).list_external_collection_jobs(
        plugin_id=None if plugin_id == "All" else plugin_id,
        status=None if status_filter == "All" else status_filter,
        limit=limit,
    )
    if profile_filter != "All":
        jobs = [job for job in jobs if (job.get("profile") or "") == profile_filter]
    return jobs


@st.cache_data(ttl=5)
def get_collection_job_profiles():
    conn = get_connection()
    try:
        cursor = conn.execute(
            """
            SELECT DISTINCT profile
            FROM external_collection_jobs
            WHERE profile IS NOT NULL AND profile != ''
            ORDER BY profile
            """
        )
        return [row["profile"] for row in cursor.fetchall()]
    finally:
        conn.close()


@st.cache_data(ttl=5)
def get_collection_schedules(plugin_id: str, enabled_filter):
    return SQLiteStore(DB_PATH).list_collection_schedules(
        plugin_id=None if plugin_id == "All" else plugin_id,
        enabled=enabled_filter,
        limit=200,
    )


@st.cache_data(ttl=5)
def get_health_summary_cached(recent_days: int):
    store = SQLiteStore(DB_PATH)
    return get_health_summary(store, recent_days=recent_days)


@st.cache_data(ttl=5)
def get_dataset_health_cached():
    store = SQLiteStore(DB_PATH)
    return get_dataset_health(store)


@st.cache_data(ttl=5)
def get_job_health_cached():
    store = SQLiteStore(DB_PATH)
    return get_job_health(store)


@st.cache_data(ttl=5)
def get_domain_health_cached():
    store = SQLiteStore(DB_PATH)
    return get_domain_health(store)


@st.cache_data(ttl=5)
def get_daily_meeting_health_cached(recent_days: int):
    store = SQLiteStore(DB_PATH)
    return get_daily_meeting_date_health(store, recent_days=recent_days)


@st.cache_data(ttl=5)
def get_context_coverage_cached():
    store = SQLiteStore(DB_PATH)
    return get_context_coverage(store)


# Sidebar
st.sidebar.title("📊 Data Collector Hub")
st.sidebar.caption("v1.0 Dashboard")
st.session_state["api_base_url"] = st.sidebar.text_input(
    "API Base URL",
    value=st.session_state.get("api_base_url", "http://127.0.0.1:8000"),
)

# Check database exists
if not DB_PATH.exists():
    st.sidebar.error(f"Database not found: {DB_PATH}")
    st.sidebar.info("Please run the initialization script first:")
    st.sidebar.code("python init_and_run.py")
    st.stop()

# Navigation
page = st.sidebar.radio(
    "Navigation",
    [
        "🏠 Home",
        "🔌 Plugins",
        "📄 Raw Data",
        "🧾 Raw Events",
        "📋 Normalized Data",
        "🚚 Collection Jobs",
        "🩺 Data Health",
        "📈 Statistics",
        "📝 Logs",
    ]
)

# Home Page
if page == "🏠 Home":
    st.title("🏠 Data Collector Hub Dashboard")
    st.markdown("---")

    # Summary metrics
    counts = get_counts()

    col1, col2, col3, col4, col5 = st.columns(5)

    with col1:
        st.metric("Plugins", counts.get('plugins', 0))

    with col2:
        st.metric("Raw Data", counts.get('raw_data', 0))

    with col3:
        st.metric("Raw Events", counts.get('raw_events', 0))

    with col4:
        st.metric("Normalized Data", counts.get('normalized_data', 0))

    with col5:
        st.metric("Logs", counts.get('logs', 0))

    st.markdown("---")

    # Recent activity
    st.subheader("📈 Recent Activity")

    stats = get_task_stats()

    if stats:
        df = pd.DataFrame([
            {
                "Plugin": row["plugin_id"],
                "Run Count": row["run_count"],
                "Fail Count": row["fail_count"],
                "Last Run": row["last_run"],
                "Consecutive Fails": row["consecutive_fails"]
            }
            for row in stats
        ])
        st.dataframe(df, use_container_width=True)
    else:
        st.info("No task statistics available yet.")

# Plugins Page
elif page == "🔌 Plugins":
    st.title("🔌 Plugins")
    st.markdown("---")

    plugins = get_plugin_list()

    if not plugins:
        st.info("No plugins registered yet.")
    else:
        for plugin in plugins:
            with st.expander(f"📦 {plugin['name']} (v{plugin['version']})"):
                col1, col2 = st.columns(2)

                with col1:
                    st.write(f"**ID:** {plugin['id']}")
                    st.write(f"**Description:** {plugin['description']}")
                    st.write(f"**Author:** {plugin['author']}")
                    st.write(f"**Tags:** {', '.join(plugin['tags'])}")

                with col2:
                    st.write(f"**Enabled:** {'✅' if plugin['enabled'] else '❌'}")
                    st.write(f"**Health Status:** {plugin['health_status']}")
                    st.write(f"**Created:** {plugin['created_at']}")
                    st.write(f"**Updated:** {plugin['updated_at']}")

                if plugin.get('config_schema'):
                    st.write("**Config Schema:**")
                    st.json(plugin['config_schema'])

                runtime_config = get_plugin_runtime_config(plugin['id'])
                st.write("**Runtime Config:**")
                config_text = st.text_area(
                    "Runtime Config JSON",
                    value=json.dumps(runtime_config, ensure_ascii=False, indent=2),
                    height=260,
                    key=f"runtime_config_{plugin['id']}"
                )
                if st.button("Save Config", key=f"save_config_{plugin['id']}"):
                    try:
                        parsed_config = json.loads(config_text)
                        if not isinstance(parsed_config, dict):
                            st.error("Runtime config must be a JSON object.")
                        else:
                            errors = validate_plugin_runtime_config(plugin['id'], parsed_config)
                            if errors:
                                st.error("Runtime config validation failed:")
                                for error in errors:
                                    st.error(error)
                            else:
                                save_plugin_runtime_config(plugin['id'], parsed_config)
                                st.cache_data.clear()
                                st.success("Runtime config saved.")
                                st.rerun()
                    except json.JSONDecodeError as exc:
                        st.error(f"Invalid JSON: {exc}")

# Raw Data Page
elif page == "📄 Raw Data":
    st.title("📄 Raw Data")
    st.info("raw_data stores embedded pipeline collector output. raw_events stores one original business record per MVP ingestion batch row.")
    st.markdown("---")

    # Filter by plugin
    plugins = get_plugin_list()
    plugin_options = ["All"] + [p["id"] for p in plugins]
    selected_plugin = st.selectbox("Filter by Plugin", plugin_options)

    # Limit
    limit = st.slider("Limit", 10, 100, 20)

    rows = get_raw_data(selected_plugin, limit)

    if not rows:
        st.info("No raw data available.")
    else:
        st.write(f"Showing {len(rows)} records:")

        for row in rows:
            data = json.loads(row["data"]) if row["data"] else {}

            with st.container():
                col1, col2, col3 = st.columns([2, 2, 1])

                with col1:
                    st.write(f"**ID:** {row['id']}")
                    st.write(f"**Plugin:** {row['plugin_id']}")
                    st.write(f"**Source:** {row['source']}")

                with col2:
                    # Try to extract title from data
                    title = data.get("title", "")
                    if title:
                        st.write(f"**Title:** {title[:50]}..." if len(title) > 50 else f"**Title:** {title}")

                with col3:
                    st.write(f"**Created:** {row['created_at']}")

                with st.expander("View Data"):
                    st.json(data)

                st.markdown("---")

# Raw Events Page
elif page == "🧾 Raw Events":
    st.title("🧾 Raw Events")
    st.markdown("---")

    dataset_options = ["All"] + get_raw_event_dataset_keys()
    selected_dataset = st.selectbox("Filter by Dataset", dataset_options)
    limit = st.slider("Limit", 10, 100, 20)

    rows = get_raw_events(selected_dataset, limit)

    if not rows:
        st.info("No raw events available.")
    else:
        st.write(f"Showing {len(rows)} raw events:")
        df = pd.DataFrame(rows)
        st.dataframe(df, use_container_width=True)

# Normalized Data Page
elif page == "📋 Normalized Data":
    st.title("📋 Normalized Data")
    st.markdown("---")

    # Filter by plugin
    plugins = get_plugin_list()
    plugin_options = ["All"] + [p["id"] for p in plugins]
    selected_plugin = st.selectbox("Filter by Plugin", plugin_options)

    # Filter by event type
    event_types = ["All"] + get_event_types()
    selected_type = st.selectbox("Filter by Event Type", event_types)

    # Limit
    limit = st.slider("Limit", 10, 100, 20)

    rows = get_normalized_data(selected_plugin, selected_type, limit)

    if not rows:
        st.info("No normalized data available.")
    else:
        st.write(f"Showing {len(rows)} records:")

        for row in rows:
            payload = json.loads(row["payload"]) if row["payload"] else {}
            entity = json.loads(row["entity"]) if row["entity"] else []

            with st.container():
                col1, col2, col3 = st.columns([2, 2, 1])

                with col1:
                    st.write(f"**ID:** {row['id']}")
                    st.write(f"**Plugin:** {row['plugin_id']}")
                    st.write(f"**Event Type:** `{row['event_type']}`")
                    st.write(f"**Event Source:** {row['event_source']}")

                with col2:
                    st.write(f"**Entity:** {entity}")
                    st.write(f"**Confidence:** {row['confidence']}")
                    st.write(f"**Event Time:** {row['event_timestamp']}")

                with col3:
                    st.write(f"**Created:** {row['created_at']}")

                with st.expander("View Payload"):
                    st.json(payload)

                st.markdown("---")

# Collection Jobs Page
elif page == "🚚 Collection Jobs":
    st.title("🚚 Collection Jobs")
    st.markdown("---")

    refresh_col, tick_col = st.columns([1, 1])
    with refresh_col:
        if st.button("Refresh Jobs"):
            st.cache_data.clear()
            st.rerun()
    with tick_col:
        if st.button("Run Scheduler Tick Now"):
            status_code, body = _api_json("POST", "/collection/v1/scheduler/tick")
            if status_code == 200:
                st.success(f"Scheduler tick completed. created_job_ids={body.get('created_job_ids', [])}")
                st.cache_data.clear()
                st.rerun()
            else:
                st.error(f"Scheduler tick failed: {body}")

    job_plugin = st.selectbox("Plugin", ["dcp", "All"], index=0, key="collection_jobs_plugin")
    summary = get_collection_job_summary(job_plugin)
    col1, col2, col3, col4, col5 = st.columns(5)
    col1.metric("Queued", summary.get("queued") or 0)
    col2.metric("Running", summary.get("running") or 0)
    col3.metric("Succeeded", summary.get("succeeded") or 0)
    col4.metric("Failed", summary.get("failed") or 0)
    col5.metric("Last Job", summary.get("last_job_at") or "-")

    st.subheader("Manual Trigger")
    dcp_runtime = get_plugin_runtime_config("dcp")
    profiles = dcp_runtime.get("collection_profiles") or {}
    profile_names = list(profiles.keys())
    selected_profile = st.selectbox("Profile", profile_names, key="collection_profile")
    selected_profile_config = profiles.get(selected_profile) or {}
    default_datasets = list(selected_profile_config.get("datasets") or [])
    all_datasets = list(dcp_runtime.get("enabled_datasets") or [])
    with st.form("manual_collection_trigger"):
        dataset_keys = st.multiselect(
            "Dataset Keys",
            options=all_datasets,
            default=default_datasets,
        )
        processing_mode = st.selectbox(
            "Processing Mode",
            ["none", "sync", "async"],
            index=["none", "sync", "async"].index(
                str(selected_profile_config.get("processing_mode") or "none")
            ),
        )
        recent_days_default = selected_profile_config.get("recent_days")
        recent_days = st.number_input(
            "Recent Days",
            min_value=0,
            step=1,
            value=int(recent_days_default or 0),
        )
        since_date = st.text_input("Since Date", value=str(selected_profile_config.get("since_date") or ""))
        until_date = st.text_input("Until Date", value=str(selected_profile_config.get("until_date") or ""))
        include_existing = st.checkbox(
            "Include Existing",
            value=bool(selected_profile_config.get("include_existing", False)),
        )
        force = st.checkbox("Force", value=bool(selected_profile_config.get("force", False)))
        due_only = st.checkbox("Due Only", value=bool(selected_profile_config.get("due_only", False)))
        submitted = st.form_submit_button("Create Collection Job")
        if submitted:
            payload = {
                "plugin_id": "dcp",
                "profile": selected_profile,
                "dataset_keys": dataset_keys,
                "processing_mode": processing_mode,
                "recent_days": int(recent_days) or None,
                "since_date": since_date or None,
                "until_date": until_date or None,
                "include_existing": include_existing,
                "force": force,
                "due_only": due_only,
            }
            status_code, body = _api_json("POST", "/collection/v1/jobs", payload)
            if status_code == 202:
                st.success(f"Created job: {body.get('job_id')}")
                st.cache_data.clear()
                st.rerun()
            elif status_code == 409:
                st.warning("已有重叠 dataset 的 queued/running job")
                st.json(body)
            else:
                st.error(f"Create job failed: {body}")

    st.subheader("Jobs")
    status_filter = st.selectbox(
        "Status",
        ["All", "queued", "running", "succeeded", "failed"],
        key="collection_jobs_status",
    )
    profile_filter = st.selectbox(
        "Profile Filter",
        ["All"] + get_collection_job_profiles(),
        key="collection_jobs_profile",
    )
    limit = st.slider("Job Limit", 10, 200, 50, key="collection_jobs_limit")
    jobs = get_collection_jobs(job_plugin, status_filter, profile_filter, limit)
    if not jobs:
        st.info("No collection jobs found.")
    else:
        st.dataframe(
            pd.DataFrame(
                [
                    {
                        "job_id": job["job_id"],
                        "plugin_id": job["plugin_id"],
                        "profile": job.get("profile"),
                        "dataset_keys": ", ".join(job.get("dataset_keys") or []),
                        "status": job["status"],
                        "mode": job.get("mode"),
                        "processing_mode": job.get("processing_mode"),
                        "created_at": job.get("created_at"),
                        "started_at": job.get("started_at"),
                        "finished_at": job.get("finished_at"),
                        "exit_code": job.get("exit_code"),
                        "error": job.get("error"),
                    }
                    for job in jobs
                ]
            ),
            use_container_width=True,
        )
        for job in jobs:
            label = f"{job['job_id']} [{job['status']}] {', '.join(job.get('dataset_keys') or [])}"
            with st.expander(label):
                st.write(f"**Command:** `{json.dumps(job.get('command') or [], ensure_ascii=False)}`")
                st.write(f"**CWD:** `{job.get('cwd')}`")
                st.write(f"**DataHub URL:** `{job.get('datahub_url')}`")
                st.write(f"**Dataset Keys:** {job.get('dataset_keys')}")
                if job.get("stdout"):
                    st.text_area("stdout tail", value=str(job["stdout"])[-4000:], height=180, key=f"stdout_{job['job_id']}")
                if job.get("stderr"):
                    st.text_area("stderr tail", value=str(job["stderr"])[-4000:], height=180, key=f"stderr_{job['job_id']}")
                if job.get("result") is not None:
                    st.write("**Result JSON:**")
                    st.json(job["result"])
                if job.get("error"):
                    st.write("**Error:**")
                    st.code(str(job["error"]))

    st.subheader("Schedules")
    schedules = get_collection_schedules(job_plugin, enabled_filter=None)
    if not schedules:
        st.info("No collection schedules found.")
    else:
        st.dataframe(
            pd.DataFrame(
                [
                    {
                        "schedule_id": schedule["schedule_id"],
                        "plugin_id": schedule["plugin_id"],
                        "profile": schedule["profile"],
                        "enabled": schedule["enabled"],
                        "schedule_cron": schedule["schedule_cron"],
                        "next_run_at": schedule.get("next_run_at"),
                        "last_triggered_at": schedule.get("last_triggered_at"),
                        "last_job_id": schedule.get("last_job_id"),
                    }
                    for schedule in schedules
                ]
            ),
            use_container_width=True,
        )
        for schedule in schedules:
            cols = st.columns([3, 1, 1, 1])
            cols[0].write(
                f"**{schedule['schedule_id']}** | cron=`{schedule['schedule_cron']}` | next_run_at={schedule.get('next_run_at') or '-'}"
            )
            if cols[1].button(
                "Enable" if not schedule["enabled"] else "Disable",
                key=f"toggle_schedule_{schedule['schedule_id']}",
            ):
                path = (
                    f"/collection/v1/schedules/{schedule['schedule_id']}/enable"
                    if not schedule["enabled"]
                    else f"/collection/v1/schedules/{schedule['schedule_id']}/disable"
                )
                status_code, body = _api_json("POST", path)
                if status_code == 200:
                    st.cache_data.clear()
                    st.rerun()
                else:
                    st.error(body)
            if cols[2].button("Trigger Profile Now", key=f"trigger_profile_{schedule['schedule_id']}"):
                status_code, body = _api_json("POST", "/collection/v1/jobs", schedule["default_request"])
                if status_code == 202:
                    st.success(f"Created job: {body.get('job_id')}")
                    st.cache_data.clear()
                    st.rerun()
                else:
                    st.error(body)
            if cols[3].button("Tick", key=f"tick_schedule_{schedule['schedule_id']}"):
                status_code, body = _api_json("POST", "/collection/v1/scheduler/tick")
                if status_code == 200:
                    st.success(body)
                    st.cache_data.clear()
                    st.rerun()
                else:
                    st.error(body)

# Data Health Page
elif page == "🩺 Data Health":
    st.title("🩺 Data Health")
    st.markdown("---")

    health_days = st.number_input("Daily Meeting Recent Days", min_value=1, max_value=365, value=14, step=1)
    if st.button("Refresh Health"):
        st.cache_data.clear()
        st.rerun()

    summary = get_health_summary_cached(int(health_days))
    dataset_health = get_dataset_health_cached()
    job_health = get_job_health_cached()
    domain_health = get_domain_health_cached()
    daily_meeting_health = get_daily_meeting_health_cached(int(health_days))
    context_health = get_context_coverage_cached()

    st.subheader("Overall Status")
    st.metric("Status", summary["overall_status"])
    if summary["reasons"]:
        st.write("**Reasons:**")
        for reason in summary["reasons"]:
            st.write(f"- {reason}")

    st.subheader("Dataset Health")
    st.dataframe(
        pd.DataFrame(
            [
                {
                    "dataset_key": dataset_key,
                    "raw_event_count": item["raw_event_count"],
                    "canonical_entity_count": item["canonical_entity_count"],
                    "relationship_count": item["relationship_count"],
                    "latest_collected_at": item["latest_collected_at"],
                    "latest_processing_status": (
                        item["latest_processing_job"]["status"]
                        if item["latest_processing_job"]
                        else None
                    ),
                }
                for dataset_key, item in dataset_health["datasets"].items()
            ]
        ),
        use_container_width=True,
    )

    st.subheader("Job Health")
    external = job_health["external_collection_jobs"]
    processing = job_health["processing_jobs"]
    col1, col2 = st.columns(2)
    with col1:
        st.write("**External Collection Jobs**")
        st.json(external["counts"])
        if external["latest_failed_jobs"]:
            st.write("Latest failed jobs:")
            st.json(external["latest_failed_jobs"])
        if external["active_running_jobs"]:
            st.write("Active running jobs:")
            st.json(external["active_running_jobs"])
    with col2:
        st.write("**Processing Jobs**")
        st.json(processing["counts"])
        if processing["latest_failed_jobs"]:
            st.write("Latest failed jobs:")
            st.json(processing["latest_failed_jobs"])
        if processing["active_running_jobs"]:
            st.write("Active running jobs:")
            st.json(processing["active_running_jobs"])

    st.subheader("Domain Health")
    domain_col1, domain_col2 = st.columns(2)
    with domain_col1:
        st.write("**Entity Counts**")
        st.json(domain_health["entity_counts"])
    with domain_col2:
        st.write("**Relationship Counts**")
        st.json(domain_health["relationship_counts"])
    st.write(
        f"unscoped_tower_sequence_count={domain_health['unscoped_tower_sequence_count']} | "
        f"line_section_known_issue_count={domain_health['line_section_known_issue_count']} | "
        f"orphan_relationship_count={domain_health['orphan_relationship_count']}"
    )

    st.subheader("Daily Meeting Health")
    st.write(f"latest_work_date={daily_meeting_health['latest_work_date']}")
    st.write(f"missing_dates={daily_meeting_health['missing_dates']}")
    st.json(daily_meeting_health["work_point_count_by_date"])

    st.subheader("Context Coverage")
    st.json(context_health)

# Statistics Page
elif page == "📈 Statistics":
    st.title("📈 Task Statistics")
    st.markdown("---")

    stats = get_statistics()

    if not stats:
        st.info("No statistics available yet.")
    else:
        df = pd.DataFrame([
            {
                "Plugin": row["plugin_id"],
                "Run Count": row["run_count"],
                "Fail Count": row["fail_count"],
                "Success Rate": f"{((row['run_count'] - row['fail_count']) / row['run_count'] * 100):.1f}%"
                if row["run_count"] > 0 else "N/A",
                "Last Run": row["last_run"],
                "Last Fail": row["last_fail"],
                "Consecutive Fails": row["consecutive_fails"]
            }
            for row in stats
        ])

        st.dataframe(df, use_container_width=True)

        # Charts
        st.subheader("📊 Visualizations")

        col1, col2 = st.columns(2)

        with col1:
            st.write("**Run Count by Plugin**")
            chart_data = df.set_index("Plugin")["Run Count"]
            st.bar_chart(chart_data)

        with col2:
            st.write("**Fail Count by Plugin**")
            chart_data = df.set_index("Plugin")["Fail Count"]
            st.bar_chart(chart_data)

# Logs Page
elif page == "📝 Logs":
    st.title("📝 Logs")
    st.markdown("---")

    # Filter by level
    level_options = ["All", "INFO", "WARNING", "ERROR"]
    selected_level = st.selectbox("Filter by Level", level_options)

    # Filter by plugin
    plugins = get_plugin_list()
    plugin_options = ["All"] + [p["id"] for p in plugins]
    selected_plugin = st.selectbox("Filter by Plugin", plugin_options)

    # Limit
    limit = st.slider("Limit", 10, 200, 50)

    rows = get_logs(selected_level, selected_plugin, limit)

    if not rows:
        st.info("No logs available.")
    else:
        st.write(f"Showing {len(rows)} log entries:")

        for row in rows:
            # Color code by level
            if row["level"] == "ERROR":
                color = "🔴"
            elif row["level"] == "WARNING":
                color = "🟡"
            else:
                color = "🟢"

            with st.container():
                col1, col2, col3 = st.columns([1, 3, 1])

                with col1:
                    st.write(f"{color} **{row['level']}**")
                    st.write(f"ID: {row['id']}")

                with col2:
                    st.write(f"**Message:** {row['message']}")
                    if row["plugin_id"]:
                        st.write(f"**Plugin:** {row['plugin_id']}")

                with col3:
                    st.write(f"**Time:** {row['created_at']}")

                if row["details"]:
                    with st.expander("View Details"):
                        try:
                            details = json.loads(row["details"])
                            st.json(details)
                        except:
                            st.text(row["details"])

                st.markdown("---")

# Footer
st.sidebar.markdown("---")
st.sidebar.caption("© 2026 Data Collector Hub v1.0")

# Auto refresh button
if st.sidebar.button("🔄 Refresh Data"):
    st.cache_data.clear()
    st.rerun()
