"""Streamlit frontend for the Distributed Audio Storage System.

Connects to the gateway via api.py (thin HTTP wrappers — LAB TOPIC 1: RPC).
Never shows a stack trace; always uses st.error() for user-visible errors.

Features:
- Sidebar: Client identity (Client A, B, C) with independent Lamport counters.
- Files Tab: Upload audio, view versions grouped by name with LATEST badge, play, delete, chunk map with roles.
- Cluster Tab: Node status cards, replication factor, load balancing algorithm, under-replicated count, bar chart, auto-refresh.
- Event Log Tab: Real-time event log with sequence, wall time, Lamport time, and details.
"""
from typing import Optional
import streamlit as st

import api

# ---------------------------------------------------------------------------
# Page config
# ---------------------------------------------------------------------------
st.set_page_config(
    page_title="🎵 Distributed Audio Storage",
    page_icon="🎵",
    layout="wide",
)

# ---------------------------------------------------------------------------
# Session state initialization for Lamport clocks
# ---------------------------------------------------------------------------
CLIENT_KEYS = ["Client A", "Client B", "Client C"]
for cname in CLIENT_KEYS:
    k = f"lamport_{cname}"
    if k not in st.session_state:
        st.session_state[k] = 0

# ---------------------------------------------------------------------------
# Sidebar
# ---------------------------------------------------------------------------
with st.sidebar:
    st.title("⚙️ System Control")
    st.write(f"**Gateway:** `{api.GATEWAY_URL}`")

    if api.gateway_health():
        st.success("Gateway: Online ✅")
    else:
        st.error("Gateway: Offline ❌")

    st.divider()

    st.subheader("👤 Client Identity")
    selected_client = st.selectbox(
        "Active Client",
        CLIENT_KEYS,
        index=0,
        help="Select which client identity to simulate. Each client tracks an independent Lamport clock.",
    )

    st.subheader("⏱️ Client Lamport Clocks")
    for cname in CLIENT_KEYS:
        val = st.session_state[f"lamport_{cname}"]
        prefix = "👉 " if cname == selected_client else ""
        st.write(f"{prefix}**{cname}:** `{val}`")

    st.divider()

    st.subheader("🛠️ Clock Override")
    override_enabled = st.checkbox("Override next upload timestamp", value=False)
    override_val: Optional[int] = None
    if override_enabled:
        override_val = int(
            st.number_input(
                "Custom Lamport timestamp",
                min_value=0,
                max_value=100000,
                value=10,
                step=1,
                help="Demonstrates that an upload with a higher timestamp wins even if it arrives earlier.",
            )
        )

    st.divider()
    auto_refresh = st.checkbox("Auto-refresh Cluster & Events (3s)", value=True)

    if st.button("🔄 Manual Refresh", key="sidebar_refresh"):
        st.rerun()

# ---------------------------------------------------------------------------
# Main title
# ---------------------------------------------------------------------------
st.title("🎵 Distributed Audio File Storage")
st.caption("Phase 3 — Bully Election · Primary Promotion Failover · Repair Loop · Lamport Clocks")

# ---------------------------------------------------------------------------
# Tabs
# ---------------------------------------------------------------------------
tab_files, tab_cluster, tab_events, tab_demo = st.tabs(
    ["📁 Files", "🖥️ Cluster", "📜 Event Log", "🎬 Demo Scripts"]
)

# ===========================================================================
# TAB 1: FILES
# ===========================================================================
with tab_files:
    # Upload section
    st.header("📤 Upload Audio")
    uploaded = st.file_uploader(
        "Choose an audio file",
        type=["mp3", "wav", "ogg", "flac", "m4a"],
        key="file_uploader",
    )

    if uploaded is not None:
        col1, col2 = st.columns([2, 1])
        with col1:
            st.write(f"**File:** `{uploaded.name}`")
            st.write(f"**Size:** {uploaded.size:,} bytes ({uploaded.size / 1024:.1f} KB)")
            current_c_val = st.session_state[f"lamport_{selected_client}"]
            send_ts_preview = override_val if (override_enabled and override_val is not None) else (current_c_val + 1)
            st.caption(f"Will upload as **{selected_client}** with Lamport timestamp **{send_ts_preview}**")
        with col2:
            if st.button("⬆️ Upload", key="upload_btn", type="primary"):
                with st.spinner(f"Uploading {uploaded.name}…"):
                    file_bytes = uploaded.read()
                    ext = uploaded.name.rsplit(".", 1)[-1].lower()
                    mime_map = {
                        "mp3": "audio/mpeg",
                        "wav": "audio/wav",
                        "ogg": "audio/ogg",
                        "flac": "audio/flac",
                        "m4a": "audio/mp4",
                    }
                    content_type = mime_map.get(ext, "application/octet-stream")

                    # LAB TOPIC 3: Lamport logical clocks
                    client_key = f"lamport_{selected_client}"
                    st.session_state[client_key] += 1
                    local_clock = st.session_state[client_key]
                    upload_lamport = override_val if (override_enabled and override_val is not None) else local_clock

                    try:
                        result = api.upload_file(
                            file_bytes=file_bytes,
                            filename=uploaded.name,
                            content_type=content_type,
                            client_id=selected_client,
                            lamport=upload_lamport,
                        )
                        gw_lamport = result.get("gateway_lamport", 0)
                        # Client advances its clock: max(client, gateway_lamport) + 1
                        st.session_state[client_key] = max(upload_lamport, gw_lamport) + 1

                        degraded_note = " ⚠️ [Degraded Placement]" if result.get("degraded") else ""
                        st.success(
                            f"✅ Uploaded **{uploaded.name}** (ID: `{result['file_id'][:8]}…`){degraded_note} — "
                            f"{result['chunk_count']} chunks stored (Lamport: {result.get('lamport_ts', 0)})"
                        )
                        st.rerun()
                    except Exception as exc:
                        _detail = getattr(getattr(exc, "response", None), "text", str(exc))
                        st.error(f"Upload failed: {_detail}")

    st.divider()

    # File listing section
    st.header("📁 Stored Files & Versions")
    col_filter1, col_filter2 = st.columns([2, 2])
    with col_filter1:
        show_all_versions = st.checkbox("Show all versions (uncheck to show only LATEST per file)", value=True)

    try:
        files = api.list_files(all_versions=show_all_versions)
    except Exception as exc:
        st.error(f"Could not fetch file list: {exc}")
        files = []

    if not files:
        st.info("No files uploaded yet.")
    else:
        # Group files by name
        grouped: dict[str, list] = {}
        for f in files:
            grouped.setdefault(f["name"], []).append(f)

        for filename, versions in grouped.items():
            st.subheader(f"🎵 {filename}")
            for f in versions:
                size_kb = f["size_bytes"] / 1024
                size_str = f"{size_kb:.1f} KB" if size_kb < 1024 else f"{size_kb/1024:.2f} MB"

                with st.container():
                    col1, col2, col3, col4, col5 = st.columns([3, 1, 1, 1, 1])
                    with col1:
                        badge = " :green-background[**🏷️ LATEST**]" if f.get("is_latest") else ""
                        st.markdown(f"**ID:** `{f['file_id']}` {badge}")
                        st.caption(
                            f"Client: **{f.get('client_id', 'default')}**  |  "
                            f"Lamport: `{f.get('lamport_ts', 0)}`  |  "
                            f"Created: {f['created_at'][:19]}"
                        )
                    with col2:
                        st.metric("Size", size_str)
                    with col3:
                        st.metric("Chunks", f["chunk_count"])
                    with col4:
                        play_key = f"play_{f['file_id']}"
                        if st.button("▶️ Play", key=play_key):
                            cache_key = f"audio_{f['file_id']}"
                            if cache_key not in st.session_state:
                                with st.spinner("Downloading…"):
                                    try:
                                        audio_bytes = api.download_file(f["file_id"])
                                        st.session_state[cache_key] = audio_bytes
                                        st.session_state["playing"] = f["file_id"]
                                    except Exception as exc:
                                        _detail = getattr(
                                            getattr(exc, "response", None), "text", str(exc)
                                        )
                                        st.error(f"Playback failed: {_detail}")
                            else:
                                st.session_state["playing"] = f["file_id"]
                    with col5:
                        del_key = f"del_{f['file_id']}"
                        if st.button("🗑️ Delete", key=del_key):
                            try:
                                api.delete_file(f["file_id"])
                                cache_key = f"audio_{f['file_id']}"
                                if cache_key in st.session_state:
                                    del st.session_state[cache_key]
                                st.success(f"Deleted version `{f['file_id'][:8]}…`")
                                st.rerun()
                            except Exception as exc:
                                _detail = getattr(
                                    getattr(exc, "response", None), "text", str(exc)
                                )
                                st.error(f"Delete failed: {_detail}")

                    # Inline player
                    cache_key = f"audio_{f['file_id']}"
                    if st.session_state.get("playing") == f["file_id"] and cache_key in st.session_state:
                        ext = f["name"].rsplit(".", 1)[-1].lower() if "." in f["name"] else ""
                        fmt_map = {
                            "mp3": "audio/mpeg",
                            "wav": "audio/wav",
                            "ogg": "audio/ogg",
                            "flac": "audio/flac",
                            "m4a": "audio/mp4",
                        }
                        st.audio(st.session_state[cache_key], format=fmt_map.get(ext, "audio/mpeg"))

                    # Chunk map expander with primary/backup roles
                    with st.expander(f"🗂️ Chunk map ({f['chunk_count']} chunks)"):
                        try:
                            detail = api.get_file(f["file_id"])
                            chunk_map = detail.get("chunk_map", [])
                            if chunk_map:
                                rows = []
                                for c in sorted(chunk_map, key=lambda x: x["idx"]):
                                    nodes_str = ", ".join(
                                        f"Node {n['node_id']} ({n['role']})"
                                        for n in c["nodes"]
                                    )
                                    rows.append(
                                        {
                                            "idx": c["idx"],
                                            "chunk_id": c["chunk_id"],
                                            "size (bytes)": c["size"],
                                            "node replicas & roles": nodes_str,
                                        }
                                    )
                                st.dataframe(rows, use_container_width=True)
                            else:
                                st.write("No chunk map available.")
                        except Exception as exc:
                            st.error(f"Could not load chunk map: {exc}")

                    st.write("")

            st.divider()

# ===========================================================================
# TAB 2: CLUSTER DASHBOARD
# ===========================================================================
with tab_cluster:
    @st.fragment(run_every="2s" if auto_refresh else None)
    def render_cluster_tab() -> None:
        st.header("🖥️ Cluster Status & Node Controls")
        try:
            status = api.cluster_status()
        except Exception as exc:
            st.error(f"Could not load cluster status: {exc}")
            return

        nodes = status.get("nodes", [])
        rf = status.get("replication_factor", 2)
        lb_algo = status.get("lb_algorithm", "least_loaded")
        under_rep = status.get("under_replicated_chunks", 0)
        leader_id = status.get("leader_id")

        # Overview Metrics
        m1, m2, m3, m4, m5 = st.columns(5)
        with m1:
            st.metric("Replication Factor", rf)
        with m2:
            st.metric("Load Balancer", lb_algo.upper())
        with m3:
            st.metric(
                "Under-Replicated Chunks",
                under_rep,
                delta=None if under_rep == 0 else f"{under_rep} degraded",
                delta_color="inverse",
            )
        with m4:
            alive_count = sum(1 for n in nodes if n["alive"])
            st.metric("Healthy Nodes", f"{alive_count} / {len(nodes)}")
        with m5:
            st.metric("Cluster Leader", f"👑 Node {leader_id}" if leader_id else "None (Electing)")

        st.subheader("📦 Chunk Distribution Across Nodes")
        if nodes:
            chart_dict = {f"Node {n['node_id']}": n["chunk_count"] for n in sorted(nodes, key=lambda x: x["node_id"])}
            st.bar_chart(chart_dict)

        st.subheader("🖧 Storage Nodes & Simulated Failure Controls")
        if nodes:
            cols = st.columns(len(nodes))
            for i, n in enumerate(sorted(nodes, key=lambda x: x["node_id"])):
                nid = n["node_id"]
                is_node_leader = n.get("is_leader", False)
                with cols[i]:
                    with st.container(border=True):
                        title_prefix = "👑 " if is_node_leader else ""
                        st.markdown(f"### {title_prefix}Node {nid}")
                        if is_node_leader:
                            st.caption(":violet-background[**👑 CLUSTER LEADER**]")

                        if n["alive"]:
                            st.markdown("Status: :green[**🟢 ONLINE**]")
                        else:
                            st.markdown("Status: :red[**🔴 FAILED (CRASHED)**]")

                        st.write(f"**Total Chunks:** `{n['chunk_count']}`")
                        st.write(f"**Primary Chunks:** `{n.get('primary_chunks', 0)}`")
                        st.write(f"**Backup Chunks:** `{n.get('backup_chunks', 0)}`")
                        st.write(f"**Bytes Stored:** `{n['bytes_used']:,}`")
                        st.write(f"**In-Flight:** `{n['active_requests']}`")

                        btn_col1, btn_col2 = st.columns(2)
                        with btn_col1:
                            if st.button("💥 Fail", key=f"fail_btn_{nid}", disabled=not n["alive"], type="secondary"):
                                try:
                                    api.fail_node(nid)
                                    st.toast(f"Node {nid} simulated crash activated!")
                                    st.rerun()
                                except Exception as exc:
                                    st.error(f"Fail call failed: {exc}")
                        with btn_col2:
                            if st.button("💚 Recover", key=f"rec_btn_{nid}", disabled=n["alive"], type="primary"):
                                try:
                                    api.recover_node(nid)
                                    st.toast(f"Node {nid} recovered!")
                                    st.rerun()
                                except Exception as exc:
                                    st.error(f"Recover call failed: {exc}")

    render_cluster_tab()

# ===========================================================================
# TAB 3: EVENT LOG
# ===========================================================================
with tab_events:
    @st.fragment(run_every="2s" if auto_refresh else None)
    def render_event_log() -> None:
        st.header("📜 System Event Log")
        st.caption("Ordered newest first. Real-time audit trail of elections, failures, primary promotions, repairs, and uploads.")
        try:
            events = api.get_events(limit=50)
            if events:
                type_badges = {
                    "election": "🗳️ election",
                    "failure": "💥 failure",
                    "repair": "🛠️ repair",
                    "promotion": "👑 promotion",
                    "recovery": "💚 recovery",
                    "upload start": "📤 upload start",
                    "upload commit": "✅ upload commit",
                    "chunk placed": "📦 chunk placed",
                    "replication degraded": "⚠️ degraded",
                    "download served": "📥 download",
                    "file deleted": "🗑️ delete",
                }
                rows = [
                    {
                        "Seq": e["seq"],
                        "Wall Time": e["wall_time"][11:19],
                        "Lamport": e["lamport"],
                        "Actor": e["actor"],
                        "Type": type_badges.get(e["type"], f"ℹ️ {e['type']}"),
                        "Detail": e["detail"],
                    }
                    for e in events
                ]
                st.dataframe(rows, use_container_width=True, height=520)
            else:
                st.info("No events recorded yet.")
        except Exception as exc:
            st.error(f"Could not load event log: {exc}")

    render_event_log()

# ===========================================================================
# TAB 4: DEMO SCRIPTS
# ===========================================================================
with tab_demo:
    st.header("🎬 Interactive Lab Demo Scenarios")
    st.caption("Follow these step-by-step numbered scripts to demonstrate all core distributed systems concepts.")

    st.markdown("""
### Scenario 1: Upload & Spread (Load Balancing & Chunking)
1. Navigate to the **📁 Files** tab.
2. Select any audio file (> 1 MB, e.g. a 2.3 MB mp3) and click **⬆️ Upload**.
3. Notice that the gateway splits the file into 512 KB chunks.
4. Switch to the **🖥️ Cluster** tab. Look at the **Chunk Distribution** chart and observe how chunks are spread evenly across all storage nodes via the `least_loaded` balancer.

---

### Scenario 2: Synchronous Primary-Backup Replication
1. In the **📁 Files** tab, click **🗂️ Chunk map** under the uploaded file.
2. Notice that every chunk has exactly **2 replicas** on distinct nodes:
   - Exactly one node is labeled `(primary)`.
   - Exactly one node is labeled `(backup)`.
3. Check the **📜 Event Log** tab to see the `chunk placed` and `upload commit` events with their Lamport timestamps.

---

### Scenario 3: Lamport Clock Conflict Resolution (Last-Writer-Wins)
1. Open the sidebar. Select **Client A**. Check **Override next upload timestamp** and enter `10`.
2. In the **📁 Files** tab, upload a file named `sample.mp3`.
3. Open the sidebar. Select **Client B**. Check **Override next upload timestamp** and enter `5` (lower logical timestamp, arrives later).
4. Upload another file named `sample.mp3`.
5. Uncheck **Show all versions** in the Files tab. Notice that **Client A's file remains the LATEST** version because Lamport $10 > 5$, resolving the concurrent conflict deterministically!
6. Now select **Client B**, set timestamp override to `11`, and upload `sample.mp3` again. Notice that Client B's new file immediately becomes the **LATEST** version.

---

### Scenario 4: Node Failure & Seamless Playback / Promotion
1. Go to the **🖥️ Cluster** tab. Note which node is the primary for Chunk 0 of an uploaded file (e.g. Node 1).
2. Click the **💥 Fail** button on that node. The node card turns red: `🔴 FAILED (CRASHED)`.
3. Switch to the **📁 Files** tab and click **▶️ Play** on the file. Playback streams and plays flawlessly!
4. Expand the **🗂️ Chunk map**: the gateway automatically promoted the alive backup replica to `primary`.
5. In the **📜 Event Log** tab, verify the `promotion` event (`"promoted N chunks from node X"`).

---

### Scenario 5: Kill the Leader, Bully Re-Election & Automatic Repair
1. In the **🖥️ Cluster** tab, identify the current leader (Node 4 with the `👑 CLUSTER LEADER` badge).
2. Click **💥 Fail** on Node 4.
3. Within 6–10 seconds, watch the **Bully Election** trigger: Node 3 detects leader heartbeat timeout and gets elected as the new `👑 CLUSTER LEADER`.
4. Observe the metric **Under-Replicated Chunks**: it rises because chunks on Node 4 lost a replica.
5. Watch the counter automatically drop back to **0** within seconds as the new leader's repair loop instructs alive nodes to pull and re-replicate missing chunks!
6. Click **💚 Recover** on Node 4. Node 4 immediately starts an election upon recovery and wins it back (highest ID wins in Bully election).
""")
