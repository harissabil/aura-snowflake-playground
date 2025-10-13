import json
import os

import numpy as np
import pandas as pd
import requests
import streamlit as st
from dotenv import load_dotenv

load_dotenv()

HOST = os.getenv("CORTEX_AGENT_HOST")
DATABASE = os.getenv("CORTEX_AGENT_DATABASE", "SNOWFLAKE_INTELLIGENCE")
SCHEMA = os.getenv("CORTEX_AGENT_SCHEMA", "AGENTS")
AGENT = os.getenv("CORTEX_AGENT_NAME", "STAFFADMINTESTAGENT")
PAT = os.getenv("SNOWFLAKE_PAT")

assert HOST and PAT, "CORTEX_AGENT_HOST and SNOWFLAKE_PAT must be set"

RUN_URL = f"https://{HOST}/api/v2/databases/{DATABASE}/schemas/{SCHEMA}/agents/{AGENT}:run"
HEADERS = {
    "Authorization": f"Bearer {PAT}",
    "Content-Type": "application/json",
    # Optional but recommended to be explicit
    "X-Snowflake-Authorization-Token-Type": "PROGRAMMATIC_ACCESS_TOKEN",
    # Some setups like seeing text/event-stream is auto-inferred; this helps:
    "Accept": "text/event-stream",
}

st.set_page_config(page_title="Cortex Agent", page_icon="❄️", layout="centered")
st.title("❄️ Cortex Agent")

# Minimal session state: list of dict messages [{role, content:[{type,text}|{type,table}|...]}]
if "messages" not in st.session_state:
    st.session_state.messages = []


# Render prior messages (very simple)
def render_message(msg: dict):
    with st.chat_message(msg["role"]):
        for item in msg.get("content", []):
            t = item.get("type")
            if t == "text":
                st.markdown(item.get("text", ""))
            elif t == "chart":
                spec = json.loads(item["chart"]["chart_spec"])
                st.vega_lite_chart(spec, use_container_width=True)
            elif t == "table":
                # item["table"]["result_set"]["data"] is 2D array; names in ["row_type"]
                rs = item["table"]["result_set"]
                data_array = np.array(rs["data"])
                columns = [c["name"] for c in rs["result_set_meta_data"]["row_type"]]
                st.dataframe(pd.DataFrame(data_array, columns=columns))
            else:
                with st.expander(t or "content"):
                    st.json(item)


for m in st.session_state.messages:
    render_message(m)


# Make a run request to the Agent
def agent_run(messages: list) -> requests.Response:
    # Agent expects {"model": "...", "messages":[...]} — "model" can be omitted; the agent config decides.
    body = {
        "messages": messages,
        "stream": True  # ensure SSE stream for deltas / tables / charts
    }
    resp = requests.post(RUN_URL, headers=HEADERS, json=body, stream=True)
    if resp.status_code >= 400:
        raise RuntimeError(f"Request failed ({resp.status_code}): {resp.text}")
    return resp


# Stream events and update UI
def stream_events(response: requests.Response):
    import json
    from collections import defaultdict

    content = st.container()
    content_map = defaultdict(content.empty)
    buffers = defaultdict(str)

    def iter_sse(resp: requests.Response):
        """Minimal SSE parser over a requests streaming response."""
        event, data_lines = None, []
        for raw in resp.iter_lines(decode_unicode=True):
            if raw is None:
                continue
            line = raw.rstrip("\n")
            if line == "":
                if data_lines:
                    yield (event or "message", "\n".join(data_lines))
                event, data_lines = None, []
                continue
            if line.startswith(":"):
                continue
            if line.startswith("event:"):
                event = line[6:].strip()
            elif line.startswith("data:"):
                data_lines.append(line[5:].lstrip())

        if data_lines:
            yield (event or "message", "\n".join(data_lines))

    spinner = st.spinner("Waiting for response...")
    spinner.__enter__()

    assistant_msg = {"role": "assistant", "content": []}

    for etype, payload in iter_sse(response):
        if etype == "response.status":
            spinner.__exit__(None, None, None)
            d = json.loads(payload)
            spinner = st.spinner(d.get("message", "Working..."))
            spinner.__enter__()

        elif etype == "response.text.delta":
            d = json.loads(payload)
            idx, text = d["content_index"], d["text"]
            buffers[idx] += text
            content_map[idx].markdown(buffers[idx])

        elif etype == "response.thinking.delta":
            d = json.loads(payload)
            idx, text = d["content_index"], d["text"]
            buffers[idx] += text
            content_map[idx].expander("Thinking", expanded=True).write(buffers[idx])

        elif etype == "response.thinking":
            d = json.loads(payload)
            content_map[d["content_index"]].expander("Thinking").write(d["text"])

        elif etype == "response.table":
            d = json.loads(payload)
            rs = d["result_set"]
            arr = np.array(rs["data"])
            cols = [c["name"] for c in rs["result_set_meta_data"]["row_type"]]
            df = pd.DataFrame(arr, columns=cols)
            content_map[d["content_index"]].dataframe(df)
            assistant_msg["content"].append({"type": "table", "table": d})

        elif etype == "response.chart":
            d = json.loads(payload)
            spec = json.loads(d["chart_spec"])
            content_map[d["content_index"]].vega_lite_chart(spec, use_container_width=True)
            assistant_msg["content"].append({"type": "chart", "chart": d})

        elif etype in ("response.tool_use", "response.tool_result"):
            try:
                d = json.loads(payload)
            except Exception:
                d = {"raw": payload}
            content_map[999].expander(etype.replace("response.", "").title()).json(d)

        elif etype == "error":
            try:
                d = json.loads(payload)
                st.error(f"Error: {d.get('message', 'Agent error')} (code: {d.get('code', '')})")
            except Exception:
                st.error(f"Agent error: {payload}")
            if st.session_state.messages:
                st.session_state.messages.pop()
            return

        elif etype == "response":
            try:
                d = json.loads(payload)
            except Exception:
                d = {"role": "assistant", "content": [{"type": "text", "text": payload}]}
            st.session_state.messages.append(d)

    spinner.__exit__(None, None, None)

    if buffers and not assistant_msg["content"]:
        merged_text = "".join(buffers[i] for i in sorted(buffers.keys()))
        assistant_msg["content"].append({"type": "text", "text": merged_text})
        st.session_state.messages.append(assistant_msg)


# Send a new user message and start streaming
def send(prompt: str):
    user_msg = {"role": "user", "content": [{"type": "text", "text": prompt}]}
    st.session_state.messages.append(user_msg)
    with st.chat_message("user"):
        st.markdown(prompt)

    with st.chat_message("assistant"):
        with st.spinner("Sending request..."):
            resp = agent_run(st.session_state.messages)
        # Expose Snowflake Request ID for debugging
        st.markdown(f"```request_id: {resp.headers.get('X-Snowflake-Request-Id')}```")
        stream_events(resp)


# Input box
if user_input := st.chat_input("Ask your Snowflake agent…"):
    send(user_input)

# Example buttons
st.markdown("### 💡 Example Questions")
cols = st.columns(2)
examples = [
    "What are the operating hours for the emergency room?",
    "What is the SOP for patient admission?",
    "What is the current capacity of the ICU?",
    "Where is the radiology department located?",
]

for i, question in enumerate(examples):
    if cols[i % 2].button(question):
        send(question)
