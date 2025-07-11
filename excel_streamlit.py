import streamlit as st
import pandas as pd
import os
import time
import hashlib
import json
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import threading

# Streamlit config
st.set_page_config(layout="wide")
st.title("📊 Excel Auto-Sync Tool v3.0 (Streamlit)")

# Initialize session state
if 'sync_active' not in st.session_state:
    st.session_state.sync_active = False
    st.session_state.last_hash = None
    st.session_state.last_sync_time = 0
    st.session_state.source_path = ""
    st.session_state.target_path = ""
    st.session_state.observer = None
    st.session_state.log_messages = ["Application started. Select files to begin."]

CONFIG_FILE = "excel_sync_config.json"

# Logging function
def log(message):
    timestamp = time.strftime("%H:%M:%S")
    st.session_state.log_messages.insert(0, f"[{timestamp}] {message}")
    if len(st.session_state.log_messages) > 20:  # Keep last 20 messages
        st.session_state.log_messages.pop()

# File operations
def get_file_hash(filepath):
    try:
        with open(filepath, 'rb') as f:
            return hashlib.md5(f.read()).hexdigest()
    except:
        return None

def save_config():
    with open(CONFIG_FILE, 'w') as f:
        json.dump({
            'source': st.session_state.source_path,
            'target': st.session_state.target_path
        }, f)

def load_config():
    if os.path.exists(CONFIG_FILE):
        try:
            with open(CONFIG_FILE, 'r') as f:
                config = json.load(f)
                st.session_state.source_path = config.get('source', '')
                st.session_state.target_path = config.get('target', '')
                st.session_state.last_hash = get_file_hash(st.session_state.source_path) if st.session_state.source_path else None
                log("Loaded previous configuration")
        except Exception as e:
            log(f"Error loading config: {str(e)}")

# Sync logic
def perform_sync():
    try:
        if time.time() - st.session_state.last_sync_time < 2:  # 2-second cooldown
            return

        start_time = time.time()
        st.session_state.last_sync_time = time.time()
        log("Starting sync...")

        df_source = pd.read_excel(st.session_state.source_path)
        df_target = pd.read_excel(st.session_state.target_path)

        # Process duplicates (keep last)
        df_source = df_source.drop_duplicates(subset=[df_source.columns[0]], keep='last')
        source_map = df_source.set_index(df_source.columns[0]).to_dict('index')

        # Update target
        update_count = 0
        for idx, row in df_target.iterrows():
            name = row[0]
            if name in source_map:
                for col in df_target.columns[1:]:
                    if col in source_map[name]:
                        old_value = df_target.at[idx, col]
                        new_value = source_map[name][col]
                        if pd.isna(old_value) or old_value != new_value:
                            df_target.at[idx, col] = new_value
                            update_count += 1

        if update_count > 0:
            df_target.to_excel(st.session_state.target_path, index=False)
            elapsed = time.time() - start_time
            log(f"Synced {update_count} cells in {elapsed:.2f} seconds")
            st.session_state.last_hash = get_file_hash(st.session_state.source_path)
            st.success(f"Synced {update_count} cells!")
        else:
            log("No changes detected in source file")
            st.info("No changes needed")

    except PermissionError:
        log("Sync failed: Target file is locked")
        st.error("Please close the target Excel file before syncing")
    except Exception as e:
        log(f"Sync error: {str(e)}")
        st.error(f"Sync failed: {str(e)}")

# Watchdog handler
class SyncHandler(FileSystemEventHandler):
    def __init__(self):
        self.last_trigger = 0

    def on_modified(self, event):
        if not event.is_directory and event.src_path == st.session_state.source_path:
            current_time = time.time()
            if current_time - self.last_trigger > 3:  # 3-second cooldown
                self.last_trigger = current_time
                new_hash = get_file_hash(event.src_path)
                if new_hash and new_hash != st.session_state.last_hash:
                    log("File change detected - auto syncing...")
                    st.session_state.last_hash = new_hash
                    perform_sync()

# Start/stop monitoring
def toggle_sync():
    if st.session_state.sync_active:
        stop_sync()
    else:
        start_sync()

def start_sync():
    if not all(os.path.exists(f) for f in [st.session_state.source_path, st.session_state.target_path]):
        st.error("One or both files no longer exist")
        return

    st.session_state.sync_active = True
    log("Auto-sync started. Monitoring for changes...")
    
    # Start file observer
    st.session_state.observer = Observer()
    event_handler = SyncHandler()
    st.session_state.observer.schedule(event_handler, path=os.path.dirname(st.session_state.source_path))
    st.session_state.observer.start()
    
    # Start periodic checking
    st.session_state.periodic_check = st.empty()
    periodic_check()

def stop_sync():
    if st.session_state.observer:
        st.session_state.observer.stop()
        st.session_state.observer.join()
    st.session_state.sync_active = False
    log("Auto-sync stopped")

def periodic_check():
    if st.session_state.sync_active:
        current_hash = get_file_hash(st.session_state.source_path)
        if current_hash and current_hash != st.session_state.last_hash:
            log("Periodic check detected changes")
            perform_sync()
            st.session_state.last_hash = current_hash
        time.sleep(5)
        st.session_state.periodic_check.empty()
        periodic_check()

# UI Layout
load_config()

col1, col2 = st.columns(2)

with col1:
    st.subheader("File Selection")
    source_file = st.file_uploader("Source Excel (Master File)", type=["xlsx", "xls"], key="source_upload")
    target_file = st.file_uploader("Target Excel (To Update)", type=["xlsx", "xls"], key="target_upload")
    
    if source_file:
        st.session_state.source_path = os.path.join("uploads", source_file.name)
        os.makedirs("uploads", exist_ok=True)
        with open(st.session_state.source_path, "wb") as f:
            f.write(source_file.getbuffer())
        st.session_state.last_hash = get_file_hash(st.session_state.source_path)
        log(f"Source file set: {source_file.name}")
    
    if target_file:
        st.session_state.target_path = os.path.join("uploads", target_file.name)
        os.makedirs("uploads", exist_ok=True)
        with open(st.session_state.target_path, "wb") as f:
            f.write(target_file.getbuffer())
        log(f"Target file set: {target_file.name}")
    
    save_config()

with col2:
    st.subheader("Sync Controls")
    if st.button("🔄 Sync Now", disabled=not (st.session_state.source_path and st.session_state.target_path)):
        perform_sync()
    
    sync_status = "🟢 ACTIVE" if st.session_state.sync_active else "🔴 INACTIVE"
    if st.button(f"{sync_status} Auto-Sync", disabled=not (st.session_state.source_path and st.session_state.target_path)):
        toggle_sync()

# Log display
st.subheader("Activity Log")
log_container = st.container(height=300, border=True)
for msg in st.session_state.log_messages:
    log_container.write(msg)

# Manual sync thread
if 'sync_thread' not in st.session_state:
    st.session_state.sync_thread = None

# Keep the app running
while True:
    time.sleep(1)