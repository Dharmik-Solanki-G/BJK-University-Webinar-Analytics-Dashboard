"""
data_loader.py — BJK University Webinar Dashboard
Handles loading, cleaning, and normalizing all data sources.
"""
import pandas as pd
import re
import os
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data')


def _read_csv_safe(filepath):
    """Read CSV with encoding fallback."""
    try:
        return pd.read_csv(filepath, encoding='utf-8')
    except UnicodeDecodeError:
        return pd.read_csv(filepath, encoding='latin-1')


def _read_csv_skip(filepath, skip):
    """Read CSV with skiprows and encoding fallback."""
    try:
        return pd.read_csv(filepath, skiprows=skip, encoding='utf-8', index_col=False)
    except UnicodeDecodeError:
        return pd.read_csv(filepath, skiprows=skip, encoding='latin-1', index_col=False)


def _normalize_columns(df):
    """Lowercase, strip, underscore column names."""
    df.columns = [c.strip().lower().replace(' ', '_') for c in df.columns]
    return df


def _normalize_emails(df):
    """Clean email column: lowercase, strip, drop nulls, dedup."""
    if 'email' not in df.columns:
        return df
    df['email'] = df['email'].astype(str).str.strip().str.lower()
    df = df[df['email'].notna() & (~df['email'].isin(['', 'nan', 'none']))]
    df = df.drop_duplicates(subset='email', keep='first')
    return df


def parse_utm_from_additional_info(info_str):
    """Extract UTM parameters from ClickFunnels Additional Info field."""
    result = {'utm_source': '', 'utm_medium': '', 'utm_campaign': '', 'utm_term': '', 'utm_content': ''}
    if not isinstance(info_str, str):
        return result
    for field in result.keys():
        match = re.search(rf'{field}.*?=>\s*"([^"]*)"', info_str)
        if match:
            val = match.group(1).strip()
            result[field] = val if val and val != 'null' else ''
    return result


def load_organic_leads():
    """Load organic leads from ClickFunnels export."""
    fp = os.path.join(DATA_DIR, 'Organic leads.csv')
    if not os.path.exists(fp):
        logger.warning(f"Organic leads file not found: {fp}")
        return pd.DataFrame()
    df = _read_csv_safe(fp)
    df = _normalize_columns(df)
    df = _normalize_emails(df)
    if 'additional_info' in df.columns:
        utm = pd.DataFrame(df['additional_info'].apply(parse_utm_from_additional_info).tolist(), index=df.index)
        for c in utm.columns:
            df[f'lead_{c}'] = utm[c]
    logger.info(f"Organic leads loaded: {len(df)} rows")
    return df


def load_paid_leads():
    """Load paid leads from ClickFunnels export."""
    fp = os.path.join(DATA_DIR, 'Paid Leads.csv')
    if not os.path.exists(fp):
        logger.warning(f"Paid leads file not found: {fp}")
        return pd.DataFrame()
    df = _read_csv_safe(fp)
    df = _normalize_columns(df)
    df = _normalize_emails(df)
    if 'additional_info' in df.columns:
        utm = pd.DataFrame(df['additional_info'].apply(parse_utm_from_additional_info).tolist(), index=df.index)
        for c in utm.columns:
            df[f'lead_{c}'] = utm[c]
    logger.info(f"Paid leads loaded: {len(df)} rows")
    return df


def load_webinar_registrants():
    """Load Zoom webinar registration report, skipping metadata header."""
    fp = os.path.join(DATA_DIR, 'Webinar regs.csv')
    if not os.path.exists(fp):
        logger.warning(f"Registrants file not found: {fp}")
        return pd.DataFrame(), {}
    metadata = {}
    with open(fp, 'r', encoding='utf-8', errors='replace') as f:
        lines = f.readlines()
    if len(lines) > 3:
        # Use regex to split while keeping quoted strings intact
        parts = re.findall(r'("(?:[^"]|"")*"|[^,]+|(?<=,)(?=,))', lines[3])
        parts = [p.strip().strip('"') for p in parts]
        
        topic = parts[0] if parts else ''
        metadata['topic'] = topic
        metadata['webinar_id'] = parts[1] if len(parts) > 1 else ''
        metadata['scheduled_time'] = parts[2] if len(parts) > 2 else ''
        metadata['scheduled_duration'] = int(parts[3]) if len(parts) > 3 and parts[3].isdigit() else 90
        metadata['total_registrants_header'] = int(parts[4]) if len(parts) > 4 and parts[4].isdigit() else 0
        metadata['cancelled_count'] = int(parts[5]) if len(parts) > 5 and parts[5].isdigit() else 0
    # Data starts after line 5 ("Attendee Details,") — header on line 6
    df = _read_csv_skip(fp, 5)
    df = _normalize_columns(df)
    df = df.dropna(how='all')
    # Remove trailing empty columns
    df = df.loc[:, ~df.columns.str.startswith('unnamed')]
    df = _normalize_emails(df)
    if 'registration_time' in df.columns:
        df['registration_time'] = pd.to_datetime(df['registration_time'], format='mixed', errors='coerce')
    logger.info(f"Webinar registrants loaded: {len(df)} rows")
    return df, metadata


def load_webinar_attendees():
    """Load Zoom attendee report — parse the Attendee Details section only."""
    fp = os.path.join(DATA_DIR, 'Webinar Attendees.csv')
    if not os.path.exists(fp):
        logger.warning(f"Attendees file not found: {fp}")
        return pd.DataFrame(), {}
    metadata = {}
    with open(fp, 'r', encoding='utf-8', errors='replace') as f:
        lines = f.readlines()
    # Parse metadata from line 4 (0-indexed 3)
    if len(lines) > 3:
        parts = []
        # Use regex to split while keeping quoted strings intact (for Topic which might contain commas)
        parts = re.findall(r'("(?:[^"]|"")*"|[^,]+|(?<=,)(?=,))', lines[3])
        parts = [p.strip().strip('"') for p in parts]
        
        # Metadata Indices for Attendee Report: 0:Topic, 1:ID, 2:Start, 3:Duration, 4:Regs, 5:Cancelled, 6:Unique, 7:Total, 8:Max
        if len(parts) > 8:
            metadata['actual_start'] = parts[2]
            metadata['actual_duration'] = int(parts[3]) if parts[3].isdigit() else 209
            metadata['total_registrants_header'] = int(parts[4]) if parts[4].isdigit() else 0
            metadata['cancelled_count'] = int(parts[5]) if parts[5].isdigit() else 0
            metadata['unique_viewers'] = int(parts[6]) if parts[6].isdigit() else 0
            metadata['total_users'] = int(parts[7]) if parts[7].isdigit() else 0
            metadata['max_concurrent'] = int(parts[8]) if parts[8].isdigit() else 0
        else:
            # Fallback to simple split if regex fails or structure is simpler
            parts = lines[3].split(',')
            metadata['actual_start'] = parts[2].strip().strip('"') if len(parts) > 2 else ''
            metadata['actual_duration'] = int(parts[3].strip()) if len(parts) > 3 and parts[3].strip().isdigit() else 209
            metadata['unique_viewers'] = int(parts[6].strip()) if len(parts) > 6 and parts[6].strip().isdigit() else 0
            metadata['total_users'] = int(parts[7].strip()) if len(parts) > 7 and parts[7].strip().isdigit() else 0
            metadata['max_concurrent'] = int(parts[8].strip()) if len(parts) > 8 and parts[8].strip().isdigit() else 0
    # Find "Attendee Details" section
    att_line = None
    for i, line in enumerate(lines):
        if 'Attendee Details' in line and i > 5:
            att_line = i + 1  # next line is header
            break
    if att_line is None:
        att_line = 12  # fallback
    df = _read_csv_skip(fp, att_line)
    df = _normalize_columns(df)
    df = df.dropna(how='all')
    df = df.loc[:, ~df.columns.str.startswith('unnamed')]
    if 'email' in df.columns:
        df['email'] = df['email'].astype(str).str.strip().str.lower()
        df = df[df['email'].notna() & (~df['email'].isin(['', 'nan', 'none']))]
    # Clean duration
    dur_col = [c for c in df.columns if 'time_in_session' in c or 'duration' in c]
    if dur_col:
        df['duration_minutes'] = pd.to_numeric(df[dur_col[0]].replace('--', pd.NA), errors='coerce').fillna(0)
    else:
        df['duration_minutes'] = 0
    # Auto-detect seconds vs minutes
    if df['duration_minutes'].median() > 1000:
        logger.info("Duration auto-detected as seconds — converting to minutes")
        df['duration_minutes'] = df['duration_minutes'] / 60.0
    # Normalize attended flag
    if 'attended' in df.columns:
        df['attended_raw'] = df['attended'].astype(str).str.strip().str.lower() == 'yes'
    else:
        df['attended_raw'] = df['duration_minutes'] > 0
    # Aggregate by email — sum durations for rejoiners
    yes = df[df['attended_raw']].copy()
    no = df[~df['attended_raw']].copy()
    if not yes.empty:
        dur_agg = yes.groupby('email')['duration_minutes'].sum().reset_index()
        dur_agg.columns = ['email', 'total_duration']
        yes = yes.drop_duplicates(subset='email', keep='first')
        yes = yes.merge(dur_agg, on='email', how='left')
        yes['duration_minutes'] = yes['total_duration']
        yes = yes.drop(columns=['total_duration'])
    no = no[~no['email'].isin(yes['email'])] if not yes.empty else no
    no = no.drop_duplicates(subset='email', keep='first')
    df = pd.concat([yes, no], ignore_index=True)
    logger.info(f"Attendees loaded: {len(df)} total, {len(yes)} attended")
    return df, metadata


def load_booked_calls():
    """Extract booked call data from calendar screenshots."""
    calls = [
        {'name': 'Malachie Louisius', 'email': 'rankfwest@gmail.com', 'date': '2026-04-04', 'session': 'Planning A', 'filename': 'Screen Shot 2026-04-04 at 3.21.01 PM.png'},
        {'name': 'R Lamont Peterson', 'email': 'r.lamont.peterson11@gmail.com', 'date': '2026-04-04', 'session': 'Planning A', 'filename': 'Screen Shot 2026-04-04 at 3.21.06 PM.png'},
        {'name': 'Antonio Brasse', 'email': 'antoniobrasse718@gmail.com', 'date': '2026-04-04', 'session': 'Planning A', 'filename': 'Screen Shot 2026-04-04 at 3.21.11 PM.png'},
        {'name': 'Jenifer Sexton', 'email': 'jeniferjsexton@gmail.com', 'date': '2026-04-03', 'session': 'Planning A', 'filename': 'Screen Shot 2026-04-04 at 3.21.17 PM.png'},
        {'name': 'John Giattino', 'email': 'johngiattino@aol.com', 'date': '2026-04-03', 'session': 'Planning A', 'filename': 'Screen Shot 2026-04-04 at 3.21.20 PM.png'},
        {'name': 'Tania Pofagi', 'email': 'alisepofagi@gmail.com', 'date': '2026-04-03', 'session': 'Planning A', 'filename': 'Screen Shot 2026-04-04 at 3.21.24 PM.png'},
        {'name': 'Jonathan Johnson', 'email': 'jonathansellsfl@gmail.com', 'date': '2026-04-03', 'session': 'Planning A', 'filename': 'Screen Shot 2026-04-04 at 3.21.28 PM.png'},
        {'name': 'Tarek Fata', 'email': 'tfata@msn.com', 'date': '2026-04-03', 'session': 'Meeting', 'filename': 'Screen Shot 2026-04-04 at 3.21.32 PM.png'},
        {'name': 'Tumelo', 'email': 'tumelosean25@proton.me', 'date': '2026-04-03', 'session': 'Planning A', 'filename': 'Screen Shot 2026-04-04 at 3.21.35 PM.png'},
        {'name': 'Matet Sabadisto', 'email': 'matetsabadisto@gmail.com', 'date': '2026-04-03', 'session': 'Planning A', 'filename': 'Screen Shot 2026-04-04 at 3.21.38 PM.png'},
        {'name': 'JAN', 'email': 'j.alkorbeh01@gmail.com', 'date': '2026-04-03', 'session': 'Planning A', 'filename': 'Screen Shot 2026-04-04 at 3.21.41 PM.png'},
        {'name': 'Daniel Kang', 'email': 'chingu1980@gmail.com', 'date': '2026-04-03', 'session': 'Planning Session', 'filename': 'Screen Shot 2026-04-04 at 3.21.44 PM.png'},
        {'name': 'Kate W', 'email': 'alleen182@gmail.com', 'date': '2026-04-03', 'session': 'Planning Session', 'filename': 'Screen Shot 2026-04-04 at 3.21.47 PM.png'},
        {'name': 'Jeffrey Littorno', 'email': 'jlittorno@hotmail.com', 'date': '2026-04-03', 'session': 'Planning A', 'filename': 'Screen Shot 2026-04-04 at 3.21.51 PM.png'},
        {'name': 'Heather Alexander', 'email': 'heathe6100@aol.com', 'date': '2026-04-03', 'session': 'Planning A', 'filename': 'Screen Shot 2026-04-04 at 3.21.54 PM.png'},
        {'name': 'Maira', 'email': 'alejacris@gmail.com', 'date': '2026-04-02', 'session': 'Planning A', 'filename': 'Screen Shot 2026-04-04 at 3.22.00 PM.png'},
        {'name': 'Oyebola Oyelami', 'email': 'smilefabulous80@gmail.com', 'date': '2026-04-02', 'session': 'Planning A', 'filename': 'Screen Shot 2026-04-04 at 3.22.03 PM.png'},
        {'name': 'Jonathan Murchison', 'email': 'murch925@gmail.com', 'date': '2026-04-02', 'session': 'Planning A', 'filename': 'Screen Shot 2026-04-04 at 3.22.06 PM.png'},
        {'name': 'Matthew Gulbranson', 'email': 'matthew@gulbranson.me', 'date': '2026-04-02', 'session': 'Planning A', 'filename': 'Screen Shot 2026-04-04 at 3.22.09 PM.png'},
        {'name': 'Bruce Snowden', 'email': 'brucesnowden89@hotmail.com', 'date': '2026-04-02', 'session': 'Planning R', 'filename': 'Screen Shot 2026-04-04 at 3.22.12 PM.png'},
        {'name': 'Hussein Ramadan', 'email': 'huss121@yahoo.com', 'date': '2026-04-02', 'session': 'Planning A', 'filename': 'Screen Shot 2026-04-04 at 3.22.17 PM.png'},
        {'name': 'Cloanne', 'email': 'cloanepele@yahoo.com', 'date': '2026-04-02', 'session': 'Planning A', 'filename': 'Screen Shot 2026-04-04 at 3.22.20 PM.png'},
        {'name': 'Jean-Pierre Uribe', 'email': 'richbydesign144@gmail.com', 'date': '2026-04-02', 'session': 'Planning A', 'filename': 'Screen Shot 2026-04-04 at 3.22.24 PM.png'},
        {'name': 'Ken Saville', 'email': 'kspuglove1@gmail.com', 'date': '2026-04-02', 'session': 'Planning A', 'filename': 'Screen Shot 2026-04-04 at 3.22.27 PM.png'},
        {'name': 'Roosevelt Crawford', 'email': 'lainrose@yahoo.com', 'date': '2026-04-02', 'session': 'Planning A', 'filename': 'Screen Shot 2026-04-04 at 3.22.31 PM.png'},
        {'name': 'Mamylix Mamylix', 'email': 'marthamlo623@gmail.com', 'date': '2026-04-02', 'session': 'Amazon Strategy', 'filename': 'Screen Shot 2026-04-04 at 3.22.34 PM.png'},
        {'name': 'De la Rosa Peñaló', 'email': 'alfy007@gmail.com', 'date': '2026-04-02', 'session': 'Planning A', 'filename': 'Screen Shot 2026-04-04 at 3.22.37 PM.png'},
        {'name': 'John Yendes', 'email': 'jyendes@hotmail.com', 'date': '2026-04-01', 'session': 'Planning A', 'filename': 'Screen Shot 2026-04-04 at 3.22.43 PM.png'},
        {'name': 'Eduardo Ernesto', 'email': 'emejia@midorick.com', 'date': '2026-04-01', 'session': 'Planning Session', 'filename': 'Screen Shot 2026-04-04 at 3.22.47 PM.png'},
        {'name': 'Dale Wofford', 'email': 'dalewofford940@proton.me', 'date': '2026-03-31', 'session': 'Amazon Strategy', 'filename': 'Screen Shot 2026-04-04 at 3.22.55 PM.png'},
        {'name': 'Hafiz', 'email': 'bayohafiz@gmail.com', 'date': '2026-03-31', 'session': 'Planning Session', 'filename': 'Screen Shot 2026-04-04 at 3.22.59 PM.png'},
        {'name': 'Mark', 'email': 'haneymark78@gmail.com', 'date': '2026-03-31', 'session': 'Amazon Strategy', 'filename': 'Screen Shot 2026-04-04 at 3.23.02 PM.png'},
        {'name': 'Kodjo Kekeh', 'email': 'kodjokek@yahoo.com', 'date': '2026-03-31', 'session': 'Planning', 'filename': 'Screen Shot 2026-04-04 at 3.23.06 PM.png'},
        {'name': 'Pam Saxon', 'email': 'psaxon@hotmail.ca', 'date': '2026-03-31', 'session': 'Amazon Strategy', 'filename': 'Screen Shot 2026-04-04 at 3.22.52 PM.png'},
    ]
    # Count total screenshots in folder
    bc_dir = os.path.join(DATA_DIR, 'Booked Calls')
    total = len([f for f in os.listdir(bc_dir) if f.endswith('.png')]) if os.path.isdir(bc_dir) else 34
    df = pd.DataFrame(calls)
    df['email'] = df['email'].str.strip().str.lower()
    logger.info(f"Booked calls: {len(df)} extracted, {total} total screenshots")
    return df, total


def load_all():
    """Load all data sources. Returns dict of DataFrames + metadata."""
    organic = load_organic_leads()
    paid = load_paid_leads()
    regs, reg_meta = load_webinar_registrants()
    atts, att_meta = load_webinar_attendees()
    booked, total_booked = load_booked_calls()
    meta = {**reg_meta, **att_meta, 'total_booked_calls': total_booked}
    return {
        'organic': organic, 'paid': paid,
        'registrants': regs, 'attendees': atts,
        'booked': booked,
    }, meta
