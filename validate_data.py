import pandas as pd
import os
import re

DATA_DIR = r'c:\Users\DHARMIK\Downloads\BJK round1\data'

def _read_csv_safe(filepath):
    try:
        return pd.read_csv(filepath, encoding='utf-8')
    except:
        return pd.read_csv(filepath, encoding='latin-1')

def _normalize_emails(df):
    if 'Email' in df.columns:
        df['email'] = df['Email'].astype(str).str.strip().str.lower()
    elif 'email' in df.columns:
        df['email'] = df['email'].astype(str).str.strip().str.lower()
    else:
        return df
    df = df[df['email'].notna() & (~df['email'].isin(['', 'nan', 'none']))]
    df = df.drop_duplicates(subset='email', keep='first')
    return df

# 1. Load Data
organic = _normalize_emails(_read_csv_safe(os.path.join(DATA_DIR, 'Organic leads.csv')))
paid = _normalize_emails(_read_csv_safe(os.path.join(DATA_DIR, 'Paid Leads.csv')))

# Webinar regs uses skip 5
regs = pd.read_csv(os.path.join(DATA_DIR, 'Webinar regs.csv'), skiprows=5, encoding='utf-8', index_col=False)
regs.columns = [c.strip() for c in regs.columns]
regs = _normalize_emails(regs)

# Webinar attendees - need to find section
with open(os.path.join(DATA_DIR, 'Webinar Attendees.csv'), 'r', encoding='utf-8', errors='replace') as f:
    lines = f.readlines()
att_line = 0
for i, line in enumerate(lines):
    if 'Attendee Details' in line:
        att_line = i + 1
        break
atts_raw = pd.read_csv(os.path.join(DATA_DIR, 'Webinar Attendees.csv'), skiprows=att_line, encoding='utf-8', index_col=False)
atts_raw.columns = [c.strip() for c in atts_raw.columns]
atts_raw = _normalize_emails(atts_raw)

# Duration calc
dur_col = [c for c in atts_raw.columns if 'Time in Session' in c or 'duration' in c.lower() or 'time_in_session' in c.lower()][0]
atts_raw['duration_minutes'] = pd.to_numeric(atts_raw[dur_col].astype(str).replace('--', '0'), errors='coerce').fillna(0)
if atts_raw['duration_minutes'].median() > 1000:
    atts_raw['duration_minutes'] = atts_raw['duration_minutes'] / 60.0

# 2. Calculate Metrics
total_registered = len(regs)
# Attended means duration > 0
atts_final = atts_raw.groupby('email')['duration_minutes'].sum().reset_index()
total_attended = len(atts_final[atts_final['duration_minutes'] > 0])
show_rate = (total_attended / total_registered) * 100 if total_registered > 0 else 0

# Source Splits
organic_emails = set(organic['email'])
paid_emails = set(paid['email'])

def get_source(email):
    if email in organic_emails: return 'Organic'
    if email in paid_emails: return 'Paid'
    return 'Unknown'

regs['source'] = regs['email'].apply(get_source)
atts_final['source'] = atts_final['email'].apply(get_source)

organic_reg = len(regs[regs['source'] == 'Organic'])
paid_reg = len(regs[regs['source'] == 'Paid'])
unknown_reg = len(regs[regs['source'] == 'Unknown'])

organic_att = len(atts_final[(atts_final['source'] == 'Organic') & (atts_final['duration_minutes'] > 0)])
paid_att = len(atts_final[(atts_final['source'] == 'Paid') & (atts_final['duration_minutes'] > 0)])
unknown_att = len(atts_final[(atts_final['source'] == 'Unknown') & (atts_final['duration_minutes'] > 0)])

organic_show_rate = (organic_att / organic_reg) * 100 if organic_reg > 0 else 0
paid_show_rate = (paid_att / paid_reg) * 100 if paid_reg > 0 else 0

# Engagement distribution (0-25, 26-50, 51-75, 76-100)
# Use median duration for baseline as app.py does? 
# Actually, let's count directly.
total_dur = atts_final['duration_minutes'].max() # or 209 from metadata
# Wait, let's just count numbers for Task 3.6
bands = [
    len(atts_final[(atts_final['duration_minutes'] / 209 * 100).between(0, 25)]),
    len(atts_final[(atts_final['duration_minutes'] / 209 * 100).between(25.01, 50)]),
    len(atts_final[(atts_final['duration_minutes'] / 209 * 100).between(50.01, 75)]),
    len(atts_final[(atts_final['duration_minutes'] / 209 * 100).between(75.01, 1000)]),
]

print(f"--- Factual Metrics ---")
print(f"Total Registered: {total_registered}")
print(f"Total Attended: {total_attended}")
print(f"Overall Show Rate: {show_rate:.1f}%")
print(f"Organic Registered: {organic_reg}")
print(f"Paid Registered: {paid_reg}")
print(f"Unknown Registered: {unknown_reg}")
print(f"Organic Attended: {organic_att}")
print(f"Paid Attended: {paid_att}")
print(f"Unknown Attended: {unknown_att}")
print(f"Organic Show Rate: {organic_show_rate:.1f}%")
print(f"Paid Show Rate: {paid_show_rate:.1f}%")
print(f"Engagement Bands: {bands}")
print(f"First 5 Emails (Regs): {regs['email'].head(5).tolist()}")
