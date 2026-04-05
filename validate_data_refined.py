import pandas as pd
import os

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

# Webinar attendees
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

# 2. Refined Golden Truth (5+ min threshold, matching guest attendees)
# Master from registrants
master = regs[['email']].copy()
# Aggregate attendees by email
atts_final = atts_raw.groupby('email')['duration_minutes'].sum().reset_index()
# Join
master = master.merge(atts_final, on='email', how='left').fillna(0)

# Dashboard counts duration >= 5
master['attended'] = master['duration_minutes'] >= 5
total_registered = len(master)
total_attended_reg = master['attended'].sum()

# Guests (Attendees not in reg list)
guest_emails = set(atts_final[atts_final['duration_minutes'] >= 5]['email']) - set(master['email'])
total_guests = len(guest_emails)

# Final Golden Truth
final_total_registered = total_registered # Dashboard counts regs as total? 
# Wait, data_pipeline line 81 adds guests to master.
# So total_registered = regs + guests ?
# No, f['total_registered'] in app.py is len(master).
# If guest_emails are added to master, total_registered increases.

final_total_attended = total_attended_reg + total_guests
# Total registered including guests?
final_total_reg = total_registered + total_guests

show_rate = (final_total_attended / final_total_reg) * 100

# Source Splits
organic_emails = set(organic['email'])
paid_emails = set(paid['email'])

def get_source(email):
    if email in paid_emails: return 'Paid'
    if email in organic_emails: return 'Organic'
    return 'Unknown'

# Master source
# Wait, guests also need source.
guest_df = pd.DataFrame({'email': list(guest_emails)})
master_with_guests = pd.concat([master, guest_df], ignore_index=True)
master_with_guests['source'] = master_with_guests['email'].apply(get_source)
master_with_guests['attended'] = master_with_guests['attended'] | master_with_guests['email'].isin(guest_emails)

# Metrics
s_org = master_with_guests[master_with_guests['source'] == 'Organic']
s_paid = master_with_guests[master_with_guests['source'] == 'Paid']

org_reg = len(s_org)
paid_reg = len(s_paid)
org_att = s_org['attended'].sum()
paid_att = s_paid['attended'].sum()

org_show_rate = (org_att / org_reg) * 100 if org_reg > 0 else 0
paid_show_rate = (paid_att / paid_reg) * 100 if paid_reg > 0 else 0

print(f"--- Refined Golden Truth (5+ min) ---")
print(f"Total Registered: {len(master_with_guests)}")
print(f"Total Attended: {int(final_total_attended)}")
print(f"Overall Show Rate: {show_rate:.1f}%")
print(f"Organic Registered: {org_reg}")
print(f"Paid Registered: {paid_reg}")
print(f"Organic Attended: {int(org_att)}")
print(f"Paid Attended: {int(paid_att)}")
print(f"Organic Show Rate: {org_show_rate:.1f}%")
print(f"Paid Show Rate: {paid_show_rate:.1f}%")
