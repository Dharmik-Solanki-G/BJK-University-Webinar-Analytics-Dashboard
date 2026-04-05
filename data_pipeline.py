"""
data_pipeline.py — BJK University Webinar Dashboard
Merges all datasets into a master DataFrame with source classification.
"""
import pandas as pd
import logging

logger = logging.getLogger(__name__)

ATTENDANCE_MIN_MINUTES = 5


def classify_ad_platform(row):
    """Determine ad platform from UTM data."""
    fields = ' '.join([
        str(row.get('lead_utm_source', '')),
        str(row.get('lead_utm_medium', '')),
        str(row.get('lead_utm_campaign', '')),
    ]).lower()
    is_meta = any(k in fields for k in ['facebook', 'fb', 'instagram', 'ig', 'meta'])
    is_google = any(k in fields for k in ['google', 'goog', 'adwords', 'youtube'])
    if is_meta and is_google:
        return 'Multi-Platform'
    if is_meta:
        return 'Meta (FB/IG)'
    if is_google:
        return 'Google Ads'
    return 'Paid (Other)'


def build_master_dataframe(data, metadata):
    """
    Build master DataFrame by merging registrants with attendance + lead source data.
    Priority: Paid > Organic > Unknown/Direct
    """
    regs = data['registrants'].copy()
    atts = data['attendees'].copy()
    organic = data['organic'].copy()
    paid = data['paid'].copy()
    booked = data['booked'].copy()

    if regs.empty:
        logger.error("No registrant data available!")
        return pd.DataFrame()

    # ── 1. Build master from registrants ──────────────────────────
    master = regs[['email']].copy()
    if 'first_name' in regs.columns and 'last_name' in regs.columns:
        master['name'] = (regs['first_name'].fillna('') + ' ' + regs['last_name'].fillna('')).str.strip()
    elif 'full_name' in regs.columns:
        master['name'] = regs['full_name']
    else:
        master['name'] = ''
    if 'registration_time' in regs.columns:
        master['registration_time'] = regs['registration_time']
    if 'approval_status' in regs.columns:
        master['approval_status'] = regs['approval_status']

    # ── 2. Merge attendance data ──────────────────────────────────
    att_cols = ['email', 'duration_minutes', 'attended_raw']
    extra = [c for c in ['join_time', 'leave_time', 'country/region_name'] if c in atts.columns]
    att_merge = atts[[c for c in att_cols + extra if c in atts.columns]].copy()
    att_merge = att_merge.drop_duplicates(subset='email', keep='first')

    master = master.merge(att_merge, on='email', how='left')
    master['duration_minutes'] = master['duration_minutes'].fillna(0)
    master['attended_raw'] = master['attended_raw'].fillna(False).infer_objects(copy=False)
    master['attended'] = master['attended_raw'] & (master['duration_minutes'] >= ATTENDANCE_MIN_MINUTES)

    # Also add attendees NOT in registrants (guests)
    guest_emails = set(atts[atts['attended_raw']]['email']) - set(master['email'])
    if guest_emails:
        guests = atts[atts['email'].isin(guest_emails)].drop_duplicates(subset='email', keep='first')
        guest_rows = pd.DataFrame({
            'email': guests['email'].values,
            'name': guests.get('first_name', pd.Series([''] * len(guests))).fillna('').values,
            'duration_minutes': guests['duration_minutes'].values,
            'attended_raw': True,
            'attended': guests['duration_minutes'].values >= ATTENDANCE_MIN_MINUTES,
        })
        master = pd.concat([master, guest_rows], ignore_index=True)
        logger.info(f"Added {len(guest_rows)} guest attendees not in registrant list")

    # ── 3. Source classification ──────────────────────────────────
    organic_emails = set(organic['email']) if not organic.empty else set()
    paid_emails = set(paid['email']) if not paid.empty else set()
    overlap = organic_emails & paid_emails
    if overlap:
        logger.info(f"Overlap between organic and paid: {len(overlap)} emails (marking as Paid)")

    def classify_source(email):
        if email in paid_emails:
            return 'Paid'
        return 'Organic'

    master['source'] = master['email'].apply(classify_source)

    # ── 4. Ad platform classification (for paid leads) ────────────
    paid_info = paid[['email'] + [c for c in paid.columns if c.startswith('lead_utm')]].drop_duplicates(subset='email')
    master = master.merge(paid_info, on='email', how='left')

    master['ad_platform'] = ''
    paid_mask = master['source'] == 'Paid'
    if paid_mask.any():
        master.loc[paid_mask, 'ad_platform'] = master[paid_mask].apply(classify_ad_platform, axis=1)

    # ── 5. Engagement percentage ──────────────────────────────────
    webinar_duration = metadata.get('actual_duration', 209)
    # Also compute from top quartile of attendees (REMOVED: always use exact webinar duration)
    master['engagement_pct'] = (master['duration_minutes'] / webinar_duration * 100).clip(0, 100).round(1)

    # ── 6. Booked call flag and images ────────────────────────────
    if not booked.empty:
        booked_dict = dict(zip(booked['email'], booked['filename']))
        master['booked_call'] = master['email'].isin(booked_dict)
        master['screenshot_filename'] = master['email'].map(booked_dict)
    else:
        master['booked_call'] = False
        master['screenshot_filename'] = None

    # ── 7. Fill UTM columns ───────────────────────────────────────
    for c in ['lead_utm_source', 'lead_utm_medium', 'lead_utm_campaign', 'lead_utm_term', 'lead_utm_content']:
        if c in master.columns:
            master[c] = master[c].fillna('')
        else:
            master[c] = ''

    # ── 8. Clean up ───────────────────────────────────────────────
    master = master.drop_duplicates(subset='email', keep='first').reset_index(drop=True)

    src_counts = master['source'].value_counts().to_dict()
    att_counts = master[master['attended']]['source'].value_counts().to_dict()
    logger.info(f"Master DataFrame: {len(master)} rows")
    logger.info(f"  Source breakdown: {src_counts}")
    logger.info(f"  Attended breakdown: {att_counts}")
    logger.info(f"  Booked calls matched: {master['booked_call'].sum()}")

    return master
