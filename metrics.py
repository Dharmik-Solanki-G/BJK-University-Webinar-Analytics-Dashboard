"""
metrics.py — BJK University Webinar Dashboard
Computes all funnel, engagement, and timeline metrics.
"""
import pandas as pd
import numpy as np


def funnel_metrics(master):
    """Top-level funnel KPIs."""
    total_reg = len(master)
    total_att = master['attended'].sum()
    show_rate = (total_att / total_reg * 100) if total_reg > 0 else 0
    return {
        'total_registered': int(total_reg),
        'total_attended': int(total_att),
        'show_rate': round(show_rate, 1),
        'total_no_show': int(total_reg - total_att),
    }


def source_metrics(master):
    """Per-source breakdown of registration, attendance, show rate, engagement."""
    total_reg = len(master)
    total_att = master['attended'].sum()
    results = {}
    for src in ['Organic', 'Paid']:
        subset = master[master['source'] == src]
        attended = subset[subset['attended']]
        reg = len(subset)
        att = len(attended)
        results[src] = {
            'registered': reg,
            'reg_share': round(reg / total_reg * 100, 1) if total_reg > 0 else 0,
            'attended': att,
            'att_share': round(att / total_att * 100, 1) if total_att > 0 else 0,
            'no_show': reg - att,
            'show_rate': round(att / reg * 100, 1) if reg > 0 else 0,
            'avg_engagement': round(attended['engagement_pct'].mean(), 1) if len(attended) > 0 else 0,
            'avg_duration': round(attended['duration_minutes'].mean(), 1) if len(attended) > 0 else 0,
        }
    return results


def platform_metrics(master):
    """Breakdown of paid leads by ad platform."""
    paid = master[master['source'] == 'Paid']
    if paid.empty:
        return {}
    result = {}
    for plat in paid['ad_platform'].unique():
        if not plat:
            continue
        subset = paid[paid['ad_platform'] == plat]
        attended = subset[subset['attended']]
        result[plat] = {
            'registered': len(subset),
            'attended': len(attended),
            'show_rate': round(len(attended) / len(subset) * 100, 1) if len(subset) > 0 else 0,
        }
    return result


def engagement_distribution(master):
    """Bucket attendees into engagement bands, split by source."""
    attended = master[master['attended']].copy()
    if attended.empty:
        return pd.DataFrame()
    bins = [0, 25, 50, 75, 100.01]
    labels = ['0-25%', '26-50%', '51-75%', '76-100%']
    attended['band'] = pd.cut(attended['engagement_pct'], bins=bins, labels=labels, include_lowest=True)
    pivot = attended.groupby(['band', 'source'], observed=False).size().unstack(fill_value=0)
    for src in ['Organic', 'Paid']:
        if src not in pivot.columns:
            pivot[src] = 0
    return pivot.reset_index()


def registration_timeline(master):
    """Group events by date, split by source."""
    # Registrations Timeline
    reg_df = master.dropna(subset=['registration_time']).copy()
    reg_df['date'] = reg_df['registration_time'].dt.date
    reg_pivot = reg_df.groupby(['date', 'source']).size().unstack(fill_value=0)
    
    # Booked Calls Timeline (using booking_date)
    booked_df = master[master['booked_call'] == True].dropna(subset=['booking_date']).copy()
    booked_pivot = booked_df.groupby(['booking_date', 'source']).size().unstack(fill_value=0)
    
    # Merge timelines
    dates = sorted(list(set(reg_pivot.index) | set(booked_pivot.index)))
    results = []
    for d in dates:
        row = {'date': pd.to_datetime(d)}
        for src in ['Organic', 'Paid']:
            row[f'{src}_reg'] = reg_pivot.loc[d, src] if (d in reg_pivot.index and src in reg_pivot.columns) else 0
            row[f'{src}_booked'] = booked_pivot.loc[d, src] if (d in booked_pivot.index and src in booked_pivot.columns) else 0
        results.append(row)
    
    return pd.DataFrame(results).sort_values('date')


def booked_calls_metrics(master):
    """Booked call conversion metrics by source."""
    total_att = master['attended'].sum()
    total_booked = master['booked_call'].sum()

    # Breakdown by behavior
    att_booked = len(master[(master['booked_call']) & (master['reg_status'] == 'Attended')])
    noshow_booked = len(master[(master['booked_call']) & (master['reg_status'] == 'No-Show')])
    direct_booked = len(master[(master['booked_call']) & (master['reg_status'] == 'Booked Only')])

    results = {
        'total_booked': int(total_booked),
        'booking_rate': round(total_booked / total_att * 100, 1) if total_att > 0 else 0,
        'att_booked': att_booked,
        'noshow_booked': noshow_booked,
        'direct_booked': direct_booked,
    }
    for src in ['Organic', 'Paid']:
        subset = master[master['source'] == src]
        att_subset = subset['attended'].sum()
        booked_subset = subset['booked_call'].sum()
        results[src] = {
            'booked': int(booked_subset),
            'booking_rate': round(booked_subset / att_subset * 100, 1) if att_subset > 0 else 0,
            'share': round(booked_subset / total_booked * 100, 1) if total_booked > 0 else 0,
            'att_booked': len(subset[(subset['booked_call']) & (subset['reg_status'] == 'Attended')]),
            'noshow_booked': len(subset[(subset['booked_call']) & (subset['reg_status'] == 'No-Show')]),
            'direct_booked': len(subset[(subset['booked_call']) & (subset['reg_status'] == 'Booked Only')]),
        }
    return results


def compute_all(master, metadata):
    """Compute all metrics at once."""
    return {
        'funnel': funnel_metrics(master),
        'source': source_metrics(master),
        'platform': platform_metrics(master),
        'engagement': engagement_distribution(master),
        'timeline': registration_timeline(master),
        'booked': booked_calls_metrics(master),
        'webinar_duration': metadata.get('actual_duration', 209),
        'unique_viewers': metadata.get('unique_viewers', 0),
        'max_concurrent': metadata.get('max_concurrent', 0),
    }

