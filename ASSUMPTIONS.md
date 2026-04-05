# Assumptions & Design Decisions

## Data Classification
- **Paid takes priority**: If an email appears in BOTH organic and paid lead lists, it is classified as "Paid" (the person converted via an ad spend funnel).
- **Attendance threshold**: Attendees must have stayed ≥5 minutes to count as "attended." Under 5 min is treated as a no-show.
- **Unknown source**: Registrants not found in either the organic or paid leads CSV are classified as "Unknown / Direct."

## Webinar Duration
- The actual webinar duration from Zoom metadata is **209 minutes** (3h 29m). This is used as the baseline for engagement % calculations.
- Engagement % is capped at 100%.

## Booked Calls
- 34 screenshot files were found in the `data/Booked Calls/` folder.
- 10 were successfully OCR-extracted (name + email) and matched against the master registrant list.
- The remaining 24 screenshots are counted in the total (34) but individual data was not extracted.
- Booked call emails are matched against the master list to flag which attendees converted.

## Data Quirks
- Zoom attendee CSVs contain metadata headers that are skipped during parsing (first 12 lines for attendees, first 5 for registrations).
- UTM parameters are embedded in a Ruby hash-like string in the ClickFunnels "Additional Info" column and are extracted via regex.
- Re-joining attendees (same email, multiple sessions) have their durations summed.
- The host (Bashar Katou) and panelist (Stephen Hilgart) are excluded from attendee counts.
