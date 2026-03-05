# Validation & Accuracy

How to verify the app is working correctly and understand its limitations.

## Cross-check with live sources

Compare our output against established vessel-tracking platforms:

| Source | URL | Use |
|--------|-----|-----|
| **MarineTraffic** | [Strait of Hormuz tracker](https://www.marinetraffic.org/hormuz-strait/ship-traffic-tracker) | Live vessel map – compare vessel counts and positions |
| **VesselFinder** | [vesselfinder.com](https://www.vesselfinder.com/) | Search by MMSI to verify individual vessels |
| **AISStream** | [aisstream.io](https://aisstream.io/) | Our data source – same AIS feed |

**How to validate:**
1. Open MarineTraffic’s Strait of Hormuz map.
2. Note vessel counts in Persian Gulf vs Gulf of Oman.
3. Compare with our heartbeat (`/stats` or Telegram).
4. Check a few MMSIs in our `/ledger` against MarineTraffic – positions should match within a few minutes.

---

## Geographic boundaries (sources)

### Strait of Hormuz (IHO 2002)

From the [International Hydrographic Organization](https://www.iho.int/):

- **Western boundary:** Line from Ras-e Dastakān, Iran (26°32′N, 55°17′E) to Al Jazīrah al Hamrā', UAE (25°43′N, 55°48′E) → roughly **55.3–55.8°E**
- **Eastern boundary:** Line from Ras Līmā', Oman (25°57′N, 56°27′E) to Damāgheh-ye Kūh, Iran (25°48′N, 57°18′E) → roughly **56.5–57.3°E**

### Our zone definitions (simplified)

| Zone | Longitude | Notes |
|------|-----------|-------|
| Persian Gulf | lon < 56.0 | West of Strait |
| Strait of Hormuz | 56.0 ≤ lon ≤ 56.5 | Narrow corridor (simplified) |
| Gulf of Oman | lon > 56.5 | East of Strait |

These are simplified for automation. The IHO Strait extends to ~57.3°E, so some “Strait” traffic may appear in our “Gulf of Oman” zone.

---

## AISStream API

- **Docs:** [aisstream.io/documentation](https://aisstream.io/documentation)
- **Bbox format:** `[[[lat1, lon1], [lat2, lon2]]]` (lat, lon per corner)
- **Our subscription:** 24–30.5°N, 48–60°E (Persian Gulf + Strait + Gulf of Oman)

---

## When is the app “working correctly”?

| Check | Expected |
|-------|----------|
| AIS messages | `AIS msgs` in heartbeat increases over time |
| Vessels tracked | `Vessels` > 0 within a few minutes of connection |
| Zone counts | PG + Strait + Oman ≈ total vessels |
| Crossings | Enter/exit alerts when tankers cross 56.25°E |
| MarineTraffic match | Similar vessel counts; MMSIs and positions align |

---

## Known limitations

1. **Ship type:** We need static data (ShipStaticData) for tanker type. Until then, vessels are tracked but may not trigger crossing alerts.
2. **AIS gaps:** Vessels can turn off AIS or be affected by GPS jamming; we only see what is transmitted.
3. **Zone boundaries:** Our zones are lon-based and simplified; they do not follow exact IHO polygons.
4. **Update rate:** AIS position reports are typically every few seconds to minutes; our data is as fresh as the last report per vessel.

---

## Validate against reference data

### From a JSON file

If you have vessel data from MarineTraffic or similar (e.g. from the browser Network tab):

```bash
python validate_zones.py reference_example.json
```

### From MarineTraffic tile URL

MarineTraffic’s web map uses a tile API. Fetch the URL from their Strait of Hormuz page:

1. Open [MarineTraffic Strait of Hormuz](https://www.marinetraffic.org/hormuz-strait/ship-traffic-tracker)
2. Open DevTools (F12) → Network tab
3. Filter by "get_data" or "json"
4. Copy the `get_data_json_4` request URL (right-click → Copy URL)
5. Run: `python validate_zones.py "PASTE_URL_HERE"`

**Note:** Tile X:22 Y:14 (z6) returns the Arabian Sea, not the Strait. Use the URL from the MarineTraffic page when you’re viewing the Strait of Hormuz.

If the fetch fails (auth/cookies), save the response body as `marinetraffic.json` and run:

```bash
python validate_zones.py marinetraffic.json
```

### Compare with our app

The script prints how our zone logic classifies each vessel. Compare with `GET /stats` from the running app.

---

## Debugging

- **`GET /stats`** – Current counts and message stats
- **`GET /ledger`** – All tracked vessels with positions
- **`GET /activity?hours=1`** – Recent position history
- **Terminal logs** – Every Telegram message is logged before sending
