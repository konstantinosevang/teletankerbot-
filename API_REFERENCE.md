# AISStream API Reference (from aisstream.io documentation)

## Connection

- **URL:** `wss://stream.aisstream.io/v0/stream`
- **Auth:** API key in subscription message (required)
- **Timeout:** Subscription message must be sent within **3 seconds** of connecting or connection is closed

## Subscription Message Format

```json
{
  "APIKey": "<your api key>",
  "BoundingBoxes": [[[lat1, lon1], [lat2, lon2]]],
  "FiltersShipMMSI": ["368207620", "367719770"],
  "FilterMessageTypes": ["PositionReport", "ShipStaticData"]
}
```

### Bounding Box Format

`[[[lat1, lon1], [lat2, lon2]]]`

- **Latitude:** -90.0 to 90.0
- **Longitude:** -180.0 to 180.0
- Order of corners does not matter
- Multiple boxes allowed (no duplicate data)

### FilterMessageTypes (optional)

Supported: `PositionReport`, `ShipStaticData`, `ExtendedClassBPositionReport`, `StandardClassBPositionReport`, `StaticDataReport`, `UnknownMessage`, `AddressedSafetyMessage`, etc.

### FiltersShipMMSI (optional)

Max 50 MMSI values, string format: `["123456789", ...]`

## Response Message Format

Each message:

```json
{
  "MessageType": "PositionReport",
  "MetaData": {
    "MMSI": 259000420,
    "ShipName": "AUGUSTSON",
    "latitude": 66.02695,
    "longitude": 12.253821,
    "time_utc": "2022-12-29 18:22:32.318353 +0000 UTC"
  },
  "Message": {
    "PositionReport": {
      "UserID": 259000420,
      "Latitude": 66.02695,
      "Longitude": 12.253821,
      "Cog": 308,
      "Sog": 0,
      "Valid": true,
      ...
    }
  }
}
```

- **MessageType:** Type of AIS message
- **MetaData:** Extra info (ship name, last position, etc.) – key may be `MetaData` or `Metadata`
- **Message:** Object with key = MessageType, value = message payload
- **UserID:** MMSI (integer)

## Error Response

```json
{ "error": "Api Key Is Not Valid" }
```

Always check for `"error"` in the response.

## Message Types Used for Tanker Tracking

| Type | Purpose |
|------|---------|
| PositionReport | Class A position (has UserID, Latitude, Longitude) |
| ShipStaticData | Name, Type, dimensions (UserID, Name, Type) |
| ExtendedClassBPositionReport | Class B extended (UserID, Lat, Lon, Name, Type) |
| StandardClassBPositionReport | Class B standard (UserID, Lat, Lon) |
| StaticDataReport | Static data in parts (ReportA=name, ReportB=type) |

## Tanker Ship Types (AIS)

Types 80–89 = Tanker
