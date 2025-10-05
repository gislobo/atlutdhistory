from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter

def geocode_address(address: str) -> tuple[float, float] | None:

    geolocator = Nominatim(user_agent="gislobo")  # set a descriptive app name
    geocode = RateLimiter(geolocator.geocode, min_delay_seconds=1)  # be polite with OSM
    location = geocode(address, exactly_one=True, addressdetails=False)
    if location is None:
        return None
    return location.latitude, location.longitude


addr = input("Enter address: ")
coords = geocode_address(addr)
if coords:
    lat, lon = coords
    print(f"Latitude: {lat}, Longitude: {lon}")
else:
    print("Address not found.")