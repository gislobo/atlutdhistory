from timezonefinderL import TimezoneFinder

lat = float(input("Enter latitude: "))
lon = float(input("Enter longitude: "))
#lat, lon = 33.754335, -84.402323

tf = TimezoneFinder()
tz = tf.timezone_at(lng=lon, lat=lat)

print(tz)