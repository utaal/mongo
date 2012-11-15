t = db.geo_s2near
t.drop();

// FYI:
// One degree of long @ 0 is 111km or so.
// One degree of lat @ 0 is 110km or so.
lat = 0
lng = 0
points = 10
for (var x = -points; x < points; x += 1) {
    for (var y = -points; y < points; y += 1) {
        t.insert({geo : { "type" : "Point", "coordinates" : [lng + x/1000.0, lat + y/1000.0]}})
    }
}

origin = { "type" : "Point", "coordinates": [ lng, lat ] }

t.ensureIndex({ geo : "s2d" })

res = t.find({ "geo" : { "$newnear" : { "$geometry" : origin, $maxDistance: 2000} } }).limit(10)
assert.eq(res.itcount(), 10)

res = t.find({ "geo" : { "$newnear" : { "$geometry" : origin } } }).limit(10)
assert.eq(res.itcount(), 10)

// Find all the points!
res = t.find({ "geo" : { "$newnear" : { "$geometry" : origin } } }).limit(10000)
assert.eq(res.itcount(), (2 * points) * (2 * points))

// longitude goes -180 to 180
// latitude goes -90 to 90
// Let's put in some perverse (polar) data and make sure we get it back.
// Points go long, lat.
t.insert({geo: { "type" : "Point", "coordinates" : [-180, -90]}})
t.insert({geo: { "type" : "Point", "coordinates" : [180, -90]}})
t.insert({geo: { "type" : "Point", "coordinates" : [180, 90]}})
t.insert({geo: { "type" : "Point", "coordinates" : [-180, 90]}})
res = t.find({ "geo" : { "$newnear" : { "$geometry" : origin } } }).limit(10000)
assert.eq(res.itcount(), (2 * points) * (2 * points) + 4)
