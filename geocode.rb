require 'google-maps'
require 'open_street_map'
require 'sqlite3'

# A geolocator is a class that provides the 'query' method,
# which return tuple: 'provenance', date, latitude, logitude, message

# Query OpenStreetMap
class OpenStreetMapGeolocator
  def initialize(email = nil)
    @client = OpenStreetMap::Client.new
    @email = email
  end

  def query(q)
    # Detect a few different cases of query

    r = @client.search(q: q, format: 'json', accept_language: 'en', email: @email)
    #$stderr.puts("OpenStreetMaps(#{q}) -> #{r}")
    return ['open-street-map',
      Time.now,
      r[0]["lat"].to_f, r[0]["lon"].to_f,
      'success']
  end
end

class GoogleMapsGeolocator
  def initialize(api_key=nil)
    Google::Maps.configure do |config|
      config.authentication_mode = Google::Maps::Configuration::API_KEY
      config.api_key = api_key
    end
  end

  def query(q)
    begin
      r = Google::Maps.geocode(q)
      return ['google-maps',
              Time.now,
              r.first.latitude, r.first.longitude,
              'success']

    rescue Google::Maps::ZeroResultsException => e
      $stderr.puts "Zero results for '#{q}' because '#{e.to_s}'"
      return ['google-maps',
              Time.now,
              nil, nil,
              'zero results']
    end
  end
end

# Impose a rate limit on queries to an external provider
class ThrottleAdaptor
  def initialize(max_queries_per_second, locator)
    @query_interval = 1 / max_queries_per_second.to_f
    @last_time = nil
    @locator = locator
  end

  def query(q)
    now = Time.now
    unless nil == @last_time
      lapse = now - @last_time
      if lapse < @query_interval
        sleep( @query_interval - lapse )
      end
    end
    @last_time = now

    return @locator.query(q)
  end
end

# Rewrite a query w.r.t. various shorthands that appear
# in our Richmond data feed
class RichmondRephraser
  def initialize(locator)
    @locator = locator
  end

  def query(blurb)
    q = blurb.dup
    q.upcase!

    # A few entries have explicit lat/lon annotations
    if q =~ /LL\(ERROR!\): (.*)$/
      # And explicit lat/lon can fail, of course.
      q = $1.strip

    elsif q =~ /LL\((-?\d+):(\d+):(\d+\.\d+),(-?\d+):(\d+):(\d+\.\d+)\)/
      lat_deg, lat_min, lat_sec = $1, $2, $3
      lon_deg, lon_min, lon_sec = $4, $5, $6

      lat_sign = lon_sign = 1
      lat_sign = -1 if lat_deg.to_f < 0
      lon_sign = -1 if lon_deg.to_f < 0

      return ['explicit-annotation',
              Time.now,
              lat_deg.to_f + lat_sign*lat_min.to_f/60 + lat_sign*lat_sec.to_f/60/60,
              lon_deg.to_f + lon_sign*lon_min.to_f/60 + lon_sign*lon_sec.to_f/60/60,
              'success']

    elsif q =~ /LL\((-?\d+\.\d+),(-?\d+\.\d+)\)/
      lat_deg = $1.to_f
      lon_deg = $2.to_f

      return ['explicit-annotation',
              Time.now,
              lat_deg, lon_deg,
              'success']
    end

    # Try to clean up our blurb
    q.sub! /(\d+)-BLK/, '\1'
    q.sub! /:\s*ALIAS.*$/, ''
    q.sub! /\s*RICH$/, ''
    q.sub! '/', ' at '
    q.sub! /^RICH: /, ''
    q.sub! '&AMP;', ' and '
    q += ', Richmond, VA'

    return @locator.query(q)
  end
end

# In-memory caching of lookups
class MemCacheAdapter
  def initialize(locator)
    @locator = locator
    @cache = {}
  end

  def query(q)
    begin
      unless @cache.include? q
        res = @locator.query(q)
        @cache[q] = res
      end

    rescue => e
      # Don't cache failure
      return [nil, nil, nil, nil, nil]
    end

    return @cache[q]
  end
end

## # Persistent cache to a sqlite file
## class DiskCacheAdapter
##   def initialize(locator)
##     dbfile = File.expand_path "~/.rva-activecalls/geolocation.cache.sqlite3"
##     @db = SQLite3::Database.new dbfile
## 
##     @locator = locator
##   end
## 
##   def query(q)
##     @db.execute("SELECT provenance, query_time, latitude, longitude, FROM geolocation_cache WHERE location=? LIMIT 1") each do |row|
##       return [row[0], Time.parse(row[1]), row[2].to_f, row[3].to_f, "success" ]
##     end
## 
##     begin
##       res = @locator.query(q)
##       @db.execute("INSERT INTO geolocation_cache (location, provenance, query_time, latitude, longitude) VALUES (?, ?, ?, ?, ?)",
##         q, res[0], res[1], res[2], res[3])
## 
##     rescue SQLite3::ConstraintException => e
##       # Rejected duplicate entries are ok
## 
##     rescue => e
##       # Don't cache failure
##       res = [nil, nil, nil, nil, nil]
##     end
## 
##     return res
##   end
## end
