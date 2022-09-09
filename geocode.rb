require 'google-maps'
require 'open_street_map'
require 'sqlite3'

# A geolocator is a class that provides the 'query' method,
# which return tuple: 'provenance', date, latitude, logitude, message

# Reject crazy responses from geolocation APIs based
# on a rough bounding-box for Richmond
class BoundingBoxFilter
  N=37.7982
  E=-77.1171
  S=37.1552
  W=-77.8422

  def initialize(geolocator)
    @geolocator = geolocator

    @stat_count_queries = 0
    @stat_count_reject = 0
  end

  attr_reader :stat_count_queries, :stat_count_reject

  def query(q)
    @stat_count_queries += 1
    r = @geolocator.query(q)

    if nil != r[0]
      if r[2] > N || r[3] > E || r[2] < S || r[3] < W
        @stat_count_reject += 1
        $stderr.puts "BBox rejects #{r.join ', '}"
        return [nil,nil,nil,nil,nil]
      end
    end

    return r
  end
end

# Try some number of geolocation strategies in order
class PrioritizedGeolocator
  def initialize(elts)
    @elts = elts
  end

  def query(q)
    @elts.each do |geolocator|
      begin
        res = geolocator.query(q)
        if nil != res[0]
          return res
        end

      rescue => e
        # meh, try the next one
      end
    end

    return [nil,nil,nil,nil,nil]
  end
end

# Query OpenStreetMap
class OpenStreetMapGeolocator
  def initialize(email = nil)
    @client = OpenStreetMap::Client.new
    @email = email
    @stat_count_queries = 0
    @stat_count_hits = 0
  end

  attr_reader :stat_count_queries, :stat_count_hits

  def query(q)
    @stat_count_queries += 1

    r = @client.search(q: q, format: 'json', accept_language: 'en', email: @email)

    #$stderr.puts("OpenStreetMaps(#{q}) -> #{r}")
    @stat_count_hits += 1
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

    @stat_count_queries = 0
    @stat_count_hits = 0
  end

  attr_reader :stat_count_queries, :stat_count_hits

  def query(q)
    @stat_count_queries += 1
    begin
      r = Google::Maps.geocode(q)
      @stat_count_hits += 1
      return ['google-maps',
              Time.now,
              r.first.latitude, r.first.longitude,
              'success']

    rescue Google::Maps::ZeroResultsException => e
      $stderr.puts "Zero results for '#{q}' because '#{e.to_s}'"
    end

    return [nil,nil,nil,nil,nil]
  end
end

# Impose a rate limit on queries to an external provider
class ThrottleAdaptor
  def initialize(max_queries_per_second, locator)
    @query_interval = 1 / max_queries_per_second.to_f
    @last_time = nil
    @locator = locator

    @stat_total_throttle = 0
  end

  attr_reader :stat_total_throttle

  def query(q)
    now = Time.now
    unless nil == @last_time
      lapse = now - @last_time
      if lapse < @query_interval
        throttle = @query_interval - lapse
        sleep( throttle )
        @stat_total_throttle += throttle
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
      lat_deg, lat_min, lat_sec = $4, $5, $6 # Not a bug, they list longitude first!
      lon_deg, lon_min, lon_sec = $1, $2, $3

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

    @stat_count_queries = 0
    @stat_count_hits = 0
  end

  attr_reader :stat_count_queries, :stat_count_hits

  def query(q)
    @stat_count_queries += 1

    begin
      if @cache.include? q
        @stat_count_hits += 1

      else
        res = @locator.query(q)
        @cache[q] = res[0...4] unless res[0] == nil
        return res
      end

    rescue => e
      # Don't cache failure
      return [nil, nil, nil, nil, nil]
    end

    return @cache[q] + ["mem-cache-hit"]
  end
end

# Persistent cache to a sqlite file
class DiskCacheAdapter
  def initialize(dbfile, locator)
    @db = SQLite3::Database.new dbfile
    @locator = locator

    @stat_count_queries = 0
    @stat_count_hits = 0

    #@db.trace { |sql| $stderr.puts "TRACE #{sql}" }
  end

  attr_reader :stat_count_queries, :stat_count_hits

  def query(q)
    @stat_count_queries += 1

    begin
      @db.execute("SELECT provenance, query_time, latitude, longitude "+
                  "FROM geolocation_cache "+
                  "WHERE location=? "+
                  "LIMIT 1", [q]).each do |row|
        @stat_count_hits += 1
        return [row[0], Time.parse(row[1]), row[2].to_f, row[3].to_f, "disk-cache-hit" ]
      end
      #$stderr.puts "Cache miss: #{q} and #{@db.errmsg}"

    rescue => e
      $stderr.puts "Cache lookup failed: #{e.to_s}"
    end

    res = [nil, nil, nil, nil, nil]
    begin
      res = @locator.query(q)
      if nil == res[0]
        return res # Don't cache failure
      end

    rescue => e
      return [nil,nil,nil,nil,nil] # Don't cache failure
    end

    begin
      @db.execute("INSERT INTO geolocation_cache (location, provenance, query_time, latitude, longitude) VALUES (?, ?, ?, ?, ?)",
        q, res[0], res[1].strftime("%Y-%m-%d %H:%M:%S"), res[2], res[3])

    rescue SQLite3::ConstraintException => e
      # Rejected duplicate entries are ok
      #$stderr.puts "Cache duplicate #{q}"

    rescue => e
      $stderr.puts "Cache insert failed: #{e.to_s}"
    end

    return res
  end
end
