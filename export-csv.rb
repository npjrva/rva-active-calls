#!/usr/bin/ruby
#
require 'csv'
require 'sqlite3'
require 'date'
require_relative './geocode.rb'
require_relative './secrets.rb'

BIN_WIDTH=160
BIN_HEIGHT=100

VIEW_N=37.60281
VIEW_E=-77.385513
VIEW_S=37.446553
VIEW_W=-77.601173

N_CENTERS=10

first_time = '2023-01-01 00:00:00'
last_time = '2024-01-01 00:00:00'

dbfile = File.expand_path "~/.rva-activecalls/db.sqlite3"
cachefile = File.expand_path "~/.rva-activecalls/geolocation-cache.sqlite3"

db = SQLite3::Database.new dbfile

geolocator = RichmondRephraser.new(
              MemCacheAdapter.new(
                DiskCacheAdapter.new(cachefile,
                    PrioritizedGeolocator.new([
                      BoundingBoxFilter.new( OpenStreetMapGeolocator.new(email=OPEN_STREET_MAPS_EMAIL)),
                      BoundingBoxFilter.new(ThrottleAdaptor.new(50,
                        GoogleMapsGeolocator.new(api_key=GOOGLE_MAPS_API_KEY)))
                    ]))))

#db.trace {|sql| $stderr.puts "Query #{sql}"}

count_failed_geolocations = 0
db.execute("SELECT time_received, agency, dispatch_area, unit, call_type, location, status "+
           "FROM calls "+
           "WHERE ? <= time_received "+
           "AND time_received < ? "+
           "ORDER BY time_received ASC "+
           #"LIMIT 10 "+
           ";", first_time, last_time
           ).each do |row|

  location = row[5]

  #$stderr.puts "Location #{location}"

  result = geolocator.query(location)
  if nil == result[0]
    row << "" # lat
    row << "" # lon
  else
    row << result[2].to_s
    row << result[3].to_s
  end

  print CSV.generate_line(row, force_quotes: true)
end


