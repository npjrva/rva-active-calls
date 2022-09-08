#!/usr/bin/ruby
#
require 'sqlite3'
require_relative './geocode.rb'
require_relative './secrets.rb'

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

count_success = 0
count_failure = 0

File.open('failures.txt', 'w') do |fout|
  db.execute("SELECT location FROM calls ORDER BY time_received DESC LIMIT 3000;").each do |row|
    l = row[0]
    result = geolocator.query(l)
    puts "#{l} -> #{result}"

    if nil != result[0]
      count_success += 1
    else
      count_failure += 1
      fout.puts l
    end
  end
end

puts "#{count_success} successful / #{count_failure} failed geolocations; wrote 'failures.txt'"

