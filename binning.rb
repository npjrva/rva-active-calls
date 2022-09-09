#!/usr/bin/ruby
#
require 'sqlite3'
require_relative './geocode.rb'
require_relative './secrets.rb'

MIN_TIME_INCLUSIVE = Time.parse("2022-08-17 12:08")
MAX_TIME_EXCLUSIVE = Time.parse("2022-09-17 12:08")

BIN_WIDTH=80
BIN_HEIGHT=50

VIEW_N=37.60281
VIEW_E=-77.385513
VIEW_S=37.446553
VIEW_W=-77.601173

N_CENTERS=5

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

def find_bin(lat,lon)
  bx =(BIN_WIDTH * (lon - VIEW_W) / (VIEW_E - VIEW_W)).to_i
  by =(BIN_HEIGHT * (lat - VIEW_S) / (VIEW_N - VIEW_S)).to_i

  raise "bx0" unless 0 <= bx
  raise "bxN #{lon} #{VIEW_W} #{VIEW_E} #{bx}" unless bx < BIN_WIDTH
  raise "by0" unless 0 <= by
  raise "byN" unless by < BIN_HEIGHT

  return [bx,by]
end

bins2locs = {}
bins2evts = {}

#db.trace {|sql| $stderr.puts "Query #{sql}"}

File.open('failures.txt', 'w') do |fout|
  db.execute("SELECT location, call_type "+
             "FROM calls "+
             #"LIMIT 10000 "+
             ";"
             ).each do |row|

    location = row[0]
    call_type = row[1]

    #$stderr.puts "Location #{location}"

    result = geolocator.query(location)
    next if nil == result[0]

    #$stderr.puts "--> latlon (#{result[2]}, #{result[3]})"

    bin = nil
    begin
      bin = find_bin(result[2], result[3])

    rescue Exception => e
      $stderr.puts "On #{location} result #{result.join ', '} is out of viewport"
      next
    end

    #$stderr.puts "Bin (#{bin.join ', '}) += #{call_type}"
    bins2locs[bin] ||= {}
    bins2locs[bin][location] = 1
    bins2evts[bin] ||= {}
    bins2evts[bin][call_type] ||= 0
    bins2evts[bin][call_type]  += 1
  end
end

count_nonempty_bins = 0
File.open("bins.txt", "w") do |fout|
  # {{{ ASCII-art map
  ranking = []
  BIN_HEIGHT.times do |y|
    BIN_WIDTH.times do |x|
      bin = [x,y]
      histo = bins2evts[bin] || {}
      sums = histo.values.sum

      ranking.append( [sums,bin] )
    end
  end
  ranking.sort!

  min = ranking[0][0]
  centers = []
  N_CENTERS.times do
    centers.append( ranking.pop )
  end
  max = ranking[-1][0]

  glyphs = ".oO@#&".split //

  BIN_HEIGHT.times do |yy|
    y = BIN_HEIGHT - yy - 1
    BIN_WIDTH.times do |x|
      bin = [x,y]
      histo = bins2evts[bin] || {}
      sums = histo.values.sum
      char = ' '
      if 0 < sums
        magnitude = [glyphs.size-1, (glyphs.size * (sums - min) / (max - min)).to_i].min
        char = glyphs[magnitude]
        centers.each_index do |idx|
          b2 = centers[idx][1]
          if bin == b2
            char = (65 + idx).chr
          end
        end
      end
      fout.print char
    end
    fout.puts '|'
  end

  centers.each_index do |idx|
    sums = centers[idx][0]
    bin = centers[idx][-1]
    char = (65 + idx).chr
    fout.puts "#{char} num events #{sums} at (#{bin[0]}, #{bin[1]})"
    ( bins2locs[bin] || {}).keys.each do |loc|
      fout.puts "\t#{loc}"
    end
    fout.puts


  end
 
  #}}}

  BIN_HEIGHT.times do |y|
    BIN_WIDTH.times do |x|
      bin = [x,y]
      next unless bins2evts.include? bin
      count_nonempty_bins += 1
      histo = bins2evts[bin] || {}
      sums = histo.values.sum

      fout.puts "Bin #{x}, #{y}       #{sums} events / #{histo.size} types"
      ( bins2locs[bin] || {}).keys.each do |loc|
        fout.puts "\t#{loc}"
      end
      fout.puts


      if histo.empty?
        fout.puts "\tempty"
      else
        # Sort descending by prevalence
        flat = histo.to_a.sort {|a,b| b[-1] <=> a[-1] }
        flat.each do |k,v|
          fout.puts(sprintf("\t%5d\t%s", v, k))
        end
      end

      fout.puts "\n"
    end
  end
end

$stderr.puts "See #{count_nonempty_bins} bins in bins.txt"

