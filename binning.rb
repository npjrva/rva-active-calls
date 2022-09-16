#!/usr/bin/ruby
#
require 'json'
require 'sqlite3'
require_relative './geocode.rb'
require_relative './secrets.rb'

BIN_WIDTH=160
BIN_HEIGHT=100

VIEW_N=37.60281
VIEW_E=-77.385513
VIEW_S=37.446553
VIEW_W=-77.601173

N_CENTERS=10


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

type2category = {}
categories={}
File.open("call-types.csv").each_line do |line|
  if line =~ /^\d+,"?(.*?)"?,(\w+)$/
    type = $1
    cat = $2 #.upcase
    type2category[type] = cat
    categories[cat] = 1
  end
end

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

first_time, last_time, count_events= nil
db.execute("SELECT min(time_received), max(time_received), count(digest) " +
           "FROM calls "+
           ";"
           ).each do |row|
  first_time, last_time, count_events = row
end


db.execute("SELECT location, call_type "+
           "FROM calls "+
           #"LIMIT 5000 "+
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

count_nonempty_bins = 0
File.open("web/rva-geojson.js","w") do |jsout|
  jsout.puts "var rvaFirst = '#{first_time}';"
  jsout.puts "var rvaLast = '#{last_time}';"
  jsout.puts "var rvaCount = '#{count_events}';"
  jsout.puts 'var rvaData = {"type":"FeatureCollection","features":['
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

        nn = VIEW_S + (y+1) * (VIEW_N - VIEW_S) / BIN_HEIGHT
        ss = VIEW_S + (y+0) * (VIEW_N - VIEW_S) / BIN_HEIGHT

        ee = VIEW_W + (x+0) * (VIEW_E - VIEW_W) / BIN_WIDTH
        ww = VIEW_W + (x+1) * (VIEW_E - VIEW_W) / BIN_WIDTH

        name = ( bins2locs[bin] || {}).keys.join "; "

        json ={'type' => 'Feature',
               'id' => "#{x}_#{y}",
               'geometry' => {'type' => 'Polygon',
                              'coordinates' => [[ [ee,nn], [ww,nn], [ww,ss], [ee,ss], [ee,nn] ]]},
               'properties' => {'name' => name,
                                'total' => sums,
                               }
              }


        if histo.empty?
          fout.puts "\tempty"
        else
          cat2count = {}
          cat2evt = {}

          histo.each_pair do |type,count|
            raise "Uncategorized type '#{type}'" unless type2category.include? type
            cat = type2category[type]

            cat2count[cat] ||= 0
            cat2count[cat]  += count

            cat2evt[cat] ||= []
            cat2evt[cat].append( [type,count] )
          end

          cat2count.to_a.sort { |a,b| b[-1] <=> a[-1] }.each do |cat,cnt|
            next if 1 > cnt

            fout.puts(sprintf("\t%5d %s", cnt, cat))
            json['properties'][cat] = cnt

            cat2evt[cat].sort { |a,b| b[-1] <=> a[-1] }.each do |type,cnt|
              fout.puts(sprintf("\t\t%5d %s", cnt, type))
            end
          end
        end

        fout.puts "\n"


        jsout.puts( JSON.dump(json) )
        jsout.puts ","

      end
    end
  end
  jsout.puts ']}'
end

$stderr.puts "See #{count_nonempty_bins} bins in bins.txt"

