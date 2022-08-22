#!/usr/bin/ruby

require 'uri'
require 'net/http'
require 'sqlite3'
require 'digest'

u = URI "https://apps.richmondgov.com/applications/activecalls/Home/ActiveCalls?"
dbfile = File.expand_path "~/.rva-activecalls/db.sqlite3"

db = SQLite3::Database.new dbfile
r = Net::HTTP.get u

count_new = 0
count_old = 0

digester = Digest::SHA1.new
cols = []
r.each_line do |line|
  line.strip!
  if line =~ /<\/tr>/
    unless cols.empty?
      cols = [ digester.hexdigest ] + cols
      begin
        db.execute("INSERT INTO calls VALUES (?, ?, ?, ?, ?, ?, ?, ?)", cols)
        count_new += 1
      rescue SQLite3::ConstraintException => e
        # I am lazily relying on sqlite to de-duplicate the list.
        # A constraint exception sigificies that we've already
        # seen this call.
        count_old += 1
      end
      cols = [] # reset for next row
      digester = Digest::SHA1.new
    end
  elsif line =~ /<td(\snowrap)?>(.*?)<\/td>/
    cell = $2.strip
    cols << cell
    digester.update cell
  end
end

$stderr.puts "Scraped #{count_new} new calls and #{count_old} old calls"

