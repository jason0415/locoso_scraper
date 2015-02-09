# Before run this script, we need to run main_get_root_cat_urls.
# encoding: utf-8
require './scraper'

THREAD_POOL_SIZE = 100

scraper = Scraper.new

leaf_cat_urls = scraper.get_leaf_cat_urls

threads = []
queue = Queue.new
leaf_cat_urls.each do |leaf_cat_url|
  queue << leaf_cat_url
end

THREAD_POOL_SIZE.times do
  threads << Thread.new do
    begin
      # We pass true to the pop method to indicate that we expect the method
      # to raise an error if the queue is empty.
      # Passing false (the default argument), would cause this method to
      # block waiting on the queue to have at least 1 item (block forever).
      while leaf_cat_url = queue.pop(true)
        scraper.save_listing_urls(leaf_cat_url)
      end
    rescue ThreadError
      # puts "#{Thread.current} - Exit!"
    end
  end
end
threads.each { |t| t.join }