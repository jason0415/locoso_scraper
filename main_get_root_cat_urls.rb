require './scraper'
require 'benchmark'

# root_urls = ['http://www.locoso.com/cate/s6662']
# Scraper.get_leaf_urls_store_in_file(root_urls)

# Benchmark.bm do |bm|
#   bm.report { puts Scraper.get_root_cat_urls }
# end
THREAD_POOL_SIZE = 100

scraper = Scraper.new

root_cat_urls = scraper.root_cat_urls

# Method 1
# Single thread
# root_cat_urls.each do |root_cat_url|
#   scraper.leaf_cat_urls(root_cat_url)
# end

# Method 2
# Multi-thread
threads = []
queue = Queue.new
root_cat_urls.each do |root_cat_url|
  queue << root_cat_url
end

THREAD_POOL_SIZE.times do
  threads << Thread.new do
    begin
      # We pass true to the pop method to indicate that we expect the method
      # to raise an error if the queue is empty.
      # Passing false (the default argument), would cause this method to
      # block waiting on the queue to have at least 1 item (block forever).
      while root_cat_url = queue.pop(true)
        scraper.save_leaf_cat_urls(root_cat_url)
      end
    rescue ThreadError
      # puts "#{Thread.current} - Exit!"
    end
  end
end
threads.each { |t| t.join }