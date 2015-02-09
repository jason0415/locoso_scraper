require 'nokogiri'
require 'open-uri'
require 'sequel'
require 'jdbc/mysql'

class Scraper

  class UrlLevel
    attr_accessor :url, :level
    def initialize(url, level)
      @url, @level = url, level
    end
  end

  ROOT_URL = 'http://www.locoso.com/catelist'
  MAX_RETRY = 10
  # MAX_LEVEL = 2 # Maximum level of BFS (starts from 1)
  THREAD_POOL_SIZE = 100

  def initialize
    @db = Sequel.connect('jdbc:mysql://localhost/scraper?user=root&password=')
  end

  def root_cat_urls
    doc = Nokogiri::HTML(open('http://www.locoso.com/catelist'))
    doc.css('a.cBlue03').collect { |link| link['href'] }
  end

  # BFS - get all leaf urls
  def save_leaf_cat_urls(root_cat_url)
    cat_urls_table = @db[:cat_urls]
    queue = Queue.new
    retry_cnt = 0 # Any local variables created within this block are accessible to only this thread.
    begin
      doc = Nokogiri::HTML(open(root_cat_url))
      sub_cat_list = doc.css('div.xiaofenlei_zhong01c2 a.cBlue3')
      if sub_cat_list.empty?
        cat_urls_table.insert(url: root_cat_url, created_at: Time.now, updated_at: Time.now)
      else
        sub_cat_list.each do |sub_link|
          queue << "http://www.locoso.com#{sub_link['href']}"
        end
      end
    rescue
      # puts "problem on page #{root_cat_url}, retry..."
      retry_cnt += 1
      if retry_cnt <= MAX_RETRY
        sleep(1)
        retry
      end
      puts "Retry exceeds #{MAX_RETRY} times, failed on page #{root_cat_url}"
    end

    threads = []
    THREAD_POOL_SIZE.times do
      threads << Thread.new do
        begin
          # We pass true to the pop method to indicate that we expect the method
          # to raise an error if the queue is empty.
          # Passing false (the default argument), would cause this method to
          # block waiting on the queue to have at least 1 item (block forever).
          while link = queue.pop(true)
            retry_cnt = 0 # Any local variables created within this block are accessible to only this thread.
            begin
              doc = Nokogiri::HTML(open(link))
              sub_cat_list = doc.css('div.xiaofenlei_zhong01c2 a.cBlue3')
              if sub_cat_list.empty?
                cat_urls_table.insert(url: link, created_at: Time.now, updated_at: Time.now)
              else
                sub_cat_list.each do |sub_link|
                  queue << "http://www.locoso.com#{sub_link['href']}"
                end
              end
            rescue
              # puts "problem on page #{link}, retry..."
              retry_cnt += 1
              if retry_cnt <= MAX_RETRY
                sleep(1)
                retry
              end
              puts "Retry exceeds #{MAX_RETRY} times, failed on page #{link}"
            end
          end
        rescue ThreadError
          # puts "#{Thread.current} - Exit!"
        end
      end
    end
    threads.each { |t| t.join }
  end

  # get all leaf cat urls from database
  def get_leaf_cat_urls
    cat_urls_table = @db[:cat_urls]
    cat_urls_table.map(:url)
  end

  def save_listing_urls(leaf_cat_url)
    listing_urls_table = @db[:listing_urls]
    queue = Queue.new
    queue << leaf_cat_url
    until queue.empty?
      link = queue.pop
      retry_cnt = 0
      begin
        doc = Nokogiri::HTML(open(link))
        categories = []
        doc.css('div.xiaofenlei_zhong01c1 a').each do |my_link|
          categories << my_link['title']
        end
        listings = []
        doc.css('div.lb01').each do |listing|
          json = {}
          json[:title] = listing.css('a').empty? ? '' : listing.css('a').first['title']
          json[:description] = ''
          json[:category] = categories.join('->')
          json[:address] = ''
          json[:phone] = ''
          json[:created_at] = Time.now
          json[:updated_at] = Time.now
          listings << json
        end
        unless listings.empty?
          listing_urls_table.multi_insert(listings)
        end
        next_page_url = ''
        next_page_url = doc.css('div.page02 a')[1]['href'] unless doc.css('div.page02 a').empty?
        unless next_page_url == '' || next_page_url == 'javascript:;'
          next_page_url = 'http://www.locoso.com' + next_page_url
          queue << next_page_url
          # save_listing_urls(next_page_url)
        end
      rescue
        # puts "problem on page #{link}, retry..."
        retry_cnt += 1
        if retry_cnt <= MAX_RETRY
          sleep(1)
          retry
        end
        puts "Retry exceeds #{MAX_RETRY} times, failed on page #{link}"
      end
    end
  end

  # BFS - get urls by max level
  # def self.cat_urls_by_max_level(root_cat_urls=[])
  #   cat_urls_table = @db[:cat_urls]
  #   threads = []
  #   queue = Queue.new
  #   root_cat_urls.each do |root_cat_url|
  #     queue << UrlLevel.new(root_cat_url, 1)
  #   end
  #   THREAD_POOL_SIZE.times do
  #     threads << Thread.new do
  #       begin
  #         # We pass true to the pop method to indicate that we expect the method
  #         # to raise an error if the queue is empty.
  #         # Passing false (the default argument), would cause this method to
  #         # block waiting on the queue to have at least 1 item (block forever).
  #         while url_level = queue.pop(true)
  #           retry_cnt = 0 # Any local variables created within this block are accessible to only this thread.
  #           begin
  #             doc = Nokogiri::HTML(open(url_level.url))
  #             sub_cat_list = doc.css('div.xiaofenlei_zhong01c2 a.cBlue3')
  #             if sub_cat_list.empty? || url_level.level == MAX_LEVEL # leaf node or reach the max level
  #               # puts "#{Thread.current} - #{url_level.url}"
  #               cat_urls_table.insert(url: url_level.url, created_at: Time.now, updated_at: Time.now)
  #             else
  #               sub_cat_list.each do |sub_link|
  #                 queue << UrlLevel.new("http://www.locoso.com#{sub_link['href']}", url_level.level+1)
  #               end
  #             end
  #           rescue
  #             puts "problem on page #{url_level.url}, retry..."
  #             retry_cnt += 1
  #             if retry_cnt <= MAX_RETRY
  #               sleep(1)
  #               retry
  #             end
  #             puts "Retry exceeds #{MAX_RETRY} times, failed on page #{url_level.url}"
  #           end
  #         end
  #       rescue ThreadError
  #         puts "#{Thread.current} - Exit!"
  #       end
  #     end
  #   end
  #   threads.each { |t| t.join }
  # end

end