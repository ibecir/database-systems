# curl https://dumps.wikimedia.org/commonswiki/20251020/commonswiki-20251020-pages-articles-multistream1.xml-p1p1500000.bz2 | bzcat | /opt/homebrew/Cellar/hbase/2.6.3/libexec/bin/hbase shell /Users/becir/Documents/BioAI/hbase-scripts/02_importwiki.rb

require 'time'
import 'org.apache.hadoop.hbase.client.ConnectionFactory'
import 'org.apache.hadoop.hbase.client.Put'
import 'org.apache.hadoop.hbase.TableName'
import 'javax.xml.stream.XMLStreamConstants'

def jbytes(*args)
  args.map { |arg| arg.to_s.to_java_bytes }
end

factory = javax.xml.stream.XMLInputFactory.newInstance
reader = factory.createXMLStreamReader(java.lang.System.in)

document = nil
buffer = nil
count = 0

# HBase 2.x connection management
connection = ConnectionFactory.createConnection(@hbase.configuration)
table = connection.getTable(TableName.valueOf('wiki'))

begin
  while reader.has_next
    type = reader.next

    if type == XMLStreamConstants::START_ELEMENT
      case reader.local_name
      when 'page' then document = {}
      when /title|timestamp|username|comment|text/ then buffer = []
      end
    elsif type == XMLStreamConstants::CHARACTERS
      buffer << reader.text unless buffer.nil?
    elsif type == XMLStreamConstants::END_ELEMENT
      case reader.local_name
      when /title|timestamp|username|comment|text/
        document[reader.local_name] = buffer.join
      when 'revision'
        # Skip if title or timestamp is missing
        next if document['title'].nil? || document['timestamp'].nil?

        key = document['title'].to_java_bytes
        ts = (Time.parse document['timestamp']).to_i
        p = Put.new(key)

        # HBase 2.x uses addColumn instead of add
        # addColumn(family, qualifier, timestamp, value)
        # Handle nil values by using empty string
        text_value = (document['text'] || '').to_java_bytes
        author_value = (document['username'] || '').to_java_bytes
        comment_value = (document['comment'] || '').to_java_bytes

        p.addColumn(*jbytes("text", ""), ts, text_value)
        p.addColumn(*jbytes("revision", "author"), ts, author_value)
        p.addColumn(*jbytes("revision", "comment"), ts, comment_value)

        table.put(p)
        count += 1

        if count % 500 == 0
          puts "#{count} records inserted (#{document['title']})"
        end
      end
    end
  end
ensure
  table.close() if table
  connection.close() if connection
end

puts "Import complete: #{count} records inserted"
exit