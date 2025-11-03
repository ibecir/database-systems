# /opt/homebrew/Cellar/hbase/2.6.3/libexec/bin/hbase shell /Users/becir/Documents/BioAI/hbase-scripts/03_links.rb
# source /Users/becir/Documents/BioAI/hbase-scripts/03_links.rb
import 'org.apache.hadoop.hbase.client.ConnectionFactory'
import 'org.apache.hadoop.hbase.client.Put'
import 'org.apache.hadoop.hbase.client.Scan'
import 'org.apache.hadoop.hbase.util.Bytes'
import 'org.apache.hadoop.hbase.TableName'
import 'org.apache.hadoop.hbase.client.Durability'

def jbytes(*args)
  return args.map { |arg| arg.to_s.to_java_bytes }
end

connection = ConnectionFactory.createConnection(@hbase.configuration)
wiki_table = connection.getTable(TableName.valueOf('wiki'))
links_table = connection.getTable(TableName.valueOf('links'))

scanner = wiki_table.getScanner(Scan.new)
linkpattern = /\[\[([^\[\]\|\:\#][^\[\]\|:]*)(?:\|([^\[\]\|]+))?\]\]/
count = 0

while (result = scanner.next())
  title = Bytes.toString(result.getRow())
  text = Bytes.toString(result.getValue(*jbytes('text', '')))

  if text
    put_to = nil
    text.scan(linkpattern) do |target, label|
      unless put_to
        put_to = Put.new(*jbytes(title))
        put_to.setDurability(Durability::SKIP_WAL)
      end

      target.strip!
      target.capitalize!
      label = '' unless label
      label.strip! if label

      put_to.addColumn(*jbytes('to', target, label))

      put_from = Put.new(*jbytes(target))
      put_from.addColumn(*jbytes('from', title, label))
      put_from.setDurability(Durability::SKIP_WAL)
      links_table.put(put_from)
    end

    links_table.put(put_to) if put_to
  end

  count += 1
  puts "#{count} pages processed (#{title})" if count % 500 == 0
end

scanner.close()
wiki_table.close()
links_table.close()
connection.close()

exit
