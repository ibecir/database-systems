import 'org.apache.hadoop.hbase.TableName'
import 'org.apache.hadoop.hbase.client.ConnectionFactory'
import 'org.apache.hadoop.hbase.client.Put'

# Helper: convert Ruby strings to Java bytes
def jbytes(*args)
  args.map { |arg| arg.to_s.to_java_bytes }
end

# Create a connection and get table reference
conn = ConnectionFactory.createConnection(@hbase.configuration)
table = conn.getTable(TableName.valueOf("wiki"))

# Create the Put for row "Home"
p = Put.new(*jbytes("Home"))
p.addColumn(*jbytes("text", "", "Hello world"))
p.addColumn(*jbytes("revision", "author", "jimbo"))
p.addColumn(*jbytes("revision", "comment", "my first edit"))

# Write to HBase
table.put(p)

# Clean up resources
table.close
conn.close
