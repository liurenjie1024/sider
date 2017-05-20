# Sider

Sider is an in-memory nosql database. The core of sider is a lock free, multi-reader, single-writer, linearizable, 
ABA-safe concurrent hash map.
 

## Current Status

Currently, only the lock free concurrent hash map has been implemented. Network support will be added in the future.