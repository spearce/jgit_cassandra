create keyspace git_store ;
use git_store ;

create column family RepositoryIndex
  with column_type = 'Standard'
   and comparator  = 'UTF8Type' 
   and comment     = 'Associates repository name to internal identifier'
   and rows_cached = 1024
   and keys_cached = 1024
   ;

create column family Repository
  with column_type = 'Standard'
   and comparator  = 'AsciiType' 
   and comment     = 'High-level metadata about a repository'
   and keys_cached = 1024
   and rows_cached = 1024
   ;

create column family Ref
  with column_type = 'Standard'
   and comparator  = 'UTF8Type'
   and comment     = 'References (branch heads/tags)'
   and keys_cached = 1024
   and rows_cached = 1024
   ;

create column family Chunk
  with column_type = 'Standard'
   and comparator  = 'AsciiType'
   and comment     = 'Compressed pack chunks, and indexes'
   and keys_cached = 1024
   ;

create column family ObjectIndex
  with column_type   = 'Standard'
   and comparator    = 'AsciiType'
   and comment       = 'Global index mapping SHA-1 to chunk'
   and keys_cached   = 2000000
   and rows_cached   = 2000000
   ;
