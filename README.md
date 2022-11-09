# Immediate Indexing

## Document Format
Documents are assumed to be in a simple **docstream** format which represents each document as a space separated series of tokens.
The first token is assumed to be the document identifier and is ignored. The remaining tokens will be ingested until the newline.
Tokens are assumed to be broken after 20 characters (bytes). All pre-processing such as case-folding and stemming should be
applied before indexing.

## Query Format
Queries are assumed to be in the same format as the documents; the first token is read as an identifier and should be an integer.
The remaining tokens on the line (which should be seperated by space) are read as the query terms. Care should be taken to stem
and pre-process the query file in the same manner as the document file. Furthermore, duplicate terms should be removed from
within a query.

## Configuration
If you want to use variable blocks, uncomment line 17 of `util.hpp` and then modify the call at line 40 of `variable_immediate_index.hpp`
to select your desired growth scheme.

If you want to index positions or turn on index compacting, check the configuration flags on lines 13-14 of `stream_index.cpp`.

If you want to change the F value for the Double-VByte scheme, look at line 6 of `compress.hpp`. 

## Build the Code
You can simply run `make` and the binaries should be built in the `bin` directory.

## Indexing
You can build indexes with the `stream_index` binary:
```
./bin/stream_indexi
Usage: ./bin/stream_index [wsj1|robust|wiki] <output_file> < /path/to/docstream
```

The first argument is used to set some basic space estimations when initializing the index structure.
The second argument sets the output file handle.
Then, stdin is used to pipe a docstream file directly into the indexer.

```
head -c 200 /path/to/wsj1.docstream
WSJ870324-0001 hl john blair is near accord to sell unit sources say hl dd dd so wall street journal j so in rel tender offers mergers acquisitions tnm marketing advertising mkt telecommunications bro

./bin/stream_index wsj1 wsj1.idx < /path/to/wsj1.docstream
```

## Conjunctive Querying
To do Boolean conjunctions, you can use the `conjunctive_query` binary:
```
./bin/conjunctive_query 
Usage: ./bin/conjunctive_query <index> <query_file> [-v(v)]
```

The arguments are hopefully clear. Note that `-v` will output per-query latency and match counts; `-vv` enables detailed profiling.

## Ranked Disjunctive Querying
To do ranked (top-k) disjunctions:
```
./bin/disjunctive_query 
Usage: ./bin/disjunctive_query <index> <query_file> <k> <num_docs_in_index> [-v]
```

Again, hopefully clear. Note that `k` is the number of results to return; `num_docs_in_index` is required for normalization; `-v` outputs per-query latency and result counts.
