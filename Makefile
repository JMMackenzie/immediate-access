all:
	g++ --std=c++17 -march=native -Wall -Wextra -O3 stream_index.cpp -o bin/stream_index
	g++ --std=c++17 -march=native -Wall -Wextra -O3 conjunctive_query.cpp -o bin/conjunctive_query
	g++ --std=c++17 -march=native -Wall -Wextra -O3 disjunctive_query.cpp -o bin/disjunctive_query

debug:
	g++ --std=c++17 -march=native -Wall -Wextra -g stream_index.cpp -o bin/d_stream_index
	g++ --std=c++17 -march=native -Wall -Wextra -g query.cpp -o bin/d_query
	g++ --std=c++17 -march=native -Wall -Wextra -g disjunctive_query.cpp -o bin/d_disjunctive_query

clean:
	rm bin/stream_index bin/d_stream_index bin/conjunctive_query bin/d_conjunctive_query bin/disjunctive_query bin/d_disjunctive_query
