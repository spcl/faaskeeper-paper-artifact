
#!/bin/bash



for clients in 1 2 4 8 16 32 64; do
	python3 log_query.py --input benchmark_output_$clients.csv --output benchmark_processed_$clients
done

