import random
import time

def generate_data():
    num_tuples = 100
    range_max = 5000
    R1 = [(i, random.randint(1, range_max)) for i in range(1, num_tuples + 1)]
    R2 = [(random.randint(1, range_max), j) for j in range(1, num_tuples + 1)]
    R3 = [(ℓ, ℓ) for ℓ in range(1, num_tuples + 1)]
    return {"R1": R1, "R2": R2, "R3": R3}

def hash_join(Ra, Rb, join_on):
    hash_map = {}
    result = []
    # Build the hash map
    for a in Ra:
        hash_map.setdefault(a[join_on[0]], []).append(a)
    # Execute the join
    for b in Rb:
        if b[join_on[1]] in hash_map:
            for a in hash_map[b[join_on[1]]]:
                result.append(a + b)
    return result

def perform_joins(relations):
    # Join R1 with R2 on R1.x = R2.y
    start_time = time.time()
    R1_R2 = hash_join(relations['R1'], relations['R2'], (1, 0))
    mid_time = time.time()
    # Join (R1 R2) with R3 on R2.j = R3.ℓ
    final_result = hash_join(R1_R2, relations['R3'], (-1, 0))
    end_time = time.time()

    # Output join timing information
    print(f"Time to join R1 and R2: {mid_time - start_time:.6f} seconds")
    print(f"Time to join (R1 R2) and R3: {end_time - mid_time:.6f} seconds")
    print(f"Total join time: {end_time - start_time:.6f} seconds")

    return [tuple(r[:2] + r[3:5] + r[6:]) for r in final_result]

# Start of the main execution
start_time_total = time.time()

# Generate the data
relations = generate_data()

# Perform the optimized hash joins
result = perform_joins(relations)

end_time_total = time.time()
print(f"Total execution time: {end_time_total - start_time_total:.6f} seconds")

# Output the results
print(f"Number of outgoing tuples: {len(result)}")
for line in result[:5]:  # Print only the first 5 results for brevity
    print(line)
