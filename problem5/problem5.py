import random
import time

def generate_data():
    # Generate R1 with specified values
    R1 = [(i, 5) for i in range(1, 1001)] + [(i, 7) for i in range(1001, 2001)] + [(2001, 2002)]
    random.shuffle(R1)  # Ensure random permutation

    # Generate R2 with specified values
    R2 = [(5, i) for i in range(1, 1001)] + [(7, i) for i in range(1001, 2001)] + [(2002, 8)]
    random.shuffle(R2)  # Ensure random permutation

    # Generate R3 with random values within specified range
    R3 = [(random.randint(2002, 3000), random.randint(1, 3000)) for _ in range(2000)] + [(8, 30)]
    random.shuffle(R3)  # Ensure random permutation

    return {"R1": R1, "R2": R2, "R3": R3}

def hash_join(Ra, Rb, key_index_a, key_index_b):
    hash_map = {}
    result = []
    # Create hash map based on join key of Ra
    for a in Ra:
        hash_map.setdefault(a[key_index_a], []).append(a)
    # Join Ra and Rb based on join key of Rb
    for b in Rb:
        if b[key_index_b] in hash_map:
            for a in hash_map[b[key_index_b]]:
                result.append(a + b)
    return result

def perform_joins(relations):
    # Start timing the join process
    start_time = time.time()
    
    # Join R1 with R2 where R1.x = R2.y
    R1_R2 = hash_join(relations['R1'], relations['R2'], 1, 0)
    
    # Join result with R3 where R2.j = R3.x
    final_result = hash_join(R1_R2, relations['R3'], 3, 0)
    
    # End timing the join process
    end_time = time.time()
    
    # Log the join times
    print(f"Total join time: {end_time - start_time:.6f} seconds")
    
    return [tuple(r[:2] + r[3:5] + r[6:]) for r in final_result]

# Generate the data
relations = generate_data()

# Perform the joins and measure total execution time
start_time_total = time.time()
result = perform_joins(relations)
end_time_total = time.time()

# Display the results
print(f"Total execution time: {end_time_total - start_time_total:.6f} seconds")
print(f"Number of outgoing tuples: {len(result)}")
for line in result[:5]:  # Display first few results
    print(line)
