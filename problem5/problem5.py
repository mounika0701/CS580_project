import random
import time

def generate_data():
    R1 = [(i, 5) for i in range(1, 1001)] + [(i, 7) for i in range(1001, 2001)]
    R1.append((2001, 2002))
    random.shuffle(R1)  # Random permutation of R1

    R2 = [(5, i) for i in range(1, 1001)] + [(7, i) for i in range(1001, 2001)]
    R2.append((2002, 8))
    random.shuffle(R2)  # Random permutation of R2

    R3 = [(random.randint(2002, 3000), random.randint(1, 3000)) for _ in range(2000)]
    R3.append((8, 30))
    random.shuffle(R3)  # Random permutation of R3

    return {"R1": R1, "R2": R2, "R3": R3}

def hash_join(Ra, Rb, join_on):
    hash_map = {}
    result = []
    for a in Ra:
        hash_map.setdefault(a[join_on[0]], []).append(a)
    for b in Rb:
        if b[join_on[1]] in hash_map:
            for a in hash_map[b[join_on[1]]]:
                result.append(a + b)
    return result

def perform_yannakakis_joins(relations):
    start_time = time.perf_counter()
    R2_semi = hash_join(relations['R2'], relations['R3'], (1, 0))  # Direct join for this scenario
    R1_R2 = hash_join(relations['R1'], R2_semi, (1, 0))
    final_result = hash_join(R1_R2, relations['R3'], (2, 0))
    end_time = time.perf_counter()
    print(f"Yannakakis joins - Total join time: {end_time - start_time:.6f} seconds")
    return final_result

def perform_sequential_joins(relations):
    start_time = time.perf_counter()
    R1_R2 = hash_join(relations['R1'], relations['R2'], (1, 0))
    final_result = hash_join(R1_R2, relations['R3'], (2, 0))
    end_time = time.perf_counter()
    print(f"Sequential joins - Total join time: {end_time - start_time:.6f} seconds")
    return final_result

# Main execution
relations = generate_data()
print("\n--- Optimized Joins (Yannakakis Algorithm) ---")
result_yannakakis = perform_yannakakis_joins(relations)
print(f"Number of outgoing tuples from Yannakakis joins: {len(result_yannakakis)}")

print("\n--- Sequential Joins ---")
result_sequential = perform_sequential_joins(relations)
print(f"Number of outgoing tuples from sequential joins: {len(result_sequential)}")
