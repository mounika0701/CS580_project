import random
import time

def generate_data():
    num_tuples = 100
    range_max = 100
    R1 = [(i, random.randint(1, range_max)) for i in range(1, num_tuples + 1)]
    R2 = [(random.randint(1, range_max), j) for j in range(1, num_tuples + 1)]
    R3 = [(ℓ, ℓ) for ℓ in range(1, num_tuples + 1)]
    return {"R1": R1, "R2": R2, "R3": R3}

def semi_join(Ra, Rb, join_on):
    projection_set = set([b[join_on[1]] for b in Rb])
    filtered_Ra = [a for a in Ra if a[join_on[0]] in projection_set]
    return filtered_Ra

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
    R2_semi = semi_join(relations['R2'], relations['R3'], (1, 0))
    R1_semi = semi_join(relations['R1'], R2_semi, (1, 0))
    R1_R2 = hash_join(R1_semi, R2_semi, (1, 0))
    final_result = hash_join(R1_R2, relations['R3'], (2, 0))
    end_time = time.perf_counter()
    print(f"Yannakakis joins - Total join time: {end_time - start_time:.6f} seconds")
    return [tuple(r[:2] + r[3:5] + r[6:]) for r in final_result]

def perform_sequential_joins(relations):
    start_time = time.perf_counter()
    result = relations['R1']
    for i in range(1, len(relations)):
        next_relation = relations[f'R{i+1}']
        result = hash_join(result, next_relation, (-1, 0))
    end_time = time.perf_counter()
    print(f"Sequential joins - Total join time: {end_time - start_time:.6f} seconds")
    return result

start_time_total = time.perf_counter()
relations = generate_data()

print("\n--- Optimized Joins (Yannakakis Algorithm) ---")
result_yannakakis = perform_yannakakis_joins(relations)
print(f"Number of outgoing tuples from Yannakakis joins: {len(result_yannakakis)}")

print("\n--- Sequential Joins ---")
result_sequential = perform_sequential_joins(relations)
print(f"Number of outgoing tuples from sequential joins: {len(result_sequential)}")

end_time_total = time.perf_counter()
print(f"\nTotal execution time for all operations: {end_time_total - start_time_total:.6f} seconds")
