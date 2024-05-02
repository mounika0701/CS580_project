# Problem 2
from collections import defaultdict

def hash_join(Ra, Rb):
    hash_map = defaultdict(list)
    for b in Rb:
        hash_map[b[0]].append(b)
    result = []
    for a in Ra:
        if a[1] in hash_map:
            for b in hash_map[a[1]]:
                result.append(a + b[1:])
    return result

def evaluate_k_line_join_query(query, relations):
    S = None
    for relation in query:
        projection = [(tuple_[i], tuple_[i+1]) for tuple_ in relations[relation] for i in range(len(tuple_)-1)]
        if S is None:
            S = projection
        else:
            S = hash_join(S, projection)
    return S

# Example usage
query = ["R1", "R2", "R3"]
relations = {
    "R1": [(1, 2), (2, 3), (3, 4)],
    "R2": [(2, 3), (3, 4), (4, 5)],
    "R3": [(3, 4), (4, 5), (5, 6)]
}

result = evaluate_k_line_join_query(query, relations)
print("Result of k-line join query:", result)
print("Number of tuples in the result:", len(result))
