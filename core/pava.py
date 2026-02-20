from typing import List

def pava(y: List[float], weights: List[float] = None) -> List[float]:
    """
    Pool-Adjacent-Violators Algorithm (PAVA) for isotonic regression.
    Enforces a monotonically non-increasing sequence (p1 >= p2 >= ... >= pn).
    This is used to project raw lognormal probabilities onto a consistent surface.
    """
    n = len(y)
    if n == 0:
        return []
    if weights is None:
        weights = [1.0] * n

    # Each block is a tuple: (value, weight, count)
    # We maintain a stack of such blocks.
    blocks = []

    for i in range(n):
        val = y[i]
        wt = weights[i]
        cnt = 1
        
        # While the current block violates the non-increasing constraint with the previous block
        # (i.e. prev_val < curr_val), merge them.
        while blocks and blocks[-1][0] < val:
            prev_val, prev_wt, prev_cnt = blocks.pop()
            # New merged value is the weighted average
            val = (prev_val * prev_wt + val * wt) / (prev_wt + wt)
            wt += prev_wt
            cnt += prev_cnt
            
        blocks.append((val, wt, cnt))

    # Reconstruct the projected array
    result = []
    for val, wt, cnt in blocks:
        result.extend([val] * cnt)
        
    return result
