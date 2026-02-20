import math
from typing import List
from dataclasses import dataclass
from core.pava import pava

@dataclass
class FairValueResult:
    strike: float
    prob: float

class QuantEngine:
    """
    Calculates moving Volatility (EWMA) and Computes Fair Value for a set of strikes,
    enforcing surface consistency via PAVA.
    """
    def __init__(self, decay_factor: float = 0.94):
        self.decay_factor = decay_factor
        self.variance = 0.0
        self.last_spot = None
        self.sigma = 0.0  # Annualized volatility
        self.samples_collected = 0
    
    def update_spot(self, price: float) -> float:
        """
        Computes 1-second sample EWMA Variance.
        Returns Annualized Volatility.
        """
        if self.last_spot is not None:
            ret = math.log(price / self.last_spot)
            self.variance = self.decay_factor * self.variance + (1 - self.decay_factor) * (ret ** 2)
            
            # Assuming ~1s updates on average. 31,536,000 seconds in a year
            self.sigma = math.sqrt(self.variance * 31536000)
            self.samples_collected += 1
            
        self.last_spot = price
        return self.sigma

    def compute_fair_values(self, spot: float, strikes: List[float], tte_years: float) -> List[FairValueResult]:
        """
        Computes Binary Call probabilities: P(S_T > K)
        Applies Isotonic Regression (PAVA) to ensure P(K1) >= P(K2) >= P(K3)...
        """
        raw_probs = []
        # Fallback vol if we haven't warmed up yet
        sigma = self.sigma if self.sigma > 0 else 0.50 
        
        for K in strikes:
            if tte_years <= 0 or spot <= 0 or sigma <= 0:
                raw_probs.append(0.5)
                continue
                
            # Black-Scholes d2 for binary option probability under Risk-Neutral measure
            # d2 = (ln(S/K) - 0.5 * sigma^2 * T) / (sigma * sqrt(T))
            d2 = (math.log(spot / K) - 0.5 * (sigma ** 2) * tte_years) / (sigma * math.sqrt(tte_years))
            
            # N(d2) approximation using erf
            prob = 0.5 * (1.0 + math.erf(d2 / math.sqrt(2.0)))
            raw_probs.append(prob)
            
        # Guarantee Surface Consistency (Guardrail #4 style internal math safety)
        # Probabilities must be monotonically decreasing as K increases.
        projected_probs = pava(raw_probs)
        
        results = []
        for K, p in zip(strikes, projected_probs):
            results.append(FairValueResult(strike=K, prob=p))
            
        return results
