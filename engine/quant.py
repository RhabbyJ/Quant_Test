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
    def __init__(self, decay_factor: float | None = None, half_life_seconds: float = 300.0):
        # If decay_factor is provided, use fixed-lambda legacy behavior.
        # Otherwise derive lambda from elapsed time and half-life.
        self.decay_factor = decay_factor
        self.half_life_seconds = max(1.0, float(half_life_seconds))
        self.variance = 0.0
        self.last_spot = None
        self.last_spot_ts_ms = None
        self.sigma = 0.0  # Annualized volatility
        self.samples_collected = 0
    
    def _lambda_for_dt(self, dt_sec: float) -> float:
        if self.decay_factor is not None:
            return float(self.decay_factor)
        # Exponential EWMA decay with target half-life.
        return math.exp(-math.log(2.0) * (dt_sec / self.half_life_seconds))

    def update_spot(self, price: float, exchange_ts_ms: int | None = None) -> float:
        """
        Computes EWMA Variance from log returns.
        Returns Annualized Volatility.
        """
        if self.last_spot is not None:
            ret = math.log(price / self.last_spot)
            if (
                exchange_ts_ms is not None
                and self.last_spot_ts_ms is not None
                and exchange_ts_ms > self.last_spot_ts_ms
            ):
                dt_sec = max(0.001, (exchange_ts_ms - self.last_spot_ts_ms) / 1000.0)
            else:
                dt_sec = 1.0

            lam = max(0.0, min(0.999999, self._lambda_for_dt(dt_sec)))
            self.variance = lam * self.variance + (1 - lam) * (ret ** 2)
             
            # Annualize from per-sample variance (dt handled in lambda, not in scaling term).
            self.sigma = math.sqrt(self.variance * 31536000)
            self.samples_collected += 1
             
        self.last_spot = price
        if exchange_ts_ms is not None:
            self.last_spot_ts_ms = exchange_ts_ms
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
