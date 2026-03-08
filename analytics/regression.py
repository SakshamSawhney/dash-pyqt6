from __future__ import annotations

import pandas as pd
import statsmodels.api as sm


def run_ols_regression(y_series: pd.Series, x_series: pd.Series) -> dict[str, float | None]:
    aligned = pd.concat({'y': y_series, 'x': x_series}, axis=1).dropna()
    if len(aligned) < 20:
        return {'beta': None, 'r_squared': None, 'p_value': None}

    x = sm.add_constant(aligned['x'])
    model = sm.OLS(aligned['y'], x)
    results = model.fit()
    return {
        'beta': float(results.params.get('x', float('nan'))),
        'r_squared': float(results.rsquared),
        'p_value': float(results.pvalues.get('x', float('nan'))),
    }