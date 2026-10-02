from pathlib import Path

import joblib
import numpy as np
import pandas as pd

MODEL_PATH = Path(__file__).resolve().parent / "model.pkl"

# Exceedance threshold: 40 ug/m3, the EU annual limit value in force today
# (Directive 2008/50/EC). It is an annual average and we predict hourly values,
# so an hour above 40 is not a legal exceedance. Reasoning in ADR-006.
THRESHOLD_UG_M3 = 40.0


def exceedance_risk(predicted_no2, threshold, steepness=0.2):
    return float(1 / (1 + np.exp(-steepness * (predicted_no2 - threshold))))


def predict(total_intensity_veh_per_hr, hour_of_day):
    model = joblib.load(MODEL_PATH)
    X = pd.DataFrame([[total_intensity_veh_per_hr, hour_of_day]],
                     columns=["total_intensity_veh_per_hr", "hour_of_day"])
    no2 = float(model.predict(X)[0])
    return {"no2_ug_m3_predicted": no2,
            "no2_exceedance_risk": exceedance_risk(no2, THRESHOLD_UG_M3)}
