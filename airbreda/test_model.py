from pathlib import Path

import joblib

from predict import predict  # pytest.ini puts airbreda/ on the path (flat layout, D-051)

HERE = Path(__file__).resolve().parent


def test_model_pkl_loads():
    joblib.load(HERE / "model.pkl")


def test_predict_returns_plausible_values():
    result = predict(total_intensity_veh_per_hr=4380, hour_of_day=11)
    assert isinstance(result["no2_ug_m3_predicted"], float)
    assert 0 <= result["no2_ug_m3_predicted"] <= 200
    assert 0 <= result["no2_exceedance_risk"] <= 1
