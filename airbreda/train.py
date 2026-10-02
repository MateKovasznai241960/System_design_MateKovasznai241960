from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_absolute_error, r2_score
import pandas as pd
import joblib

df = pd.read_csv("training_data.csv")
X = df[["total_intensity_veh_per_hr", "hour_of_day"]]
y = df["no2_ug_m3"]

# No train/test split: there are too few rows to hold any out (task 007).
model = LinearRegression()
model.fit(X, y)

# model.pkl lives in airbreda/ next to predict.py and is baked into the
# dashboard image (task 008). Run from airbreda/: python train.py (D-076).
joblib.dump(model, "model.pkl")

# Evaluated on the training rows themselves, so this shows how well the line
# fits the rows it was fitted to, not how well it predicts a new hour.
# With fewer than 2 rows R2 is undefined and scikit-learn returns nan.
predicted = model.predict(X)
print(f"rows: {len(df)} (no train/test split)")
print(f"R2 (training rows): {r2_score(y, predicted)}")
print(f"MAE (training rows): {mean_absolute_error(y, predicted)} ug/m3")
print(f"coef_ [total_intensity_veh_per_hr, hour_of_day]: {model.coef_}")
print(f"intercept_: {model.intercept_}")
