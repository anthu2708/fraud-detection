"""Download creditcard.csv from Kaggle and copy to data/."""
import shutil
from pathlib import Path

import kagglehub

path = kagglehub.dataset_download("mlg-ulb/creditcardfraud")
src = Path(path) / "creditcard.csv"
dst = Path("data/creditcard.csv")

shutil.copy(src, dst)
print(f"Saved → {dst}  ({dst.stat().st_size / 1e6:.1f} MB)")
