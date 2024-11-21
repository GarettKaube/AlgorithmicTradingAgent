import pandas as pd
import numpy as np
from sklearn.model_selection import KFold
from sklearn.decomposition import PCA
from sklearn.preprocessing import LabelEncoder, StandardScaler
from hmmlearn.hmm import GaussianHMM
import sys
sys.path.insert(0, r"C:\Users\armym\OneDrive\New folder (2)\Documents\ds\financialMLgh\scripts")
from label import triple_barrier_labeling

import pytest

np.random.seed(42)

@pytest.fixture
def test_data():
    length = 5
    dates = pd.date_range(start="2019-12-20", periods=5, freq='1d')
    data = {
        'close': np.random.randn(length),
        'standardized_spread': np.random.randn(length),
        'low': np.random.randn(length),
        'high': np.random.randn(length),
        'adj close': np.random.randn(length),
        'volume': np.random.randn(length)
    }
    
    df = pd.DataFrame(data, index=dates)
    df.index.name='date'
    return df


def test_getEvents_non_meta(test_data):
    np.random.seed(42)
    labels = triple_barrier_labeling(test_data, 0.1, 0.1, 2)['label']
    print(labels)
    expected_labels = [1, -1, -1, 1, 0]

    assert labels.to_list() == expected_labels

