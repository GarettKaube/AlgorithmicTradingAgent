"""Code and modifications from Advances in Financial Machine Learning by Marcos Lopez De Prado"""

import pandas as pd
import numpy as np
from sklearn.model_selection import KFold
from sklearn.preprocessing import LabelEncoder, StandardScaler
from hmmlearn.hmm import GaussianHMM

def npNumCoEvents(closeIdx, t1, molecule):
    t1 = t1.fillna(closeIdx[-1])
    t1 = t1[t1 >= molecule[0]]
    t1 = t1.loc[:t1[molecule].max()]
    iloc = closeIdx.searchsorted(np.array([t1.index[0], t1.max()]))

    count = pd.Series(0, index=closeIdx[iloc[0]: iloc[1]+1])
    for tIn, tOut in t1.items():
        count.loc[tIn:tOut] += 1
    
    return count.loc[molecule[0]: t1[molecule].max()]


def getTrainTimes(t1, testTimes):
    trn = t1.copy()
    for i, j in testTimes.items():
        df0 = trn[(i <= trn.index) & (trn.index <= j)].index
        df1 = trn[(i <= trn) & (trn <= j)].index
        df2 = trn[(trn.index <= i) & (j <= trn)].index
        trn = trn.drop(df0.union(df1).union(df2))
    return trn


def getEmbargoTimes(times,
                    pctEmbargo):
    """
    SNIPPET 7.2
    """
    step = int(times.shape[0]*pctEmbargo)
    if step == 0:
        mbrg = pd.Series(times, index=times)
    else:
        mbrg = pd.Series(times[step:], index=times[:-step])
        mbrg = mbrg.append(pd.Series(times[-1], index=times[-step:]))
    return mbrg


class PurgedKFold(KFold):
    """
    SNIPPET 7.3
    """
    def __init__(self, n, n_folds=3, t1=None, pctEmbargo=0.):
        if not isinstance(t1, pd.Series):
            raise ValueError('Label Through Dates must be a pd.Series')
        super(PurgedKFold, self).__init__(
            n_splits=n_folds, shuffle=False, random_state=None)  
        self.t1 = t1
        self.pctEmbargo = pctEmbargo

    def split(self, X, y=None, groups=None):
        if (X.index == self.t1.index).sum() != len(self.t1):
            raise ValueError('X and ThruDateValues must have the same index')

        indices = np.arange(X.shape[0])
        mbrg = int(X.shape[0]*self.pctEmbargo)     
        test_starts = [(i[0], i[-1]+1) for i in
                       np.array_split(np.arange(X.shape[0]), self.n_splits)]   
        for i, j in test_starts:
            t0 = self.t1.index[i]  # start of test set
            test_indices = indices[i:j]
            maxT1Idx = self.t1.index.searchsorted(self.t1[test_indices].max())
            
            train_indices = self.t1.index.searchsorted(
                self.t1[self.t1 <= t0].index)
            if maxT1Idx < X.shape[0]:  # right train (with embargo)
                train_indices = np.concatenate(
                    (train_indices, indices[maxT1Idx+mbrg:]))
            yield train_indices, test_indices


def get_transition_probs(hmm, n_periods_ahead, data, ret_states, P, possible_states):
    ret_states_ = ret_states.copy() 
    state_probabilities = hmm.predict_proba(data)
    states = possible_states.shape[0]
    for t in range(1, n_periods_ahead + 1):
        # Drop last state transition probabilites so columns don't sum to 1
        for s in range(states-1):
            ret_states_[f'ret_problable_transition{t}{s}'] = np.dot(state_probabilities, P**t)[:,s]
    return ret_states_


def get_next_probable_transition(hmm, n_periods_ahead, data, ret_states, P, possible_states):
    ret_states_ = ret_states.copy() 
    state_probabilities = hmm.predict_proba(data)
    states = possible_states.shape[0]
    for t in range(1, n_periods_ahead + 1):
        # Drop last state transition probabilites so columns don't sum to 1
        
        ret_states_[f'ret_problable_transition{t}'] = np.argmax(np.dot(state_probabilities, P**t), axis=1)
    return ret_states_


def train_hmm(train_idx, test_idx, data_index, roll=40):
    """data_index is the date time index of the original data
    while train_idx, test_idx contain integers
    """
    print("Estimating HMM")
    n_vars = 1
    vars = ['standardized_spread']
    try:
        close = pd.read_csv(
            "C:/Users/armym/OneDrive/New folder (2)/Documents/ds/financialML/data/modeling_data/btc_eth_spread_train2024.csv", 
            parse_dates=['date'], index_col=0)
    except Exception as e:
        print(e)
        close = pd.read_csv("./fincode/btc_eth_spread_tran.csv", parse_dates=['date'], index_col=0)

    ret_scaler = StandardScaler()
    import statsmodels.api as sm

    close.index = pd.to_datetime(close.index)

    
    ret = close[vars].dropna()

    print(ret.head())
    # Split data into train test sets
    train_start, train_end = data_index[min(train_idx)], data_index[max(train_idx)]
    test_start, test_end = data_index[min(test_idx)], data_index[max(test_idx)]
    
    ret_train = ret.loc[(ret.index >= train_start) & (ret.index <= train_end)]
    
    ret_train_np = ret_train.to_numpy().reshape(-1,n_vars) #ret_scaler.fit_transform(ret_train).reshape(-1, n_vars)

    ret_test = ret.loc[(ret.index >= test_start) & (ret.index <= test_end)]
    ret_test_np = ret_test.to_numpy().reshape(-1,n_vars) #ret_scaler.transform(ret_test).reshape(-1, n_vars)
    
 
    # # Initial means for each state
    mus = np.array([[0], [0]])

    # Fit HMM on train data
    hmm = GaussianHMM(
        n_components=2, 
        n_iter=3_000,
        random_state= 7, 
        covariance_type="full",
        means_prior=mus
        )\
        .fit(ret_train_np[-roll:])
    P = hmm.transmat_
    
    # Predict states on train and test
    ret_states_train = pd.DataFrame(hmm.predict(ret_train_np), index=ret_train.index, columns=["return_states"]).astype(int)
    ret_states_test = pd.DataFrame(hmm.predict(ret_test_np), index=ret_test.index, columns=["return_states"]).astype(int)
    
    all_states = pd.concat([ret_states_train, ret_states_test],axis=0)["return_states"].unique()
    # Create columns containing 3 step ahead transition probabilities
    ret_states_train = get_next_probable_transition(hmm, 2, ret_train_np,  ret_states_train, P, all_states)
    ret_states_test = get_next_probable_transition(hmm, 2, ret_test_np,  ret_states_test, P, all_states)
    
    return ret_states_train, ret_states_test


def cvScore(clf,
            X,
            y,
            sample_weight,
            scoring='neg_log_loss',
            t1=None,
            cv=None,
            cvGen=None,
            pctEmbargo=None,
            pca=False,
            n_pca_components=2, 
            scaling=False, 
            feature_selection=False):
    if scoring not in ['neg_log_loss', 'accuracy', 'f1', 'roc']:
        raise Exception('wrong scoring method.')
    from sklearn.metrics import log_loss, accuracy_score, f1_score, roc_auc_score
    from sklearn.feature_selection import RFE
    from sklearn.tree import DecisionTreeClassifier
    from hmmlearn.hmm import GaussianHMM
    from sklearn.preprocessing import FunctionTransformer
    from sklearn.compose import ColumnTransformer

    if cvGen is None:
        cvGen = PurgedKFold(n = X.shape[0], n_folds=cv, t1=t1,
                            pctEmbargo=pctEmbargo)  # purged
    score = []
    
    #y = pd.Series(encoder.fit_transform(y), index=y.index)
    for i, (train, test) in enumerate(cvGen.split(X=X)):
        print(f'Fold: {i}')
        X_train = X.iloc[train, :]
        X_train_copy = X.iloc[train, :]
        X_test = X.iloc[test, :]
    

        for col in X_train.columns:
            if col not in X_test.columns:
                X_test[col] = 0
        X_test = X_test[X_train.columns]

        encoder = LabelEncoder()
        
        no_op_transformer = FunctionTransformer(lambda x: x)
        if scaling:
            tf = ColumnTransformer(
                [(scaling, StandardScaler(), list(X_train.columns.drop(['ret_problable_transition10',
                    'ret_problable_transition11']))),
                    ("identity", no_op_transformer, list(X_train.columns))
                ]
            )
            X_train = tf.fit_transform(X_train)
            X_test = tf.transform(X_test)

        # These steps can also go into a sklearn pipeline
        
        try:
            fit = clf.fit(X=X_train, y=encoder.fit_transform(y.iloc[train]),
                        sample_weight=sample_weight.iloc[train].values)
        except Exception as e:
            try:
                fit = clf.fit(X=X_train, y=encoder.fit_transform(y.iloc[train]),
                        classifier__sample_weight=sample_weight.iloc[train].values)
            except Exception as e:
                fit = clf.fit(X=X_train, y=encoder.fit_transform(y.iloc[train]),
                        classifier__pipe_classifier__sample_weight=sample_weight.iloc[train].values)
            except Exception as e:
                fit = clf.fit(X=X_train, y=encoder.fit_transform(y.iloc[train]),
                        classifier__pipe_classifier__sample_weights=sample_weight.iloc[train].values)
        if scoring == 'neg_log_loss':
            prob = fit.predict_proba(X_test)
            score_ = \
                log_loss(
                    y.iloc[test], prob, sample_weight=sample_weight.iloc[test].values, labels=clf.classes_)
        elif scoring == 'accuracy':
            pred = fit.predict(X_test)
            score_ = accuracy_score(
                y.iloc[test], pred, sample_weight=sample_weight.iloc[test].values)
        elif scoring == "f1":
            pred = fit.predict(X_test)
            score_ = f1_score(
                encoder.fit_transform(y.iloc[test]), pred, sample_weight=sample_weight.iloc[test].values, labels=clf.classes_, average='weighted')
        else:
            print("y_test classes:", y.iloc[test].value_counts())
            try:
                score_ = roc_auc_score(y.iloc[test], fit.predict_proba(X_test)[:,1])
            except Exception as e:
                score_ = roc_auc_score(y.iloc[test], fit.predict_proba(X_test), multi_class="ovr")
        score.append(score_)
    return np.array(score)