# Algorithmic Trading Agent using XGBoost and hidden Markov models
### Modeling Process:
Using the spread calculations as in Fu, Kang, Hong, and Kim (2024)
the return spread between Bitcoin-USD and Ethereum-USD is calculated and a rolling standardization is applied to it:

$$StardizedSpread_t = \frac{Spread_t - \mu_t}{\sigma_t},$$

$$\mu_t = \frac{1}{L}\sum_{i=0}^{L}Spread_{t-i},$$

$$\sigma_t^2 = \frac{1}{L-1}\sum_{i=0}^{L}(Spread_{t-i} - \mu_t)^2$$

Where $\mu_t$ and $\sigma_t$ are the moving average and moving standard deviation of the spread and $L$ is the size of the window.

The spread is then labeled using the triple barrier method. 
Given a spread observation: $s_ {t_i}$ and $s_ {t_ {i+1}},..., s_ {t_{i+n}}$ 
are the spread values after ${t_i}$ and $t_ {i+n}$ is the timestamp of the max holding period. The label function is as follows:

$$ UT(s_ {t_i})  = \text{min}(\\{ t_{j} :  s_ {t_j} \geq U, i < j < i+n \\} \cup \\{ t_{i+n} \\}) $$
$$ LT(s_ {t_i})  = \text{min}(\\{ t_{j} :  s_ {t_j} \leq L, i < j< i+n \\} \cup \\{ t_{i+n} \\}) $$

$$
f(s_ {t_i}) =
\begin{cases} 
1, & \text{if } UT(s_ {t_i}) \gt LT(s_ {t_i}) \\
-1, & \text{if } UT(s_ {t_i}) \lt LT(s_ {t_i}) \\
0, & else
\end{cases}
$$

Where $U\text{ and }L$ are the upper and lower thresholds for labeling. $UT\text{ and }LT$ are the first touch times for each barrier.
The following plot shows the results:

![newplot](https://github.com/user-attachments/assets/5a7d82b9-d225-43bb-8007-0d84cdb5ecb4)
When training the model, the 0 label was rare so observations with label 0 are dropped.

Next, features such as the entropy of price direction of both assets are calculated and technical features.
The standard OHLCV features for each asset were also included.

A 2 state hidden markov model with Gaussian emmisions is trained to detect hidden regimes in the spread.
These hidden regimes are used as inputs to the final model that produces signals. The most probable transistions for the Markov process are used as features and defined as:

$$\tilde{S}_ {t+i}= \underset{{k\in\\{0,1\\}}}{\text{argmax} }P(S_{t+i}=k | S_{t}=s)$$ 

where $S_t$ is the hidden state at time $t$. We also estimate the hidden states given the observations that maximizes:

$$ P(\mathbf{S}_n = (i_1, ..., i_n) | \mathbf{X}_n = \mathbf{x}_n) = \frac{P(\mathbf{S}_n = (i_1, ..., i_n), \mathbf{X}_n = \mathbf{x}_n)}{P(\mathbf{X}_n = \mathbf{x}_n)}$$

This is calculated using the Viterbi Algorithm. The following plot shows the estimated hidden states:

![output](https://github.com/user-attachments/assets/5cc1f747-1a7e-4e53-bb61-afa8f322ab3c)

Next, XGBoost was selected as the model using Nested Purged K-fold Cross Validation as covered in "Advances in Financial Machine Learning" by Marcos Lopez de Prado (the non nested was covered). Optuna was used to tune hyperparameters with a Parzen-Tree Estimator Bayesian optimizer.
The final Sklearn pipeline contains a custom estimator to generate the features from the HMM then the XGBoost model is fitted. The model is fitted on the earliest observation in 2017 up to 2023-07-31. The test set is on data from 2023-08-1 to 2024-11-1. The out of sample performance was exceptional with an accuracy of approximatly 80%, log loss of 0.43, and F1 score of 0.78.
The final model is logged to a MLflow tracking server.

### Backtesting:
Backtrader was used for backtesting. The strategy as outlined in the paper is as follows:
- When the XGBoost predicts 1, BTC is overperforming so we buy ETH expecting it to catch up to BTC.
- When the XGBoost predicts 0, ETH is overperforming so we buy BTC expecting it to catch up to BTC.
- We hold until any of the 3 barriers are touched.
  
The out of sample results with a constant position size of 40% and starting capital of 100,000:

| Metric              | Value             |
|---------------------|-------------------|
|Final Portfolio Value| 1297290.84        |
|Cumulative Return    | 0.2758925832934964|
|Sharpe Ratio | 1.0722631487780885|
|Profit_ratio| 1.2972908434492345|
|drawdown pct| 0.056595542654838855|
|Drawdown cash| 77825.45301171998|

More rigorous backtesting still needs to be done. Such as backtesting on synthetic data.

### Software:
The overall architecture is shown in the figure:
![FinancialAssetsDataModel-Page-2(2)](https://github.com/user-attachments/assets/1bc5885e-2408-49d4-8403-c07b02516c30)

#### Dashboard
A streamlit dashboard was developed to monitor infrastructure, the spread, prices, model predictions, and the current positions:

![dashboard](https://github.com/user-attachments/assets/07d0ee51-0f29-43d9-8db0-02bc4966f1cb)



References:

1. Fu, Ning, Mingu Kang, Joongi Hong, and Suntae Kim. 2024. "Enhanced Genetic-Algorithm-Driven Triple Barrier Labeling Method and Machine Learning Approach for Pair Trading Strategy in Cryptocurrency Markets" Mathematics 12, no. 5: 780. https://doi.org/10.3390/math12050780
2. Lopez de Prado, Marcos. Advances in Financial Machine Learning. Hoboken, NJ: Wiley, 2018.

