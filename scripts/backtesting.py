import backtrader as bt
import numpy as np

class Strategy(bt.Strategy):
    params = (
        ('t1_days', 2),
        ("upper", 0.17),
        ("lower", 0.11),
    )
    def __init__(self):
        self.buyprice = None
        self.hold = False

        self.orders = []
        self.trailing_order = None
        self.order = None

        self.btc_prices = self.datas[0].close
        
        self.eth_prices = self.datas[1].close
        self.labels = self.datas[0].label
        self.standardized_spread = self.datas[0].standardized_spread

        self.days_in_trade = 0

        self.mu = np.mean(self.standardized_spread)
        self.std = np.std(self.standardized_spread)

    def log(self, txt, dt=None):
        ''' Logging function fot this strategy'''
        dt = dt or self.datas[0].datetime.date(0)
        print('%s, %s, %s' % (dt, self.datas[0].datetime.time(0), txt))


    def notify_order(self, order):
        if order.status in [order.Submitted, order.Accepted]:
            # Buy/Sell order submitted/accepted to/by broker - Nothing to do
            return

        # Check if an order has been completed
        # Attention: broker could reject order if not enough cash
        if order.status in [order.Completed]:
            if order.isbuy():
                self.log(
                    'BUY EXECUTED, Price: %.2f, Cost: %.2f, Comm %.2f' %
                    (order.executed.price,
                     order.executed.value,
                     order.executed.comm))

                self.buyprice = order.executed.price
                self.buycomm = order.executed.comm
            else:  # Sell
                self.log('SELL EXECUTED, Price: %.2f, Cost: %.2f, Comm %.2f' %
                         (order.executed.price,
                          order.executed.value,
                          order.executed.comm))

            self.bar_executed = len(self)
            self.buyprice = order.executed.price

        elif order.status in [order.Canceled, order.Margin, order.Rejected]:
            self.log('Order Canceled/Margin/Rejected', order.status)

        if order == self.order:
            self.order = None
        elif order == self.trailing_order:
            self.trailing_order = None

    def notify_trade(self, trade):
        if not trade.isclosed:
            return

        self.log('OPERATION PROFIT, GROSS %.2f, NET %.2f' %
                 (trade.pnl, trade.pnlcomm))
        
    def next(self):
        position1 = self.getposition(self.datas[0])
        position2 = self.getposition(self.datas[1])
        if self.hold:
            if not position1 and not position2:
                self.buy()
        else:
            if not position1 and not position2: 
                # Buying conditions
                if self.labels[0] == 1:
                    # Buy eth
                    print("buying eth")
                    self.buy(data = self.datas[1])
                    self.upper = self.standardized_spread[0]*(1 + self.params.upper)  
                    self.lower = self.standardized_spread[0]*(1 - self.params.lower)

                elif self.labels[0]  == -1:
                    # Buy BTC
                    print("buying btc")
                    self.buy(data = self.datas[0])
                    
                    self.upper = self.standardized_spread[0]*(1 + self.params.upper)  
                    self.lower = self.standardized_spread[0]*(1 - self.params.lower)
            
            if position1 or position2:
                
                # Position closing conditions
                if self.days_in_trade == self.params.t1_days:
                    self.close(data=self.datas[1])
                    self.close(data=self.datas[0])
                    self.days_in_trade = 0
                if  self.standardized_spread[0] > 0:   
                    if self.standardized_spread[0] >= self.upper or self.standardized_spread[0] <= self.lower:
                        self.close(data=self.datas[1])
                        self.close(data=self.datas[0])
                        self.days_in_trade = 0
                    else:
                        self.days_in_trade += 1 
                elif self.standardized_spread[0] <= 0:
                    if self.standardized_spread[0] <= self.upper or self.standardized_spread[0] >= self.lower:
                        self.close(data=self.datas[1])
                        self.close(data=self.datas[0])
                        self.days_in_trade = 0
                    else:
                        self.days_in_trade += 1 


if __name__ == "__main__":
    pass
