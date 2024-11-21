import pandas as pd

def triple_barrier_labeling(data, take_profit, stop_loss, holding_period):
    labels = []
    touch_times = []

    for index, row in data.iterrows():
        entry_price = row['standardized_spread']
        entry_time = index

        end_time = entry_time + pd.Timedelta(days=holding_period)
        # Define the window for holding period
        window = data[(data.index > entry_time) & (data.index <= end_time)]

        upper = entry_price*(1 + take_profit)
        lower = entry_price*(1 - stop_loss)
        
        # Initialize label as neutral
        label = 0
        touch_time = end_time
        # Check if barriers are hit within the holding period
        for t1, future_row in window.iterrows():
            future_price = future_row['standardized_spread']
            
            if future_price >= upper:
                label = 1  # Take profit hit
                touch_time = t1
                break

            if future_price <= lower:
                label = -1  # Stop loss hit
                touch_time = t1
                break
        
        # Append the label to the list
        labels.append(label)
        touch_times.append(touch_time)
    
    # Add the labels to the DataFrame
    data['label'] = labels
    data['t1'] = touch_times
    return data


if __name__ == "__main__":
    pass
