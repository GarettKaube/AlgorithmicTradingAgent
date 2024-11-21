

def get_variables(ti):
    """ Gets the names of required variables that will be
    used for transformations
    """
    data = ti.xcom_pull(task_ids="Extract_Data_From_Snowflake")

    assets = list(data["SYMBOL_CODE"].unique())
    close_vars = ["CLOSE_"+sym for sym in assets]
    bt_vars = ["CLOSE_DIRECTION_"+sym for sym in assets]
    return assets, close_vars, bt_vars


def get_close_chg_vars(ti):
    """ Creates list of lower case columns names that 
    contains the string: "close_change"
    """
    t_data = ti.xcom_pull(task_ids="Transform_Snowflake_Data")
    close_change_vars = [col for col in t_data.columns if "close_change" in col.lower()]
    return close_change_vars