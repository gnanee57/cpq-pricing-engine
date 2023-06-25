import csv
import math
import pickle
from io import StringIO
import os

import pandas as pd

from configure.config import app
from exceptions.preprocess_exceptions import DelimiterError, ColNotFoundError, ColExtraFoundError, ColLessFoundError, \
    DataDuplicateError, DataNotFoundError, RowEmptyError, DataDuplicateError1, DataDuplicateError2, ValueNegativeError
from models.models import PreprocessedData


def preprocess_alp(file):
    # with open(file, "r") as f:
    #     if str(csv.Sniffer().sniff(f.read()).delimiter) != "|":
    #         raise DelimiterError()

    alp_data = pd.read_csv(file, sep='|', header=None,
                           names=['EUC_CODE', 'DATE', 'FORECAST_VAL'], index_col=False)
    expected_columns = ['EUC_CODE', 'DATE', 'FORECAST_VAL']
    columns = alp_data.columns.tolist()

    if not all(col in columns for col in expected_columns):
        raise ColNotFoundError()

    days_data = alp_data['DATE'].str[:10].unique()
    for day in days_data:
        if alp_data[alp_data['DATE'].str[:10] == day].duplicated().any():
            raise DataDuplicateError()

    if (alp_data['FORECAST_VAL'] < 0).any():
        raise ValueNegativeError()

    alp_data['DATE'] = pd.to_datetime(alp_data['DATE'], format="%d/%m/%Y")
    transformed_alp_data = alp_data.pivot(index='DATE',
                                          columns='EUC_CODE', values='FORECAST_VAL').reset_index()
    startDate = transformed_alp_data['DATE'].iloc[0]
    endDate = transformed_alp_data['DATE'].iloc[-1]

    expected_days = pd.date_range(start=startDate, end=endDate, freq='MS').strftime('%d/%m/%Y')
    missing_days = set(expected_days) - set(days_data)
    if missing_days:
        raise DataNotFoundError()
    return {"ALP": pickle.dumps(transformed_alp_data)}, startDate, endDate


def preprocess_transportation(file):
    with open(file, 'r') as f:
        transport_csv = f.read()
        if str(csv.Sniffer().sniff(transport_csv).delimiter) != ",":
            raise DelimiterError()

    transport_csv = transport_csv.replace("NA", "NOT_AVAIL")
    transportation_data = pd.read_csv(StringIO(transport_csv), sep=',')

    expected_columns = ["LDZ", "Exit Zone", "Min AQ", "Max AQ", "Units", "Read type", "Charge", "Customer type"]
    columns = transportation_data.columns.tolist()
    if not all(col in columns for col in expected_columns):
        print(columns)
        raise ColNotFoundError()

    processed_transport_data = transportation_data.melt(
        id_vars=["LDZ", "Exit Zone", "Min AQ", "Max AQ", "Units", "Read type", "Charge", "Customer type",
                 "Description"], var_name="Month", value_name="Transport_Cost")

    months_data = processed_transport_data['Month'].str[:7].unique()
    if len(columns[9:]) != len(months_data):
        raise DataDuplicateError()

    if processed_transport_data['Transport_Cost'].isnull().any():
        print('Transport_Cost')
        raise RowEmptyError()
    if processed_transport_data['LDZ'].isna().any():
        print('LDZ')
        raise RowEmptyError()
    if processed_transport_data['Exit Zone'].isna().any():
        print('Exit Zone')
        raise RowEmptyError()
    if processed_transport_data['Charge'].isna().any():
        print('Charge')
        raise RowEmptyError()
    if processed_transport_data['Read type'].isna().any():
        print('Read type')
        raise RowEmptyError()
    if processed_transport_data['Customer type'].isna().any():
        print('Customer type')
        raise RowEmptyError()

    processed_transport_data['Month'] = pd.to_datetime(processed_transport_data['Month'], format="%Y-%m")
    processed_transport_data = processed_transport_data.reset_index()
    processed_transport_data['Date'] = processed_transport_data['Month'] \
        .apply(pd.date_range, freq='MS', periods=2) \
        .apply(lambda ds: pd.date_range(*ds, closed='left'))

    # processed_transport_data['Transport_Cost'] /= processed_transport_data['Date'].apply(len)
    processed_transport_data = processed_transport_data.explode('Date')

    ecn_df = processed_transport_data[processed_transport_data['Charge'] == 'ECN'].reset_index(drop=True)

    gnt_df = processed_transport_data[processed_transport_data['Charge'] == 'GNT'].reset_index(drop=True)

    zco_df, zco_ranges = preprocess_transportation_df_by_type(processed_transport_data, 'ZCO')
    zca_df, zca_ranges = preprocess_transportation_df_by_type(processed_transport_data, 'ZCA')
    cca_df, cca_ranges = preprocess_transportation_df_by_type(processed_transport_data, 'CCA')

    cfi_df, cfi_ranges = preprocess_transportation_df_by_type2(processed_transport_data, 'CFI')
    solr_df, solr_ranges = preprocess_transportation_df_by_type3(processed_transport_data, 'SoLR')

    ranges_dict = {
        "ZCO": zco_ranges,
        "ZCA": zca_ranges,
        "CCA": cca_ranges,
        "CFI": cfi_ranges,
        "SoLR": solr_ranges
    }

    with open(os.path.join(app.config['PROCESSED_OUT'], 'transport_ranges.pickle'), 'wb') as handle:
        pickle.dump(ranges_dict, handle, protocol=pickle.HIGHEST_PROTOCOL)

    startDate = gnt_df['Date'].iloc[0]
    endDate = gnt_df['Date'].iloc[-1]

    expected_months = pd.date_range(start=startDate, end=endDate, freq='MS').strftime('%Y-%m')
    missing_months = set(expected_months) - set(months_data)
    if missing_months:
        raise DataNotFoundError()

    return {
        "ECN": pickle.dumps(ecn_df),
        "GNT": pickle.dumps(gnt_df),
        "ZCO": pickle.dumps(zco_df),
        "ZCA": pickle.dumps(zca_df),
        "CCA": pickle.dumps(cca_df),
        "CFI": pickle.dumps(cfi_df),
        "SoLR": pickle.dumps(solr_df)
    }, startDate, endDate


def preprocess_unigas(file):
    with open(file, "r") as f:
        if str(csv.Sniffer().sniff(f.read()).delimiter) != ",":
            raise DelimiterError()

    uni_data = pd.read_csv(file, sep=',')

    expected_columns = ["LDZ", "MeterClass", "AQLower", "AQHigher"]
    columns = uni_data.columns.tolist()
    if not all(col in columns for col in expected_columns):
        raise ColNotFoundError()

    uni_data = uni_data.drop(['MarketSectorCode'], axis=1)

    processed_uni_data = uni_data.melt(id_vars=["LDZ", "MeterClass", "AQLower", "AQHigher", "Unit"], var_name="Month",
                                       value_name="Uni_Cost")

    months_data = processed_uni_data['Month'].str[:7].unique()
    if len(columns[6:]) != len(months_data):
        raise DataDuplicateError()

    if processed_uni_data['Uni_Cost'].isnull().any() or \
            processed_uni_data['LDZ'].isnull().any() or \
            processed_uni_data['MeterClass'].isnull().any():
        print('Uni_Cost', processed_uni_data['Uni_Cost'].isnull().any())
        print('LDZ', processed_uni_data['LDZ'].isnull().any())
        print('MeterClass', processed_uni_data['MeterClass'].isnull().any())
        raise RowEmptyError()

    processed_uni_data['Month'] = pd.to_datetime(processed_uni_data['Month'], format="%Y-%m")

    processed_uni_data['Uni_Cost'] = processed_uni_data['Uni_Cost'].map(lambda x: x.replace('%', '')).astype(float)
    processed_uni_data['AQLower'] = processed_uni_data['AQLower'].map(lambda x: x.replace(',', '')).astype('Int64')
    processed_uni_data['AQHigher'] = processed_uni_data['AQHigher'].astype(str).map(lambda x: x.replace(',', '')) \
        .astype(float).apply(lambda x: int(x) if not math.isnan(x) else math.nan)

    processed_uni_data['Date'] = processed_uni_data['Month'] \
        .apply(pd.date_range, freq='MS', periods=2) \
        .apply(lambda ds: pd.date_range(*ds, closed='left'))

    # processed_uni_data['Uni_Cost'] /= processed_uni_data['Date'].apply(len)

    processed_uni_data = processed_uni_data.explode('Date').reset_index(drop=True)

    processed_uni_data = processed_uni_data.drop(['Unit', 'Month'], axis=1)

    with open(os.path.join(app.config['PROCESSED_OUT'], 'uni_gas_ranges.pickle'), 'wb') as handle:
        pickle.dump(processed_uni_data[['AQLower', 'AQHigher']].drop_duplicates().values.tolist(),
                    handle, protocol=pickle.HIGHEST_PROTOCOL)

    processed_uni_data_pivot = processed_uni_data.pivot(index=['LDZ', 'Date'],
                                                        columns=['MeterClass', 'AQLower', 'AQHigher'])

    processed_uni_data_pivot.columns = ['_'.join(map(str, filter(None, col))) for col in
                                        processed_uni_data_pivot.columns.values]

    processed_uni_data_pivot = processed_uni_data_pivot.reset_index()

    startDate = processed_uni_data_pivot['Date'].iloc[0]
    endDate = processed_uni_data_pivot['Date'].iloc[-1]

    expected_months = pd.date_range(start=startDate, end=endDate, freq='MS').strftime('%Y-%m')
    missing_months = set(expected_months) - set(months_data)
    if missing_months:
        raise DataNotFoundError()

    return {"UNI_GAS": pickle.dumps(processed_uni_data_pivot)}, startDate, endDate


def preprocess_ggl_fixed(file):
    with open(file, "r") as f:
        if str(csv.Sniffer().sniff(f.read()).delimiter) != ",":
            raise DelimiterError()

    ggl_df = pd.read_csv(file, sep=',')
    columns = ggl_df.columns.tolist()
    columns_len = len(columns)

    if columns_len != 4 and columns_len < 4:
        raise ColLessFoundError()
    if columns_len != 4 and columns_len > 4:
        raise ColExtraFoundError()

    ggl_df.columns = ['Month', 'Unit', 'currency', 'Charge']
    ggl_df = ggl_df.drop(['Unit', 'currency'], axis=1)

    # if 'Month' not in columns or 'Unit' not in columns or 'currency' not in columns or 'Charge' not in columns:
    #     raise ColNotFoundError()

    if ggl_df['Month'].isnull().any() or ggl_df['Charge'].isnull().any():
        raise RowEmptyError()

    months_data = ggl_df['Month'].str[:7].unique()
    for month in months_data:
        if ggl_df[ggl_df['Month'].str[:7] == month].duplicated().any():
            raise DataDuplicateError()

    ggl_df['Month'] = pd.to_datetime(ggl_df['Month'], format="%Y-%m")
    ggl_df['Date'] = ggl_df['Month'] \
        .apply(pd.date_range, freq='MS', periods=2) \
        .apply(lambda ds: pd.date_range(*ds, closed='left'))
    # ggl_df['Charge'] /= ggl_df['Date'].apply(len)
    ggl_df = ggl_df.explode('Date').reset_index(drop=True)
    ggl_df = ggl_df.drop(['Month'], axis=1)

    startDate = ggl_df['Date'].iloc[0]
    endDate = ggl_df['Date'].iloc[-1]

    expected_months = pd.date_range(start=startDate, end=endDate, freq='MS').strftime('%Y-%m')
    missing_months = set(expected_months) - set(months_data)
    if missing_months:
        raise DataNotFoundError()

    return {"GGL_Fixed": pickle.dumps(ggl_df)}, startDate, endDate


def preprocess_ggl_variable(file):
    with open(file, "r") as f:
        if str(csv.Sniffer().sniff(f.read()).delimiter) != ",":
            raise DelimiterError()

    gglv_df = pd.read_csv(file, sep=',')
    gglv_df = gglv_df.loc[:, ~gglv_df.columns.str.contains('^Unnamed')]
    columns = gglv_df.columns.tolist()
    columns_len = len(columns)

    if columns_len != 4 and columns_len < 4:
        raise ColLessFoundError()
    if columns_len != 4 and columns_len > 4:
        raise ColExtraFoundError()

    gglv_df.columns = ['Month', 'Unit', 'currency', 'Charge']

    gglv_df = gglv_df.drop(['Unit', 'currency'], axis=1)

    # if 'Month' not in columns or 'Unit' not in columns or 'currency' not in columns or 'Charge' not in columns:
    #     raise ColNotFoundError()

    months_data = gglv_df['Month'].str[:7].unique()
    for month in months_data:
        if gglv_df[gglv_df['Month'].str[:7] == month].duplicated().any():
            raise DataDuplicateError()

    if gglv_df['Month'].isnull().any() or gglv_df['Charge'].isnull().any():
        raise RowEmptyError()

    gglv_df['Month'] = pd.to_datetime(gglv_df['Month'], format="%Y-%m")
    gglv_df['Date'] = gglv_df['Month'] \
        .apply(pd.date_range, freq='MS', periods=2) \
        .apply(lambda ds: pd.date_range(*ds, closed='left'))
    # gglv_df['Charge'] /= gglv_df['Date'].apply(len)
    gglv_df = gglv_df.explode('Date').reset_index(drop=True)
    gglv_df = gglv_df.drop(['Month'], axis=1)

    startDate = gglv_df['Date'].iloc[0]
    endDate = gglv_df['Date'].iloc[-1]

    expected_months = pd.date_range(start=startDate, end=endDate, freq='MS').strftime('%Y-%m')
    missing_months = set(expected_months) - set(months_data)
    if missing_months:
        raise DataNotFoundError()

    return {"GGL_Variable": pickle.dumps(gglv_df)}, startDate, endDate


def preprocess_xo_admin(file):
    with open(file, "r") as f:
        if str(csv.Sniffer().sniff(f.read()).delimiter) != ",":
            raise DelimiterError()

    xo_admin_df = pd.read_csv(file, sep=',')
    columns = xo_admin_df.columns.tolist()
    columns_len = len(columns)

    if columns_len != 4 and columns_len < 4:
        raise ColLessFoundError()
    if columns_len != 4 and columns_len > 4:
        raise ColExtraFoundError()

    xo_admin_df.columns = ['Month', 'Unit', 'currency', 'Charge']

    xo_admin_df = xo_admin_df.drop(['Unit', 'currency'], axis=1)

    # if 'Month' not in columns or 'Unit' not in columns or 'currency' not in columns or 'Charge' not in columns:
    #     raise ColNotFoundError()

    months_data = xo_admin_df['Month'].str[:7].unique()
    for month in months_data:
        if xo_admin_df[xo_admin_df['Month'].str[:7] == month].duplicated().any():
            raise DataDuplicateError()

    if xo_admin_df['Month'].isnull().any() or xo_admin_df['Charge'].isnull().any():
        raise RowEmptyError()

    xo_admin_df['Month'] = pd.to_datetime(xo_admin_df['Month'], format="%Y-%m")
    xo_admin_df['Date'] = xo_admin_df['Month'] \
        .apply(pd.date_range, freq='MS', periods=2) \
        .apply(lambda ds: pd.date_range(*ds, closed='left'))
    # xo_admin_df['Charge'] /= xo_admin_df['Date'].apply(len)
    xo_admin_df = xo_admin_df.explode('Date').reset_index(drop=True)
    xo_admin_df = xo_admin_df.drop(['Month'], axis=1)

    startDate = xo_admin_df['Date'].iloc[0]
    endDate = xo_admin_df['Date'].iloc[-1]

    expected_months = pd.date_range(start=startDate, end=endDate, freq='MS').strftime('%Y-%m')
    missing_months = set(expected_months) - set(months_data)
    if missing_months:
        raise DataNotFoundError()

    return {"XO_ADMIN": pickle.dumps(xo_admin_df)}, startDate, endDate


def preprocess_timing_risk(file):
    with open(file, "r") as f:
        if str(csv.Sniffer().sniff(f.read()).delimiter) != ",":
            raise DelimiterError()

    timing_risk_df = pd.read_csv(file, sep=',')

    columns = timing_risk_df.columns.tolist()
    columns_len = len(columns)

    if columns_len != 4 and columns_len < 4:
        raise ColLessFoundError()
    if columns_len != 4 and columns_len > 4:
        raise ColExtraFoundError()

    timing_risk_df.columns = ['Min', 'High', 'Unit', 'Charge']
    timing_risk_df = timing_risk_df.drop(['Unit'], axis=1)

    # if 'Min' not in columns or 'High' not in columns or 'Unit' not in columns or 'Charge' not in columns:
    #     raise ColNotFoundError()

    if timing_risk_df['Min'].isnull().any() or timing_risk_df['High'].isnull().any() or \
            timing_risk_df['Charge'].isnull().any():
        raise RowEmptyError()

    timing_risk_df['Charge'] = timing_risk_df['Charge'].map(lambda x: x.replace('%', ''))\
        .astype(float)

    timing_risk_list = []
    for row in timing_risk_df.to_dict('records'):
        timing_risk_list += [(row['Min'], row['High'], row['Charge'])]

    with open(os.path.join(app.config['PROCESSED_OUT'], 'timing_risk.pickle'), 'wb') as handle:
        pickle.dump(timing_risk_list, handle, protocol=pickle.HIGHEST_PROTOCOL)

    return {"Timing_Risk": pickle.dumps(timing_risk_df)}


def preprocess_hedging(file, file_cat):
    with open(file, "r") as f:
        if str(csv.Sniffer().sniff(f.read()).delimiter) != ",":
            raise DelimiterError()

    hedging_cost_df = pd.read_csv(file, sep=',')
    columns = hedging_cost_df.columns.tolist()
    columns_len = len(columns)

    if columns_len != 4 and columns_len < 4:
        raise ColLessFoundError()
    if columns_len != 4 and columns_len > 4:
        raise ColExtraFoundError()

    hedging_cost_df.columns = ['Month', 'Unit', 'currency', 'Charge']
    hedging_cost_df = hedging_cost_df.drop(['Unit', 'currency'], axis=1)

    # if 'Month' not in columns or 'Unit' not in columns or 'currency' not in columns or 'Charge' not in columns:
    #     raise ColNotFoundError()

    months_data = hedging_cost_df['Month'].str[:7].unique()
    for month in months_data:
        if hedging_cost_df[hedging_cost_df['Month'].str[:7] == month].duplicated().any():
            raise DataDuplicateError()

    if hedging_cost_df['Month'].isnull().any() or hedging_cost_df['Charge'].isnull().any():
        raise RowEmptyError()

    # hedging_cost_df = hedging_cost_df.drop(['Unit', 'currency'], axis=1)
    hedging_cost_df['Charge'] = hedging_cost_df['Charge'].astype(str).map(lambda x: x.replace('%', '')).astype(float)
    hedging_cost_df['Month'] = pd.to_datetime(hedging_cost_df['Month'], format="%Y-%m")
    hedging_cost_df['Date'] = hedging_cost_df['Month'] \
        .apply(pd.date_range, freq='MS', periods=2) \
        .apply(lambda ds: pd.date_range(*ds, closed='left'))
    # hedging_cost_df['Charge'] /= hedging_cost_df['Date'].apply(len)
    hedging_cost_df = hedging_cost_df.explode('Date').reset_index(drop=True)
    hedging_cost_df = hedging_cost_df.drop(['Month'], axis=1)

    startDate = hedging_cost_df['Date'].iloc[0]
    endDate = hedging_cost_df['Date'].iloc[-1]

    expected_months = pd.date_range(start=startDate, end=endDate, freq='MS').strftime('%Y-%m')
    missing_months = set(expected_months) - set(months_data)
    if missing_months:
        raise DataNotFoundError()

    return {file_cat: pickle.dumps(hedging_cost_df)}, startDate, endDate


def preprocess_cts(file, file_cat):
    with open(file, "r") as f:
        if str(csv.Sniffer().sniff(f.read()).delimiter) != "|":
            raise DelimiterError()

    cts_df = pd.read_csv(file, sep='|')

    expected_columns = ['HH_Indicator', 'Product_Indicator', 'Gain_Retain', 'Third_Party_Indicator',
                        'Volume_Lower_Limit', 'Volume_Upper_Limit', 'MPAN_Lower_Limit', 'MPAN_Upper_Limit',
                        'p/kWh_Charge_(4dp)', 'p/MPAN_Charge_(2dp)']
    columns = cts_df.columns.tolist()
    if not all(col in columns for col in expected_columns):
        raise ColNotFoundError()

    cts_df = cts_df.drop(['Contract_Volume', 'Number_of_MPANs', 'Import_Export', 'Reference_Only'], axis=1)
    cts_df = cts_df.rename(columns={'p/kWh_Charge_(4dp)': 'CTS_UNIT', 'p/MPAN_Charge_(2dp)': 'CTS_STANDING'})

    if cts_df['CTS_UNIT'].isnull().any() or \
            cts_df['CTS_STANDING'].isnull().any() or \
            cts_df['HH_Indicator'].isnull().any() or \
            cts_df['Product_Indicator'].isnull().any() or \
            cts_df['Gain_Retain'].isnull().any() or \
            cts_df['Third_Party_Indicator'].isnull().any():
        raise RowEmptyError()

    with open(os.path.join(app.config['PROCESSED_OUT'], 'cts_aq_ranges.pickle'), 'wb') as handle:
        pickle.dump(cts_df[['Volume_Lower_Limit', 'Volume_Upper_Limit']].drop_duplicates().values.tolist(),
                    handle, protocol=pickle.HIGHEST_PROTOCOL)

    with open(os.path.join(app.config['PROCESSED_OUT'], 'cts_mprn_ranges.pickle'), 'wb') as handle:
        pickle.dump(cts_df[['MPAN_Lower_Limit', 'MPAN_Upper_Limit']].drop_duplicates().values.tolist(),
                    handle, protocol=pickle.HIGHEST_PROTOCOL)
    cts_df_pivot = cts_df.pivot(
        columns=['HH_Indicator', 'Product_Indicator', 'Gain_Retain', 'Third_Party_Indicator', 'Volume_Lower_Limit',
                 'Volume_Upper_Limit', 'MPAN_Lower_Limit', 'MPAN_Upper_Limit'])
    cts_df_pivot.columns = ['_'.join(map(str, filter(None, col))) for col in
                            cts_df_pivot.columns.values]
    cts_df_pivot = cts_df_pivot.sum(axis=0).to_frame().T

    with open(os.path.join(app.config['PROCESSED_OUT'], 'cts_data.pickle'), 'wb') as handle:
        pickle.dump(cts_df_pivot.to_dict('records'), handle, protocol=pickle.HIGHEST_PROTOCOL)

    return {file_cat: pickle.dumps(cts_df_pivot)}


def preprocess_cobd(file, file_cat):
    with open(file, "r") as f:
        if str(csv.Sniffer().sniff(f.read()).delimiter) != "|":
            raise DelimiterError()

    cobd_df = pd.read_csv(file, sep='|', na_filter=False)

    expected_columns = ['CV_Score', 'CRA_Lower_Limit', 'CRA_Upper_Limit', 'PB_Lower_Limit', 'PB_Upper_Limit',
                        'Gain_Retain', 'DD_Indicator', 'Bill_Frequency', 'Value', 'Uplifted_Value']
    columns = cobd_df.columns.tolist()
    if not all(col in columns for col in expected_columns):
        raise ColNotFoundError()

    if cobd_df.duplicated().any():
        raise DataDuplicateError2()

    cobd_df = cobd_df.drop(['Max_Contract_Length', 'Deposit_Months'], axis=1)

    if cobd_df['Gain_Retain'].isnull().any() or \
            cobd_df['DD_Indicator'].isnull().any() or \
            cobd_df['Bill_Frequency'].isnull().any() or \
            cobd_df['Value'].isnull().any() or \
            cobd_df['Uplifted_Value'].isnull().any():
        raise RowEmptyError()

    with open(os.path.join(app.config['PROCESSED_OUT'], 'cobd_cra_ranges.pickle'), 'wb') as handle:
        pickle.dump(cobd_df[['CRA_Lower_Limit', 'CRA_Upper_Limit']].drop_duplicates().values.tolist(),
                    handle, protocol=pickle.HIGHEST_PROTOCOL)

    with open(os.path.join(app.config['PROCESSED_OUT'], 'cobd_pb_ranges.pickle'), 'wb') as handle:
        pickle.dump(cobd_df[['PB_Lower_Limit', 'PB_Upper_Limit']].drop_duplicates().values.tolist(),
                    handle, protocol=pickle.HIGHEST_PROTOCOL)
    cobd_df_pivot = cobd_df.pivot(
        columns=['CV_Score', 'CRA_Lower_Limit', 'CRA_Upper_Limit', 'PB_Lower_Limit', 'PB_Upper_Limit',
                 'Gain_Retain', 'DD_Indicator', 'Bill_Frequency'])
    cobd_df_pivot.columns = ['_'.join(map(str, filter(None, col))) for col in
                             cobd_df_pivot.columns.values]
    cobd_df_pivot = cobd_df_pivot.sum(axis=0).to_frame().T

    with open(os.path.join(app.config['PROCESSED_OUT'], 'cobd_data.pickle'), 'wb') as handle:
        pickle.dump(cobd_df_pivot.to_dict('records'), handle, protocol=pickle.HIGHEST_PROTOCOL)

    return {file_cat: pickle.dumps(cobd_df_pivot)}


def preprocess_copt(file, file_cat):
    with open(file, "r") as f:
        if str(csv.Sniffer().sniff(f.read()).delimiter) != ",":
            raise DelimiterError()

    copt_df = pd.read_csv(file, sep=',')
    columns = copt_df.columns.tolist()
    columns_len = len(columns)

    if columns_len != 2 and columns_len < 2:
        raise ColLessFoundError()
    if columns_len != 2 and columns_len > 2:
        raise ColExtraFoundError()

    copt_df.columns = ['Effective_Month', 'Value']
    # if 'Effective_Month' not in columns or 'Value' not in columns:
    #     raise ColNotFoundError()

    months_data = copt_df['Effective_Month'].str[:7].unique()
    for month in months_data:
        if copt_df[copt_df['Effective_Month'].str[:7] == month].duplicated().any():
            raise DataDuplicateError()

    if copt_df['Effective_Month'].isnull().any() or copt_df['Value'].isnull().any():
        raise RowEmptyError()
    copt_df['Effective_Month'] = pd.to_datetime(copt_df['Effective_Month'], format="%Y-%m")
    copt_df_to_save = copt_df
    copt_df['Date'] = copt_df['Effective_Month'] \
        .apply(pd.date_range, freq='MS', periods=2) \
        .apply(lambda ds: pd.date_range(*ds, closed='left'))
    # copt_df['Value'] /= copt_df['Date'].apply(len)
    copt_df = copt_df.explode('Date').reset_index(drop=True)
    copt_df = copt_df.drop(['Effective_Month'], axis=1)

    startDate = copt_df['Date'].iloc[0]
    endDate = copt_df['Date'].iloc[-1]

    expected_months = pd.date_range(start=startDate, end=endDate, freq='MS').strftime('%Y-%m')
    missing_months = set(expected_months) - set(months_data)
    if missing_months:
        raise DataNotFoundError()

    return {file_cat: pickle.dumps(copt_df_to_save)}, startDate, endDate


def preprocess_load_factor(file, file_category):
    with open(file, "r") as f:
        if str(csv.Sniffer().sniff(f.read()).delimiter) != ",":
            raise DelimiterError()
    lf_df = pd.read_csv(file, sep=',', header=None, names=["EUC", "Load_Factor"])
    columns = lf_df.columns.tolist()
    columns_len = len(columns)

    if columns_len != 2 and columns_len < 2:
        raise ColLessFoundError()
    if columns_len != 2 and columns_len > 2:
        raise ColExtraFoundError()

    euc_unique = lf_df['EUC'].unique()
    for euc in euc_unique:
        if lf_df[lf_df['EUC'] == euc].duplicated().any():
            raise DataDuplicateError1()

    if lf_df['EUC'].isnull().any() or \
            lf_df['Load_Factor'].isnull().any():
        raise RowEmptyError()

    transformed_lf_df = lf_df.pivot_table(columns='EUC')

    transformed_lf_df.to_json(os.path.join(app.config['PROCESSED_OUT'], 'load_factor.json'), orient='records')

    return {file_category: pickle.dumps(lf_df)}


def preprocess_tpi_limits(file, fileCategory):
    with open(file, "r") as f:
        if str(csv.Sniffer().sniff(f.read()).delimiter) != "|":
            raise DelimiterError()

    tpi_limits_df = pd.read_csv(file, sep='|')

    columns = tpi_limits_df.columns.tolist()
    columns_len = len(columns)

    if columns_len != 4 and columns_len < 4:
        raise ColLessFoundError()
    if columns_len != 4 and columns_len > 4:
        raise ColExtraFoundError()

    tpi_unique = tpi_limits_df['TPI'].unique()
    for tpi in tpi_unique:
        if tpi_limits_df[tpi_limits_df['TPI'] == tpi].duplicated().any():
            raise DataDuplicateError1()

    tpi_code_unique = tpi_limits_df['TPI_ID'].unique()
    for tpi in tpi_code_unique:
        if tpi_limits_df[tpi_limits_df['TPI_ID'] == tpi].duplicated().any():
            raise DataDuplicateError1()

    tpi_limits_df = tpi_limits_df.drop(['TPI'], axis=1)

    return {fileCategory: pickle.dumps(tpi_limits_df)}


def get_preprocessed_file(fileCategory):
    return PreprocessedData.query.filter(PreprocessedData.dataFrameType == fileCategory)\
        .order_by(PreprocessedData.id.desc()).first()


def preprocess_transportation_df_by_type(df, charge):
    processed_df = df[df['Charge'] == charge].reset_index(drop=True)
    processed_df['Min AQ'] = processed_df['Min AQ'].astype("Int64")
    processed_df['Max AQ'] = processed_df['Max AQ'].apply(lambda x: int(x) if not math.isnan(x) else '')
    processed_df = processed_df.fillna('')

    df_ranges = processed_df[['Min AQ', 'Max AQ']].drop_duplicates().values.tolist()

    processed_df = processed_df.drop(['Exit Zone', 'Read type', 'Charge', 'Description', 'Customer type', 'Month'],
                                     axis=1)
    processed_df_pivot = processed_df.pivot(index=['LDZ', 'Date'], columns=['Min AQ', 'Max AQ', 'Units'])
    processed_df_pivot.columns = ['_'.join(map(str, filter(None, col))) for col in processed_df_pivot.columns.values]
    return processed_df_pivot.reset_index(), df_ranges


def preprocess_transportation_df_by_type2(df, charge):
    processed_df = df[df['Charge'].str.contains(charge)].reset_index(drop=True)
    processed_df['Min AQ'] = processed_df['Min AQ'].astype("Int64")
    processed_df['Max AQ'] = processed_df['Max AQ'].apply(lambda x: int(x) if not math.isnan(x) else '')
    processed_df = processed_df.fillna('')

    df_ranges = processed_df[['Min AQ', 'Max AQ']].drop_duplicates().values.tolist()

    processed_df = processed_df.drop(['Exit Zone', 'Read type', 'Customer type', 'Description', 'Month'], axis=1)
    processed_df_pivot = processed_df.pivot(index=['LDZ', 'Date', 'Min AQ', 'Max AQ', 'Units'], columns=['Charge'])
    processed_df_pivot.columns = ['_'.join(map(str, filter(None, col))) for col in
                                  processed_df_pivot.columns.values]
    return processed_df_pivot.reset_index(), df_ranges


def preprocess_transportation_df_by_type3(df, charge):
    processed_df = df[df['Charge'].str.contains(charge)].reset_index(drop=True)
    processed_df['Min AQ'] = processed_df['Min AQ'].astype("Int64")
    processed_df['Max AQ'] = processed_df['Max AQ'].apply(lambda x: int(x) if not math.isnan(x) else '')
    processed_df = processed_df.fillna('')

    df_ranges = processed_df[['Min AQ', 'Max AQ']].drop_duplicates().values.tolist()

    processed_df = processed_df.drop(['Exit Zone', 'Read type', 'Charge', 'Description', 'Month'], axis=1)
    processed_df_pivot = processed_df.pivot(index=['LDZ', 'Date', 'Min AQ', 'Max AQ', 'Units'],
                                            columns=['Customer type'])
    processed_df_pivot.columns = ['_'.join(map(str, filter(None, col)))
                                  for col in processed_df_pivot.columns.values]
    return processed_df_pivot.reset_index(), df_ranges


def preprocess_forecast_input_band_range(file, file_cat):
    forecast_input_band_range_df = pd.read_csv(file, sep='|')

    with open(os.path.join(app.config['PROCESSED_OUT'], 'forcast_band_ranges.pickle'), 'wb') as handle:
        pickle.dump(forecast_input_band_range_df.to_dict('records'), handle, protocol=pickle.HIGHEST_PROTOCOL)
    return {file_cat: pickle.dumps(forecast_input_band_range_df)}
