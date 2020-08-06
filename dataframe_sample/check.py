import pandas

df = pandas.read_csv('result.csv')
print(df.shape)
print(df.head())

ifilter = df['PRODUCT_ID'] == 1029743
print(ifilter)
print(df[ifilter]['Product_single_price'])