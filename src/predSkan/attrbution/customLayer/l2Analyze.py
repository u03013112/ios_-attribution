import pandas as pd

df = pd.read_csv('/src/data/doc/customLayer/ret.csv')

for message in (
    'ios l2 test0.010000',
    'ios l2 test0.100000',
    'ios l2 test1.000000',
    'ios l2 test10.000000',
    'ios l2 test100.000000',
    'ios l2 test1000',
    'ios l2 test10000',
    'ios l2 test100000',
    'ios l2 test1000000',
    'ios l2 test10000000',
    'ios l2 test100000000',
    ):

    df0 = df.loc[df.message == message]
    c0 = df0.loc[df0.val_loss < 20,'message'].count()
    c2 = df0.loc[:,'message'].count()
    print(message,c0/c2)