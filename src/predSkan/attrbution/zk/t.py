import pandas as pd

def main():
    for filename in [
            '/src/data/zk2/funplus02AdvUidMutiDays_20230911.csv',
            '/src/data/zk2/funplus02AdvUidMutiDays_20230912.csv',
        ]:
        df = pd.read_csv(filename)
        print(filename,df['googleadwords_int_rate'].sum())

    for filename in [
            '/src/data/zk2/funplus02AdvUidMutiDays13_20230911.csv',
            '/src/data/zk2/funplus02AdvUidMutiDays13_20230912.csv',
        ]:
        df = pd.read_csv(filename)
        print(filename,df['googleadwords_int_rate'].sum())

if __name__ == '__main__':
    main()
