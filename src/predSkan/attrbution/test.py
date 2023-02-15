import pandas as pd

df = pd.DataFrame(data = {
    'a':[1,2,3,4,5]
})

a = df.sample()

a = a.append(df.sample())
a = a.append(df.sample())
a = a.append(df.sample())

print(a,df)