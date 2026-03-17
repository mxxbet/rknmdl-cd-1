import pandas as pd

print("Script gestart")

# Lees inputbestand uit de repository
df = pd.read_excel("tickers_consumer_defensive.xlsx")

print("Input succesvol gelezen")
print(df.head())

# Voor test: neem eerste 10 rijen
result = df.head(10).copy()

# Schrijf resultaat weg
result.to_excel("output.xlsx", index=False)

print("output.xlsx succesvol aangemaakt")
