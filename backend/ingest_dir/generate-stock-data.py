# Sample data
data = [
    ("AAPL", "2024-06-03", 135.24),
    ("GOOGL", "2024-06-03", 2550.67),
    ("MSFT", "2024-06-03", 249.68),
    ("AMZN", "2024-06-03", 3286.33)
]

# Convert data to CSV string
csvString = "\n".join([f"{symbol},{date},{price}" for symbol, date, price in data])

# Save CSV string to a file
with open("sample-stock-data.csv", "w") as file:
    file.write(csvString)
