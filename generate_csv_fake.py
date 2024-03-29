import csv
import faker

# Create fake data generator
fake = faker.Faker()

# Generate 1000 records
records = []
for i in range(1000000):
    id=i
    name = fake.name()
    email = fake.email()
    address = fake.address()
    records.append([i,name, email, address])

# Write records to CSV file
with open('records.csv', mode='w') as file:
    writer = csv.writer(file)
    writer.writerows(records)
