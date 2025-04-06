import polars as pl
import random
from datetime import datetime, timedelta

def synthesize_data(n=1000, excursion_rate=0.1):
    facilities = [
        "Johns Hopkins Cell Therapy Lab (Baltimore, MD)",
        "Cleveland Clinic (Cleveland, OH)",
        "Mayo Clinic (Rochester, MN)",
        "MD Anderson (Houston, TX)",
        "UPenn Cell Therapy Center (Philadelphia, PA)",
        "Dana-Farber Cancer Institute (Boston, MA)"
    ]
    companies = [
        "Johns Hopkins Cell Therapy Lab",
        "Bristol Myers Squibb",
        "Novartis",
        "Kit Pharma",
        "Legend Biotech",
    ]
    airports = ['BWI', 'IAH', 'JFK', 'CLE', 'ORD']
    carriers = ['Cryoport', 'FedEx Health', 'UPS ColdChain']
    product_types = ['Stem Cell', 'CAR-T', 'iPSC-derived']

    shipment_routes = []
    shipment_flights = []
    shipment_events = []
    shipment_temperatures = []

    for i in range(n):
        shipment_id = f"SHP{i:04}"
        had_excursion = 1 if random.random() < excursion_rate else 0

        # -- ROUTE TABLE --
        origin_facility = random.choice(facilities)
        destination_facility = random.choice([f for f in facilities if f != origin_facility])
        carrier = random.choice(carriers)
        product_type = random.choice(product_types)
        start_time = datetime(2024, 3, 1) + timedelta(days=random.randint(0, 60))
        end_time = start_time + timedelta(hours=random.randint(8, 48))
        status = 'failed' if had_excursion else 'success'
        company = random.choice(companies)

        shipment_routes.append([
            shipment_id,
            company,
            origin_facility,
            destination_facility,
            start_time.date().isoformat(),
            end_time.isoformat(),
            carrier,
            product_type,
            status
        ])

        # -- EVENT TABLE --
        pickup_time = start_time
        in_transit_time = start_time + timedelta(hours=(end_time - start_time).total_seconds() / 3600 / 2)
        delivery_time = end_time

        shipment_events.extend([
            [shipment_id, pickup_time.isoformat() + 'Z', "pickup", origin_facility],
            [shipment_id, in_transit_time.isoformat() + 'Z', "in_transit", f"En route to {destination_facility}"],
            [shipment_id, delivery_time.isoformat() + 'Z', "delivery", destination_facility],
        ])

        # -- FLIGHT TABLE --
        dep_airport = random.choice(airports)
        arr_airport = random.choice([a for a in airports if a != dep_airport])
        scheduled_departure = start_time + timedelta(hours=2)
        delay_minutes = random.randint(0, 15) if not had_excursion else random.randint(60, 240)
        actual_departure = scheduled_departure + timedelta(minutes=delay_minutes)
        arrival_time = actual_departure + timedelta(hours=3)

        shipment_flights.append([
            shipment_id,
            f"FX{random.randint(1000, 9999)}",
            dep_airport,
            arr_airport,
            scheduled_departure.isoformat() + 'Z',
            actual_departure.isoformat() + 'Z',
            delay_minutes,
            arrival_time.isoformat() + 'Z'
        ])

        # -- TEMPERATURE TABLE --
        temp_start = start_time
        for j in range(4):
            ts = temp_start + timedelta(hours=j * 6)
            if had_excursion and j == random.randint(1, 2):
                temp = round(random.uniform(-55, -50), 2)  # excursion
            else:
                temp = round(random.uniform(-80, -70), 2)
            shipment_temperatures.append([
                shipment_id,
                ts.isoformat() + 'Z',
                temp
            ])
    # print(shipment_routes)

    # Convert to Polars DataFrames
    df_routes = pl.DataFrame(
        shipment_routes,
        schema=[
            "shipment_id", "company", "origin_facility", "destination_facility",
            "start_time", "end_time", "carrier", "product_type", "status"
        ],
        orient="row"
    )

    df_flights = pl.DataFrame(
        shipment_flights,
        schema=[
            "shipment_id", "flight_number", "departure_airport", "arrival_airport",
            "scheduled_departure", "actual_departure", "delay_minutes", "arrival_time"
        ],
        orient="row"
    )

    df_events = pl.DataFrame(
        shipment_events,
        schema=["shipment_id", "timestamp", "event_type", "location"],
        orient="row"
    )

    df_temperatures = pl.DataFrame(
        shipment_temperatures,
        schema=["shipment_id", "timestamp", "temperature_c"],
        orient="row"
    )

    return df_routes, df_flights, df_events, df_temperatures

routes, flights, events, temps = synthesize_data(n=1000, excursion_rate=0.15)

# Save to CSV
routes.write_csv("data/shipment_routes.csv")
flights.write_csv("data/shipment_flights.csv")
events.write_csv("data/shipment_events.csv")
temps.write_csv("data/shipment_temperatures.csv")