import polars as pl
import random
from datetime import datetime, timedelta

def construct_facility_map(facilities):
    """Create a map of risk factors between facility pairs"""
    res = {}
    for i in range(len(facilities)):
        for j in range(len(facilities)):
            if i != j:
                # Some facility pairs will be riskier than others
                res[(facilities[i], facilities[j])] = random.uniform(0.01, 0.05)
    return res

def get_season(date):
    """Determine season from date"""
    month = date.month
    if month in [12, 1, 2]:
        return "winter"
    elif month in [3, 4, 5]:
        return "spring"
    elif month in [6, 7, 8]:
        return "summer"
    else:
        return "fall"

def get_day_of_week(date):
    """Get day of week from date"""
    return date.strftime("%A")

def calculate_excursion_probability(
    facility_pair,
    facility_map,
    carrier,
    product_type,
    day_of_week,
    season,
    delay_minutes,
    airport_pair,
    flight_duration,
    had_customs_hold,
    shipment_duration,
    base_prob
):
    """Calculate probability of temperature excursion based on risk factors"""    
    # Adjustment factors (multiplicative effect - above 1.0 increases risk, below 1.0 decreases risk)
    
    # Facility pair risk factor
    facility_factor = 1.0 + facility_map.get(facility_pair, 0.02)
    
    # Carrier risk factors
    carrier_factor = {
        'Cryoport': 0.7,             # Reliable carrier (reduces risk)
        'FedEx Health': 1.2,         # Average carrier (slightly increases risk)
        'UPS ColdChain': 1.0         # Neutral
    }.get(carrier, 1.0)
    
    # Product type risk factors
    product_factor = {
        'Stem Cell': 1.5,            # Most sensitive
        'CAR-T': 1.2,                # Moderately sensitive
        'iPSC-derived': 0.8          # Most stable
    }.get(product_type, 1.0)
    
    # Day of week risk factors (weekends are riskier)
    day_factor = {
        'Monday': 0.9,
        'Tuesday': 0.85,
        'Wednesday': 0.8,
        'Thursday': 0.9,
        'Friday': 1.1,
        'Saturday': 1.3,
        'Sunday': 1.2
    }.get(day_of_week, 1.0)
    
    # Seasonal risk factors
    season_factor = {
        'winter': 0.8,
        'spring': 1.0,
        'summer': 1.5,
        'fall': 1.1
    }.get(season, 1.0)
    
    # Delay risk factor (longer delays = higher risk)
    # No delay: 1.0x, 1 hour delay: ~1.2x, 3 hour delay: ~1.6x
    delay_factor = 1.0 + (delay_minutes / 300)
    
    # Airport pair risk (some pairs are riskier)
    airport_risk_map = {
        ('JFK', 'IAH'): 1.2,
        ('ORD', 'JFK'): 1.3,
        ('BWI', 'IAH'): 0.9,
        ('CLE', 'JFK'): 1.1,
        ('IAH', 'ORD'): 1.2
    }
    airport_factor = airport_risk_map.get(airport_pair, 1.0)
    # Try the reverse pair if not found
    if airport_pair not in airport_risk_map:
        airport_factor = airport_risk_map.get((airport_pair[1], airport_pair[0]), 1.0)
    
    # Flight duration factor (longer flights = higher risk)
    # 1.5hr flight: ~1.1x, 3hr flight: ~1.2x, 4.5hr flight: ~1.3x
    flight_duration_factor = 1.0 + (flight_duration / 20)
    
    # Customs hold factor - major risk increase
    customs_factor = 1.8 if had_customs_hold else 1.0
    
    # Shipment duration factor (longer shipments = higher risk)
    # 8hr: ~1.1x, 24hr: ~1.3x, 48hr: ~1.6x
    shipment_duration_factor = 1.0 + (shipment_duration / 120)
    
    # Calculate final probability by applying all factors to base probability
    # Each factor multiplies the risk
    final_prob = base_prob
    final_prob *= facility_factor
    final_prob *= carrier_factor
    final_prob *= product_factor
    final_prob *= day_factor
    final_prob *= season_factor
    final_prob *= delay_factor
    final_prob *= airport_factor
    final_prob *= flight_duration_factor
    final_prob *= customs_factor
    final_prob *= shipment_duration_factor
    
    # Cap the probability at 0.95
    return min(0.95, final_prob)

def generate_temperature(excursion_prob, normal_range=(-80, -60), excursion_range=(-59, -50)):
    """Generate a temperature based on excursion probability"""
    if random.random() <= excursion_prob:
        # Generate an excursion temperature (above -60°C)
        return round(random.uniform(*excursion_range), 2)
    else:
        # Generate a normal temperature
        return round(random.uniform(*normal_range), 2)

def synthesize_data(n=1000, base_excursion_rate=0.1):
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

    facility_map = construct_facility_map(facilities)

    shipment_routes = []
    shipment_flights = []
    shipment_events = []
    shipment_temperatures = []
    
    # Track shipments with excursions for analysis
    shipments_with_excursions = set()

    for i in range(n):
        shipment_id = f"SHP{i:04}"
        
        # -- ROUTE TABLE --
        origin_facility = random.choice(facilities)
        destination_facility = random.choice([f for f in facilities if f != origin_facility])
        carrier = random.choice(carriers)
        product_type = random.choice(product_types)
        company = random.choice(companies)
        
        # Generate dates
        start_time = datetime(2024, 1, 1) + timedelta(days=random.randint(0, 364))
        shipment_duration_hours = random.randint(8, 48)
        end_time = start_time + timedelta(hours=shipment_duration_hours)
        
        # Get day of week and season
        day_of_week = get_day_of_week(start_time)
        season = get_season(start_time)
        
        # -- FLIGHT TABLE --
        dep_airport = random.choice(airports)
        arr_airport = random.choice([a for a in airports if a != dep_airport])
        scheduled_departure = start_time + timedelta(hours=2)
        delay_minutes = random.randint(0, 180)  # 0-3 hours delay
        actual_departure = scheduled_departure + timedelta(minutes=delay_minutes)
        flight_duration_hours = random.uniform(1.5, 4.5)  # 1.5-4.5 hours
        arrival_time = actual_departure + timedelta(hours=flight_duration_hours)
        
        # Random customs hold
        had_customs_hold = random.random() < 0.1  # 10% chance of customs hold
        
        # Calculate excursion probability based on all factors
        excursion_prob = calculate_excursion_probability(
            (origin_facility, destination_facility),
            facility_map,
            carrier,
            product_type,
            day_of_week,
            season,
            delay_minutes,
            (dep_airport, arr_airport),
            flight_duration_hours,
            had_customs_hold,
            shipment_duration_hours,
            base_excursion_rate
        )
        
        # Add to routes table (without status column)
        shipment_routes.append([
            shipment_id,
            company,
            origin_facility,
            destination_facility,
            start_time.date().isoformat(),
            end_time.isoformat(),
            carrier,
            product_type,
            delay_minutes,
            had_customs_hold,
            day_of_week,
            season,
            round(excursion_prob, 4)  # Store the calculated probability for validation
        ])

        # -- EVENT TABLE --
        pickup_time = start_time
        in_transit_time = start_time + timedelta(hours=shipment_duration_hours / 2)
        delivery_time = end_time

        events = [
            [shipment_id, pickup_time.isoformat() + 'Z', "pickup", origin_facility],
            [shipment_id, in_transit_time.isoformat() + 'Z', "in_transit", f"En route to {destination_facility}"],
            [shipment_id, delivery_time.isoformat() + 'Z', "delivery", destination_facility],
        ]
        
        # Add customs event if applicable
        if had_customs_hold:
            customs_time = actual_departure + timedelta(hours=flight_duration_hours + random.uniform(0.5, 2))
            events.append([shipment_id, customs_time.isoformat() + 'Z', "customs_hold", arr_airport])
            
        shipment_events.extend(events)

        # Add to flights table
        shipment_flights.append([
            shipment_id,
            f"FX{random.randint(1000, 9999)}",
            dep_airport,
            arr_airport,
            scheduled_departure.isoformat() + 'Z',
            actual_departure.isoformat() + 'Z',
            delay_minutes,
            arrival_time.isoformat() + 'Z',
            round(flight_duration_hours, 2)  # Adding flight duration to table
        ])

        # -- TEMPERATURE TABLE --
        temp_start = start_time
        # More frequent temperature readings
        reading_count = max(6, shipment_duration_hours // 4)  # At least 6 readings
        
        had_excursion_in_readings = False
        
        for j in range(reading_count):
            ts = temp_start + timedelta(hours=j * (shipment_duration_hours / reading_count))
            
            # Vary excursion probability through shipment journey
            # Higher chance of excursion in middle of journey
            journey_factor = 1.0
            if j > reading_count // 4 and j < 3 * reading_count // 4:
                journey_factor = 1.2  # Higher risk in middle of journey
                
            # Generate temperature based on probability
            temperature = generate_temperature(excursion_prob * journey_factor)

            print(temperature)
            # BOOKMARK
            
            # Track if this shipment had an excursion
            if temperature > -60:
                had_excursion_in_readings = True
                
            shipment_temperatures.append([
                shipment_id,
                ts.isoformat() + 'Z',
                temperature
            ])
        
        # Track shipments with excursions for later analysis
        if had_excursion_in_readings:
            shipments_with_excursions.add(shipment_id)

    # Convert to Polars DataFrames
    df_routes = pl.DataFrame(
        shipment_routes,
        schema=[
            "shipment_id", "company", "origin_facility", "destination_facility",
            "start_time", "end_time", "carrier", "product_type", 
            "delay_minutes", "had_customs_hold", "day_of_week", "season", "excursion_probability"
        ],
        orient="row"
    )

    df_flights = pl.DataFrame(
        shipment_flights,
        schema=[
            "shipment_id", "flight_number", "departure_airport", "arrival_airport",
            "scheduled_departure", "actual_departure", "delay_minutes", "arrival_time", "flight_duration_hours"
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

    return df_routes, df_flights, df_events, df_temperatures, shipments_with_excursions

# Generate data
routes, flights, events, temps, excursion_shipments = synthesize_data(n=1000, base_excursion_rate=0.01)

# Save to CSV
routes.write_csv("data/shipment_routes.csv")
flights.write_csv("data/shipment_flights.csv")
events.write_csv("data/shipment_events.csv")
temps.write_csv("data/shipment_temperatures.csv")

# Optional: Print some statistics to confirm model is working as expected
excursion_count = len(excursion_shipments)
total_count = routes.height
print(f"Total shipments: {total_count}")
print(f"Shipments with temperature excursions: {excursion_count} ({excursion_count/total_count*100:.2f}%)")

# Create a function to calculate excursion rates by factor
def calculate_excursion_rates(df, column_name, excursion_set):
    print(f"\nExcursion rates by {column_name}:")
    for value in sorted(df[column_name].unique().to_list()):
        rows = df.filter(pl.col(column_name) == value)
        shipment_ids = rows["shipment_id"].to_list()
        total = len(shipment_ids)
        excursions = sum(1 for sid in shipment_ids if sid in excursion_set)
        if total > 0:
            print(f"  {value}: {excursions/total*100:.2f}% ({excursions}/{total})")

# Check excursion rates by various factors
calculate_excursion_rates(routes, "carrier", excursion_shipments)
calculate_excursion_rates(routes, "season", excursion_shipments)
calculate_excursion_rates(routes, "day_of_week", excursion_shipments)
calculate_excursion_rates(routes, "had_customs_hold", excursion_shipments)
calculate_excursion_rates(routes, "product_type", excursion_shipments)

# Calculate average excursion probability
print(f"\nAverage calculated excursion probability: {routes['excursion_probability'].mean():.4f}")
print(f"Actual excursion rate: {excursion_count/total_count:.4f}")

# Calculate average delay for shipments with and without excursions
if excursion_count > 0 and excursion_count < total_count:
    excursion_shipment_rows = routes.filter(pl.col("shipment_id").is_in(list(excursion_shipments)))
    non_excursion_shipment_rows = routes.filter(~pl.col("shipment_id").is_in(list(excursion_shipments)))

    avg_delay_with_excursion = excursion_shipment_rows["delay_minutes"].mean()
    avg_delay_without_excursion = non_excursion_shipment_rows["delay_minutes"].mean()

    print(f"\nAverage delay minutes:")
    print(f"  Shipments with excursions: {avg_delay_with_excursion:.2f} minutes")
    print(f"  Shipments without excursions: {avg_delay_without_excursion:.2f} minutes")