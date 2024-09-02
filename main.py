import data
from datetime import datetime

SQLITE_URI = 'sqlite:///data/flights.sqlite3'
IATA_LENGTH = 3

def delayed_flights_by_airline(data_manager):
    airline_input = input("Enter airline name: ")
    results = data_manager.get_delayed_flights_by_airline(airline_input)
    print_results(results)

def delayed_flights_by_airport(data_manager):
    valid = False
    while not valid:
        airport_input = input("Enter origin airport IATA code: ")
        if airport_input.isalpha() and len(airport_input) == IATA_LENGTH:
            valid = True
    results = data_manager.get_delayed_flights_by_airport(airport_input)
    print_results(results)

def flight_by_id(data_manager):
    valid = False
    while not valid:
        try:
            id_input = int(input("Enter flight ID: "))
        except Exception:
            print("Try again...")
        else:
            valid = True
    results = data_manager.get_flight_by_id(id_input)
    print_results(results)

def flights_by_date(data_manager):
    valid = False
    while not valid:
        try:
            date_input = input("Enter date in DD/MM/YYYY format: ")
            date = datetime.strptime(date_input, '%d/%m/%Y')
        except ValueError as e:
            print("Try again...", e)
        else:
            valid = True
    results = data_manager.get_flights_by_date(date.day, date.month, date.year)
    print_results(results)

def print_results(results):
    if results is None:
        results = []

    print(f"Got {len(results)} results.")
    for result in results:
        try:
            delay = int(result['DELAY']) if result['DELAY'] else 0
            origin = result['ORIGIN_AIRPORT']
            dest = result['DESTINATION_AIRPORT']
            airline = result['AIRLINE']
        except (ValueError, KeyError) as e:
            print("Error showing results: ", e)
            return

        if delay and delay > 0:
            print(f"{result['ID']}. {origin} -> {dest} by {airline}, Delay: {delay} Minutes")
        else:
            print(f"{result['ID']}. {origin} -> {dest} by {airline}")

def custom_exit(data_manager):
    print("Goodbye, thanks for visiting!")
    exit()

def show_menu_and_get_input():
    print("Menu:")
    for key, value in FUNCTIONS.items():
        print(f"{key}. {value[1]}")

    while True:
        try:
            choice = int(input())
            if choice in FUNCTIONS:
                return FUNCTIONS[choice][0]
        except ValueError:
            pass
        print("Try again...")

FUNCTIONS = {
    1: (flight_by_id, "Show flight by ID"),
    2: (flights_by_date, "Show flights by date"),
    3: (delayed_flights_by_airline, "Delayed flights by airline"),
    4: (delayed_flights_by_airport, "Delayed flights by origin airport"),
    5: (custom_exit, "Exit")
}

def main():
    data_manager = data.FlightData(SQLITE_URI)
    while True:
        choice_func = show_menu_and_get_input()
        choice_func(data_manager)

if __name__ == "__main__":
    main()