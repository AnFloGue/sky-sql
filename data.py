import sqlalchemy
from sqlalchemy import create_engine, text

QUERY_FLIGHT_BY_ID = """
SELECT flights.*, airlines.AIRLINE as airline, flights.ID as FLIGHT_ID, flights.DEPARTURE_DELAY as DELAY
FROM flights
JOIN airlines ON flights.AIRLINE = airlines.ID
WHERE flights.ID = :id
"""

QUERY_DELAYED_FLIGHTS_BY_AIRPORT = """
SELECT flights.*, airlines.AIRLINE as airline, flights.ID as FLIGHT_ID, flights.DEPARTURE_DELAY as DELAY
FROM flights
JOIN airlines ON flights.AIRLINE = airlines.ID
WHERE flights.ORIGIN_AIRPORT = :airport AND flights.DEPARTURE_DELAY >= 20 AND flights.DEPARTURE_DELAY IS NOT NULL
"""

QUERY_FLIGHTS_BY_DATE = """
SELECT flights.*, airlines.AIRLINE as airline, flights.ID as FLIGHT_ID, flights.DEPARTURE_DELAY as DELAY
FROM flights
JOIN airlines ON flights.AIRLINE = airlines.ID
WHERE flights.YEAR = :year AND flights.MONTH = :month AND flights.DAY = :day
"""

QUERY_DELAYED_FLIGHTS_BY_AIRLINE = """
SELECT flights.*, airlines.AIRLINE as airline, flights.ID as FLIGHT_ID, flights.DEPARTURE_DELAY as DELAY
FROM flights
JOIN airlines ON flights.AIRLINE = airlines.ID
WHERE airlines.AIRLINE = :airline AND flights.DEPARTURE_DELAY >= 20 AND flights.DEPARTURE_DELAY IS NOT NULL
"""



class FlightData:
    def __init__(self, db_uri):
        self._engine = create_engine(db_uri)
    
    def _execute_query(self, query, params):
        try:
            with self._engine.connect() as connection:
                result = connection.execute(text(query), params)
                return [dict(row._mapping) for row in result]
        except sqlalchemy.exc.SQLAlchemyError as e:
            print(f"Error executing query: {e}")
            return []

    def get_flight_by_id(self, flight_id):
        params = {'id': flight_id}
        return self._execute_query(QUERY_FLIGHT_BY_ID, params)

    def get_delayed_flights_by_airport(self, airport):
        params = {'airport': airport}
        return self._execute_query(QUERY_DELAYED_FLIGHTS_BY_AIRPORT, params)

    def get_flights_by_date(self, day, month, year):
        params = {'day': day, 'month': month, 'year': year}
        return self._execute_query(QUERY_FLIGHTS_BY_DATE, params)

    def get_delayed_flights_by_airline(self, airline):
        params = {'airline': airline}
        return self._execute_query(QUERY_DELAYED_FLIGHTS_BY_AIRLINE, params)

    def __del__(self):
        self._engine.dispose()